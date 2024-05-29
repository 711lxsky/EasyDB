package top.lxsky711.easydb.core.im;

import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.log.ErrorMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.core.common.SubArray;
import top.lxsky711.easydb.core.dm.DataItem;
import top.lxsky711.easydb.core.tm.TMSetting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @Author: 711lxsky
 * @Description: B+树节点实现类
 * 结构： NodeHead: [LeafFlag][KeysCount][SiblingUID]
 *                 LeafFlag byte类型，标识当前节点是否是叶子节点
 *                 KeyCount short类型，标识当前节点的关键字数量
 *                 SiblingUID long类型，标识当前节点的兄弟节点UID
 *       NodeBody: [SonNode0Uid][Key0][SonNode1Uid]...[SonNodeNUid][KeyN]
 *                  SonNodeUid 子节点Uid(唯一标识)
 *                  Key 索引关键字
 *       注意： 在叶子节点中 uid 和 key 一一对应，存储的就是底层数据
 *             而在非叶子节点中， uid0 是没有配对值的， 因为默认其左侧是无限小， key0是和uid1配对的， 是uid1子节点中的最小数据，而keyN是MAX_VALUE无限大，以方便查找
 * 每个Node都存储在一条DataItem中
 */
public class BPlusTreeNode {

    /**
     * 存放一个B+树的引用，方便使用dm
     */
    private BPlusTree bPlusTree;

    /**
     * 方便管理数据
     */
    private DataItem dataItem;

    /**
     * 节点数据
     */
    private SubArray nodeData;

    /**
     * 当前节点的唯一标识uid
     */
    long nodeUid;

    /**
     * @Author: 711lxsky
     * @Description: 借助B+树和节点uid加载节点
     */
    public static BPlusTreeNode loadBPlusTreeNode(BPlusTree tree, long nodeUid) throws WarningException, ErrorException {
        DataItem nodeDataItem = tree.getDm().readDataItem(nodeUid);
        if(Objects.isNull(nodeDataItem)){
            Log.logErrorMessage(ErrorMessage.B_PLUS_TREE_NODE_DATA_ERROR);
            return null;
        }
        BPlusTreeNode node = new BPlusTreeNode();
        node.nodeUid = nodeUid;
        node.bPlusTree = tree;
        node.dataItem = nodeDataItem;
        node.nodeData = nodeDataItem.getDataRecord();
        return node;
    }

    /**
     * @Author: 711lxsky
     * @Description: 新建一个根节点，字节数组形式
     */
    public static byte[] buildRootNodeBytes(long leftSonUid, long rightSonUid, long rightKey){
        SubArray nodeData = new SubArray(new byte[IMSetting.NODE_SIZE], 0, IMSetting.NODE_SIZE);
        setNodeLeafFlag(nodeData, false);
        setNodeKeysCount(nodeData, IMSetting.ROOT_NODE_KEYS_COUNT_DEFAULT);
        setNodeSiblingUid(nodeData, IMSetting.NODE_SIBLING_UID_DEFAULT);
        // 注意，leftSon虽然和rightKey同属一个kth,但是leftSon对应的key是无限小，rightSon才是和rightKey对应的，且是rightSon节点存储的数据最小值
        setNodeKthSon(nodeData, leftSonUid, IMSetting.ROOT_NODE_LEFT_SON_DEFAULT_KTH);
        setNodeKthKey(nodeData, rightKey, IMSetting.ROOT_NODE_LEFT_SON_DEFAULT_KTH);
        setNodeKthSon(nodeData, rightSonUid, IMSetting.ROOT_NODE_RIGHT_SON_DEFAULT_KTH);
        // 最右侧的Key是没有son与之对应的，为的就是实现一个边界
        setNodeKthKey(nodeData, IMSetting.NODE_LAST_KEY_DEFAULT, IMSetting.ROOT_NODE_RIGHT_SON_DEFAULT_KTH);
        return nodeData.rawData;
    }

    /**
     * @Author: 711lxsky
     * @Description: 新建一个叶子节点，字节数组形式
     */
    public static byte[] buildLeafNodeBytes(){
        SubArray nodeData = new SubArray(new byte[IMSetting.NODE_SIZE], 0, IMSetting.NODE_SIZE);
        setNodeLeafFlag(nodeData, true);
        setNodeKeysCount(nodeData, IMSetting.LEAF_NODE_KEYS_COUNT_DEFAULT);
        setNodeSiblingUid(nodeData, IMSetting.NODE_SIBLING_UID_DEFAULT);
        return nodeData.rawData;
    }

    /**
     * @Author: 711lxsky
     * @Description: 释放一个引用
     */
    public void releaseOneReference() throws WarningException, ErrorException {
        this.dataItem.releaseOneReference();
    }

    /**
     * @Author: 711lxsky
     * @Description: 在某个节点中遍历寻找满足Key条件的数据定位，也就是下一层节点
     */
    public IMSetting.SearchNextNodeResult searchNext(long tarKey){
        this.dataItem.readLock();
        try{
            IMSetting.SearchNextNodeResult result = new IMSetting.SearchNextNodeResult();
            // 先拿到当前节点的key数量
            short nodeKeysCount = getNodeKeysCount(this.nodeData);
            // 逐个遍历找到第一个大于tarKey的key
            for(short index = 0; index < nodeKeysCount; index ++){
                long nodeKthKey = getNodeKthKey(this.nodeData, index);
                if(nodeKthKey > tarKey){
                    // 如果是叶子节点，这里的uid就是某个磁盘位置，和key对应
                    // 如果是非叶子节点，这里uid实际是kth - 1位置key值对应的节点标识，且这个节点中的所有数据都小于等于tarKey
                    result.nodeUid = getNodeKthSonUid(this.nodeData, index);
                    result.nodeSiblingUid = IMSetting.NODE_SIBLING_UID_DEFAULT;
                    return result;
                }
            }
            // 没有找到，返回兄弟节点的uid
            result.nodeUid = IMSetting.NODE_UID_DEFAULT;
            result.nodeSiblingUid = getNodeSiblingUid(this.nodeData);
            return result;
        }
        finally {
            this.dataItem.readUnlock();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 在叶子节点中进行范围搜索
     */
    public IMSetting.LeafSearchRangeNodeResult leafSearchRange(long leftKey, long rightKey){
        this.dataItem.readLock();
        try{
            // 当前操作都是在叶子节点中进行，非叶子节点范围搜索是没有意义的
            short nodeKeysCount = getNodeKeysCount(this.nodeData);
            int kth = 0;
            // 先定位到 >= leftKey位置
            while(kth < nodeKeysCount && getNodeKthKey(this.nodeData, kth) < leftKey){
                kth ++;
            }
            List<Long> resultUidList = new ArrayList<>();
            // 再将所有满足 <= rightKey 的数据定位放进返回列表
            while(kth < nodeKeysCount && getNodeKthKey(this.nodeData, kth) <= rightKey){
                resultUidList.add(getNodeKthSonUid(this.nodeData, kth));
                kth ++;
            }
            long resultSiblingUid = IMSetting.NODE_SIBLING_UID_DEFAULT;
            if(kth == nodeKeysCount){
                // 没找完，兄弟节点中可能还含有符合条件的
                resultSiblingUid = getNodeSiblingUid(this.nodeData);
            }
            IMSetting.LeafSearchRangeNodeResult result = new IMSetting.LeafSearchRangeNodeResult();
            result.nodeUidList = resultUidList;
            result.nodeSiblingUid = resultSiblingUid;
            return result;
        }
        finally {
            this.dataItem.readUnlock();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 插入某个节点，并且判断是否需要分裂，如有需要完成分裂
     */
    public IMSetting.InsertAndSplitNodeResult insertAndSplit(long insertNodeUid, long insertNodeKey) throws WarningException, ErrorException {
        IMSetting.InsertAndSplitNodeResult result = new IMSetting.InsertAndSplitNodeResult();
        result.nodeNewSonUid = IMSetting.NODE_UID_DEFAULT;
        boolean insertSuccess = false;
        this.dataItem.beforeModify();
        try {
            // 插入
            insertSuccess = this.insertNode(insertNodeUid, insertNodeKey);
            if(! insertSuccess){
                result.nodeSiblingUid = getNodeSiblingUid(this.nodeData);
                return result;
            }
            if(judgeNeedSplit()){
                IMSetting.SplitNodeResult splitResult = this.splitNode();
                result.nodeNewSonUid = splitResult.nodeNewSonUid;
                result.nodeNewKey = splitResult.nodeNewKey;
            }
            return result;
        }
        finally {
            if(insertSuccess){
                // 插入不成功，撤销数据更改
                this.dataItem.afterModify(TMSetting.SUPER_TRANSACTION_XID);
            }
            else {
                // 成功
                this.dataItem.unBeforeModify();
            }
        }
    }

    private boolean insertNode(long insertNodeUid, long insertNodeKey){
        short nodeKeysCount = getNodeKeysCount(this.nodeData);
        int kth = 0;
        // 先找到第一个 >= key 的位置
        while(kth < nodeKeysCount && getNodeKthKey(this.nodeData, kth) < insertNodeKey){
            kth ++;
        }
        // 如果没有找到而且当前节点有兄弟节点，那就返回 false 用以后续操作去兄弟节点寻找
        if(kth == nodeKeysCount && getNodeSiblingUid(this.nodeData) != IMSetting.NODE_SIBLING_UID_DEFAULT){
            return false;
        }
        // 这里可能是找到了， 也可能是没有兄弟节点只能插在当前节点
        if(selfIsLeafNode()){
            // 如果是叶子节点的话，直接插入，uid和key是成套配对的
            moveBackNodeFromKth(this.nodeData, kth);
            setNodeKthSon(this.nodeData, insertNodeUid, kth);
            setNodeKthKey(this.nodeData, insertNodeKey, kth);
            setNodeKeysCount(this.nodeData, (short) (nodeKeysCount + 1));
        }
        else {
            // 注意，如果是非叶子节点的话，kth位置的key是和 kth + 1位置的uid对应的， 为其最小记录值
            // 所以当前 kth 位置的 uid 和 kth - 1位置的key对应，不需要移动
            // 只需要将 kth + 1位置及以后的数据(包含kth + 1的uid)后移，原本 kth 位置的 key 放到kth + 1后段，
            moveBackNodeFromKth(this.nodeData, kth + 1);
            long nodeKthKey = getNodeKthKey(this.nodeData, kth);
            setNodeKthKey(this.nodeData, nodeKthKey, kth + 1);
            // 将插入的key放在kth位置，插入的uid放在kth + 1前段
            setNodeKthKey(this.nodeData, insertNodeKey, kth);
            setNodeKthSon(this.nodeData, insertNodeUid, kth + 1);

            // 这样非叶子节点的对应关系依然成立

            // 或者也可以直接从 kth 位置开始后移，然后插入数据将 kth 位置 key 和 kth + 1 位置的 uid 覆盖
            /*
            moveBackNodeFromKth(this.nodeData, kth);
            setNodeKthKey(this.nodeData, insertNodeKey, kth);
            setNodeKthSon(this.nodeData, insertNodeUid, kth + 1);
            */

            setNodeKeysCount(this.nodeData, (short) (nodeKeysCount + 1));
        }
        return true;
    }

    /**
     * @Author: 711lxsky
     * @Description: 分裂节点
     */
    private IMSetting.SplitNodeResult splitNode() throws WarningException, ErrorException {
        SubArray newNodeData = new SubArray(new byte[IMSetting.NODE_SIZE], 0, IMSetting.NODE_SIZE);
        // 按照当前节点的性质复制一下
        setNodeLeafFlag(newNodeData, judgeNodeIsLeaf(this.nodeData));
        // 2个NODE_BALANCE_NUMBER,所以对半分
        setNodeKeysCount(newNodeData, (short) IMSetting.NODE_BALANCE_NUMBER);
        // 因为分裂出的节点放在当前节点的右侧，所以当前节点的兄弟节点uid赋给分裂节点
        setNodeSiblingUid(newNodeData, getNodeSiblingUid(this.nodeData));
        // 右侧的大的半边给分裂节点
        copyNodeDataFromKth(this.nodeData, newNodeData, IMSetting.NODE_BALANCE_NUMBER);
        // 分裂节点的数据保存，同时获取uid
        long newNodeUid = this.bPlusTree.getDm().insertData(TMSetting.SUPER_TRANSACTION_XID, newNodeData.rawData);
        // 当前节点也只剩一半数据
        setNodeKeysCount(this.nodeData, (short) IMSetting.NODE_BALANCE_NUMBER);
        // 设置当前节点的兄弟节点为分裂节点
        setNodeSiblingUid(this.nodeData, newNodeUid);
        IMSetting.SplitNodeResult result = new IMSetting.SplitNodeResult();
        result.nodeNewSonUid = newNodeUid;
        // 每个节点的对应的key值都是最小数据
        result.nodeNewKey = getNodeKthKey(this.nodeData, IMSetting.NODE_KEY_POS_DEFAULT);
        return result;
    }

    /**
     * @Author: 711lxsky
     * @Description: 实例使用的判断是否是叶子节点方法
     */
    public boolean selfIsLeafNode(){
        this.dataItem.readLock();
        try {
            return getNodeLeafFlag(this.nodeData) == IMSetting.NODE_LEAF_TRUE;
        }
        finally {
            this.dataItem.readUnlock();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 静态方法式判断当前节点是否是叶子节点
     */
    public static boolean judgeNodeIsLeaf(SubArray nodeData){
        return getNodeLeafFlag(nodeData) == IMSetting.NODE_LEAF_TRUE;
    }

    /**
     * @Author: 711lxsky
     * @Description: 根据布尔值设置当前节点叶子标志
     */
    private static void setNodeLeafFlag(SubArray nodeData, boolean isLeaf){
        if(isLeaf){
            nodeData.rawData[nodeData.start + IMSetting.LEAF_NODE_FLAG_OFFSET] = IMSetting.NODE_LEAF_TRUE;
        }
        else {
            nodeData.rawData[nodeData.start + IMSetting.LEAF_NODE_FLAG_OFFSET] = IMSetting.NODE_LEAF_FALSE;
        }
    }

    private static byte getNodeLeafFlag(SubArray nodeData){
        return nodeData.rawData[nodeData.start + IMSetting.LEAF_NODE_FLAG_OFFSET];
    }

    private static void setNodeKeysCount(SubArray nodeData, short keysCount){
        System.arraycopy(ByteParser.shortToBytes(keysCount), 0, nodeData.rawData,
                nodeData.start + IMSetting.NODE_KEYS_COUNT_OFFSET, IMSetting.NODE_KEY_COUNT_LENGTH);
    }

    private static short getNodeKeysCount(SubArray nodeData){
        byte[] nodeKeysCountBytes = Arrays.copyOfRange(nodeData.rawData, nodeData.start + IMSetting.NODE_KEYS_COUNT_OFFSET,
                nodeData.start + IMSetting.NODE_KEYS_COUNT_OFFSET + IMSetting.NODE_KEY_COUNT_LENGTH);
        return ByteParser.parseBytesToShort(nodeKeysCountBytes);
    }

    private static void setNodeSiblingUid(SubArray nodeData, long siblingUid){
        System.arraycopy(ByteParser.longToBytes(siblingUid), 0, nodeData.rawData,
                nodeData.start + IMSetting.NODE_SIBLING_UID_OFFSET, IMSetting.NODE_SIBLING_UID_LENGTH);
    }

    private static long getNodeSiblingUid(SubArray nodeData){
        byte[] nodeSiblingUidBytes = Arrays.copyOfRange(nodeData.rawData, nodeData.start + IMSetting.NODE_SIBLING_UID_OFFSET,
                nodeData.start + IMSetting.NODE_SIBLING_UID_OFFSET + IMSetting.NODE_SIBLING_UID_LENGTH);
        return ByteParser.parseBytesToLong(nodeSiblingUidBytes);
    }

    private static void setNodeKthSon(SubArray nodeData,long sonUid, int kth){
        int sonUidOffset = nodeData.start + IMSetting.NODE_HEAD_SIZE + kth * IMSetting.NODE_SON_COUPLE_SIZE;
        System.arraycopy(ByteParser.longToBytes(sonUid), 0, nodeData.rawData,
                sonUidOffset, IMSetting.NODE_UID_LENGTH);
    }

    private static long getNodeKthSonUid(SubArray nodeData, int kth){
        int sonUidOffset = nodeData.start + IMSetting.NODE_HEAD_SIZE + kth * IMSetting.NODE_SON_COUPLE_SIZE;
        byte[] sonUidBytes = Arrays.copyOfRange(nodeData.rawData, sonUidOffset, sonUidOffset + IMSetting.NODE_UID_LENGTH);
        return ByteParser.parseBytesToLong(sonUidBytes);
    }

    private static void setNodeKthKey(SubArray nodeData, long key, int kth){
        int keyOffset = nodeData.start + IMSetting.NODE_HEAD_SIZE + kth * IMSetting.NODE_SON_COUPLE_SIZE + IMSetting.NODE_UID_LENGTH;
        System.arraycopy(ByteParser.longToBytes(key), 0, nodeData.rawData,
                keyOffset, IMSetting.INDEX_KEY_LENGTH);
    }

    private static long getNodeKthKey(SubArray nodeData, int kth){
        int keyOffset = nodeData.start + IMSetting.NODE_HEAD_SIZE + kth * IMSetting.NODE_SON_COUPLE_SIZE + IMSetting.NODE_UID_LENGTH;
        byte[] keyBytes = Arrays.copyOfRange(nodeData.rawData, keyOffset, keyOffset + IMSetting.INDEX_KEY_LENGTH);
        return ByteParser.parseBytesToLong(keyBytes);
    }

    /**
     * @Author: 711lxsky
     * @Description: 后移节点数据方法
     */
    private static void moveBackNodeFromKth(SubArray nodeData, int kth){
        // 注意这里是逆序移动，用以防止数据丢失
        // 因为从后往前，偏移量的是 NODE_SON_COUPLE_SIZE， 所以 begin 算的时候是 kth + 1
        int begin = nodeData.start + IMSetting.NODE_HEAD_SIZE + (kth + 1) * IMSetting.NODE_SON_COUPLE_SIZE;
        int end = nodeData.start + IMSetting.NODE_SIZE - 1;
        for(int i = end; i >= begin; i --){
            nodeData.rawData[i] = nodeData.rawData[i - IMSetting.NODE_SON_COUPLE_SIZE];
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 从 kth 位置开始，复制节点数据
     */
    private static void copyNodeDataFromKth(SubArray srcNodeData,SubArray destNodeData, int kth){
        int dataOffset = srcNodeData.start + IMSetting.NODE_SON_COUPLE_SIZE * kth;
        System.arraycopy(srcNodeData.rawData, dataOffset, destNodeData.rawData,
                destNodeData.start + IMSetting.NODE_HEAD_SIZE, srcNodeData.end - dataOffset);
    }

    /**
     * @Author: 711lxsky
     * @Description: 判断当前节点是否需要分裂， 条件是达到 平衡因子 的两倍
     */
    private boolean judgeNeedSplit(){
        return getNodeKeysCount(this.nodeData) == IMSetting.NODE_BALANCE_NUMBER * 2;
    }
}
