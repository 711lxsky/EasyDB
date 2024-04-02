package top.lxsky711.easydb.core.im;

import top.lxsky711.easydb.common.data.ByteParser;
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
 *       NodeBody: [SonNode1Uid][Key1][SonNode2Uid]...[SonNodeNUid][KeyN]
 *                  SonNodeUid 子节点Uid(唯一标识)
 *                  Key 索引关键字
 * 每个Node都存储在一条DataItem中
 */
public class BPlusTreeNode {

    private BPlusTree bPlusTree;

    private DataItem dataItem;

    private SubArray nodeData;

    long nodeUid;

    public static BPlusTreeNode newBPlusTreeNode(BPlusTree tree, long uid){
        DataItem nodeDataItem = tree.getDm().readDataItem(uid);
        if(Objects.isNull(nodeDataItem)){
            Log.logErrorMessage(ErrorMessage.B_PLUS_TREE_NODE_DATA_ERROR);
            return null;
        }
        BPlusTreeNode node = new BPlusTreeNode();
        node.nodeUid = uid;
        node.bPlusTree = tree;
        node.dataItem = nodeDataItem;
        node.nodeData = nodeDataItem.getDataRecord();
        return node;
    }

    public static byte[] buildRootNodeBytes(long leftSonUid, long rightSonUid, long key){
        SubArray nodeData = new SubArray(new byte[IMSetting.NODE_SIZE], 0, IMSetting.NODE_SIZE);
        setNodeLeafFlag(nodeData, false);
        setNodeKeysCount(nodeData, IMSetting.ROOT_NODE_KEYS_COUNT_DEFAULT);
        setNodeSiblingUid(nodeData, IMSetting.NODE_SIBLING_UID_DEFAULT);
        setNodeKthSon(nodeData, leftSonUid, IMSetting.ROOT_NODE_LEFT_SON_DEFAULT_KTH);
        setNodeKthKey(nodeData, key, IMSetting.ROOT_NODE_LEFT_SON_DEFAULT_KTH);
        setNodeKthSon(nodeData, rightSonUid, IMSetting.ROOT_NODE_RIGHT_SON_DEFAULT_KTH);
        setNodeKthKey(nodeData, IMSetting.NODE_LAST_KEY_DEFAULT, IMSetting.ROOT_NODE_RIGHT_SON_DEFAULT_KTH);
        return nodeData.rawData;
    }

    public static byte[] buildLeafNodeBytes(){
        SubArray nodeData = new SubArray(new byte[IMSetting.NODE_SIZE], 0, IMSetting.NODE_SIZE);
        setNodeLeafFlag(nodeData, true);
        setNodeKeysCount(nodeData, IMSetting.LEAF_NODE_KEYS_COUNT_DEFAULT);
        setNodeSiblingUid(nodeData, IMSetting.NODE_SIBLING_UID_DEFAULT);
        return nodeData.rawData;
    }

    public void releaseOneReference(){
        this.dataItem.releaseOneReference();
    }

    public IMSetting.SearchNextNodeResult searchNext(long tarKey){
        this.dataItem.readLock();
        try{
            IMSetting.SearchNextNodeResult result = new IMSetting.SearchNextNodeResult();
            short nodeKeysCount = getNodeKeysCount(this.nodeData);
            for(short index = 0; index < nodeKeysCount; index ++){
                long nodeKthKey = getNodeKthKey(this.nodeData, index);
                if(nodeKthKey > tarKey){
                    result.nodeUid = getNodeKthSonUid(this.nodeData, index);
                    result.nodeSiblingUid = IMSetting.NODE_SIBLING_UID_DEFAULT;
                    return result;
                }
            }
            result.nodeUid = IMSetting.NODE_UID_DEFAULT;
            result.nodeSiblingUid = getNodeSiblingUid(this.nodeData);
            return result;
        }
        finally {
            this.dataItem.readUnlock();
        }
    }

    public IMSetting.SearchRangeNodeResult searchRange(long leftKey, long rightKey){
        this.dataItem.readLock();
        try{
            short nodeKeysCount = getNodeKeysCount(this.nodeData);
            int kth = 0;
            while(kth < nodeKeysCount && getNodeKthKey(this.nodeData, kth) < leftKey){
                kth ++;
            }
            List<Long> resultUidList = new ArrayList<>();
            while(kth < nodeKeysCount && getNodeKthKey(this.nodeData, kth) <= rightKey){
                resultUidList.add(getNodeKthSonUid(this.nodeData, kth));
                kth ++;
            }
            long resultSiblingUid = IMSetting.NODE_SIBLING_UID_DEFAULT;
            if(kth == nodeKeysCount){
                resultSiblingUid = getNodeSiblingUid(this.nodeData);
            }
            IMSetting.SearchRangeNodeResult result = new IMSetting.SearchRangeNodeResult();
            result.nodeUidList = resultUidList;
            result.nodeSiblingUid = resultSiblingUid;
            return result;
        }
        finally {
            this.dataItem.readUnlock();
        }
    }

    public IMSetting.InsertAndSplitNodeResult insertAndSplit(long insertNodeUid, long insertNodeKey){
        IMSetting.InsertAndSplitNodeResult result = new IMSetting.InsertAndSplitNodeResult();
        boolean insertSuccess = false;
        this.dataItem.beforeModify();
        try {
            insertSuccess = this.insertNode(insertNodeUid, insertNodeKey);
            if(! insertSuccess){
                result.nodeSiblingUid = getNodeSiblingUid(this.nodeData);
                return result;
            }
            if(judgeNeedSplit()){

            }
        }
        finally {
            if(insertSuccess){
                this.dataItem.afterModify(TMSetting.SUPER_TRANSACTION_XID);
            }
            else {
                this.dataItem.unBeforeModify();
            }
        }
    }

    private boolean insertNode(long insertNodeUid, long insertNodeKey){
        short nodeKeysCount = getNodeKeysCount(this.nodeData);
        int kth = 0;
        while(kth < nodeKeysCount && getNodeKthKey(this.nodeData, kth) < insertNodeKey){
            kth ++;
        }
        if(kth == nodeKeysCount && getNodeSiblingUid(this.nodeData) != IMSetting.NODE_SIBLING_UID_DEFAULT){
            return false;
        }
        if(selfIsLeafNode()){
            // 如果是叶子节点的话，直接插入，uid和key是成套配对的
            moveBackNodeFromKth(this.nodeData, kth);
            setNodeKthSon(this.nodeData, insertNodeUid, kth);
            setNodeKthKey(this.nodeData, insertNodeKey, kth);
            setNodeKeysCount(this.nodeData, (short) (nodeKeysCount + 1));
        }
        else {
            // 注意，如果是非叶子节点的话，就把Kth位置的Uid看成是第K个子节点的位置标记，Kth位置的Key是这个子节点存储数据记录的最大值
            // 这里将Key后移，是因为下一层的子节点发生分裂，Key传递给了Kth + 1 位置，也就是Uid对应的子节点
            long nodeKthKey = getNodeKthKey(this.nodeData, kth);
            setNodeKthKey(this.nodeData, insertNodeKey, kth);
            moveBackNodeFromKth(this.nodeData, kth + 1);
            setNodeKthKey(this.nodeData, nodeKthKey, kth + 1);
            setNodeKthSon(this.nodeData, insertNodeUid, kth + 1);
            setNodeKeysCount(this.nodeData, (short) (nodeKeysCount + 1));
        }
        return true;
    }

    private IMSetting.SplitNodeResult splitNode(){
        SubArray newNodeData = new SubArray(new byte[IMSetting.NODE_SIZE], 0, IMSetting.NODE_SIZE);
        setNodeLeafFlag(newNodeData, judgeNodeIsLeaf(this.nodeData));
        setNodeKeysCount(newNodeData, (short) IMSetting.NODE_BALANCE_NUMBER);
        setNodeSiblingUid(newNodeData, getNodeSiblingUid(this.nodeData));
        copyNodeDataFromKth(this.nodeData, newNodeData, IMSetting.NODE_BALANCE_NUMBER);
        long newNodeUid = this.bPlusTree.getDm().insertData(TMSetting.SUPER_TRANSACTION_XID, newNodeData.rawData);
        setNodeKeysCount(this.nodeData, (short) IMSetting.NODE_BALANCE_NUMBER);
        setNodeSiblingUid(this.nodeData, newNodeUid);
        IMSetting.SplitNodeResult result = new IMSetting.SplitNodeResult();
        result.nodeNewSonUid = newNodeUid;
        result.nodeNewKey = getNodeKthKey(this.nodeData, );
    }

    public boolean selfIsLeafNode(){
        this.dataItem.readLock();
        try {
            return getNodeLeafFlag(this.nodeData) == IMSetting.NODE_LEAF_TRUE;
        }
        finally {
            this.dataItem.readUnlock();
        }
    }

    public static boolean judgeNodeIsLeaf(SubArray nodeData){
        return getNodeLeafFlag(nodeData) == IMSetting.NODE_LEAF_TRUE;
    }

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

    private static void moveBackNodeFromKth(SubArray nodeData, int kth){
        int begin = nodeData.start + IMSetting.NODE_HEAD_SIZE + (kth + 1) * IMSetting.NODE_SON_COUPLE_SIZE;
        int end = nodeData.start + IMSetting.NODE_SIZE - 1;
        for(int i = end; i >= begin; i --){
            nodeData.rawData[i] = nodeData.rawData[i - IMSetting.NODE_SON_COUPLE_SIZE];
        }
    }

    private static void copyNodeDataFromKth(SubArray srcNodeData,SubArray destNodeData, int kth){
        int dataOffset = srcNodeData.start + IMSetting.NODE_SON_COUPLE_SIZE * kth;
        System.arraycopy(srcNodeData.rawData, dataOffset, destNodeData.rawData,
                destNodeData.start + IMSetting.NODE_HEAD_SIZE, srcNodeData.end - dataOffset);
    }

    private boolean judgeNeedSplit(){
        return getNodeKeysCount(this.nodeData) == IMSetting.NODE_BALANCE_NUMBER * 2;
    }
}
