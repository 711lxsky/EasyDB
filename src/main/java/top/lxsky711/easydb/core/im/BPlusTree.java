package top.lxsky711.easydb.core.im;

import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.common.log.ErrorMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.core.common.SubArray;
import top.lxsky711.easydb.core.dm.DataItem;
import top.lxsky711.easydb.core.dm.DataManager;
import top.lxsky711.easydb.core.tm.TMSetting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: 711lxsky
 * @Description: B+树实现类
 * 索引的数据直接被插入数据库文件中，不需要经过版本管理
 */

public class BPlusTree {

    /**
     * @Author: 711lxsky
     * @Description: 数据管理
     */
    private DataManager dm;

    /**
     * @Author: 711lxsky
     * @Description: 用以对 rootUid 数据进行处理
     */
    private DataItem rootUidDataItem;

    /**
     * @Author: 711lxsky
     * @Description: 自身资源锁
     */
    private Lock selfLock;

    public DataManager getDm() {
        return dm;
    }

    public void setDm(DataManager dm) {
        this.dm = dm;
    }

    /**
     * @Author: 711lxsky
     * @Description: 构建B+树
     */
    public static long createBPlusTree(DataManager dm){
        byte[] rootNodeDataBytes = BPlusTreeNode.buildLeafNodeBytes();
        long rootUid = dm.insertData(TMSetting.SUPER_TRANSACTION_XID, rootNodeDataBytes);
        // 插入操作生成的唯一uid,注意，非rootUid，而是获取到DataItem对象的uid
        return dm.insertData(TMSetting.SUPER_TRANSACTION_XID, ByteParser.longToBytes(rootUid));
    }

    /**
     * @Author: 711lxsky
     * @Description: 加载B+树
     */
    public static BPlusTree loadBPlusTree(DataManager dm, long rootUidPosUid){
        DataItem rootUidDataItem = dm.readDataItem(rootUidPosUid);
        if(Objects.isNull(rootUidDataItem)){
            Log.logErrorMessage(ErrorMessage.IMPORTANT_DATA_ERROR);
            return null;
        }
        BPlusTree tree = new BPlusTree();
        tree.dm = dm;
        tree.rootUidDataItem = rootUidDataItem;
        tree.selfLock = new ReentrantLock();
        return tree;
    }

    /**
     * @Author: 711lxsky
     * @Description: 从 rootUidDataItem 中获取 rootUid
     */
    private long getRootUid(){
        this.selfLock.lock();
        try {
            SubArray rootUidData = this.rootUidDataItem.getDataRecord();
            byte[] rootUidBytes = Arrays.copyOfRange(rootUidData.rawData, rootUidData.start, rootUidData.start + IMSetting.NODE_UID_LENGTH);
            return ByteParser.parseBytesToLong(rootUidBytes);
        }
        finally {
            this.selfLock.unlock();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 更新 rootUid 到磁盘中
     */
    private void updateRootUid(long leftSonUid, long rightSonUid, long rightKey){
        this.selfLock.lock();
        try{
            byte[] newRootDataBytes = BPlusTreeNode.buildRootNodeBytes(leftSonUid, rightSonUid, rightKey);
            long newRootUid = this.dm.insertData(TMSetting.SUPER_TRANSACTION_XID, newRootDataBytes);
            this.rootUidDataItem.beforeModify();
            SubArray rootUidData = this.rootUidDataItem.getDataRecord();
            System.arraycopy(ByteParser.longToBytes(newRootUid), 0, rootUidData.rawData, rootUidData.start, IMSetting.NODE_UID_LENGTH);
            this.rootUidDataItem.afterModify(TMSetting.SUPER_TRANSACTION_XID);
        }
        finally {
            this.selfLock.unlock();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 搜索满足条件的下一层节点
     */
    private long searchNextNode(long nodeUid, long key){
        while(true){
            BPlusTreeNode node = BPlusTreeNode.loadBPlusTreeNode(this, nodeUid);
            if(Objects.isNull(node)){
                Log.logErrorMessage(ErrorMessage.IMPORTANT_DATA_ERROR);
                return IMSetting.NODE_UID_ERROR_DEFAULT;
            }
            IMSetting.SearchNextNodeResult searchResult = node.searchNext(key);
            node.releaseOneReference();
            // 判断有没有找到
            if(searchResult.nodeUid != IMSetting.NODE_UID_DEFAULT){
                // 找到，直接返回
                return searchResult.nodeUid;
            }
            // 去往兄弟节点找
            nodeUid = searchResult.nodeSiblingUid;
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 查找某个符合条件的叶子节点
     */
    private long searchLeafNode(long nodeUid, long key){
        BPlusTreeNode node = BPlusTreeNode.loadBPlusTreeNode(this, nodeUid);
        if(Objects.isNull(node)){
            Log.logErrorMessage(ErrorMessage.IMPORTANT_DATA_ERROR);
            return IMSetting.NODE_UID_ERROR_DEFAULT;
        }
        boolean nodeIsLeaf = node.selfIsLeafNode();
        if(nodeIsLeaf){
            // 是叶子节点就直接返回
            return nodeUid;
        }
        else {
            // 不是，再递归到下一层去找
            long nextNodeUid = this.searchNextNode(nodeUid, key);
            return searchLeafNode(nextNodeUid, key);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 在叶子节点中进行范围搜索
     */
    public List<Long> searchRangeNodes(long leftKey, long rightKey){
        long curRootUid = this.getRootUid();
        // 先定位到叶子节点
        long leafNodeUid = this.searchLeafNode(curRootUid, leftKey);
        List<Long> tarNodeUidList = new ArrayList<>();
        while(true){
            BPlusTreeNode leafNode = BPlusTreeNode.loadBPlusTreeNode(this, leafNodeUid);
            if(Objects.isNull(leafNode)){
                Log.logErrorMessage(ErrorMessage.IMPORTANT_DATA_ERROR);
                tarNodeUidList.add(IMSetting.NODE_UID_ERROR_DEFAULT);
                break;
            }
            // 再从这个叶子节点开始范围搜索
            IMSetting.LeafSearchRangeNodeResult result = leafNode.leafSearchRange(leftKey, rightKey);
            leafNode.releaseOneReference();
            tarNodeUidList.addAll(result.nodeUidList);
            if(result.nodeSiblingUid == IMSetting.NODE_SIBLING_UID_DEFAULT){
                // 没有符合条件的了
                break;
            }
            else {
                // 兄弟节点中可能还有数据满足要求
                leafNodeUid = result.nodeSiblingUid;
            }
        }
        return tarNodeUidList;
    }

    /**
     * @Author: 711lxsky
     * @Description: 暴露给外部模块，插入节点
     */
    public void insertNode(long insertNodeUid, long key){
        long curRootUid = this.getRootUid();
        IMSetting.InsertNodeResult result = internalInsertNode(curRootUid, insertNodeUid, key);
        if(Objects.isNull(result)){
            Log.logErrorMessage(ErrorMessage.IMPORTANT_DATA_ERROR);
            return;
        }
        if(result.nodeNewSonUid != IMSetting.NODE_UID_DEFAULT){
            // 当前节点也发生了分裂，需要更新根节点
            this.updateRootUid(curRootUid, result.nodeNewSonUid, result.nodeNewKey);
        }

    }

    /**
     * @Author: 711lxsky
     * @Description: 内部插入封装
     */
    private IMSetting.InsertNodeResult internalInsertNode(long nodeUid, long insertNodeUid, long key){
        BPlusTreeNode node = BPlusTreeNode.loadBPlusTreeNode(this, nodeUid);
        if(Objects.isNull(node)){
            Log.logErrorMessage(ErrorMessage.IMPORTANT_DATA_ERROR);
            return null;
        }
        boolean nodeIsLeaf = node.selfIsLeafNode();
        node.releaseOneReference();
        IMSetting.InsertNodeResult result;
        if(nodeIsLeaf){
            // 是叶子节点，直接插入返回结果
            result = this.insertAndSplitNode(nodeUid, insertNodeUid, key);
        }
        else {
            // 非叶子节点，定位到下一层
            long nextNodeUid = this.searchNextNode(nodeUid, key);
            IMSetting.InsertNodeResult insertResult = this.internalInsertNode(nextNodeUid, insertNodeUid, key);
            // 通过递归，数据已经插入到了叶子节点之中
            if(Objects.isNull(insertResult)){
                Log.logErrorMessage(ErrorMessage.IMPORTANT_DATA_ERROR);
                return null;
            }
            if(insertResult.nodeNewSonUid != IMSetting.NODE_UID_DEFAULT){
                // 发生了节点分裂， 处理分裂节点的插入
                result = this.insertAndSplitNode(nodeUid, insertResult.nodeNewSonUid, insertResult.nodeNewKey);
            }
            else {
                result = new IMSetting.InsertNodeResult();
                result.nodeNewSonUid = IMSetting.NODE_UID_DEFAULT;
            }
        }
        return result;
    }

    /**
     * @Author: 711lxsky
     * @Description: 插入并在必要时分裂节点
     */
    private IMSetting.InsertNodeResult insertAndSplitNode(long nodeUid, long tarUid, long key){
        while(true){
            BPlusTreeNode node = BPlusTreeNode.loadBPlusTreeNode(this, nodeUid);
            if(Objects.isNull(node)){
                Log.logErrorMessage(ErrorMessage.IMPORTANT_DATA_ERROR);
                return null;
            }
            // 当前节点插入数据
            IMSetting.InsertAndSplitNodeResult iasnr = node.insertAndSplit(tarUid, key);
            node.releaseOneReference();
            if(iasnr.nodeSiblingUid != IMSetting.NODE_SIBLING_UID_DEFAULT){
                // 没有插入成功，继续向兄弟节点尝试插入
                nodeUid = iasnr.nodeSiblingUid;
            }
            else {
                // 插入成功
                IMSetting.InsertNodeResult result = new IMSetting.InsertNodeResult();
                result.nodeNewSonUid = iasnr.nodeNewSonUid;
                result.nodeNewKey = iasnr.nodeNewKey;
                return result;
            }
        }
    }

    public void close(){
        this.rootUidDataItem.releaseOneReference();
    }

}
