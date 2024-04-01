package top.lxsky711.easydb.core.im;

import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.common.log.ErrorMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.core.common.SubArray;
import top.lxsky711.easydb.core.dm.DataItem;

import java.util.Arrays;
import java.util.Objects;

/**
 * @Author: 711lxsky
 * @Description: B+树节点实现类
 * 结构： NodeHead: [LeafFlag][KeysCount][SiblingUID]
 *                 LeafFlag byte类型，标识当前节点是否是叶子节点
 *                 KeyCount short类型，标识当前节点的关键字数量
 *                 SiblingUID long类型，标识当前节点的兄弟节点UID
 *       NodeBody: [SonNode1][Key1][SonNode2]...[SonNodeN][KeyN]
 *                  SonNode 子节点
 *                  Key 索引关键字
 * 每个Node都存储在一条DataItem中
 */
public class BPlusTreeNode {

    private BPlusTree bPlusTree;

    private DataItem dataItem;

    private SubArray nodeData;

    long nodeUid;

    public static BPlusTreeNode buildBPlusTreeNode(BPlusTree tree, long uid){
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

    public boolean isLeafNode(){
        this.dataItem.readLock();
        try {
            return getNodeLeafFlag(this.nodeData) == IMSetting.NODE_LEAF_TRUE;
        }
        finally {
            this.dataItem.readUnlock();
        }
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
}
