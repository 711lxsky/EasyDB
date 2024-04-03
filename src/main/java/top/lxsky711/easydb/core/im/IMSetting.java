package top.lxsky711.easydb.core.im;

import top.lxsky711.easydb.common.data.DataSetting;

import java.util.List;

/**
 * @Author: 711lxsky
 * @Description: 索引管理器配置
 */

public class IMSetting {

    // 节点的叶子标识偏移量
    public static final int LEAF_NODE_FLAG_OFFSET = 0;

    // 是叶子节点
    public static byte NODE_LEAF_TRUE = 1;

    // 非叶子节点
    public static byte NODE_LEAF_FALSE = 0;

    // 节点的叶子标识长度
    public static final int LEAF_NODE_FLAG_LENGTH = DataSetting.BYTE_BYTE_SIZE;

    // 节点键值数量偏移量
    public static final int NODE_KEYS_COUNT_OFFSET = LEAF_NODE_FLAG_OFFSET + LEAF_NODE_FLAG_LENGTH;

    // 根节点默认的键值数量
    public static final short ROOT_NODE_KEYS_COUNT_DEFAULT = 2;

    // 叶子节点默认的键值数量
    public static final short LEAF_NODE_KEYS_COUNT_DEFAULT = 0;

    // 节点键值数量长度
    public static final int NODE_KEY_COUNT_LENGTH = DataSetting.SHORT_BYTE_SIZE;

    // 节点的兄弟节点uid偏移量
    public static final int NODE_SIBLING_UID_OFFSET = NODE_KEYS_COUNT_OFFSET + NODE_KEY_COUNT_LENGTH;

    // 节点的兄弟节点默认uid
    public static final long NODE_SIBLING_UID_DEFAULT = 0L;

    // 节点的兄弟节点uid长度
    public static final int NODE_SIBLING_UID_LENGTH = DataSetting.LONG_BYTE_SIZE;

    // 节点头部大小
    public static final int NODE_HEAD_SIZE = NODE_SIBLING_UID_OFFSET + NODE_SIBLING_UID_LENGTH;

    // 节点平衡因子
    public static final int NODE_BALANCE_NUMBER = 32;

    // 根节点左子节点默认的kth定位值
    public static final int ROOT_NODE_LEFT_SON_DEFAULT_KTH = 0;

    // 根节点右子节点默认的kth定位值
    public static final int ROOT_NODE_RIGHT_SON_DEFAULT_KTH = 1;

    // 节点uid默认值
    public static final long NODE_UID_DEFAULT = 0L;

    // 节点uid错误默认值
    public static final long NODE_UID_ERROR_DEFAULT = -1;

    // 节点uid长度
    public static final int NODE_UID_LENGTH = DataSetting.LONG_BYTE_SIZE;

    // 节点键值默认定位值
    public static final int NODE_KEY_POS_DEFAULT = 0;

    // 节点最后一个键值默认值
    public static final long NODE_LAST_KEY_DEFAULT = Long.MAX_VALUE;

    // 索引键值长度
    public static final int INDEX_KEY_LENGTH = DataSetting.LONG_BYTE_SIZE;

    // 节点 uid 与 key 对大小
    public static final int NODE_SON_COUPLE_SIZE = NODE_UID_LENGTH + INDEX_KEY_LENGTH;

    // 节点大小
    public static final int NODE_SIZE = NODE_HEAD_SIZE + NODE_SON_COUPLE_SIZE * (NODE_BALANCE_NUMBER + 1) * 2;

    // 搜索下一层节点的返回结果
    public static class SearchNextNodeResult {
        public long nodeUid;
        public long nodeSiblingUid;
    }

    // 叶子节点搜索范围节点的返回结果
    public static class LeafSearchRangeNodeResult {
        public List<Long> nodeUidList;

        public long nodeSiblingUid;
    }

    // 插入并分裂节点的返回结果
    public static class InsertAndSplitNodeResult {
        public long nodeSiblingUid;

        public long nodeNewSonUid;

        public long nodeNewKey;
    }

    // 分裂节点的返回结果
    public static class SplitNodeResult {
        public long nodeNewSonUid;

        public long nodeNewKey;
    }

    // 插入节点的返回结果
    public static class InsertNodeResult {
        public long nodeNewSonUid;

        public long nodeNewKey;
    }

}
