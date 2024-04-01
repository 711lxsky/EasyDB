package top.lxsky711.easydb.core.im;

import top.lxsky711.easydb.common.data.DataSetting;

/**
 * @Author: 711lxsky
 * @Description: 索引管理器配置
 */

public class IMSetting {

    public static final int LEAF_NODE_FLAG_OFFSET = 0;

    public static byte NODE_LEAF_TRUE = 1;

    public static byte NODE_LEAF_FALSE = 0;

    public static final int LEAF_NODE_FLAG_LENGTH = DataSetting.BYTE_BYTE_SIZE;

    public static final int NODE_KEYS_COUNT_OFFSET = LEAF_NODE_FLAG_OFFSET + LEAF_NODE_FLAG_LENGTH;

    public static final short ROOT_NODE_KEYS_COUNT_DEFAULT = 2;

    public static final short LEAF_NODE_KEYS_COUNT_DEFAULT = 0;

    public static final int NODE_KEY_COUNT_LENGTH = DataSetting.SHORT_BYTE_SIZE;

    public static final int NODE_SIBLING_UID_OFFSET = NODE_KEYS_COUNT_OFFSET + NODE_KEY_COUNT_LENGTH;

    public static final long NODE_SIBLING_UID_DEFAULT = -1;

    public static final int NODE_SIBLING_UID_LENGTH = DataSetting.LONG_BYTE_SIZE;

    public static final int NODE_HEAD_SIZE = NODE_SIBLING_UID_OFFSET + NODE_SIBLING_UID_LENGTH;

    public static final int NODE_BALANCE_NUMBER = 32;

    public static final int ROOT_NODE_LEFT_SON_DEFAULT_KTH = 0;

    public static final int ROOT_NODE_RIGHT_SON_DEFAULT_KTH = 1;

    public static final int NODE_UID_LENGTH = DataSetting.LONG_BYTE_SIZE;

    public static final long NODE_LAST_KEY_DEFAULT = Long.MAX_VALUE;

    public static final int INDEX_KEY_LENGTH = DataSetting.LONG_BYTE_SIZE;

    public static final int NODE_SON_COUPLE_SIZE = NODE_UID_LENGTH + INDEX_KEY_LENGTH;

    public static final int NODE_SIZE = NODE_HEAD_SIZE + NODE_SON_COUPLE_SIZE * (NODE_BALANCE_NUMBER + 1) * 2;


}
