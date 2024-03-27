package top.lxsky711.easydb.core.dm;

import top.lxsky711.easydb.common.data.DataSetting;

/**
 * @Author: 711lxsky
 * @Description: DataItem的设置
 */

public class DataItemSetting {

    public static final int ERROR_INSERT_RESULT = -1;

    public static final int INSET_MAX_RETRY_TIME = 5;

    public static final int DATA_START_POS = 0;

    public static final int DATA_VALID_OFFSET = 0;

    public static final int DATA_VALID_LENGTH = DataSetting.BYTE_BYTE_SIZE;

    public static final int DATA_SIZE_OFFSET = DATA_VALID_OFFSET + DATA_VALID_LENGTH;

    public static final int DATA_SIZE_LENGTH = DataSetting.SHORT_BYTE_SIZE;

    public static final int DATA_DATA_OFFSET = DATA_SIZE_OFFSET + DATA_SIZE_LENGTH;

    public static final byte DATA_VALID = 6;

    public static final byte DATA_INVALID = 0;

}
