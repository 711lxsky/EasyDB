package top.lxsky711.easydb.core.dm.logger;

import top.lxsky711.easydb.common.data.DataSetting;

/**
 * @Author: 711lxsky
 * @Description: 日志配置
 */

public class LoggerSetting {

    // 用于计算日志校验和的种子
    public static final int LOGGER_SEED = 711;

    public static final int LOGGER_HEADER_OFFSET = 0;

    public static final int LOGGER_HEADER_LENGTH = 4;

    public static final int LOGGER_LOG_OFFSET = LOGGER_HEADER_OFFSET + LOGGER_HEADER_LENGTH;

    public static final int LOGGER_LOG_SIZE_OFFSET = 0;

    public static final int LOGGER_LOG_SIZE_LENGTH = 4;

    public static final int LOGGER_LOG_CHECKSUM_OFFSET = LOGGER_LOG_SIZE_OFFSET + LOGGER_LOG_SIZE_LENGTH;

    public static final int LOGGER_LOG_CHECKSUM_LENGTH = 4;
    public static final int LOGGER_LOG_DATA_OFFSET = LOGGER_LOG_CHECKSUM_OFFSET + LOGGER_LOG_CHECKSUM_LENGTH;

    public static final String LOGGER_FILE_SUFFIX = ".log";

    public static final byte LOG_TYPE_INSERT = 1;

    public static final byte LOG_TYPE_UPDATE = 2;

    public static final int REDO = 3;

    public static final int UNDO = 4;

    // 插入日志
    public static class InsertLog {

        public byte type;

        public long xid;

        public int pageNumber;

        public short offset;

        public byte[] data;
    }

    // 更新日志
    public static class UpdateLog {

        public byte type;

        public long xid;

        public int pageNumber;

        public short offset;

        public byte[] oldData;

        public byte[] newData;
    }

    public static final byte LOG_TYPE_OFFSET = 0;

    public static final int LOG_TYPE_LENGTH = DataSetting.BYTE_BYTE_SIZE;

    public static final int LOG_XID_OFFSET = LOG_TYPE_OFFSET + LOG_TYPE_LENGTH;

    public static final int LOG_XID_LENGTH = DataSetting.LONG_BYTE_SIZE;

    public static final int LOG_PAGE_NUMBER_OFFSET = LOG_XID_OFFSET + LOG_XID_LENGTH;

    public static final int LOG_PAGE_NUMBER_LENGTH = DataSetting.INT_BYTE_SIZE;

    public static final int LOG_OFFSET_OFFSET = LOG_PAGE_NUMBER_OFFSET + LOG_PAGE_NUMBER_LENGTH;

    public static final int LOG_OFFSET_LENGTH = DataSetting.SHORT_BYTE_SIZE;

    public static final int LOG_DATA_OFFSET = LOG_OFFSET_OFFSET + LOG_OFFSET_LENGTH;

    /* 因为两种日志的结构差别不大，所以 偏移量就设为共用

    public static final int INSET_LOG_TYPE_OFFSET = 0;

    public static int INSET_LOG_XID_OFFSET = INSET_LOG_TYPE_OFFSET + LOG_TYPE_LENGTH;

    public static int INSET_LOG_PAGE_NUMBER_OFFSET = INSET_LOG_XID_OFFSET + LOG_XID_LENGTH;

    public static int INSET_LOG_OFFSET_OFFSET = INSET_LOG_PAGE_NUMBER_OFFSET + LOG_PAGE_NUMBER_LENGTH;

    public static int INSET_LOG_DATA_OFFSET = INSET_LOG_OFFSET_OFFSET + LOG_OFFSET_LENGTH;



    public static int UPDATE_LOG_TYPE_OFFSET = 0;

    public static int UPDATE_LOG_XID_OFFSET = UPDATE_LOG_TYPE_OFFSET + LOG_TYPE_LENGTH;

    public static int UPDATE_LOG_PAGE_NUMBER_OFFSET = UPDATE_LOG_XID_OFFSET + LOG_XID_LENGTH;

    public static int UPDATE_LOG_OFFSET_OFFSET = UPDATE_LOG_PAGE_NUMBER_OFFSET + LOG_PAGE_NUMBER_LENGTH;

    public static int UPDATE_LOG_DATA_OFFSET = UPDATE_LOG_OFFSET_OFFSET + LOG_OFFSET_LENGTH;

     */

}
