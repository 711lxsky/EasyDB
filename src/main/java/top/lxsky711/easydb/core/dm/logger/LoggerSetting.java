package top.lxsky711.easydb.core.dm.logger;

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

}
