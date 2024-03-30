package top.lxsky711.easydb.common.log;

/**
 * @Author: 711lxsky
 */

public class ErrorMessage {

    public static final String BAD_FILE
            = "This file has error!";

    public static final String BAD_XID
            = "This XID is wrong!";

    public static final String BAD_PAGE_NUMBER
            = "This page-number is wrong!";

    public static final String BAD_OFFSET
            = "This offset is wrong!";

    public static final String BAD_RANDOM_ACCESS_FILE
            = "This RandomAccessFile has error!";

    public static final String BAD_XID_FILE
            = "This XID_File has error!";

    public static final String BAD_PAGE_FILE
            = "This PAGE_File has error!";

    public static final String BAD_LOG_FILE
            = "The LOG_File has error!";

    public static final String BAD_XID_FILE_HEADER
            = "The XID_File's header is wrong!";

    public static final String BAD_TM
            = "The Transaction-Manager is wrong";

    public static final String BAD_FILE_CHANNEL
            = "The file-channel is bad!";

    public static final String FILE_CHANNEL_USE_ERROR
            = "The file-channel's operation has some error!";

    public static final String FILE_CLOSE_ERROR
            = "There are some mistakes happened during the File close!";

    public static final String CACHE_RESOURCE_NUMBER_ERROR
            = "The cache resource number is wrong, please check it!";

    public static final String LOG_TYPE_ERROR =
            "The log's type may has error which is unknown, check it for real need!";

    public static final String VERSION_CONTROL_RESOURCE_ERROR
            = "Version control resource disorder!";
}
