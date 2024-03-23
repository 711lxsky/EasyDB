package top.lxsky711.easydb.common.log;

/**
 * @Author: 711lxsky
 */

public class ErrorMessage {

    public static final String BAD_FILE
            = "This file has error!";

    public static final String BAD_RANDOM_ACCESS_FILE
            = "This RandomAccessFile has error!";

    public static final String BAD_XID_FILE
            = "This XID_File has error!";

    public static final String BAD_PAGE_FILE
            = "This PAGE_File has error!";

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
}
