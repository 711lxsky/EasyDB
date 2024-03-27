package top.lxsky711.easydb.common.log;

/**
 * @Author: 711lxsky
 */

public class WarningMessage {

    public static final String FILE_CHANNEL_WRITE_NOT_ENOUGH
            = "The file-channel's write operation is not enough for need!";

    public static final String FILE_CHANNEL_DATA_REFRESH_ERROR
            = "The file-channel's data refresh operation has some error!";

    public static final String FILE_CREATE_ERROR
            = "There are some mistakes happened during the File create!";

    public static final String FILE_NOT_EXIST
            = "The file is not exist!";

    public static final String FILE_USE_ERROR
            = "The file can't be read or write!";

    public static final String CACHE_FULL
             = "The cache is full， up to the max resource set number!";

    public static final String PAGE_CACHE_RESOURCE_TOO_LESS
            = "The page-cache resource is too less!";

    public static final String FILE_LENGTH_SET_ERROR
            = "There are some error happened during the file length set!";

    public static final String PAGE_FREE_SPACE_NOT_ENOUGH
            = "The page's free space is not enough for this data!";

    public static final String LOG_FILE_MAYBE_ERROR
            = "The log-file record maybe has some error for its data";

    public static final String LOG_CHECKSUM_ERROR
            = "The log's checksum has wrong";

    public static final String FILE_CHANNEL_TRUNCATE_ERROR
            = "The file-channel's truncate operation has some error!";

    public static final String DATA_TOO_LARGE
            = "The data is too large to insert into the page!";

    public static final String CONCURRENCY_HIGH
            = "The current concurrency is high, so the database is busy!";
}
