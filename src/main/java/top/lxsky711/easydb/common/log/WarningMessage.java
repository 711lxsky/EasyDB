package top.lxsky711.easydb.common.log;

/**
 * @Author: 711lxsky
 */

public class WarningMessage {

    public static final String FILE_CHANNEL_WRITE_NOT_ENOUGH
            = "The file-channel's write operation is not enough for need!";

    public static final String FILE_CHANNEL_DATA_REFRESH_ERROR
            = "The file-channel's data refresh operation has some error!";

    public static final String FILE_CHANNEL_GER_INFORMATION_ERROR
            = "There are some error happened during getting information from the file-channel!";

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

    public static final String NAME_IS_NULL
            = "The name is null!";

    public static final String NAME_TOO_LONG
            = "The name is too long!";

    public static final String NAME_FIRST_CHAR_IS_NOT_LETTER
            = "The first character of the name must to be a letter!";

    public static final String NAME_CONTAIN_SPECIAL_CHAR
            = "The name can't contain special character!";

    public static final String CONCURRENCY_HIGH
            = "The current concurrency is high, so the database is busy!";

    public static final String TRANSACTION_ISOLATION_LEVEL_UNKNOWN
            = "The transaction isolation level is unknown or not support!";

    public static final String TRANSACTION_IS_ABORTED
            = "The transaction is aborted!";

    public static final String VERSION_HOPPING_OCCUR
            = "The transaction hopping occurs!";

    public static final String STATEMENT_NOT_SUPPORT
            = "The statement is not supported or unknown!";

    public static final String STRING_IS_INVALID
            = "The string is invalid!";

    public static final String DATA_TYPE_IS_INVALID
            = "The data type is invalid or not support!";

    public static final String NO_INDEX
            = "There is none index!";

    public static final String INDEX_IS_NOT_EXIST
            = "The index is not exist!";

    public static final String COMPARE_OPERATOR_IS_INVALID
            = "The compare operator is invalid or not support!";

    public static final String EXPRESSION_IS_INVALID
            = "The expression is invalid!";

    public static final String LOGIC_OPERATOR_IS_INVALID
            = "The logic operator is invalid or not support!";
}
