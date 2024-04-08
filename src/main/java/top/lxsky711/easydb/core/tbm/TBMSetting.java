package top.lxsky711.easydb.core.tbm;

/**
 * @Author: 711lxsky
 * @Description: TBM表管理器配置类
 */

public class TBMSetting {

    // 事务开始返回结果
    public static class BeginResult {

        public long transactionXid;

        public byte[] result;

    }

    public static long FIELD_INDEX_DEFAULT = 0L;

    public static class Frontiers {
        long leftFrontier;

        long rightFrontier;
    }

    public static final long LEFT_FRONTIER_DEFAULT = Long.MIN_VALUE;

    public static final long RIGHT_FRONTIER_DEFAULT = Long.MAX_VALUE;

    public static class BytesDataParseResult{

        public Object value;

        public int shiftFoots;
    }

    public static final String DELIMITER = ", ";

    public static final String PREFIX_DELIMITER = "[";

    public static final String SECOND_PREFIX_DELIMITER = "{";

    public static final String SUFFIX_DELIMITER = "]";

    public static final String SECOND_SUFFIX_DELIMITER = "}";

    public static final String BOOTER_SUFFIX = ".bt";

    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

    public static final long TABLE_UID_DEFAULT = 0L;

    public static final String LINE_FEED = "\n";
}
