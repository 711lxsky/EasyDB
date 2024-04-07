package top.lxsky711.easydb.common.data;

/**
 * @Author: 711lxsky
 * @Description: 数据全局配置类
 */

public class DataSetting {

    public static final int DATA_START_POS_DEFAULT = 0;

    public static final long DATA_SEED = 711L;

    public static final int DATA_CACHE_DEFAULT_SIZE = 10000;

    public static final int LONG_BYTE_SIZE = Long.SIZE / Byte.SIZE;

    public static final int INT_BYTE_SIZE = Integer.SIZE / Byte.SIZE;

    public static final int SHORT_BYTE_SIZE = Short.SIZE / Byte.SIZE;

    public static final int BYTE_BYTE_SIZE = 1;

    public static final int NAME_MAX_LENGTH = 64;

    public static final String DATA_INT32 = "int32";

    public static final String DATA_INT64 = "int64";

    public static final String DATA_STRING = "string";

    public static final String[] DATA_TYPES_DEFAULT =
            {
                    DATA_INT32, DATA_INT64, DATA_STRING
            };

    public static final String LOGIC_AND = "and";

    public static final String LOGIC_OR = "or";

    public static final String[] LOGIC_DEFAULT =
            {
                    LOGIC_AND, LOGIC_OR
            };

    public static final String COMPARE_EQUAL = "=";

    public static final String COMPARE_LARGER = ">";

    public static final String COMPARE_SMALLER = "<";

    public static final String[] COMPARE_DEFAULT =
            {
                    COMPARE_EQUAL, COMPARE_LARGER, COMPARE_SMALLER
            };

    public static class StringBytes{

        public String str;

        public int strLength;

        public final int strLengthSize = INT_BYTE_SIZE;
    }

}
