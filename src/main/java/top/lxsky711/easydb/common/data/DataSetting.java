package top.lxsky711.easydb.common.data;

/**
 * @Author: 711lxsky
 * @Description: 数据全局配置类
 */

public class DataSetting {

    public static final int DATA_CACHE_DEFAULT_SIZE = 10000;

    public static final int LONG_BYTE_SIZE = Long.SIZE / Byte.SIZE;

    public static final int INT_BYTE_SIZE = Integer.SIZE / Byte.SIZE;

    public static final int SHORT_BYTE_SIZE = Short.SIZE / Byte.SIZE;

    public static final int BYTE_BYTE_SIZE = 1;

    public static final int NAME_MAX_LENGTH = 64;

    public static final String[] DATA_TYPES_DEFAULT =
            {
                    "int32", "int64", "string"
            };

}
