package top.lxsky711.easydb.server;

/**
 * @Author: 711lxsky
 * @Description: 服务端配置
 */

public class ServerSetting {

    public static final long EXECUTOR_TRANSACTION_XID_DEFAULT = -1L;

    public static final int SERVER_SOCKET_PORT = 9875;

    public static final long MEMORY_SIZE_DEFAULT = (1 << 20) * 64;

    public static final long KB = 1 << 10;

    public static final String KB_UNIT = "KB";

    public static final long MB = 1 << 20;

    public static final String MB_UNIT = "MB";

    public static final long GB = 1 << 30;

    public static final String GB_UNIT = "GB";

    public static final String OPTION_OPEN = "open";

    public static final String OPTION_OPEN_DESCRIPTION = "-open DadaBasePath";

    public static final String OPTION_CREATE = "create";

    public static final String OPTION_CREATE_DESCRIPTION = "-create DataBasePath";

    public static final String OPTION_MEMORY = "memory";

    public static final String OPTION_MEMORY_DESCRIPTION = "-memory 64MB";

    public static final int OPTION_MEMORY_LENGTH_MIN = 2;

    public static final String OPTION_USAGE = "Usage: launcher (open|create) DataBasePath";
}
