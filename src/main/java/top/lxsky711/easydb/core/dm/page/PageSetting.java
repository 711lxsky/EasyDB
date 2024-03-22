package top.lxsky711.easydb.core.dm.page;

/**
 * @Author: 711lxsky
 * @Description: 针对Page的一些配置
 */

public class PageSetting {

    // 页面大小 8KB
    public static final int PAGE_SIZE = 1 << 13;

    public static final int PAGE_CACHE_MIN_SIZE = 8;

    public static final String PAGE_FILE_SUFFIX = ".pg";

}
