package top.lxsky711.easydb.core.dm.page;

/**
 * @Author: 711lxsky
 * @Description: 针对Page的一些配置
 */

public class PageSetting {

    // 页面大小 8KB
    public static final int PAGE_SIZE = 1 << 13;

    // 默认页面最小大小
    public static final int PAGE_DEFAULT_MIN_SIZE = 0;

    // 第一页的默认页号
    public static final int PAGE_ONE_DEFAULT_NUMBER = 1;

    // 普通页的默认起始页号
    public static final int PAGE_X_DEFAULT_START_NUMBER = 2;

    // 页面缓存最小数量
    public static final int PAGE_CACHE_MIN_SIZE = 8;

    // 第一页的有效检查偏移量
    public static final int PAGE_ONE_VALID_CHECK_OFFSET = 100;

    // 第一页的有效检查长度
    public static final int PAGE_ONE_VALID_CHECK_LENGTH = 8;

    // 普通页的头部偏移量， 这个头部用以记录当前页的空闲位置偏移
    public static final short PAGE_X_HEADER_OFFSET = 0;

    // 普通页的头部长度
    public static final short PAGE_X_HEADER_LENGTH = 2;

    // 普通页的最大空闲空间
    public static final int PAGE_X_MAX_FREE_SPACE = PAGE_SIZE - PAGE_X_HEADER_LENGTH;

    // 页面文件后缀
    public static final String PAGE_FILE_SUFFIX = ".pg";

}
