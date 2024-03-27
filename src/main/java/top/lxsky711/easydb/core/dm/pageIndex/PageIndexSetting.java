package top.lxsky711.easydb.core.dm.pageIndex;

import top.lxsky711.easydb.core.dm.page.PageSetting;

/**
 * @Author: 711lxsky
 * @Description: 页面索引设置
 */

public class PageIndexSetting {

    // 页面分隔的间隔数量(最大空闲区间数量)
    public static final int PAGE_INTERVAL_NUMBER = 40;

    // 一个间隔的大小
    public static final int PAGE_INTERVAL_SIZE = PageSetting.PAGE_SIZE / PAGE_INTERVAL_NUMBER;
}
