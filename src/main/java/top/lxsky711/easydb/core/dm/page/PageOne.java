package top.lxsky711.easydb.core.dm.page;

import top.lxsky711.easydb.common.data.RandomUtil;

import java.util.Arrays;

/**
 * @Author: 711lxsky
 * @Description: 第一个页面，用来做检查启动
 * DB启动时，给100~107字节处填入一个随机字节ValidCheck，DB关闭时再将其拷贝到108～115字节处
 * <p>
 * 所以结构是[暂时没用的填充数据][valid_check_open][valid_check_close][暂时没用的填充数据]
 * 其实这里填充数据，可以抽一些拿出来作元数据的存储，比如版本号、数据库/表名等
 * </p>
 * 数据库每次启动时，会检查两处字节是否相同，以此判断上一次是否正常关闭。如果非正常关闭，就需要执行数据恢复
 */

public class PageOne {

    /**
     * @Author: 711lxsky
     * @Description: 初始化第一页校验码
     */
    public static byte[] init() {
        byte[] raw = new byte[PageSetting.PAGE_SIZE];
        setVCWithPageDataOpen(raw);
        return raw;
    }

    /**
     * @Author: 711lxsky
     * @Description: 打开页面时设置校验码
     */
    public static void setVCWithPageOpen(Page page){
        page.setDirtyStatus(true);
        setVCWithPageDataOpen(page.getPageData());
    }


    /**
     * @Author: 711lxsky
     * @Description: 开页面时设置校验码的操作封装
     */
    private static void setVCWithPageDataOpen(byte[] pageData){
        byte[] validCheck = RandomUtil.randomBytes(PageSetting.PAGE_ONE_VALID_CHECK_LENGTH);
        System.arraycopy(validCheck, 0,
                pageData, PageSetting.PAGE_ONE_VALID_CHECK_OFFSET,
                PageSetting.PAGE_ONE_VALID_CHECK_LENGTH);
    }

    /**
     * @Author: 711lxsky
     * @Description: 关闭页面时设置校验码
     */
    public static void setVCWithPageClose(Page page){
        page.setDirtyStatus(true);
        setVCWithPageDataClose(page.getPageData());
    }

    /**
     * @Author: 711lxsky
     * @Description: 关页面时设置校验码的操作封装
     */
    private static void setVCWithPageDataClose(byte[] pageData){
        System.arraycopy(pageData, PageSetting.PAGE_ONE_VALID_CHECK_OFFSET,
                pageData, PageSetting.PAGE_ONE_VALID_CHECK_OFFSET + PageSetting.PAGE_ONE_VALID_CHECK_LENGTH,
                PageSetting.PAGE_ONE_VALID_CHECK_LENGTH);
    }

    /**
     * @Author: 711lxsky
     * @Description: 根据页面数据检查校验码
     */
    public static boolean checkVCWithPage(Page page){
        return checkVCWithPageData(page.getPageData());
    }

    /**
     * @Author: 711lxsky
     * @Description: 检查页面数据中的校验码的操作封装
     */
    private static boolean checkVCWithPageData(byte[] pageData){
        byte[] vcOpen = Arrays.copyOfRange(pageData, PageSetting.PAGE_ONE_VALID_CHECK_OFFSET, PageSetting.PAGE_ONE_VALID_CHECK_OFFSET + PageSetting.PAGE_ONE_VALID_CHECK_LENGTH);
        byte[] vcClose = Arrays.copyOfRange(pageData, PageSetting.PAGE_ONE_VALID_CHECK_OFFSET + PageSetting.PAGE_ONE_VALID_CHECK_LENGTH, PageSetting.PAGE_ONE_VALID_CHECK_OFFSET + 2 * PageSetting.PAGE_ONE_VALID_CHECK_LENGTH);
        return java.util.Arrays.equals(vcOpen, vcClose);
    }
}
