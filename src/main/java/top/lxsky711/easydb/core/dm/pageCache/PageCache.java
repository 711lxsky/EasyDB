package top.lxsky711.easydb.core.dm.pageCache;

import top.lxsky711.easydb.core.dm.page.Page;

/**
 * @Author: 711lxsky
 * @Description: 页面缓存接口，对接计数缓存框架，规范化操作
 */

public interface PageCache {

    /**
     * @Author: 711lxsky
     * @Description: 拿到数据库文件中的页数，表示有多少个页面
     */
    int getPageNumber();

    /**
     * @Author: 711lxsky
     * @Description: 新建一个页面到数据库文件，并放入需要存放的数据，返回页号
     * 这里并没有自动放到缓存里
     */
    int buildNewPageWithData(byte[] initData);

    /**
     * @Author: 711lxsky
     * @Description: 根据页号，从缓存中获取页面
     */
    Page getPageByPageNumber(int pageNumber);

    /**
     * @Author: 711lxsky
     * @Description: 释放一个页面资源引用
     */
    void releaseOnePageReference(Page page);

    /**
     * @Author: 711lxsky
     * @Description: 这个方法是用来恢复数据或者其他场景下，截断缓存文件
     */
    void truncatePageWithMPageNum(int maxPageNumber);

    /**
     * @Author: 711lxsky
     * @Description: 刷新页面缓存到文件中
     */
    void flushPage(Page page);

    /**
     * @Author: 711lxsky
     * @Description: 关闭页面缓存，这里是基于RandomAccessFile实现，需要关闭资源
     */
    void close();

}
