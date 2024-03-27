package top.lxsky711.easydb.core.dm.page;

/**
 * @Author: 711lxsky
 * @Description: 参考大部分数据库设计，将数据的读写操作封装为以页面Page为单位
 * 另外，同一条数据不允许跨页存储，所以单条数据的大小不能超过一个页面的大小
 */

public interface Page {

    /**
     * @Author: 711lxsky
     * @Description: 加锁，或者这个页面读写的时候上锁，保证数据一致性
     */
    void lock();

    /**
     * @Author: 711lxsky
     * @Description: 释放锁
     */
    void unlock();

    /**
     * @Author: 711lxsky
     * @Description: 释放页面缓存
     */
    void releaseOneReference();

    /**
     * @Author: 711lxsky
     * @Description: 设置页面数据的脏标记
     */
    void setDirtyStatus(Boolean status);

    /**
     * @Author: 711lxsky
     * @Description: 判断页面是不是脏数据
     */
    boolean isDirty();

    /**
     * @Author: 711lxsky
     * @Description: 获取页面的页号
     */
    int getPageNumber();

    /**
     * @Author: 711lxsky
     * @Description: 获取页面数据的字节形式的原始数据
     */
    byte[] getPageData();
}
