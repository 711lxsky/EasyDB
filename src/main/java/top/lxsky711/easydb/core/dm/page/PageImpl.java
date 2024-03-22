package top.lxsky711.easydb.core.dm.page;

import top.lxsky711.easydb.core.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: 711lxsky
 * @Description: 页面接口的一般实现类
 */

public class PageImpl implements Page{

    // 页面页号，从 1 开始
    private int pageNumber;

    // 页面包含的字节数组形式数据
    private byte[] data;

    // 脏状态标记
    // 脏的话意味着缓存中的数据和内存/持久层中的数据不一致，缓存驱逐时务必写回
    private boolean dirtyStatus;

    // 页面缓存
    private PageCache pageCache;

    // 页面锁，资源控制
    private Lock lock;

    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pageCache = pc;
        this.lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        this.lock.lock();
    }

    @Override
    public void unlock() {
        this.lock.unlock();
    }

    @Override
    public void releaseCache() {
        this.pageCache.releaseOnePageReference(this);
    }

    @Override
    public void setDirtyStatus(Boolean status) {
        this.dirtyStatus = status;
    }

    @Override
    public boolean isDirty() {
        return this.dirtyStatus;
    }

    @Override
    public int getPageNumber() {
        return this.pageNumber;
    }

    @Override
    public byte[] getPageData() {
        return this.data;
    }
}
