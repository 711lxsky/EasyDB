package top.lxsky711.easydb.core.dm.pageCache;

import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.file.FileManager;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;
import top.lxsky711.easydb.core.common.AbstractCache;
import top.lxsky711.easydb.core.dm.page.Page;
import top.lxsky711.easydb.core.dm.page.PageImpl;
import top.lxsky711.easydb.core.dm.page.PageSetting;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: 711lxsky
 * @Description:  因为页面数据的数据源就是文件系统，所以数据的读写基于文件即可,包裹成Page
 */

public class PageCacheImpl extends AbstractCache<Page> implements PageCache{

    /**
     * 页面数据文件
     */
    private final RandomAccessFile pageDataFile;

    /**
     * 数据文件通道
     */
    private final FileChannel pageFileChannel;

    /**
     * 记录当前打开的页面数据文件所含页面的页数，
     * 此数据在打开时就会被计算，并在创建页面时自增
     */
    private final AtomicInteger pageNumbers;

    /**
     * 放锁，防止多个线程同时操作文件造成数据不一致
     */
    private final Lock pageFileLock;

    public PageCacheImpl(RandomAccessFile raf, int maxResourceNum) throws WarningException, ErrorException {
        super(maxResourceNum);
        if(maxResourceNum < PageSetting.PAGE_CACHE_MIN_SIZE){
            Log.logWarningMessage(WarningMessage.PAGE_CACHE_RESOURCE_TOO_LESS);
        }
        long pageDataFileLength = FileManager.getRAFileLength(raf);
        this.pageDataFile = raf;
        this.pageFileChannel = raf.getChannel();
        this.pageFileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) (pageDataFileLength / PageSetting.PAGE_SIZE));
    }


    @Override
    public int getPagesNumber() {
        return this.pageNumbers.intValue();
    }

    @Override
    public int buildNewPageWithData(byte[] initData) throws WarningException, ErrorException {
        // 文件中新增页面数据，页数自增
        int newPageNumber = this.pageNumbers.incrementAndGet();
        Page newPage = new PageImpl(newPageNumber, initData, null);
        // 数据刷到文件中去
        this.flushPage(newPage);
        return newPageNumber;
    }

    @Override
    public Page getPageByPageNumber(int pageNumber) throws WarningException, ErrorException {
        return super.getResource(pageNumber);
    }

    @Override
    public void releaseOneReference(Page page) throws WarningException, ErrorException {
        super.releaseOneReference(page.getPageNumber());
    }

    @Override
    public void truncatePageWithMPageNum(int maxPageNumber) throws WarningException {
        long truncatedPageFileSize = getPageDataOffset(maxPageNumber + 1);
        FileManager.setFileNewLength(this.pageDataFile, truncatedPageFileSize);
    }

    @Override
    public void flushPage(Page page) throws WarningException, ErrorException {
        int pageNumber = page.getPageNumber();
        long pageDataOffset = getPageDataOffset(pageNumber);
        ByteBuffer data = ByteBuffer.wrap(page.getPageData());
        pageFileLock.lock();
        try {
            FileManager.writeByteDataIntoFileChannel(this.pageFileChannel, pageDataOffset, data);
            FileManager.forceRefreshFileChannel(this.pageFileChannel, false);
        }
        finally {
            pageFileLock.unlock();    
        }
    }

    @Override
    public void close() throws ErrorException {
        FileManager.closeFileAndChannel(this.pageFileChannel, this.pageDataFile);
    }

    @Override
    protected Page getCacheFromDataSourceByKey(long cacheKey) throws ErrorException {
        int pageNumber = (int) cacheKey;
        long pageDataOffset = getPageDataOffset(pageNumber);
        ByteBuffer pageData = ByteBuffer.allocate(PageSetting.PAGE_SIZE);
        pageFileLock.lock();
        try {
            FileManager.readByteDataIntoFileChannel(this.pageFileChannel, pageDataOffset, pageData);
        }
        finally {
            pageFileLock.unlock();
        }
        return new PageImpl(pageNumber, pageData.array(), this);
    }

    @Override
    protected void releaseCacheForObject(Page page) throws WarningException, ErrorException {
        if(page.isDirty()){
            flushPage(page);
            page.setDirtyStatus(false);
        }
    }
    
    /**
     * @Author: 711lxsky
     * @Description: 获取某个页面在页面数据文件中的起始偏移量 
     */
    private static long getPageDataOffset(int pageNumber){
        return (long) (pageNumber - 1) * PageSetting.PAGE_SIZE;
    }
    
}
