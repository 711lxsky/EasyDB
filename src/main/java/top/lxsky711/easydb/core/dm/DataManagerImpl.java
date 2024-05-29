package top.lxsky711.easydb.core.dm;

import top.lxsky711.easydb.common.data.DataSetting;
import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;
import top.lxsky711.easydb.core.common.AbstractCache;
import top.lxsky711.easydb.core.dm.logger.Logger;
import top.lxsky711.easydb.core.dm.logger.LoggerSetting;
import top.lxsky711.easydb.core.dm.page.Page;
import top.lxsky711.easydb.core.dm.page.PageOne;
import top.lxsky711.easydb.core.dm.page.PageSetting;
import top.lxsky711.easydb.core.dm.page.PageX;
import top.lxsky711.easydb.core.dm.pageCache.PageCache;
import top.lxsky711.easydb.core.dm.pageIndex.PageIndex;
import top.lxsky711.easydb.core.dm.pageIndex.PageInfo;

import java.util.Objects;

/**
 * @Author: 711lxsky
 * @Description:
 */

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{

    private final PageCache pageCache;

    private final Logger logger;

    private final PageIndex pageIndex;

    private Page pageOne;

    public DataManagerImpl(PageCache pageCache, Logger logger) throws ErrorException {
        super(DataSetting.DATA_CACHE_DEFAULT_SIZE);
        this.pageCache = pageCache;
        this.logger = logger;
        this.pageIndex = new PageIndex();
    }

    /**
     * @Author: 711lxsky
     * @Description: 创建并初始化第一页
     */
    public void initPageOne() throws WarningException, ErrorException {
        int pageNumber = this.pageCache.buildNewPageWithData(PageOne.init());
        assert pageNumber == PageSetting.PAGE_ONE_DEFAULT_NUMBER;
        this.pageOne = this.pageCache.getPageByPageNumber(pageNumber);
        this.flushPageOne();
    }

    /**
     * @Author: 711lxsky
     * @Description: 第一页数据刷盘
     */
    public void flushPageOne() throws WarningException, ErrorException {
        this.pageCache.flushPage(this.pageOne);
    }


    /**
     * @Author: 711lxsky
     * @Description: 设置第一页VC
     */
    public void setPageOneVCOpen(){
        PageOne.setVCWithPageOpen(this.pageOne);
    }

    /**
     * @Author: 711lxsky
     * @Description: 检查第一页VC
     */
    public boolean loadAndCheckPageOne() throws WarningException, ErrorException {
        this.pageOne = this.pageCache.getPageByPageNumber(PageSetting.PAGE_ONE_DEFAULT_NUMBER);
        return PageOne.checkVCWithPage(this.pageOne);
    }

    /**
     * @Author: 711lxsky
     * @Description: 初始化页面索引
     */
    public void initPageIndex() throws WarningException, ErrorException {
        int pagesNum= this.pageCache.getPagesNumber();
        for(int i = PageSetting.PAGE_X_DEFAULT_START_NUMBER; i <= pagesNum; i ++){
            Page page = this.pageCache.getPageByPageNumber(i);
            this.pageIndex.addFreeSpaceForPage(page.getPageNumber(), PageX.getFreeSpaceForPage(page));
            page.releaseOneReference();
        }
    }

    @Override
    protected DataItem getCacheFromDataSourceByKey(long uid) throws WarningException, ErrorException {
        int pageNumber = Logger.getPageNumberFromUid(uid);
        short offset = Logger.getOffsetFromUid(uid);
        Page page = this.pageCache.getPageByPageNumber(pageNumber);
        return DataItem.buildDataItem(page, offset, this);
    }

    @Override
    protected void releaseCacheForObject(DataItem dataItem) throws WarningException, ErrorException {
        dataItem.getPage().releaseOneReference();
    }

    @Override
    public DataItem readDataItem(long uid) throws WarningException, ErrorException {
        DataItem dataItem = super.getResource(uid);
        if(! dataItem.isValid()){
            dataItem.releaseOneReference();
            return null;
        }
        return dataItem;
    }

    @Override
    public long insertData(long xid, byte[] data) throws WarningException, ErrorException {
        // 先包裹成DataRecord格式
        byte[] newDataRecord = DataItem.buildDataRecord(data);
        int newDataRecordSize = newDataRecord.length;
        // 拿到大小
        if( newDataRecordSize > PageSetting.PAGE_X_MAX_FREE_SPACE){
            Log.logWarningMessage(WarningMessage.DATA_TOO_LARGE);
            return DataItemSetting.ERROR_INSERT_RESULT;
        }
        PageInfo properPageInfo = null;
        // 间歇向页面索引申请页面资源，因为可能存在某些空间被其他事务占用或者是空间不够
        for(int i = 0; i < DataItemSetting.INSET_MAX_RETRY_TIME; i++){
            properPageInfo = this.pageIndex.selectOnePage(newDataRecordSize);
            if(Objects.nonNull(properPageInfo)){
                break;
            }
            else {
                // 因为当前没有找到合适的页面，所以猜测其他事务请求比较激烈或者空间不够，加入新空间
                int newPageNumber = this.pageCache.buildNewPageWithData(PageX.init());
                this.pageIndex.addFreeSpaceForPage(newPageNumber, PageSetting.PAGE_X_MAX_FREE_SPACE);
            }
        }
        // 还是没有拿到合适页面
        if(Objects.isNull(properPageInfo)){
            Log.logWarningMessage(WarningMessage.CONCURRENCY_HIGH);
            return DataItemSetting.ERROR_INSERT_RESULT;
        }
        Page curPage = null;
        try {
            // 从页面缓存中拿到页面
            curPage = this.pageCache.getPageByPageNumber(properPageInfo.pageNumber);
            // 先把日志写了
            byte[] log = Logger.buildLogBytes(LoggerSetting.LOG_TYPE_INSERT, xid, curPage.getPageNumber(), PageX.getFreeSpaceOffsetFromPage(curPage), newDataRecord);
            this.logger.writeLog(log);
            // 再插入数据到页面中，拿到数据偏移量
            short dataOffsetInPage = PageX.insertDataIntoPage(curPage, newDataRecord);
            // 资源释放
            curPage.releaseOneReference();
            return Logger.parsePageNumberAndOffsetToUid(properPageInfo.pageNumber, dataOffsetInPage);
        }finally {
            if(Objects.nonNull(curPage)){
                this.pageIndex.addFreeSpaceForPage(properPageInfo.pageNumber, PageX.getFreeSpaceForPage(curPage));
            }
            else {
                this.pageIndex.addFreeSpaceForPage(properPageInfo.pageNumber, PageSetting.PAGE_DEFAULT_MIN_SIZE);
            }
        }
    }

    @Override
    public void writeLog(byte[] log) throws WarningException, ErrorException {
        this.logger.writeLog(log);
    }

    @Override
    public void releaseOneDataItem(long uid) throws WarningException, ErrorException {
        super.releaseOneReference(uid);
    }

    @Override
    public void close() throws ErrorException, WarningException {
        super.close();
        this.logger.close();
        // 注意PageOne关闭时设置VC
        PageOne.setVCWithPageClose(this.pageOne);
        this.pageOne.releaseOneReference();
        this.pageCache.close();
    }
}
