package top.lxsky711.easydb.core.dm;

import top.lxsky711.easydb.common.data.DataSetting;
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
import top.lxsky711.easydb.core.tm.TransactionManager;

import java.util.Objects;

/**
 * @Author: 711lxsky
 * @Description:
 */

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{

    private TransactionManager tm;

    private PageCache pageCache;

    private Logger logger;

    private PageIndex pageIndex;

    private Page pageOne;

    public DataManagerImpl(TransactionManager tm, PageCache pageCache, Logger logger) {
        super(DataSetting.DATA_CACHE_DEFAULT_SIZE);
        this.pageCache = pageCache;
        this.logger = logger;
        this.tm = tm;
        this.pageIndex = new PageIndex();
    }

    public void initPageOne(){
        int pageNumber = this.pageCache.buildNewPageWithData(PageOne.init());
        assert pageNumber == PageSetting.PAGE_ONE_DEFAULT_NUMBER;
        this.pageOne = this.pageCache.getPageByPageNumber(pageNumber);
        this.pageCache.flushPage(this.pageOne);
    }

    public void flushPageOne(){
        this.pageCache.flushPage(this.pageOne);
    }


    public void setPageOneVCOpen(){
        PageOne.setVCWithPageOpen(this.pageOne);
    }

    public boolean loadAndCheckPageOne(){
        this.pageOne = this.pageCache.getPageByPageNumber(PageSetting.PAGE_ONE_DEFAULT_NUMBER);
        return PageOne.checkVCWithPage(this.pageOne);
    }

    public void initPageIndex(){
        int pageAllNumber = this.pageCache.getPageNumber();
        for(int i = 2; i <= pageAllNumber; i ++){
            Page page = this.pageCache.getPageByPageNumber(i);
            this.pageIndex.addFreeSpaceForPage(page.getPageNumber(), PageX.getFreeSpaceForPage(page));
            page.releaseOneReference();
        }
    }

    @Override
    protected DataItem getCacheFromDataSourceByKey(long uid) {
        int pageNumber = Logger.getPageNumberFromUid(uid);
        short offset = Logger.getOffsetFromUid(uid);
        Page page = this.pageCache.getPageByPageNumber(pageNumber);
        return DataItem.buildDataItem(page, offset, this);
    }

    @Override
    protected void releaseCacheForObject(DataItem dataItem) {
        dataItem.getPage().releaseOneReference();
    }

    @Override
    public DataItem readData(long uid) {
        DataItem dataItem = super.getResource(uid);
        if(! dataItem.isValid()){
            dataItem.releaseOneReference();
            return null;
        }
        return dataItem;
    }

    @Override
    public long insertData(long xid, byte[] data) {
        byte[] dataRecord = DataItem.buildDataRecord(data);
        int dataRecordSize = dataRecord.length;
        if( dataRecordSize > PageSetting.PAGE_X_MAX_FREE_SPACE){
            Log.logWarningMessage(WarningMessage.DATA_TOO_LARGE);
            return DataItemSetting.ERROR_INSERT_RESULT;
        }
        PageInfo  properPageInfo = null;
        for(int i = 0; i < DataItemSetting.INSET_MAX_RETRY_TIME; i++){
            properPageInfo = this.pageIndex.selectOnePage(dataRecordSize);
            if(Objects.nonNull(properPageInfo)){
                break;
            }
            else {
                // 因为当前没有找到合适的页面，所以猜测其他事务请求比较激烈或者空间不够，所以加入新空间
                int newPageNumber = this.pageCache.buildNewPageWithData(PageX.init());
                this.pageIndex.addFreeSpaceForPage(newPageNumber, PageSetting.PAGE_X_MAX_FREE_SPACE);
            }
        }
        if(Objects.isNull(properPageInfo)){
            Log.logWarningMessage(WarningMessage.CONCURRENCY_HIGH);
            return DataItemSetting.ERROR_INSERT_RESULT;
        }
        Page curPage = null;
        try {
            curPage = this.pageCache.getPageByPageNumber(properPageInfo.pageNumber);
            byte[] log = Logger.buildLogBytes(LoggerSetting.LOG_TYPE_INSERT, xid, curPage.getPageNumber(), PageX.getFreeSpaceOffsetFromPage(curPage), dataRecord);
            this.logger.writeLog(log);
            short dataOffsetInPage = PageX.insertDataIntoPage(curPage, dataRecord);
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

    public void writeLog(byte[] log){
        this.logger.writeLog(log);
    }

    public void releaseOneDataItem(long uid){
        super.releaseOneReference(uid);
    }

    @Override
    public void close() {
        super.close();
        this.logger.close();
        PageOne.setVCWithPageClose(this.pageOne);
        this.pageOne.releaseOneReference();
        this.pageCache.close();
    }
}
