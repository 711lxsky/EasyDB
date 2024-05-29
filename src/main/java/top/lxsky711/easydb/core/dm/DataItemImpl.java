package top.lxsky711.easydb.core.dm;

import com.google.common.primitives.Bytes;
import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.core.common.SubArray;
import top.lxsky711.easydb.core.dm.logger.Logger;
import top.lxsky711.easydb.core.dm.logger.LoggerSetting;
import top.lxsky711.easydb.core.dm.page.Page;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Author: 711lxsky
 * @Description: DataItem实现类
 */

public class DataItemImpl implements DataItem{

    private final long uid;

    // 数据记录，注意这里rawDataRecord里面的rawData可能存储了非常多的冗余数据
    private final SubArray rawDataRecord;

    private final byte[] oldDataRecord;

    private final Page page;

    private final DataManager dm;

    private final Lock readLock;

    private final Lock writeLock;

    public DataItemImpl(SubArray dataRecord, byte[] oldData, Page page, long uid, DataManager dm){
        this.rawDataRecord = dataRecord;
        this.oldDataRecord = oldData;
        this.page = page;
        this.uid = uid;
        this.dm = dm;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
    }

    @Override
    public SubArray getDataRecord() {
        return new SubArray(
                this.rawDataRecord.rawData,
                this.rawDataRecord.start + DataItemSetting.DATA_DATA_OFFSET,
                this.rawDataRecord.end);
    }

    @Override
    public SubArray getRawDataRecord() {
        return this.rawDataRecord;
    }

    @Override
    public boolean isValid(){
        return this.rawDataRecord.rawData[this.rawDataRecord.start + DataItemSetting.DATA_VALID_OFFSET] == DataItemSetting.DATA_VALID;
    }

    @Override
    public void beforeModify() {
        // 先获取写锁
        this.writeLock.lock();
        this.page.setDirtyStatus(true);
        // 备份原始数据
        System.arraycopy(this.rawDataRecord.rawData, this.rawDataRecord.start,
                this.oldDataRecord, DataItemSetting.DATA_START_POS, this.oldDataRecord.length);
    }

    @Override
    public void unBeforeModify() {
        // 恢复原始数据
        System.arraycopy(this.oldDataRecord, DataItemSetting.DATA_START_POS,
                this.rawDataRecord.rawData, this.rawDataRecord.start, this.oldDataRecord.length);
        this.writeLock.unlock();
    }

    @Override
    public void afterModify(long xid) throws WarningException, ErrorException {
        int pageNumber = Logger.getPageNumberFromUid(this.uid);
        short offset = Logger.getOffsetFromUid(this.uid);
        // 包裹update类型的日志
        byte[] newDataRecord = Arrays.copyOfRange(this.rawDataRecord.rawData, this.rawDataRecord.start, this.rawDataRecord.end);
        byte[] logData = Bytes.concat(this.oldDataRecord, newDataRecord);
        byte[] newUpdateLog = Logger.buildLogBytes(LoggerSetting.LOG_TYPE_UPDATE, xid, pageNumber, offset,logData);
        this.dm.writeLog(newUpdateLog);
    }

    @Override
    public void releaseOneReference() throws WarningException, ErrorException {
        this.dm.releaseOneDataItem(this.uid);
    }

    @Override
    public void readLock() {
        this.readLock.lock();
    }

    @Override
    public void readUnlock() {
        this.readLock.unlock();
    }

    @Override
    public void writeLock() {
        this.writeLock.lock();
    }

    @Override
    public void writeUnlock() {
        this.writeLock.unlock();
    }

    @Override
    public Page getPage() {
        return this.page;
    }

    @Override
    public long getUid() {
        return this.uid;
    }

    @Override
    public byte[] getOldDataRecord() {
        return this.oldDataRecord;
    }
}
