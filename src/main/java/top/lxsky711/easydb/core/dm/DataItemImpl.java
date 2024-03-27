package top.lxsky711.easydb.core.dm;

import com.google.common.primitives.Bytes;
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

    private long uid;

    private SubArray dataRecord;

    private byte[] oldDataRecord;

    private Page page;

    private DataManager dm;

    private Lock readLock;

    private Lock writeLock;

    public DataItemImpl(SubArray dataRecord, byte[] oldData, Page page, long uid, DataManager dm){
        this.dataRecord = dataRecord;
        this.oldDataRecord = oldData;
        this.page = page;
        this.uid = uid;
        this.dm = dm;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
    }

    @Override
    public SubArray getData() {
        return new SubArray(
                this.dataRecord.rawData,
                this.dataRecord.start + DataItemSetting.DATA_DATA_OFFSET,
                this.dataRecord.end);
    }

    @Override
    public SubArray getDataRecord() {
        return dataRecord;
    }

    @Override
    public boolean isValid(){
        return this.dataRecord.rawData[this.dataRecord.start + DataItemSetting.DATA_VALID_OFFSET] == DataItemSetting.DATA_VALID;
    }

    @Override
    public void beforeModify() {
        // 先获取写锁
        this.writeLock.lock();
        this.page.setDirtyStatus(true);
        System.arraycopy(this.dataRecord.rawData, this.dataRecord.start,
                this.oldDataRecord, DataItemSetting.DATA_START_POS, this.oldDataRecord.length);
    }

    @Override
    public void unBeforeModify() {
        System.arraycopy(this.oldDataRecord, DataItemSetting.DATA_START_POS,
                this.dataRecord.rawData, this.dataRecord.start, this.oldDataRecord.length);
        this.writeLock.unlock();
    }

    @Override
    public void afterModify(long xid) {
        int pageNumber = Logger.getPageNumberFromUid(this.uid);
        short offset = Logger.getOffsetFromUid(this.uid);
        byte[] newData = Arrays.copyOfRange(this.dataRecord.rawData, this.dataRecord.start, this.dataRecord.end);
        byte[] logData = Bytes.concat(this.oldDataRecord, newData);
        byte[] newUpdateLog = Logger.buildLogBytes(LoggerSetting.LOG_TYPE_UPDATE, xid, pageNumber, offset,logData);
        this.dm.writeLog(newUpdateLog);
    }

    @Override
    public void releaseOneReference() {
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
