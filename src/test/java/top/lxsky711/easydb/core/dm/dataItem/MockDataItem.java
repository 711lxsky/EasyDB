package top.lxsky711.easydb.core.dm.dataItem;

import top.lxsky711.easydb.core.common.SubArray;
import top.lxsky711.easydb.core.dm.DataItem;
import top.lxsky711.easydb.core.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Author: 711lxsky
 * @Description:
 */

public class MockDataItem implements DataItem {

    private SubArray rawDataRecord;
    private byte[] oldDataRecord;
    private long uid;
    private Lock rLock;
    private Lock wLock;

    public static MockDataItem newMockDataItem(long uid, SubArray rawDataRecord) {
        MockDataItem di = new MockDataItem();
        di.rawDataRecord = rawDataRecord;
        di.oldDataRecord = new byte[rawDataRecord.end - rawDataRecord.start];
        di.uid = uid;
        ReadWriteLock l = new ReentrantReadWriteLock();
        di.rLock = l.readLock();
        di.wLock = l.writeLock();
        return di;
    }

    @Override
    public SubArray getDataRecord() {
        return rawDataRecord;
    }

    @Override
    public SubArray getRawDataRecord() {
        return rawDataRecord;
    }

    @Override
    public boolean isValid() {
        return false;
    }

    @Override
    public void beforeModify() {
        wLock.lock();
        System.arraycopy(rawDataRecord.rawData, rawDataRecord.start, oldDataRecord, 0, oldDataRecord.length);
    }

    @Override
    public void unBeforeModify() {
        wLock.unlock();
        System.arraycopy(oldDataRecord, 0, rawDataRecord.rawData, rawDataRecord.start, oldDataRecord.length);
    }

    @Override
    public void afterModify(long xid) {
        wLock.unlock();
    }

    @Override
    public void releaseOneReference() {

    }

    @Override
    public void readLock() {
        rLock.lock();
    }

    @Override
    public void readUnlock() {
        rLock.unlock();
    }

    @Override
    public void writeLock() {
        wLock.lock();
    }

    @Override
    public void writeUnlock() {
        wLock.unlock();
    }

    @Override
    public Page getPage() {
        return null;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldDataRecord() {
        return oldDataRecord;
    }
}
