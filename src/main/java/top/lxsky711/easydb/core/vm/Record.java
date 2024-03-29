package top.lxsky711.easydb.core.vm;

import com.google.common.primitives.Bytes;
import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.core.common.SubArray;
import top.lxsky711.easydb.core.dm.DataItem;

import java.util.Arrays;
import java.util.Objects;

/**
 * @Author: 711lxsky
 * @Description: 记录实现类
 * 一个记录只有一个版本
 * 结构： [XMIN][XMAX][Data]
 * XMIN表示的是创建这个记录的事务XID,也就是在此事务之后的事务才有可能拿到这个记录
 * XMAX表示的是删除这个记录的事务XID,也就是在此事务之前的事务才有可能拿到这个记录,前两者都是8字节long
 * Data是这个事务持有的数据
 */

public class Record {

    private long uid;

    // 一条记录存储在一个DataItem中
    private DataItem dataItem;

    private VersionManager vm;

    public Record(long uid, DataItem dataItem, VersionManager vm) {
        this.uid = uid;
        this.dataItem = dataItem;
        this.vm = vm;
    }

    public static Record buildRecord(long uid, DataItem dataItem, VersionManager vm) {
        if(Objects.isNull(dataItem)){
            return null;
        }
        return new Record(uid, dataItem, vm);
    }

    public static byte[] wrapDataToRecordBytes(long xid, byte[] data){
        byte[] xmin = ByteParser.longToBytes(xid);
        byte[] xmax = new byte[VMSetting.RECORD_XMAX_LENGTH];
        return Bytes.concat(xmin, xmax, data);
    }

    public long getUid() {
        return this.uid;
    }

    public long getXMIN(){
        this.dataItem.readLock();
        try {
            SubArray dataRecord = this.dataItem.getDataRecord();
            return ByteParser.parseBytesToLong(Arrays.copyOfRange(dataRecord.rawData, dataRecord.start + VMSetting.RECORD_XMIN_OFFSET
                    , dataRecord.start + VMSetting.RECORD_XMAX_OFFSET));
        }
        finally {
            this.dataItem.readUnlock();
        }
    }

    public long getXMAX(){
        this.dataItem.readLock();
        try {
            SubArray dataRecord = this.dataItem.getDataRecord();
            return ByteParser.parseBytesToLong(Arrays.copyOfRange(dataRecord.rawData, dataRecord.start + VMSetting.RECORD_XMAX_OFFSET
                    , dataRecord.start + VMSetting.RECORD_DATA_OFFSET));
        }
        finally {
            this.dataItem.readUnlock();
        }
    }

    public byte[] getData(){
        this.dataItem.readLock();
        try {
            SubArray dataRecord = this.dataItem.getDataRecord();
            byte[] justData = new byte[dataRecord.end - dataRecord.start - VMSetting.RECORD_DATA_OFFSET];
            System.arraycopy(dataRecord.rawData, dataRecord.start + VMSetting.RECORD_DATA_OFFSET
                    , justData, 0, justData.length);
            return justData;
        }
        finally {
            this.dataItem.readUnlock();
        }
    }

    public void setXMAX(long xid){
        this.dataItem.beforeModify();
        try {
            SubArray dataRecord = this.dataItem.getDataRecord();
            System.arraycopy(ByteParser.longToBytes(xid), 0,
                    dataRecord.rawData, dataRecord.start + VMSetting.RECORD_XMAX_OFFSET
                    , VMSetting.RECORD_XMAX_LENGTH);
        }
        finally {
            this.dataItem.afterModify(xid);
        }
    }

    public void releaseOneReference(){
        this.dataItem.releaseOneReference();
    }

}
