package top.lxsky711.easydb.core.vm;

import com.google.common.primitives.Bytes;
import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
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

    /**
     * 同DataItem， 这个标识是 页号 + 偏移量
     */
    private final long uid;

    /**
     * 一条记录存储在一个DataItem中
     */
    private final DataItem dataItem;

    public Record(long uid, DataItem dataItem) {
        this.uid = uid;
        this.dataItem = dataItem;
    }

    /**
     * @Author: 711lxsky
     * @Description: 构建记录
     */
    public static Record buildRecord(long uid, DataItem dataItem) {
        if(Objects.isNull(dataItem)){
            return null;
        }
        return new Record(uid, dataItem);
    }

    /**
     * @Author: 711lxsky
     * @Description: 包装原始数据为记录的字节数组形式
     */
    public static byte[] wrapDataToRecordBytes(long xid, byte[] data){
        byte[] xmin = ByteParser.longToBytes(xid);
        byte[] xmax = new byte[VMSetting.RECORD_XMAX_LENGTH];
        return Bytes.concat(xmin, xmax, data);
    }

    public long getUid() {
        return this.uid;
    }

    /**
     * @Author: 711lxsky
     * @Description: 获取记录的XMIN
     */
    public long getXMIN(){
        this.dataItem.readLock();
        try {
            SubArray dataRecord = this.dataItem.getDataRecord();
            return ByteParser.parseBytesToLong(Arrays.copyOfRange(dataRecord.rawData, dataRecord.start + VMSetting.RECORD_XMIN_OFFSET,
                    dataRecord.start + VMSetting.RECORD_XMAX_OFFSET));
        }
        finally {
            this.dataItem.readUnlock();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 获取记录XMAX
     */
    public long getXMAX(){
        this.dataItem.readLock();
        try {
            SubArray dataRecord = this.dataItem.getDataRecord();
            return ByteParser.parseBytesToLong(Arrays.copyOfRange(dataRecord.rawData, dataRecord.start + VMSetting.RECORD_XMAX_OFFSET,
                    dataRecord.start + VMSetting.RECORD_DATA_OFFSET));
        }
        finally {
            this.dataItem.readUnlock();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 获取记录原始数据
     */
    public byte[] getData(){
        this.dataItem.readLock();
        try {
            SubArray dataRecord = this.dataItem.getDataRecord();
            byte[] justData = new byte[dataRecord.end - dataRecord.start - VMSetting.RECORD_DATA_OFFSET];
            System.arraycopy(dataRecord.rawData, dataRecord.start + VMSetting.RECORD_DATA_OFFSET,
                    justData, 0, justData.length);
            return justData;
        }
        finally {
            this.dataItem.readUnlock();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 设置记录XMAX
     */
    public void setXMAX(long xid) throws WarningException, ErrorException {
        this.dataItem.beforeModify();
        try {
            SubArray dataRecord = this.dataItem.getDataRecord();
            System.arraycopy(ByteParser.longToBytes(xid), 0,
                    dataRecord.rawData, dataRecord.start + VMSetting.RECORD_XMAX_OFFSET, VMSetting.RECORD_XMAX_LENGTH);
        }
        finally {
            this.dataItem.afterModify(xid);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 释放记录引用，实际上就是释放一个DataItem引用
     */
    public void releaseOneReference() throws WarningException, ErrorException {
        this.dataItem.releaseOneReference();
    }

}
