package top.lxsky711.easydb.core.dm;

import com.google.common.primitives.Bytes;
import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.core.common.SubArray;
import top.lxsky711.easydb.core.dm.logger.Logger;
import top.lxsky711.easydb.core.dm.page.Page;

import java.util.Arrays;

/**
 * @Author: 711lxsky
 * @Description: DM层向上提供的数据抽象接口，上层模块通过地址，向DM请求到相应的DataItem,再获取数据
 * DataItem中保存的数据结构DataRecord格式：[Valid][DataSize][Data]
 * Valid: 1字节, 用于标记数据是否有效，1有效， 0无效
 * DataSize: 2字节， 标识Data的大小
 */

public interface DataItem {

    /**
     * @Author: 711lxsky
     * @Description: 获取DataRecord数据记录，非完整原始数据
     */
    SubArray getDataRecord();

    /**
     * @Author: 711lxsky
     * @Description: 获取完整原始数据记录
     */
    SubArray getRawDataRecord();

    /**
     * @Author: 711lxsky
     * @Description: 检查数据是否有效
     */
    boolean isValid();

    /**
     * @Author: 711lxsky
     * @Description: 在修改数据之前进行的操作
     */
    void beforeModify();

    /**
     * @Author: 711lxsky
     * @Description: 撤销修改操作
     */
    void unBeforeModify();

    /**
     * @Author: 711lxsky
     * @Description: 数据修改之后的操作
     */
    void afterModify(long xid);

    /**
     * @Author: 711lxsky
     * @Description: 释放一个引用
     */
    void releaseOneReference();

    /**
     * @Author: 711lxsky
     * @Description: 读锁申请
     */
    void readLock();

    /**
     * @Author: 711lxsky
     * @Description: 读锁释放
     */
    void readUnlock();

    /**
     * @Author: 711lxsky
     * @Description: 写锁申请
     */
    void writeLock();

    /**
     * @Author: 711lxsky
     * @Description: 写锁释放
     */
    void writeUnlock();

    /**
     * @Author: 711lxsky
     * @Description: 拿到数据页
     */
    Page getPage();

    /**
     * @Author: 711lxsky
     * @Description: 获取DataItem的唯一标识
     */
    long getUid();

    /**
     * @Author: 711lxsky
     * @Description: 获取原始数据记录
     */
    byte[] getOldDataRecord();

    /**
     * @Author: 711lxsky
     * @Description: 包裹Data构建DataRecord
     */
    static byte[] buildDataRecord(byte[] data){
        byte[] valid = new byte[]{DataItemSetting.DATA_VALID};
        byte[] size = ByteParser.shortToBytes((short)data.length);
        return Bytes.concat(valid, size, data);
    }

    /**
     * @Author: 711lxsky
     * @Description: 构建DataItem
     */
    static DataItem buildDataItem(Page page, short offset, DataManager dm){
        byte[] rawData = page.getPageData();
        // 注意这里是获取DataARecord中的DataSize
        byte[] dataItemDataSizeBytes = Arrays.copyOfRange(rawData, offset + DataItemSetting.DATA_SIZE_OFFSET, offset + DataItemSetting.DATA_DATA_OFFSET);
        short dataItemDataSize = ByteParser.parseBytesToShort(dataItemDataSizeBytes);
        // 得到整个DataRecord的大小
        short dataRecordLength = (short)(DataItemSetting.DATA_DATA_OFFSET + dataItemDataSize);
        long uid = Logger.parsePageNumberAndOffsetToUid(page.getPageNumber(), offset);
        // 转换成SubArray的形式进行构建DataItem
        SubArray dataRecord = new SubArray(rawData, offset, offset + dataRecordLength);
        return new DataItemImpl(dataRecord, new byte[dataRecordLength], page, uid, dm);
    }

    /**
     * @Author: 711lxsky
     * @Description: 设置DataRecord为无效
     */
    static void setDataRecordInvalid(byte[] dataRecord){
        dataRecord[DataItemSetting.DATA_VALID_OFFSET] = DataItemSetting.DATA_INVALID;
    }
}
