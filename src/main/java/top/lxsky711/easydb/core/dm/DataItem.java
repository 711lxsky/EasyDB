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
 * DataItem中保存的数据结构DataStruct格式：[Valid][DataSize][Data]
 * Valid: 1字节, 用于标记数据是否有效，1有效， 0无效
 * DataSize: 2字节， 标识Data的大小
 */

public interface DataItem {

    SubArray getData();

    SubArray getDataRecord();

    boolean isValid();

    void beforeModify();

    void unBeforeModify();

    void afterModify(long xid);

    void releaseOneReference();

    void readLock();

    void readUnlock();

    void writeLock();

    void writeUnlock();

    Page getPage();

    long getUid();

    byte[] getOldDataRecord();

    static byte[] buildDataRecord(byte[] data){
        byte[] valid = new byte[]{DataItemSetting.DATA_VALID};
        byte[] size = ByteParser.shortToBytes((short)data.length);
        return Bytes.concat(valid, size, data);
    }

    static DataItem buildDataItem(Page page, short offset, DataManager dm){
        byte[] rawData = page.getPageData();
        byte[] dataItemDataSizeBytes = Arrays.copyOfRange(rawData, offset + DataItemSetting.DATA_SIZE_OFFSET, offset + DataItemSetting.DATA_DATA_OFFSET);
        short dataItemDataSize = ByteParser.parseBytesToShort(dataItemDataSizeBytes);
        short dataItemLength = (short)(DataItemSetting.DATA_DATA_OFFSET + dataItemDataSize);
        long uid = Logger.parsePageNumberAndOffsetToUid(page.getPageNumber(), offset);
        SubArray dataRecord = new SubArray(rawData, offset, offset + dataItemLength);
        return new DataItemImpl(dataRecord, new byte[dataItemLength], page, uid, dm);
    }


    static void setDataRecordInvalid(byte[] dataRecord){
        dataRecord[DataItemSetting.DATA_VALID_OFFSET] = DataItemSetting.DATA_INVALID;
    }
}
