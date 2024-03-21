package top.lxsky711.easydb.common.data;

import java.nio.ByteBuffer;

/**
 * @Author: 711lxsky
 * @Date: 2024-03-21
 */

public class ByteParser {

    public static long parseBytesToLong(byte[] bytes){
        ByteBuffer buffer = ByteBuffer.wrap(bytes, ParseSetting.DATA_START_POS, ParseSetting.LONG_SIZE);
        return buffer.getLong();
    }

    public static byte[] longToBytes(long value) {
        // allocate的容量是以字节为基本单位，所以需要申请数据量对应的long类型所需字节数
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }
}
