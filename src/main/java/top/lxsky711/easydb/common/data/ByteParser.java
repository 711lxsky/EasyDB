package top.lxsky711.easydb.common.data;

import java.nio.ByteBuffer;

/**
 * @Author: 711lxsky
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

    public static int parseBytesToInt(byte[] bytes){
        ByteBuffer buffer = ByteBuffer.wrap(bytes, ParseSetting.DATA_START_POS, ParseSetting.INT_SIZE);
        return buffer.getInt();
    }

    public static byte[] intToBytes(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static short parseBytesToShort(byte[] bytes){
        ByteBuffer buffer = ByteBuffer.wrap(bytes, ParseSetting.DATA_START_POS, ParseSetting.SHORT_SIZE);
        return buffer.getShort();
    }

    public static byte[] shortToBytes(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }
}
