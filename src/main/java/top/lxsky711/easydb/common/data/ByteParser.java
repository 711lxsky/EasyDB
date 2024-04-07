package top.lxsky711.easydb.common.data;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @Author: 711lxsky
 */

public class ByteParser {

    public static long parseBytesToLong(byte[] bytes){
        ByteBuffer buffer = ByteBuffer.wrap(bytes, DataSetting.DATA_START_POS_DEFAULT, DataSetting.LONG_BYTE_SIZE);
        return buffer.getLong();
    }

    public static byte[] longToBytes(long value) {
        // allocate的容量是以字节为基本单位，所以需要申请数据量对应的long类型所需字节数
        return ByteBuffer.allocate(DataSetting.LONG_BYTE_SIZE).putLong(value).array();
    }

    public static int parseBytesToInt(byte[] bytes){
        ByteBuffer buffer = ByteBuffer.wrap(bytes, DataSetting.DATA_START_POS_DEFAULT, DataSetting.INT_BYTE_SIZE);
        return buffer.getInt();
    }

    public static byte[] intToBytes(int value) {
        return ByteBuffer.allocate(DataSetting.INT_BYTE_SIZE).putInt(value).array();
    }

    public static short parseBytesToShort(byte[] bytes){
        ByteBuffer buffer = ByteBuffer.wrap(bytes, DataSetting.DATA_START_POS_DEFAULT, DataSetting.SHORT_BYTE_SIZE);
        return buffer.getShort();
    }

    public static byte[] shortToBytes(short value) {
        return ByteBuffer.allocate(DataSetting.SHORT_BYTE_SIZE).putShort(value).array();
    }

    public static byte[] stringToBytes(String str){
        byte[] strLengthBytes = intToBytes(str.length());
        return Bytes.concat(strLengthBytes, str.getBytes());
    }

    public static DataSetting.StringBytes parseBytesToString(byte[] bytes) {
        DataSetting.StringBytes stringBytes = new DataSetting.StringBytes();
        stringBytes.strLength = parseBytesToInt(Arrays.copyOf(bytes, stringBytes.strLengthSize));
        stringBytes.str = new String(Arrays.copyOfRange(bytes, stringBytes.strLengthSize, stringBytes.strLengthSize + stringBytes.strLength));
        return stringBytes;
    }
}
