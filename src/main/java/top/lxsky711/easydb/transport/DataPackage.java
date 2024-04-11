package top.lxsky711.easydb.transport;

/**
 * @Author: 711lxsky
 * @Description: 传输数据包实现类
 * 数据结构： [IsException(1byte)][Data
 * 如果IsException为1，则Data为异常信息， 否则为正常数据
 */

public class DataPackage {

    // 原始数据
    byte[] data;

    // 异常信息
    Exception exception;

    public DataPackage(byte[] data, Exception exception) {
        this.data = data;
        this.exception = exception;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getException() {
        return exception;
    }
}
