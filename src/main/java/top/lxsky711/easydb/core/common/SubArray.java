package top.lxsky711.easydb.core.common;

/**
 * @Author: 711lxsky
 * @Description: 实现某个数组或者数据块的共享
 * 使得这段数据中的一部分可以被单独拿出来， 差不多也就是数据分片
 */

public class SubArray {

    // 原始数据
    public byte[] rawData;

    // 数据开始位置标记
    public int start;

    // 数据结束位置标记
    public int end;

    public SubArray(byte[] rawData, int start, int end) {
        this.rawData = rawData;
        this.start = start;
        this.end = end;
    }
}
