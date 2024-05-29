package top.lxsky711.easydb.core.dm.logger;

import com.google.common.primitives.Bytes;
import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.file.FileManager;
import top.lxsky711.easydb.common.log.ErrorMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: 711lxsky
 * @Description: 日志实现类
 */

public class LoggerImpl implements Logger{

    /**
     * 日志文件
     */
    private final RandomAccessFile logFile;

    /**
     * 日志文件通道
     */
    private final FileChannel logFileChannel;

    /**
     * 资源锁
     */
    private final Lock lock;

    /**
     * 日志文件位置指针
     */
    private long logFileLocationPointer;

    /**
     * 日志文件原始长度，读取日志的时候不去改动
     */
    private long logFileOriginLength;

    /**
     * 日志文件总校验和
     */
    private int logsChecksum;

    LoggerImpl(RandomAccessFile raf){
        this.logFile = raf;
        this.logFileChannel = raf.getChannel();
        this.lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, int logsChecksum){
        this.logFile = raf;
        this.logFileChannel = raf.getChannel();
        this.lock = new ReentrantLock();
        this.logsChecksum = logsChecksum;
    }

    /**
     * @Author: 711lxsky
     * @Description: 创建日志时初始化
     */
    protected void initCreate() throws WarningException, ErrorException {
        ByteBuffer logsCheckSumBuffer = ByteBuffer.allocate(LoggerSetting.LOGGER_HEADER_LENGTH);
        FileManager.writeByteDataIntoFileChannel(this.logFileChannel, LoggerSetting.LOGGER_HEADER_OFFSET, logsCheckSumBuffer);
        FileManager.forceRefreshFileChannel(this.logFileChannel, false);
    }

    /**
     * @Author: 711lxsky
     * @Description: 打开日志时初始化
     */
    protected void initOpen() throws ErrorException, WarningException {
        long fileSize = FileManager.getRAFileLength(this.logFile);
        // 先检查是否有日志头
        if(fileSize < LoggerSetting.LOGGER_HEADER_LENGTH){
            Log.logErrorMessage(ErrorMessage.BAD_LOG_FILE);
        }
        ByteBuffer logsCheckSumBuffer = ByteBuffer.allocate(LoggerSetting.LOGGER_HEADER_LENGTH);
        FileManager.readByteDataIntoFileChannel(this.logFileChannel, LoggerSetting.LOGGER_HEADER_OFFSET, logsCheckSumBuffer);
        // 读取日志校验和
        int logsCheckSum = ByteParser.parseBytesToInt(logsCheckSumBuffer.array());
        this.logFileOriginLength = fileSize;
        this.logsChecksum = logsCheckSum;
        // 检查日志并删除脏的尾部
        this.checkLogAndRemoveTail();
    }

    @Override
    public void writeLog(byte[] data) throws WarningException, ErrorException {
        byte[] log = this.wrapDataBytesToLog(data);
        ByteBuffer logBuffer = ByteBuffer.wrap(log);
        this.lock.lock();
        try {
            FileManager.writeByteDataIntoFileChannel(this.logFileChannel, FileManager.getFileChannelSize(this.logFileChannel), logBuffer);
        }
        finally {
            this.lock.unlock();
        }
        this.updateLogsChecksum(log);
    }

    @Override
    public void rewind() {
        this.logFileLocationPointer = LoggerSetting.LOGGER_LOG_OFFSET;
    }

    @Override
    public byte[] readNextLogData() throws ErrorException, WarningException {
        this.lock.lock();
        try {
            // 读取下一条完整日志
            byte[] nextLog = this.internalReadNextLog();
            if(Objects.nonNull(nextLog)){
                return this.getLogDataFromLog(nextLog);
            }
            return null;
        }
        finally {
            this.lock.unlock();
        }
    }

    @Override
    public void truncate(long length) throws WarningException {
        this.lock.lock();
        FileManager.truncateFileChannel(this.logFileChannel, length);
        this.lock.unlock();
    }

    @Override
    public void close() throws ErrorException {
        FileManager.closeFileAndChannel(this.logFileChannel, this.logFile);
    }

    /**
     * @Author: 711lxsky
     * @Description: 更新日志校验和
     */
    private void updateLogsChecksum(byte[] log) throws WarningException, ErrorException {
        this.logsChecksum = this.calculateChecksum(this.logsChecksum, log);
        byte[] logsChecksumBytes = ByteParser.intToBytes(this.logsChecksum);
        ByteBuffer logsChecksumBuffer = ByteBuffer.wrap(logsChecksumBytes);
        FileManager.writeByteDataIntoFileChannel(this.logFileChannel, LoggerSetting.LOGGER_HEADER_OFFSET, logsChecksumBuffer);
        FileManager.forceRefreshFileChannel(this.logFileChannel, false);
    }

    /**
     * @Author: 711lxsky
     * @Description: 检查日志并删除脏的尾部
     */
    private void checkLogAndRemoveTail() throws WarningException, ErrorException {
        this.rewind();
        int logsChecksum = 0;
        byte[] log;
        while(Objects.nonNull(log = this.internalReadNextLog())){
            logsChecksum  = this.calculateChecksum(logsChecksum, log);
        }
        // 这里检查有误我只是警告了一下
        if(logsChecksum != this.logsChecksum){
            Log.logWarningMessage(WarningMessage.LOG_CHECKSUM_ERROR);
        }
        this.truncate(this.logFileLocationPointer);
        FileManager.seekRandomAccessFile(this.logFile, this.logFileLocationPointer);
        rewind();
    }

    /**
     * @Author: 711lxsky
     * @Description: 根据种子计算校验和
     */
    private int calculateChecksum(int logChecksum, byte[] log){
        for(byte littleData : log){
            logChecksum = logChecksum * LoggerSetting.LOGGER_SEED + littleData;
        }
        return logChecksum;
    }

    /**
     * @Author: 711lxsky
     * @Description: 内部读取下一条完整日志的封装
     */
    private byte[] internalReadNextLog() throws ErrorException, WarningException {
        // 先看有没有下一条日志的大小记录
        if(this.logFileLocationPointer + LoggerSetting.LOGGER_LOG_DATA_OFFSET >= this.logFileOriginLength){
            return null;
        }
        ByteBuffer nextLogSizeBuffer = ByteBuffer.allocate(LoggerSetting.LOGGER_LOG_SIZE_LENGTH);
        FileManager.readByteDataIntoFileChannel(this.logFileChannel, this.logFileLocationPointer, nextLogSizeBuffer);
        int nextLogSize = ByteParser.parseBytesToInt(nextLogSizeBuffer.array());
        // 再在当前文件指针位置加上下一条完整日志长度，看有没有数据
        if(this.logFileLocationPointer + LoggerSetting.LOGGER_LOG_DATA_OFFSET + nextLogSize > this.logFileOriginLength){
            Log.logWarningMessage(WarningMessage.LOG_FILE_MAYBE_ERROR);
            return null;
        }
        ByteBuffer nextLogBuffer = ByteBuffer.allocate(LoggerSetting.LOGGER_LOG_DATA_OFFSET + nextLogSize);
        FileManager.readByteDataIntoFileChannel(this.logFileChannel, this.logFileLocationPointer, nextLogBuffer);
        // 拿到下一条完整日志
        byte[] nextLog = nextLogBuffer.array();
        byte[] nextLogChecksumBytes = this.getLogCheckSumBytesFromLog(nextLog);
        byte[] nextLogData = this.getLogDataFromLog(nextLog);
        int nextLogChecksumCalculated = calculateChecksum(0, nextLogData);
        int nextLogChecksumRead = ByteParser.parseBytesToInt(nextLogChecksumBytes);
        // 计算值和读取值比较
        if(nextLogChecksumCalculated != nextLogChecksumRead){
            Log.logWarningMessage(WarningMessage.LOG_CHECKSUM_ERROR);
            return null;
        }
        this.logFileLocationPointer += nextLog.length;
        return nextLog;
    }

    /**
     * @Author: 711lxsky
     * @Description: 将日志数据封装成完整数据返回
     */
    private byte[] wrapDataBytesToLog(byte[] logData){
        byte[] size = ByteParser.intToBytes(logData.length);
        byte[] checksum = ByteParser.intToBytes(this.calculateChecksum(0, logData));
        return Bytes.concat(size, checksum, logData);
    }

    /**
     * @Author: 711lxsky
     * @Description: 从日志中获取校验和字节形式的封装
     */
    private byte[] getLogCheckSumBytesFromLog(byte[] log){
        return Arrays.copyOfRange(log, LoggerSetting.LOGGER_LOG_CHECKSUM_OFFSET, LoggerSetting.LOGGER_LOG_DATA_OFFSET);
    }

    /**
     * @Author: 711lxsky
     * @Description: 从日志中获取数据字节形式的封装
     */
    private byte[] getLogDataFromLog(byte[] log){
        return Arrays.copyOfRange(log, LoggerSetting.LOGGER_LOG_DATA_OFFSET, log.length);
    }

}
