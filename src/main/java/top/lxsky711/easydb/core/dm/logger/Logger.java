package top.lxsky711.easydb.core.dm.logger;

import com.google.common.primitives.Bytes;
import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.file.FileManager;
import top.lxsky711.easydb.common.log.ErrorMessage;
import top.lxsky711.easydb.common.log.Log;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Objects;

/**
 * @Author: 711lxsky
 * @Description: 每次对底层数据操作时，都会记录一条日志在磁盘上
 * 以供数据库崩溃之后，恢复数据使用
 * <p>
 * 日志文件记录格式：
 * [LogsChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * LogsChecksum 为后续所有日志计算的Checksum，4字节int类型
 * Log1...LogN是常规日志数据
 * BadTail 是在数据库崩溃时，没有来得及写完的日志数据，这个 BadTail 不一定存在
 * </p>
 * <p>
 * 单条日志记录格式：
 * [Size][Checksum][Data]
 * Size标记Data字段的字节数, 4字节int类型
 * Checksum是该条数据的校验和， 4字节int类型
 * Data是实际的数据
 * </P>
 */

public interface Logger {

    /**
     * @Author: 711lxsky
     * @Description: 写入日志
     */
    void writeLog(byte[] data) throws WarningException, ErrorException;

    /**
     * @Author: 711lxsky
     * @Description: 定位指针到日志数据起始位置
     */
    void rewind();

    /**
     * @Author: 711lxsky
     * @Description: 读取下一条日志信息
     */
    byte [] readNextLogData() throws ErrorException, WarningException;

    /**
     * @Author: 711lxsky
     * @Description: 截断日志文件到指定长度
     */
    void truncate(long length) throws WarningException;

    /**
     * @Author: 711lxsky
     * @Description: 关闭日志
     */
    void close() throws ErrorException;

    static LoggerImpl create(String logFileFullName) throws WarningException, ErrorException {
        File newFile = FileManager.createFile(logFileFullName + LoggerSetting.LOGGER_FILE_SUFFIX);
        return buildLoggerWithFile(newFile, false);
    }

    static LoggerImpl open(String logFileFullName) throws WarningException, ErrorException {
        File newFile = FileManager.openFile(logFileFullName + LoggerSetting.LOGGER_FILE_SUFFIX);
        return buildLoggerWithFile(newFile, true);
    }

    static LoggerImpl buildLoggerWithFile(File file, boolean isOpen) throws WarningException, ErrorException {
        if(Objects.nonNull(file)){
            RandomAccessFile logFile = FileManager.buildRAFile(file);
            if(Objects.nonNull(logFile)){
                if(isOpen){
                    LoggerImpl logger = new LoggerImpl(logFile);
                    logger.initOpen();
                    return logger;
                }
                LoggerImpl logger = new LoggerImpl(logFile, 0);
                logger.initCreate();
                return logger;
            }
            Log.logErrorMessage(ErrorMessage.BAD_LOG_FILE);
            return null;
        }
        Log.logErrorMessage(ErrorMessage.BAD_FILE);
        return null;
    }

    // 这里的参数 log 实际上一条日志数据的纯数据部分，下面的也是
    static byte getLogType(byte[] log) throws ErrorException {
        byte logType =  log[LoggerSetting.LOG_TYPE_OFFSET];
        if(logType != LoggerSetting.LOG_TYPE_INSERT && logType != LoggerSetting.LOG_TYPE_UPDATE){
            Log.logErrorMessage(ErrorMessage.LOG_TYPE_ERROR);
        }
        return logType;
    }

    static byte[] getLogXIDBytes(byte[] log){
        return Arrays.copyOfRange(log, LoggerSetting.LOG_XID_OFFSET, LoggerSetting.LOG_PAGE_NUMBER_OFFSET);
    }

    static long getLogXID(byte[] log) throws ErrorException {
        long logXid =  ByteParser.parseBytesToLong(getLogXIDBytes(log));
        if(logXid <= 0){
            Log.logErrorMessage(ErrorMessage.BAD_XID);
        }
        return logXid;
    }

    static byte[] getLogPageNumberBytes(byte[] log){
        return Arrays.copyOfRange(log, LoggerSetting.LOG_PAGE_NUMBER_OFFSET, LoggerSetting.LOG_OFFSET_OFFSET);
    }

    static int getLogPageNumber(byte[] log) throws ErrorException {
        int logPageNumber = ByteParser.parseBytesToInt(getLogPageNumberBytes(log));
        if(logPageNumber <= 0){
            Log.logErrorMessage(ErrorMessage.BAD_PAGE_NUMBER);
        }
        return logPageNumber;
    }

    static byte[] getLogOffsetBytes(byte[] log){
        return Arrays.copyOfRange(log, LoggerSetting.LOG_OFFSET_OFFSET, LoggerSetting.LOG_DATA_OFFSET);
    }

    static short getLogOffset(byte[] log) throws ErrorException {
        short logOffset = ByteParser.parseBytesToShort(getLogOffsetBytes(log));
        if(logOffset < 0){
            Log.logErrorMessage(ErrorMessage.BAD_OFFSET);
        }
        return logOffset;
    }

    static byte[] getLogData(byte[] log){
        return Arrays.copyOfRange(log, LoggerSetting.LOG_DATA_OFFSET, log.length);
    }

    static byte[] buildLogBytes(byte logType, long xid, int pageNumber, short offset, byte[] data){
        byte[] typeBytes = new byte[]{logType};
        byte[] xidBytes = ByteParser.longToBytes(xid);
        byte[] pageNumberBytes = ByteParser.intToBytes(pageNumber);
        byte[] offsetBytes = ByteParser.shortToBytes(offset);
        return Bytes.concat(typeBytes, xidBytes, pageNumberBytes, offsetBytes, data);
    }

    static LoggerSetting.InsertLog parseLogBytesToInsertLog(byte[] log) throws ErrorException {
        LoggerSetting.InsertLog insertLog = new LoggerSetting.InsertLog();
        insertLog.type = LoggerSetting.LOG_TYPE_INSERT;
        insertLog.pageNumber = getLogPageNumber(log);
        insertLog.offset = getLogOffset(log);
        insertLog.data = getLogData(log);
        return insertLog;
    }

    static LoggerSetting.UpdateLog parseLogBytesToUpdateLog(byte[] log) throws ErrorException {
        LoggerSetting.UpdateLog updateLog = new LoggerSetting.UpdateLog();
        updateLog.type = LoggerSetting.LOG_TYPE_UPDATE;
        updateLog.xid = getLogXID(log);
        updateLog.pageNumber = getLogPageNumber(log);
        updateLog.offset = getLogOffset(log);
        byte[] logData = getLogData(log);
        int dataLength = logData.length;
        updateLog.oldData = Arrays.copyOfRange(logData, 0, dataLength / 2);
        updateLog.newData = Arrays.copyOfRange(logData, dataLength / 2, dataLength);
        return updateLog;
    }

    static long parsePageNumberAndOffsetToUid(int pageNumber, short offset){
        return (((long)pageNumber) << Integer.SIZE) | (long)offset ;
    }

    static int getPageNumberFromUid(long uid){
        return (int)(uid >> Integer.SIZE);
    }

    static short getOffsetFromUid(long uid){
        return (short)(uid & ((1L << Short.SIZE) - 1));
    }

}
