package top.lxsky711.easydb.core.dm.logger;

import top.lxsky711.easydb.common.file.FileManager;
import top.lxsky711.easydb.common.log.ErrorMessage;
import top.lxsky711.easydb.common.log.Log;

import java.io.File;
import java.io.RandomAccessFile;
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
    void writeLog(byte[] data);

    /**
     * @Author: 711lxsky
     * @Description: 定位指针到日志数据起始位置
     */
    void rewind();

    /**
     * @Author: 711lxsky
     * @Description: 读取下一条日志信息
     */
    byte [] readNextLogData();

    /**
     * @Author: 711lxsky
     * @Description: 截断日志文件到指定长度
     */
    void truncate(long length);

    /**
     * @Author: 711lxsky
     * @Description: 关闭日志
     */
    void close();

    static LoggerImpl create(String logFileFullName){
        File newFile = FileManager.createFile(logFileFullName + LoggerSetting.LOGGER_FILE_SUFFIX);
        return buildLoggerWithFile(newFile, false);
    }

    static LoggerImpl open(String logFileFullName){
        File newFile = FileManager.openFile(logFileFullName + LoggerSetting.LOGGER_FILE_SUFFIX);
        return buildLoggerWithFile(newFile, true);
    }

    static LoggerImpl buildLoggerWithFile(File file, boolean isOpen){
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


}
