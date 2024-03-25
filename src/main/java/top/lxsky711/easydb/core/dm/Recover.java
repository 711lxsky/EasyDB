package top.lxsky711.easydb.core.dm;

import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.common.log.InfoMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.core.dm.logger.Logger;
import top.lxsky711.easydb.core.dm.logger.LoggerSetting;
import top.lxsky711.easydb.core.dm.pageCache.PageCache;
import top.lxsky711.easydb.core.tm.TransactionManager;

import java.util.Arrays;
import java.util.Objects;

import static top.lxsky711.easydb.core.dm.logger.LoggerSetting.REDO;

/**
 * @Author: 711lxsky
 * @Description: 执行恢复类
 */

public class Recover {

    public static void recover(TransactionManager tm, Logger logger, PageCache pageCache){
        Log.logInfo(InfoMessage.RECOVER_START);
        logger.rewind();
        byte[] log;
        int maxPageNumber = 0;
        while(Objects.nonNull(log = logger.readNextLogData())){
             int curLogPageNumber = getLogPageNumber(log);
             maxPageNumber = Math.max(maxPageNumber, curLogPageNumber);
        }
        if(maxPageNumber == 0){
            maxPageNumber = 1;
        }
        pageCache.truncatePageWithMPageNum(maxPageNumber);
        Log.logInfo(InfoMessage.PAGE_TRUNCATE);


    }

    /**
     * @Author: 711lxsky
     * @Description: 重做所有已经完成的日志
     */
    private static void redoTransactions(TransactionManager tm, Logger logger, PageCache pc){
        logger.rewind();
        byte[] log;
        while(Objects.nonNull(log = logger.readNextLogData())){
            byte logType = getLogType(log);
            long logXid = getLogXID(log);
            if(! tm.isActive(logXid)){
                switch (logType){
                    case LoggerSetting.LOG_TYPE_INSERT:
                        doInsetLog(pc, log, REDO);
                        break;
                    case LoggerSetting.LOG_TYPE_UPDATE:
                        doUpdateLog(pc, log, REDO);
                        break;
                }
            }

        }

    }

    private static void doInsetLog(PageCache pc, byte[] log, int redoOrUndo){
    }

    private static byte getLogType(byte[] log){
        byte logType =  log[LoggerSetting.LOG_TYPE_OFFSET];

    }

    private static byte[] getLogXIDBytes(byte[] log){
        return Arrays.copyOfRange(log, LoggerSetting.LOG_XID_OFFSET, LoggerSetting.LOG_PAGE_NUMBER_OFFSET);
    }

    private static long getLogXID(byte[] log){
        return ByteParser.parseBytesToLong(getLogXIDBytes(log));
    }

    private static byte[] getLogPageNumberBytes(byte[] log){
        return Arrays.copyOfRange(log, LoggerSetting.LOG_PAGE_NUMBER_OFFSET, LoggerSetting.LOG_OFFSET_OFFSET);
    }

    private static int getLogPageNumber(byte[] log){
        return ByteParser.parseBytesToInt(getLogPageNumberBytes(log));
    }

    private static byte[] getLogOffsetBytes(byte[] log){
        return Arrays.copyOfRange(log, LoggerSetting.LOG_OFFSET_OFFSET, LoggerSetting.LOG_DATA_OFFSET);
    }

    private static short getLogOffset(byte[] log){
        return ByteParser.parseBytesToShort(getLogOffsetBytes(log));
    }

    private static byte[] getLogData(byte[] log){
        return Arrays.copyOfRange(log, LoggerSetting.LOG_DATA_OFFSET, log.length);
    }

    private static LoggerSetting.InsertLog parseLogBytesToInsertLog(byte[] log){
        LoggerSetting.InsertLog insertLog = new LoggerSetting.InsertLog();
        insertLog.type = LoggerSetting.LOG_TYPE_INSERT;
        insertLog.xid = getLogXID(log);
        insertLog.pageNumber = getLogPageNumber(log);
        insertLog.offset = getLogOffset(log);
        insertLog.data = getLogData(log);
        return insertLog;
    }

    private static LoggerSetting.UpdateLog parseLogBytesToUpdateLog(byte[] log){
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

}
