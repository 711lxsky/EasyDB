package top.lxsky711.easydb.core.dm;

import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.log.InfoMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.core.dm.logger.Logger;
import top.lxsky711.easydb.core.dm.logger.LoggerSetting;
import top.lxsky711.easydb.core.dm.page.Page;
import top.lxsky711.easydb.core.dm.page.PageX;
import top.lxsky711.easydb.core.dm.pageCache.PageCache;
import top.lxsky711.easydb.core.tm.TransactionManager;

import java.util.*;

import static top.lxsky711.easydb.core.dm.logger.LoggerSetting.REDO;

/**
 * @Author: 711lxsky
 * @Description: 执行恢复类
 */

public class Recover {

    /**
     * @Author: 711lxsky
     * @Description: 恢复数据
     */
    public static void recover(TransactionManager tm, Logger logger, PageCache pageCache) throws WarningException, ErrorException {
        Log.logInfo(InfoMessage.RECOVER_START);
        logger.rewind();
        byte[] log;
        int maxPageNumber = 0;
        while(Objects.nonNull(log = logger.readNextLogData())){
             int curLogPageNumber = Logger.getLogPageNumber(log);
             maxPageNumber = Math.max(maxPageNumber, curLogPageNumber);
        }
        if(maxPageNumber == 0){
            maxPageNumber = 1;
        }
        // 先截断页面文件
        pageCache.truncatePageWithMPageNum(maxPageNumber);
        Log.logInfo(InfoMessage.PAGE_TRUNCATE);
        // 重做所有已经完成的日志
        Log.logInfo(InfoMessage.TRANSACTIONS_REDO_START);
        redoTransactions(tm, logger, pageCache);
        Log.logInfo(InfoMessage.TRANSACTIONS_REDO_OVER);
        // 撤销所有未完成的事务
        Log.logInfo(InfoMessage.TRANSACTIONS_UNDO_START);
        undoTransactions(tm, logger, pageCache);
        Log.logInfo(InfoMessage.TRANSACTIONS_UNDO_OVER);
        Log.logInfo(InfoMessage.RECOVER_OVER);
    }

    /**
     * @Author: 711lxsky
     * @Description: 重做所有已经完成的日志
     */
    private static void redoTransactions(TransactionManager tm, Logger logger, PageCache pc) throws WarningException, ErrorException {
        logger.rewind();
        byte[] log;
        while(Objects.nonNull(log = logger.readNextLogData())){
            byte logType = Logger.getLogType(log);
            long logXid = Logger.getLogXID(log);
            // 单条日志处理
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

    /**
     * @Author: 711lxsky
     * @Description: 撤销所有未完成的事务
     */
    private static void undoTransactions(TransactionManager tm, Logger logger, PageCache pc) throws WarningException, ErrorException {
        Map<Long, List<byte[]>> waitUndoLogs = new HashMap<>();
        logger.rewind();
        byte[] log;
        // 先将日志过滤一边， 找到所有需要撤销的日志
        while(Objects.nonNull(log = logger.readNextLogData())){
            long logXid = Logger.getLogXID(log);
            if(tm.isActive(logXid)){
                if(!waitUndoLogs.containsKey(logXid)){
                    waitUndoLogs.put(logXid, new ArrayList<>());
                }
                waitUndoLogs.get(logXid).add(log);
            }
        }
        for(Map.Entry<Long, List<byte[]>> undoLog : waitUndoLogs.entrySet()){
            // 单个xid对应的日志倒序进行undo
            List<byte[]> logs = undoLog.getValue();
            for(int i = logs.size() - 1; i >= 0; i --){
                byte logType = Logger.getLogType(logs.get(i));
                switch (logType){
                    case LoggerSetting.LOG_TYPE_INSERT:
                        doInsetLog(pc, logs.get(i), LoggerSetting.UNDO);
                        break;
                    case LoggerSetting.LOG_TYPE_UPDATE:
                        doUpdateLog(pc, logs.get(i), LoggerSetting.UNDO);
                        break;
                }
            }
            tm.abort(undoLog.getKey());
        }

    }

    /**
     * @Author: 711lxsky
     * @Description: 进行插入日志的恢复
     */
    private static void doInsetLog(PageCache pc, byte[] log, int redoOrUndo) throws ErrorException, WarningException {
        LoggerSetting.InsertLog insertLog = Logger.parseLogBytesToInsertLog(log);
        Page page = pc.getPageByPageNumber(insertLog.pageNumber);
        try {
            if(redoOrUndo == LoggerSetting.UNDO){
                DataItem.setDataRecordInvalid(insertLog.data);
            }
            PageX.recoverInsert(page, insertLog.data, insertLog.offset);
        }
        finally {
            page.releaseOneReference();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 进行更新日志的恢复
     */
    private static void doUpdateLog(PageCache pc, byte[] log, int redoOrUndo) throws WarningException, ErrorException {
        LoggerSetting.UpdateLog updateLog = Logger.parseLogBytesToUpdateLog(log);
        Page page = pc.getPageByPageNumber(updateLog.pageNumber);
        byte[] useData;
        if(redoOrUndo == REDO){
            useData = updateLog.newData;
        }
        else {
            useData = updateLog.oldData;
        }
        try {
            PageX.recoverUpdate(page, useData, updateLog.offset);
        }
        finally {
            page.releaseOneReference();
        }
    }



}
