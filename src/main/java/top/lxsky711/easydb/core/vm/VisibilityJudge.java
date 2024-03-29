package top.lxsky711.easydb.core.vm;

import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;
import top.lxsky711.easydb.core.tm.TransactionManager;

/**
 * @Author: 711lxsky
 * @Description: 记录相对事务可见性的判断实现类
 */

public class VisibilityJudge {

    /**
     * @Author: 711lxsky
     * @Description: 判断记录是否出现版本跳跃
     */
    public static boolean judgeVersionHopping(TransactionManager tm, Transaction transaction, Record record){
        if(transaction.getTransactionIsolationLevel() == VMSetting.TRANSACTION_ISOLATION_LEVEL_READ_COMMITTED){
            return false;
        }
        long recordXmax = record.getXMAX();
        return tm.isCommitted(recordXmax) && (recordXmax > transaction.getXid() || transaction.isInSnapshot(recordXmax));
    }

    /**
     * @Author: 711lxsky
     * @Description: 针对特定事务隔离级别，判断记录对事务是否可见
     */
    public static boolean judgeVisibility(TransactionManager tm, Transaction transaction, Record record){
        long transactionXid = transaction.getXid();
        long recordXmin = record.getXMIN();
        long recordXmax = record.getXMAX();
        // 记录由当前事务创建且未被删除，可见
        if(transactionXid == recordXmin && recordXmax == VMSetting.RECORD_XMAX_DEFAULT) {
            return true;
        }
        switch (transaction.getTransactionIsolationLevel()){
            case VMSetting.TRANSACTION_ISOLATION_LEVEL_READ_COMMITTED:
                return judgeForReadCommitted(tm, transactionXid, recordXmin, recordXmax);
            case VMSetting.TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ:
                return judgeForRepeatableRead(tm, transaction, recordXmin, recordXmax);
            default:
                Log.logWarningMessage(WarningMessage.TRANSACTION_ISOLATION_LEVEL_UNKNOWN);
                return false;
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 读提交级别判断
     */
    private static boolean judgeForReadCommitted(TransactionManager tm, long transactionXid, long recordXmin, long recordXmax){
        // 如果记录由某个已经提交的事务创建
        if(tm.isCommitted(recordXmax)){
            // 如果还未被删除，则可见
            if(recordXmax == VMSetting.RECORD_XMAX_DEFAULT){
                return true;
            }
            // 如果记录已被删除，则判断删除事务是否已经提交
            // 未提交则可见， 已提交则不可见
            if(recordXmax != recordXmin){
                return ! tm.isCommitted(recordXmax);
            }
        }
        // 其他情况不可见
        return false;
    }

    /**
     * @Author: 711lxsky
     * @Description: 重复读级别判断
     */
    private static boolean judgeForRepeatableRead(TransactionManager tm, Transaction transaction, long recordXmin, long recordXmax){
        long transactionXid = transaction.getXid();
        // 如果记录由某个已经提交的事务创建，且该事务在当前事务执行之前提交
        if(tm.isCommitted(recordXmin) && (recordXmin < transactionXid && !transaction.isInSnapshot(recordXmin))){
            // 未被删除，可见
            if(recordXmax == VMSetting.RECORD_XMAX_DEFAULT){
                return true;
            }
            // 被删除，则判断删除事务是否已经提交，或者在当前事务执行之后执行/提交
            if(recordXmax != recordXmin){
                return ! tm.isCommitted(recordXmax) || recordXmax > transactionXid || transaction.isInSnapshot(recordXmax);
            }
        }
        // 其他情况不可见
        return false;
    }

}
