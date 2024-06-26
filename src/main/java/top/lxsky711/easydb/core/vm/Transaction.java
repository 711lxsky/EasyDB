package top.lxsky711.easydb.core.vm;

import top.lxsky711.easydb.core.tm.TMSetting;

import java.util.Map;
import java.util.Set;

/**
 * @Author: 711lxsky
 * @Description: VM模块对事务的抽象
 */

public class Transaction {

    /**
     * 抽象事务的XID
     */
    private long xid;

    /**
     * 事务隔离级别
     */
    private final int transactionIsolationLevel;

    /**
     * 当前事务执行(开始)时的活跃事务XID快照集合
     */
    private Set<Long> snapshotXIDsForActiveTransaction;

    /**
     * 意外终止标志，后续出现问题，这个成员变量会被设置为true，表示事务选择中止
     */
    private boolean accidentalTermination;

    public Transaction(long xid, int transactionIsolationLevel) {
        this.xid = xid;
        this.transactionIsolationLevel = transactionIsolationLevel;
        this.accidentalTermination = false;
    }

    /**
     * @Author: 711lxsky
     * @Description: 构建一个抽象事务
     */
    public static Transaction buildTransaction(long xid, int transactionIsolationLevel, Map<Long, Transaction> activeTransactions){
        Transaction transaction = new Transaction(xid, transactionIsolationLevel);
        if(transactionIsolationLevel != VMSetting.TRANSACTION_ISOLATION_LEVEL_READ_COMMITTED){
            transaction.snapshotXIDsForActiveTransaction = activeTransactions.keySet();
        }
        return transaction;
    }

    public static Transaction buildSuperTransaction(){
        return new Transaction(TMSetting.SUPER_TRANSACTION_XID, VMSetting.TRANSACTION_ISOLATION_LEVEL_READ_COMMITTED);
    }

    /**
     * @Author: 711lxsky
     * @Description: 判断某个事务是否在当前快照中
     */
    public boolean isInSnapshot(long xid){
        if(xid == TMSetting.SUPER_TRANSACTION_XID){
            return false;
        }
        return snapshotXIDsForActiveTransaction.contains(xid);
    }

    public long getXid() {
        return xid;
    }

    public void setXid(long xid) {
        this.xid = xid;
    }

    public int getTransactionIsolationLevel() {
        return transactionIsolationLevel;
    }

    public boolean isAccidentalTermination() {
        return accidentalTermination;
    }

    public void setAccidentalTermination(boolean accidentalTermination) {
        this.accidentalTermination = accidentalTermination;
    }
}
