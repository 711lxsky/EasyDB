package top.lxsky711.easydb.core.vm;

import top.lxsky711.easydb.common.data.DataSetting;
import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.log.InfoMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;
import top.lxsky711.easydb.core.common.AbstractCache;
import top.lxsky711.easydb.core.dm.DataItem;
import top.lxsky711.easydb.core.dm.DataManager;
import top.lxsky711.easydb.core.tm.TransactionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: 711lxsky
 * @Description: 版本管理器实现类
 */

public class VersionManagerImpl extends AbstractCache<Record> implements VersionManager{

    private final TransactionManager tm;

    private final DataManager dm;

    /**
     * 当前活跃的事务快照
     */
    private final Map<Long, Transaction> activeTransactions;

    private final VersionLockManager vlm;

    private final Lock selfLock;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) throws ErrorException {
        super(DataSetting.DATA_CACHE_DEFAULT_SIZE);
        this.tm = tm;
        this.dm = dm;
        this.activeTransactions = new HashMap<>();
        Transaction superTransaction = Transaction.buildSuperTransaction();
        this.activeTransactions.put(superTransaction.getXid(), superTransaction);
        this.vlm = new VersionLockManager();
        this.selfLock = new ReentrantLock();
    }

    @Override
    protected Record getCacheFromDataSourceByKey(long uid) throws WarningException, ErrorException {
        DataItem dataItem = this.dm.readDataItem(uid);
        return Record.buildRecord(uid, dataItem);
    }

    @Override
    protected void releaseCacheForObject(Record record) throws WarningException, ErrorException {
        record.releaseOneReference();
    }

    @Override
    public long begin(int transactionIsolationLevel) throws WarningException, ErrorException {
        this.selfLock.lock();
        try {
            long xid = this.tm.begin();
            Transaction newTransaction = Transaction.buildTransaction(xid, transactionIsolationLevel, this.activeTransactions);
            this.activeTransactions.put(xid, newTransaction);
            return xid;
        }
        finally {
            this.selfLock.unlock();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 检查事务是否已经意外终止
     */
    private Transaction checkTransactionAborted(long xid) throws WarningException {
        this.selfLock.lock();
        Transaction tarTransaction = this.activeTransactions.get(xid);
        this.selfLock.unlock();
        if(tarTransaction.isAccidentalTermination()){
            Log.logWarningMessage(WarningMessage.TRANSACTION_IS_ABORTED);
            return null;
        }
        return tarTransaction;
    }

    @Override
    public byte[] read(long xid, long uid) throws WarningException, ErrorException {
        Transaction tarTransaction = this.checkTransactionAborted(xid);
        if(Objects.isNull(tarTransaction)){
            return null;
        }
        Record tarRecord = super.getResource(uid);
        try {
            if(VisibilityJudge.judgeVisibility(this.tm, tarTransaction, tarRecord)){
                return tarRecord.getData();
            }
            else {
                return null;
            }
        }
        finally {
            super.releaseOneReference(tarRecord.getUid());
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws WarningException, ErrorException {
        Transaction tarTransaction = this.checkTransactionAborted(xid);
        if(Objects.isNull(tarTransaction)){
            return VMSetting.TRANSACTION_XID_ERROR_DEFAULT;
        }
        byte[] recordBytes = Record.wrapDataToRecordBytes(xid, data);
        return this.dm.insertData(xid, recordBytes);
    }

    @Override
    public boolean delete(long xid, long uid) throws WarningException, ErrorException {
        Transaction tarTransaction = this.checkTransactionAborted(xid);
        if(Objects.isNull(tarTransaction)){
            return false;
        }
        Record tarRecord = super.getResource(uid);
        try {
            if(! VisibilityJudge.judgeVisibility(this.tm, tarTransaction, tarRecord)){
                return false;
            }
            Lock transactionGetRecordLock = this.vlm.tryToAcquireResourseLock(xid, uid);
            if(Objects.nonNull(transactionGetRecordLock)){
                transactionGetRecordLock.lock();
                transactionGetRecordLock.unlock();
            }
            // 无法重复删除
            if(tarRecord.getXMAX() == xid){
                return false;
            }
            if(VisibilityJudge.judgeVersionHopping(this.tm, tarTransaction, tarRecord)){
                Log.logWarningMessage(WarningMessage.VERSION_HOPPING_OCCUR);
                tarTransaction.setAccidentalTermination(true);
                this.internAbortTransaction(xid);
                return false;
            }
            tarRecord.setXMAX(xid);
            return true;
        }
        catch (Exception e){
            tarTransaction.setAccidentalTermination(true);
            this.internAbortTransaction(xid);
            return false;
        }
        finally {
            super.releaseOneReference(tarRecord.getUid());
        }
    }

    @Override
    public void commit(long xid) throws WarningException, ErrorException {
        Transaction tarTransaction = this.checkTransactionAborted(xid);
        if(Objects.nonNull(tarTransaction)){
            this.selfLock.lock();
            this.activeTransactions.remove(xid);
            this.selfLock.unlock();
            this.vlm.removeOneTransaction(xid);
            this.tm.commit(xid);
        }
    }

    @Override
    public void abort(long xid) throws WarningException, ErrorException {
        this.internAbortTransaction(xid);
    }

    /**
     * @Author: 711lxsky
     * @Description: 内部事务撤销方法
     */
    private void internAbortTransaction(long xid) throws WarningException, ErrorException {
        Log.logInfo(InfoMessage.TRYING_TO_REVOKE_TRANSACTION);
        Transaction tarTransaction = this.checkTransactionAborted(xid);
        if(Objects.nonNull(tarTransaction)){
//            if(! tarTransaction.isAccidentalTermination()){
                this.activeTransactions.remove(xid);
                this.vlm.removeOneTransaction(xid);
                this.tm.abort(xid);
//            }
        }
        Log.logInfo(InfoMessage.REVOKE_TRANSACTION_DONE);
    }

}
