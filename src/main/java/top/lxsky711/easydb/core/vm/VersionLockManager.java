package top.lxsky711.easydb.core.vm;

import top.lxsky711.easydb.common.data.CollectionUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: 711lxsky
 * @Description:
 */

public class VersionLockManager {

    // 某个XID事务已经控制的Record记录列表， 一个XID事务可以控制多个Record记录
    private Map<Long, List<Long>> transactionControlledRecords;

    // 某个Record记录被哪个XID事务持有，一个Record记录只能被一个XID事务持有
    private Map<Long, Long> recordControlledByTransaction;

    // 等待获取某个Record记录的XID事务列表，一个Record记录可以被多个XID事务等待
    private Map<Long, List<Long>> recordWaitByTransactions;


    // 某个XID事务，在等待获取目标Record记录， 一个XID事务只能等待一个Record记录
    private Map<Long, Long> transactionWaitForRecord;

    // 正在等待资源的XID事务，携带锁
    private Map<Long, Lock> transactionWaitWithLock;

    // 内部进程资源锁
    private Lock selfLock;

    public VersionLockManager() {
        this.transactionControlledRecords = new HashMap<>();
        this.recordControlledByTransaction = new HashMap<>();
        this.recordWaitByTransactions = new HashMap<>();
        this.transactionWaitForRecord = new HashMap<>();
        this.transactionWaitWithLock = new HashMap<>();
        this.selfLock = new ReentrantLock();
    }

    public Lock tryToAcquireResourseLock(long xid, long uid){
        this.selfLock.lock();
        try{
            // 先看XID事务是否已经持有了目标Record记录
            if(CollectionUtil.judgeElementInListMap(this.transactionControlledRecords, xid, uid)){
                return null;
            }
            // 如果目标Record记录没有被任何XID事务持有
            if(! this.recordControlledByTransaction.containsKey(uid)){
                this.recordControlledByTransaction.put(uid, xid);
                CollectionUtil.putElementIntoListMap(this.transactionControlledRecords, xid, uid);
                return null;
            }
            // 已经被其他XID事务持有
            this.transactionWaitForRecord.put(xid, uid);
            CollectionUtil.putElementIntoListMap(this.recordWaitByTransactions, uid, xid);

        }
        finally {
            this.selfLock.unlock();
        }
    }
}
