package top.lxsky711.easydb.core.vm;

import top.lxsky711.easydb.common.data.CollectionUtil;
import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.log.ErrorMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: 711lxsky
 * @Description: 版本控制锁/事务并发管理器
 */

public class VersionLockManager {

    /**
     * 某个XID事务已经控制的Record记录列表， 一个XID事务可以控制多个Record记录
     */
    private final Map<Long, List<Long>> transactionControlledRecords;

    /**
     * 某个Record记录被哪个XID事务持有，一个Record记录只能被一个XID事务持有
     */
    private final Map<Long, Long> recordControlledByTransaction;

    /**
     * 等待获取某个Record记录的XID事务列表，一个Record记录可以被多个XID事务等待
     */
    private final Map<Long, List<Long>> recordWaitByTransactions;

    /**
     * 某个XID事务，在等待获取目标Record记录， 一个XID事务只能等待一个Record记录
     */
    private final Map<Long, Long> transactionWaitForRecord;

    /**
     * 正在等待资源的XID事务，携带锁
     */
    private final Map<Long, Lock> transactionWaitWithLock;

    /**
     * 内部进程资源锁
     */
    private final Lock selfLock;

    public VersionLockManager() {
        this.transactionControlledRecords = new HashMap<>();
        this.recordControlledByTransaction = new HashMap<>();
        this.recordWaitByTransactions = new HashMap<>();
        this.transactionWaitForRecord = new HashMap<>();
        this.transactionWaitWithLock = new HashMap<>();
        this.selfLock = new ReentrantLock();
    }

    /**
     * @Author: 711lxsky
     * @Description: 事务尝试获取记录资源
     * 如果可以直接成功拿到，就返回null
     * 否则需要等待，返回锁
     */
    public Lock tryToAcquireResourseLock(long xid, long uid) throws WarningException, ErrorException {
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
            if(detectDeadlock()){
                // 这里先注释掉，因为死锁检测到之后，会尝试撤销某个事务，所以就先不删掉记录
//                this.transactionWaitForRecord.remove(xid);
//                CollectionUtil.removeElementFromListMap(this.recordWaitByTransactions, uid, xid);
                Log.logWarningMessage(WarningMessage.VERSION_CONTROL_DEAD_LOCK_OCCUR);
            }
            // 放入等待队列
            Lock waitLock = new ReentrantLock();
            waitLock.lock();
            this.transactionWaitWithLock.put(xid, waitLock);
            return waitLock;
        }
        finally {
            this.selfLock.unlock();
        }
    }


    // 死锁检测事务戳
    private Map<Long, Integer> transactionStamp;

    // 事务戳标记
    private int stampMark;

    /**
     * @Author: 711lxsky
     * @Description: 死锁检测
     */
    private boolean detectDeadlock() throws ErrorException {
        this.transactionStamp = new HashMap<>();
        this.stampMark = VMSetting.VERSION_LOCK_DEADLOCK_DETECT_RING_STAMP_DEFAULT;
        // 从已经获取资源的事务中寻找
        for(long transactionXid : this.transactionControlledRecords.keySet()){
            // 当前事务戳
            Integer xidStamp = this.transactionStamp.get(transactionXid);
            // 该事务已经被标记过，跳过检测
            if(Objects.nonNull(xidStamp) && xidStamp > VMSetting.VERSION_LOCK_DEADLOCK_DETECT_RING_STAMP_DEFAULT){
                continue;
            }
            // 自增，dfs测环
            this.stampMark ++;
            if(this.deepFirstSearchForDeadLock(transactionXid)){
                return true;
            }
        }
        return false;
    }

    /**
     * @Author: 711lxsky
     * @Description: 深度优先搜索检测死锁
     */
    private boolean deepFirstSearchForDeadLock(long searchXid) throws ErrorException {
        Integer searchXidStamp = this.transactionStamp.get(searchXid);
        // 存在环，有死锁
        if(Objects.nonNull(searchXidStamp) && searchXidStamp == this.stampMark){
            return true;
        }
        // 已经被检查，且不在当前检查路径中
        if(Objects.nonNull(searchXidStamp) && searchXidStamp < this.stampMark){
            return false;
        }
        // 标记事务戳
        this.transactionStamp.put(searchXid, this.stampMark);
        Long waitingRecordUid = this.transactionWaitForRecord.get(searchXid);
        // 看当前事务有没有正在等待的Record记录
        if(Objects.isNull(waitingRecordUid)){
            return false;
        }
        // 向上定位资源(相当于有向图的一条边指向后继节点)
        Long tarRecordControlledTransactionXid = recordControlledByTransaction.get(waitingRecordUid);
        if(Objects.isNull(tarRecordControlledTransactionXid)){
            Log.logErrorMessage(ErrorMessage.VERSION_CONTROL_RESOURCE_ERROR);
        }
        // 看获取目标记录的事务是否会造成死锁
        return this.deepFirstSearchForDeadLock(tarRecordControlledTransactionXid);
    }

    /**
     * @Author: 711lxsky
     * @Description: 移除某个事务
     */
    public void removeOneTransaction(long xid){
        this.selfLock.lock();
        try{
            // 拿到需要清除的事务持有的Record记录列表
            List<Long> controlledRecords = this.transactionControlledRecords.get(xid);
            if(Objects.nonNull(controlledRecords)){
                while (! controlledRecords.isEmpty()){
                    // 对每个Record记录，选择一个等待的事务来控制
                    Long uid = controlledRecords.remove(0);
                    this.selectOneWaitingTransactionToControlRecord(uid);
                }
            }
            this.transactionControlledRecords.remove(xid);
            this.transactionWaitForRecord.remove(xid);
            this.transactionWaitWithLock.remove(xid);
        }
        finally {
            this.selfLock.unlock();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 选择某个等待的事务来持有记录
     */
    public void selectOneWaitingTransactionToControlRecord(long uid){
        // 将原本被事务持有的记录删除
        this.recordControlledByTransaction.remove(uid);
        // 拿到正在等待这个记录的事务
        List<Long> waitingTransactions = this.recordWaitByTransactions.get(uid);
        if(Objects.nonNull(waitingTransactions)){
            while(! waitingTransactions.isEmpty()){
                // 按等待顺序选择事务来控制记录
                long waitingTransactionXid = waitingTransactions.remove(0);
                // 同时这个等待事务曾经申请持有记录成功
                if(this.transactionWaitWithLock.containsKey(waitingTransactionXid)){
                    this.recordControlledByTransaction.put(uid, waitingTransactionXid);
                    // 释放等待锁
                    Lock waitingLock = this.transactionWaitWithLock.get(waitingTransactionXid);
                    waitingLock.unlock();
                    this.transactionWaitWithLock.remove(waitingTransactionXid);
                    break;
                }
            }
            if(waitingTransactions.isEmpty()){
                this.recordWaitByTransactions.remove(uid);
            }
        }
    }
}
