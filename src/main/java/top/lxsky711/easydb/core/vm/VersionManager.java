package top.lxsky711.easydb.core.vm;

import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.core.dm.DataManager;
import top.lxsky711.easydb.core.tm.TransactionManager;

/**
 * @Author: 711lxsky
 * @Description: 版本管理器
 */

public interface VersionManager {

    /**
     * @Author: 711lxsky
     * @Description: 事务开始
     */
    long begin(int transactionIsolationLevel) throws WarningException, ErrorException;

    /**
     * @Author: 711lxsky
     * @Description: 读取记录中的数据
     */
    byte[] read(long xid, long uid) throws WarningException, ErrorException;

    /**
     * @Author: 711lxsky
     * @Description: 插入记录数据
     */
    long insert(long xid, byte[] data) throws WarningException, ErrorException;

    /**
     * @Author: 711lxsky
     * @Description: 删除记录
     * 实现的是将记录的XMAX设置为当前事务XID,这样后续事务就无法读取到这条记录
     */
    boolean delete(long xid, long uid) throws WarningException, ErrorException;

    /**
     * @Author: 711lxsky
     * @Description: 事务提交
     */
    void commit(long xid) throws WarningException, ErrorException;

    /**
     * @Author: 711lxsky
     * @Description: 事务撤销
     */
    void abort(long xid) throws WarningException, ErrorException;

    static VersionManagerImpl buildVersionManager(TransactionManager tm, DataManager dm) throws ErrorException {
        return new VersionManagerImpl(tm, dm);
    }
}
