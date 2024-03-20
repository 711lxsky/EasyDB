package top.lxsky711.easydb.core.tm;

/*
  @Author: 711lxsky
 */

/**
 * 每个事务都有一个 XID，这个XID唯一标识此事务，且XID从1开始自增，不可重复
 * 事务状态有3种： 0 -> active 正在执行，尚未结束   1 -> committed 事务已经提交   2 -> aborted 事务已经撤销回滚
 * 另外规定， XID = 0 是一个超级事务，可以在没有申请的事务的情况下执行某些操作。且超级事务状态永远是committed
 *
 * <p>
 *
 * TransactionManager 是事务管理器，提供接口供其他模块调用，创建事务、查询事务状态
 * 其维护一个 XID 格式的文件，用以记录事务状态
 * 文件结构： [t_cnt 事务个数(8字节)][t_status 事务状态(1字节)]...
 * 比如： [t_cnt=3][t_status1=1][t_status2=0][t_status3=2] 表示有3个事务，其中XID=1事务已经提交，XID=2事务正在执行，XID=3事务已经回滚
 * 所以，某个XID = x_id 的事务状态存储在 (x_id - 1) + 8 字节位置(XID=0的超级事务不需记录)
 */



public interface TransactionManager {

    // 开启新事务
    long begin();

    // 提交新事务
    void commit(long xid);

    // 取消事务
    void abort(long xid);

    // 查询某个事务状态是否为活动状态
    boolean isActive(long xid);

    // 查询某个事务状态是否为已提交状态
    boolean isCommitted(long xid);

    // 查询某个事务状态是否为已回滚状态
    boolean isAborted(long xid);

    // 关闭事务管理器
    void close();
}
