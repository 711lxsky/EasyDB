package top.lxsky711.easydb.core.tm;

/**
 * @Author: 711lxsky
 *
 * 对事务管理器的一些设置
 */

public class TMSetting {

    // XID文件头(事务数量记录)的偏移量
    public static final int XID_FILE_HEADER_OFFSET = 0;

    // XID头(事务数量记录)长度
    public static final int XID_FILE_HEADER_LENGTH = 8;

    // 每个事务状态的占用长度
    public static final int TRANSACTION_STATUS_SIZE = 1;

    // 事务状态，正在执行，已提交，已撤销
    public static final byte TRANSACTION_ACTIVE = 0;
    public static final byte TRANSACTION_COMMITTED = 1;
    public static final byte TRANSACTION_ABORTED = 2;

    // 超级事务XID，永远为已提交状态
    public static final long SUPER_TRANSACTION_XID = 0;

    // XID文件后缀
    public static final String XID_FILE_SUFFIX = ".xid";

}
