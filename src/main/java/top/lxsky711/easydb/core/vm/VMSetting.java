package top.lxsky711.easydb.core.vm;

import top.lxsky711.easydb.common.data.DataSetting;

/**
 * @Author: 711lxsky
 * @Description: VM配置
 */

public class VMSetting {

    public static final int RECORD_XMIN_OFFSET = 0;

    public static final int RECORD_XMIN_LENGTH = DataSetting.LONG_BYTE_SIZE;

    public static final int RECORD_XMAX_OFFSET = RECORD_XMIN_OFFSET + RECORD_XMIN_LENGTH;

    public static final int RECORD_XMAX_DEFAULT = 0;

    public static final int RECORD_XMAX_LENGTH = DataSetting.LONG_BYTE_SIZE;

    public static final int RECORD_DATA_OFFSET = RECORD_XMAX_OFFSET + RECORD_XMAX_LENGTH;


    // 事务隔离级别--读已提交
    public static final int TRANSACTION_ISOLATION_LEVEL_READ_COMMITTED = 51;

    // 事务隔离级别--可重复读
    public static final int TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ = 52;
}
