package top.lxsky711.easydb.core.tbm;

/**
 * @Author: 711lxsky
 * @Description: TBM表管理器配置类
 */

public class TBMSetting {

    // 事务开始返回结果
    public static class BeginResult {

        public long xid;

        public byte[] result;

    }
}
