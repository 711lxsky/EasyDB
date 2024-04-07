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

    public static long FIELD_INDEX_DEFAULT = 0L;

    public static class Frontiers {
        long leftFrontier;

        long rightFrontier;
    }

    public static final long LEFT_FRONTIER_DEFAULT = Long.MIN_VALUE;

    public static final long RIGHT_FRONTIER_DEFAULT = Long.MAX_VALUE;
}
