package top.lxsky711.easydb.core.sp;

import java.util.List;

/**
 * @Author: 711lxsky
 * @Description: 语句解析器配置类
 */

public class SPSetting {

    public static final String TOKEN_END_DEFAULT = "";

    public static final String TOKEN_BEGIN_DEFAULT = "begin";

    public static class Begin {
        public int TRANSACTION_ISOLATION_LEVEL;
    }

    public static final String TOKEN_ISOLATION_DEFAULT = "isolation";

    public static final String TOKEN_LEVEL_DEFAULT = "level";

    public static final String TOKEN_READ_DEFAULT = "read";

    public static final String TOKEN_COMMITTED_DEFAULT = "commit";

    public static final String TOKEN_REPEATABLE_DEFAULT = "repeatable";

    public static final String TOKEN_COMMIT_DEFAULT = "commit";

    public static class Commit{

    }

    public static final String TOKEN_ABORT_DEFAULT = "abort";

    public static class Abort{

    }

    public static final String TOKEN_CREATE_DEFAULT = "create";

    public static class Create{

        public String tableName;

        public List<String> fieldsName;

        public List<String> fieldsType;

        public List<String> indexs;

    }

    public static final String TOKEN_TABLE_DEFAULT = "table";

    public static final String TOKEN_INDEX_DEFAULT = "index";

}
