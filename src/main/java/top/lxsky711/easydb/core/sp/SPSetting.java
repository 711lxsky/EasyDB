package top.lxsky711.easydb.core.sp;

import java.util.List;

/**
 * @Author: 711lxsky
 * @Description: 语句解析器配置类
 */

public class SPSetting {

    // 析词器结束标志返回的符号
    public static final String TOKEN_END_DEFAULT = "";


    // begin语句，事务开始
    public static class Begin {
        public int TRANSACTION_ISOLATION_LEVEL;
    }

    public static final String TOKEN_BEGIN_DEFAULT = "begin";

    public static final String TOKEN_ISOLATION_DEFAULT = "isolation";

    public static final String TOKEN_LEVEL_DEFAULT = "level";

    public static final String TOKEN_READ_DEFAULT = "read";

    public static final String TOKEN_COMMITTED_DEFAULT = "commit";

    public static final String TOKEN_REPEATABLE_DEFAULT = "repeatable";

    // Commit语句， 事务提交
    public static class Commit{

    }

    public static final String TOKEN_COMMIT_DEFAULT = "commit";

    // Abort语句，事务撤销
    public static class Abort{

    }

    public static final String TOKEN_ABORT_DEFAULT = "abort";

    // Create语句，创建表
    public static class Create{

        public String tableName;

        // 字段名
        public List<String> fieldsName;

        // 字段类型
        public List<String> fieldsType;

        // 索引
        public List<String> indexs;

    }

    public static final String TOKEN_TABLE_DEFAULT = "table";

    public static final String TOKEN_INDEX_DEFAULT = "index";

    public static final String TOKEN_CREATE_DEFAULT = "create";

    // Drop语句，删除表
    public static class Drop{

        public String tableName;
    }

    public static final String TOKEN_DROP_DEFAULT = "drop";

    // Expression表达式
    public static class Expression{

        public String fieldName;

        public String compare;

        public String value;
    }

    // Where语句，条件语句
    public static class Where {

        public Expression expression1;

        public String logic;

        public Expression expression2;

    }

    public static final String TOKEN_WHERE_DEFAULT = "where";

    // Select语句，查询数据
    public static class Select{

        public String tableName;

        public List<String> fieldsName;

        public Where where;
    }

    public static final String TOKEN_SELECT_DEFAULT = "select";

    public static final String TOKEN_FROM_DEFAULT = "from";

    // Insert语句，插入数据
    public static class Insert{

        public String tableName;

        public List<String> values;
    }

    public static final String TOKEN_INSERT_DEFAULT = "insert";

    public static final String TOKEN_INTO_DEFAULT = "into";

    public static final String TOKEN_VALUES_DEFAULT = "values";

    // Delete语句，删除数据
    public static class Delete {

        public String tableName;

        public Where where;

    }

    public static final String TOKEN_DELETE_DEFAULT = "delete";

    public static class Update {

        public String tableName;

        public String fieldName;

        public String value;

        public Where where;

    }

    public static final String TOKEN_UPDATE_DEFAULT = "update";

    public static final String TOKEN_SET_DEFAULT = "set";

    // Show语句，显示信息
    public static class Show {

    }

    public static final String TOKEN_SHOW_DEFAULT = "show";

}
