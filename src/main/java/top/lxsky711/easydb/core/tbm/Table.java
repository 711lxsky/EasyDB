package top.lxsky711.easydb.core.tbm;

import java.util.List;

/**
 * @Author: 711lxsky
 * @Description: 数据表实现类
 * 表结构: [TableName][NextTable][Field1Uid][Field2Uid]...[FieldNUid]
 */

public class Table {

    private TableManager tbm;

    long uid;

    String name;

    long nextTableUid;

    List<Field> fieldList;

    public static Table loadTable(TableManager tbm, long tableUid){
        byte[] table
    }

}
