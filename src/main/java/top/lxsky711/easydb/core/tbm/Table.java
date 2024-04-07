package top.lxsky711.easydb.core.tbm;

import com.google.common.primitives.Bytes;
import top.lxsky711.easydb.common.data.*;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;
import top.lxsky711.easydb.core.dm.DataManager;
import top.lxsky711.easydb.core.sp.SPSetting;
import top.lxsky711.easydb.core.tm.TMSetting;
import top.lxsky711.easydb.core.vm.VersionManager;

import java.util.*;

/**
 * @Author: 711lxsky
 * @Description: 数据表实现类
 * 表结构: [TableName][NextTable][Field1Uid][Field2Uid]...[FieldNUid]
 */

public class Table {

    private TableManager tbm;

    private long uid;

    private String name;

    private long nextTableUid;

    private List<Field> fields;

    private Table(TableManager tbm, String tableName, long nextTableUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextTableUid = nextTableUid;
        this.fields = new ArrayList<>();
    }

    private Table(TableManager tbm, long tableUid) {
        this.tbm = tbm;
        this.uid = tableUid;
        this.fields = new ArrayList<>();
    }

    public static Table createTable(long transactionXid, TableManager tbm, long nextTableUid, SPSetting.Create create) {
        Table table = new Table(tbm, create.tableName, nextTableUid);
        int fieldNum = create.fieldsName.size();
        for (int i = 0; i < fieldNum; i++) {
            String fieldName = create.fieldsName.get(i);
            String fieldType = create.fieldsType.get(i);
            boolean isIndex = CollectionUtil.judgeElementInList(create.indexs, fieldName);
            table.fields.add(Field.createField(transactionXid, table, fieldName, fieldType, isIndex));
        }
        table.persistSelf(transactionXid);
        return table;
    }

    private void persistSelf(long transactionXid) {
        byte[] tableNameBytes = ByteParser.stringToBytes(this.name);
        byte[] nextTableUidBytes = ByteParser.longToBytes(this.nextTableUid);
        int fieldsUidBytesSize = this.fields.size() * DataSetting.LONG_BYTE_SIZE;
        byte[] fieldsUidBytes = new byte[fieldsUidBytesSize];
        int offset = 0;
        for (Field field : this.fields) {
            System.arraycopy(ByteParser.longToBytes(field.getFieldUid()), 0, fieldsUidBytes, offset, DataSetting.LONG_BYTE_SIZE);
            offset += DataSetting.LONG_BYTE_SIZE;
        }
        byte[] tableInfoBytes = Bytes.concat(tableNameBytes, nextTableUidBytes, fieldsUidBytes);
        this.uid = this.tbm.getDM().insertData(transactionXid, tableInfoBytes);
    }

    public static Table loadTable(TableManager tbm, long tableUid) {
        byte[] tableInfoBytes = tbm.getVM().read(TMSetting.SUPER_TRANSACTION_XID, tableUid);
        Table table = new Table(tbm, tableUid);
        table.parseSelf(tableInfoBytes);
        return table;
    }

    private void parseSelf(byte[] tableInfoBytes) {
        DataSetting.StringBytes tableNameInfo = ByteParser.parseBytesToString(tableInfoBytes);
        this.name = tableNameInfo.str;
        int readPosition = tableNameInfo.strLength + tableNameInfo.strLengthSize;
        this.nextTableUid = ByteParser.parseBytesToLong(Arrays.copyOfRange(tableInfoBytes, readPosition, readPosition + DataSetting.LONG_BYTE_SIZE));
        readPosition += DataSetting.LONG_BYTE_SIZE;
        int tableInfoSize = tableInfoBytes.length;
        while (readPosition < tableInfoSize) {
            long fieldUid = ByteParser.parseBytesToLong(Arrays.copyOfRange(tableInfoBytes, readPosition, readPosition + DataSetting.LONG_BYTE_SIZE));
            this.fields.add(Field.loadField(this, fieldUid));
            readPosition += DataSetting.LONG_BYTE_SIZE;
        }
    }

    public DataManager getDM() {
        return this.tbm.getDM();
    }

    public VersionManager getVM() {
        return this.tbm.getVM();
    }

    private List<Long> internSearchDefault(){
        Field field0 = null;
        for (Field field : this.fields) {
            if (field.isIndex()) {
                field0 = field;
                break;
            }
        }
        if (Objects.isNull(field0)) {
            Log.logWarningMessage(WarningMessage.NO_INDEX);
            return null;
        }
        TBMSetting.Frontiers frontiers = field0.getSearchFrontiersDefault();
        return field0.rangeSearch(frontiers.leftFrontier, frontiers.rightFrontier);
    }

    private List<Long> internSearch(SPSetting.Expression expression){
        Field field0 = null;
        for (Field field : this.fields) {
            if (StringUtil.stringEqual(field.getFieldName(), expression.fieldName)) {
                field0 = field;
                break;
            }
        }
        if (Objects.isNull(field0)) {
            Log.logWarningMessage(WarningMessage.INDEX_IS_NOT_EXIST);
            return null;
        }
        TBMSetting.Frontiers frontiers0 = field0.getSearchFrontiers(expression);
        return field0.rangeSearch(frontiers0.leftFrontier, frontiers0.rightFrontier);
    }

    private List<Long> analyzeWhere(SPSetting.Where where) {
        if (Objects.isNull(where)) {
            return this.internSearchDefault();
        } else {
            if(Objects.isNull(where.expression1)){
                Log.logWarningMessage(WarningMessage.EXPRESSION_IS_INVALID);
                return null;
            }
            List<Long> result0 = this.internSearch(where.expression1);
            if(Objects.isNull(where.expression2)){
                return result0;
            }
            if(StringUtil.stringIsBlank(where.logic)){
                Log.logWarningMessage(WarningMessage.LOGIC_OPERATOR_IS_INVALID);
                return null;
            }
            List<Long> result1 = this.internSearch(where.expression2);
            // 再分别求交集和并集
            return DataParser.analyzeTwoListWithLogic(where.logic, result0, result1);
        }
    }


}
