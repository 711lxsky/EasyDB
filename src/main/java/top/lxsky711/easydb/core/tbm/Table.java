package top.lxsky711.easydb.core.tbm;

import com.google.common.primitives.Bytes;
import top.lxsky711.easydb.common.data.*;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;
import top.lxsky711.easydb.core.dm.DataManager;
import top.lxsky711.easydb.core.sp.SPSetting;
import top.lxsky711.easydb.core.tm.TMSetting;
import top.lxsky711.easydb.core.vm.VersionManager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
        byte[] tableNameBytes = StringUtil.stringToBytes(this.name);
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
        DataSetting.StringBytes tableNameInfo = StringUtil.parseBytesToString(tableInfoBytes);
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

    public long getNextTableUid(){
        return this.nextTableUid;
    }

    public String getTableName(){
        return this.name;
    }

    public long getTableUid(){
        return this.uid;
    }

    public String select(long transactionXid, SPSetting.Select select){
        List<Long> targetUidList = this.analyzeWhere(select.where);
        StringBuilder sb = new StringBuilder();
        if(Objects.isNull(targetUidList)){
            return null;
        }
        for (Long uid : targetUidList) {
            byte[] raw = this.tbm.getVM().read(transactionXid, uid);
            if(raw == null) continue;
            Map<String, Object> entry = this.parseBytesToEntry(raw);
            sb.append(this.parseEntryToString(entry)).append(TBMSetting.LINE_FEED);
        }
        return sb.toString();
    }
    public void insert(long transactionXid, SPSetting.Insert insert){
        Map<String, Object> entry = this.parseValuesToEntry(insert.values);
        byte[] entryBytes = this.parseEntryToBytes(entry);
        long uid = this.tbm.getVM().insert(transactionXid, entryBytes);
        this.internInsert(uid, entry);
    }

    public int delete(long transactionXid, SPSetting.Delete delete){
        List<Long> tarUidList = this.analyzeWhere(delete.where);
        int count = 0;
        if(Objects.isNull(tarUidList)){
            return count;
        }
        for(Long uid : tarUidList){
            if(this.tbm.getVM().delete(transactionXid, uid)){
                count ++;
            }
        }
        return count;
    }

    public int update(long transactionXid, SPSetting.Update update){
        List<Long> tarUidList = this.analyzeWhere(update.where);
        int count = 0;
        if(Objects.isNull(tarUidList)){
            return count;
        }
        Field tarField = this.seekFieldWithName(update.fieldName);
        if (Objects.isNull(tarField)) {
            Log.logWarningMessage(WarningMessage.INDEX_IS_NOT_EXIST);
            return count;
        }
        Object value = DataParser.parseStringToData(update.value, tarField.getFieldType());
        for(Long uid : tarUidList){
            byte[] entryBytes = this.tbm.getVM().read(transactionXid, uid);
            if(Objects.isNull(entryBytes) || entryBytes.length == 0){
                continue;
            }
            this.tbm.getVM().delete(transactionXid, uid);
            Map<String, Object> entry = this.parseBytesToEntry(entryBytes);
            entry.put(tarField.fieldName, value);
            entryBytes = this.parseEntryToBytes(entry);
            long newUid = this.tbm.getVM().insert(transactionXid, entryBytes);
            count ++;
            this.internInsert(newUid, entry);
        }
        return count;
    }

    private void internInsert(long uid,  Map<String, Object> entry){
        for(Field field : this.fields){
            if(field.isIndex()){
                long key = TBMSetting.RIGHT_FRONTIER_DEFAULT;
                if (entry != null) {
                    key = DataParser.parseDataToLong(entry.get(field.fieldName), field.getFieldType());
                }
                field.insert(uid, key);
            }
        }
    }

    private Map<String, Object> parseValuesToEntry(List<String> values){
        if(values.size() != this.fields.size()){
            Log.logWarningMessage(WarningMessage.INSERT_VALUES_NOT_MATCH);
            return null;
        }
        Map<String, Object> entry = new HashMap<>();
        int filedNum = this.fields.size();
        for(int i = 0; i < filedNum; i++){
            Field field = this.fields.get(i);
            String strValue = values.get(i);
            Object fieldTypeFormat = DataParser.parseDataTypeToFormat(field.getFieldType());
            if(! DataParser.judgeTypeSame(fieldTypeFormat, strValue)){
                Log.logWarningMessage(WarningMessage.INSERT_VALUES_NOT_MATCH);
                return null;
            }
            Object value = DataParser.parseStringToData(values.get(i), field.getFieldType());
            entry.put(field.getFieldName(), value);
        }
        return entry;
    }

    private byte[] parseEntryToBytes(Map<String, Object> entry){
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (Field field : fields) {
            try {
                byte[] data = DataParser.parseDataToBytes(entry.get(field.fieldName), field.getFieldType());
                if(Objects.isNull(data)){
                    return null;
                }
                baos.write(data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return baos.toByteArray();
    }

    private Map<String, Object> parseBytesToEntry(byte[] bytesEntry){
        int readPosition = 0;
        Map<String, Object> entry = new HashMap<>();
        for(Field field : this.fields){
            TBMSetting.BytesDataParseResult result = field.parseBytesData(Arrays.copyOfRange(bytesEntry, readPosition, bytesEntry.length));
            entry.put(field.getFieldName(), result.value);
            readPosition += result.shiftFoots;
        }
        return entry;
    }

    private String parseEntryToString(Map<String, Object> record){
        StringJoiner sj = new StringJoiner(TBMSetting.DELIMITER, TBMSetting.PREFIX_DELIMITER, TBMSetting.SUFFIX_DELIMITER);
        for (Field field : fields) {
            sj.add(DataParser.parseDataToString(record.get(field.fieldName), field.getFieldType()));
        }
        return sj.toString();
    }

    private Field getFirstIndexField(){
        Field tarField = null;
        for (Field field : this.fields) {
            if (field.isIndex()) {
                tarField = field;
                break;
            }
        }
        return tarField;
    }

    private List<Long> internSearchDefault(){
        Field firstIndexField = this.getFirstIndexField();
        if (Objects.isNull(firstIndexField)) {
            Log.logWarningMessage(WarningMessage.NO_INDEX);
            return null;
        }
        TBMSetting.Frontiers frontiers = firstIndexField.getSearchFrontiersDefault();
        return firstIndexField.rangeSearch(frontiers.leftFrontier, frontiers.rightFrontier);
    }

    private Field seekFieldWithName(String tarFieldName){
        Field tarField = null;
        for (Field field : this.fields) {
            if (StringUtil.stringEqual(field.getFieldName(), tarFieldName)) {
                tarField = field;
                break;
            }
        }
        return tarField;
    }

    private List<Long> internSearch(SPSetting.Expression expression){
        Field tarField = this.seekFieldWithName(expression.fieldName);
        if (Objects.isNull(tarField)) {
            Log.logWarningMessage(WarningMessage.INDEX_IS_NOT_EXIST);
            return null;
        }
        TBMSetting.Frontiers frontiers0 = tarField.getSearchFrontiers(expression);
        return tarField.rangeSearch(frontiers0.leftFrontier, frontiers0.rightFrontier);
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

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner(TBMSetting.DELIMITER, TBMSetting.SECOND_PREFIX_DELIMITER, TBMSetting.SECOND_SUFFIX_DELIMITER);
        for (int i = 0; i < this.fields.size(); i++) {
            Field field = this.fields.get(i);
            sj.add(field.toString());

            // Add trailing "}" only for the last field
            if (i == this.fields.size() - 1) {
                sj.setEmptyValue(TBMSetting.SECOND_PREFIX_DELIMITER + TBMSetting.SECOND_SUFFIX_DELIMITER); // Optional: handle empty fields list
            }
        }

        return sj.toString();
    }

}
