package top.lxsky711.easydb.core.tbm;

import com.google.common.primitives.Bytes;
import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.common.data.DataParser;
import top.lxsky711.easydb.common.data.DataSetting;
import top.lxsky711.easydb.common.data.StringUtil;
import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;
import top.lxsky711.easydb.core.dm.DataManager;
import top.lxsky711.easydb.core.im.BPlusTree;
import top.lxsky711.easydb.core.sp.SPSetting;
import top.lxsky711.easydb.core.tm.TMSetting;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: 711lxsky
 * @Description: 表字段实现类
 * 将字符串转化为byte数组持久化的时候，需要以 [size][data]的格式存储
 * 字段结构： [FieldName][FieldType][IndexUid]
 *          存储形式： [nameSize][nameSata][typeSize][typeData][indexUid]
 * 如果当前字段没有索引，那么 IndexUid = 0
 */

public class Field {

    // 字段唯一标识
    private long uid;

    // 字段所属表
    private final Table tableAttributed;

    // 字段名称
    String fieldName;

    // 字段类型
    String fieldType;

    // 字段索引uid
    private long indexUid;

    // 字段索引对应的B+树
    private BPlusTree bPlusTree;

    private Field(Table tableAttributed, String fieldName, String fieldType) {
        this.tableAttributed = tableAttributed;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.indexUid = TBMSetting.FIELD_INDEX_DEFAULT;
        this.bPlusTree = null;
    }

    private Field(long uid, Table tableAttributed){
        this.uid = uid;
        this.tableAttributed = tableAttributed;
        this.indexUid = TBMSetting.FIELD_INDEX_DEFAULT;
        this.bPlusTree = null;
    }

    /**
     * @Author: 711lxsky
     * @Description: 创建字段
     */
    public static Field createField(long TransactionXid, Table tableAttributed, String fieldName, String fieldType, boolean isIndex) throws WarningException, ErrorException {
        Field field = new Field(tableAttributed, fieldName, fieldType);
        if(isIndex) {
            DataManager dmForAttributedTable = tableAttributed.getDM();
            field.indexUid = BPlusTree.createBPlusTree(tableAttributed.getDM());
            field.bPlusTree = BPlusTree.loadBPlusTree(dmForAttributedTable, field.indexUid);
        }
        field.persistSelf(TransactionXid);
        return field;
    }

    /**
     * @Author: 711lxsky
     * @Description: 持久化字段
     */
    private void persistSelf(long transactionXid) throws WarningException, ErrorException {
        byte[] fieldNameBytes = StringUtil.stringToBytes(this.fieldName);
        byte[] fieldTypeBytes = StringUtil.stringToBytes(this.fieldType);
        byte[] indexUidBytes = ByteParser.longToBytes(this.indexUid);
        byte[] fieldInfoBytes = Bytes.concat(fieldNameBytes, fieldTypeBytes, indexUidBytes);
        this.uid = this.tableAttributed.getDM().insertData(transactionXid, fieldInfoBytes);
    }

    /**
     * @Author: 711lxsky
     * @Description: 加载字段
     */
    public static Field loadField(Table tableAttributed, long fieldUid) throws WarningException, ErrorException {
        byte[] fieldInfoBytes = tableAttributed.getVM().read(TMSetting.SUPER_TRANSACTION_XID, fieldUid);
        Field field = new Field(fieldUid, tableAttributed);
        field.parseSelf(fieldInfoBytes);
        return field;
    }

    /**
     * @Author: 711lxsky
     * @Description: 解析字段
     */
    private void parseSelf(byte[] fieldInfoBytes) throws WarningException, ErrorException {
        DataSetting.StringBytes fieldNameInfo = StringUtil.parseBytesToString(fieldInfoBytes);
        this.fieldName = fieldNameInfo.str;
        int readPosition = fieldNameInfo.strLength + fieldNameInfo.strLengthSize;
        DataSetting.StringBytes fieldTypeInfo = StringUtil.parseBytesToString(Arrays.copyOfRange(fieldInfoBytes, readPosition, fieldInfoBytes.length));
        this.fieldType = fieldTypeInfo.str;
        readPosition += fieldTypeInfo.strLength + fieldTypeInfo.strLengthSize;
        this.indexUid = ByteParser.parseBytesToLong(Arrays.copyOfRange(fieldInfoBytes, readPosition, readPosition + DataSetting.LONG_BYTE_SIZE));
        if(this.indexUid != TBMSetting.FIELD_INDEX_DEFAULT){
            this.bPlusTree = BPlusTree.loadBPlusTree(this.tableAttributed.getDM(), this.indexUid);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 判断当前字段是否是索引
     */
    public boolean isIndex(){
        return this.indexUid != TBMSetting.FIELD_INDEX_DEFAULT;
    }

    public long getFieldUid() {
        return this.uid;
    }

    public String getFieldName(){
        return this.fieldName;
    }

    public String getFieldType(){
        return this.fieldType;
    }

    // 获取默认的搜索范围
    public TBMSetting.Frontiers getSearchFrontiersDefault(){
        TBMSetting.Frontiers frontiers = new TBMSetting.Frontiers();
        frontiers.leftFrontier = TBMSetting.LEFT_FRONTIER_DEFAULT;
        frontiers.rightFrontier = TBMSetting.RIGHT_FRONTIER_DEFAULT;
        return frontiers;
    }

    /**
     * @Author: 711lxsky
     * @Description: 根据不同逻辑运算符，获取搜索范围
     */
    public TBMSetting.Frontiers getSearchFrontiers(SPSetting.Expression expression) throws WarningException {
        TBMSetting.Frontiers frontiers = new TBMSetting.Frontiers();
        Object value = DataParser.parseStringToData(expression.value, this.fieldType);
        long frontier = DataParser.parseDataToLong(value, this.fieldType);
        switch (expression.compare){
            case DataSetting.COMPARE_SMALLER:
                frontiers.leftFrontier = TBMSetting.LEFT_FRONTIER_DEFAULT;
                frontiers.rightFrontier = frontier;
                if(frontiers.rightFrontier > 0){
                    frontiers.rightFrontier --;
                }
                return frontiers;
            case DataSetting.COMPARE_EQUAL:
                frontiers.leftFrontier = frontier;
                frontiers.rightFrontier = frontier;
                return frontiers;
            case DataSetting.COMPARE_LARGER:
                frontiers.leftFrontier = frontier + 1;
                frontiers.rightFrontier = TBMSetting.RIGHT_FRONTIER_DEFAULT;
                return frontiers;
            default:
                Log.logWarningMessage(WarningMessage.COMPARE_OPERATOR_IS_INVALID);
                return null;
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 范围搜索
     */
    public List<Long> rangeSearch(long left, long right) throws WarningException, ErrorException {
        return this.bPlusTree.searchRangeNodes(left, right);
    }

    /**
     * @Author: 711lxsky
     * @Description: 插入数据
     */
    public void insert(long uid, long key) throws WarningException, ErrorException {
        this.bPlusTree.insertNode(uid, key);
    }

    /**
     * @Author: 711lxsky
     * @Description: 解析字节数组数据
     */
    public TBMSetting.BytesDataParseResult parseBytesData(byte[] bytesData) throws WarningException {
        TBMSetting.BytesDataParseResult result = new TBMSetting.BytesDataParseResult();
        switch(this.fieldType){
            case DataSetting.DATA_INT32:
                result.value = ByteParser.parseBytesToInt(Arrays.copyOf(bytesData, DataSetting.INT_BYTE_SIZE));
                result.shiftFoots = DataSetting.INT_BYTE_SIZE;
                return result;
            case DataSetting.DATA_INT64:
                result.value = ByteParser.parseBytesToLong(Arrays.copyOf(bytesData, DataSetting.LONG_BYTE_SIZE));
                result.shiftFoots = DataSetting.LONG_BYTE_SIZE;
                return result;
            case DataSetting.DATA_STRING:
                DataSetting.StringBytes stringBytes = StringUtil.parseBytesToString(bytesData);
                result.value = stringBytes.str;
                result.shiftFoots = stringBytes.strLengthSize + stringBytes.strLength;
                return result;
            default:
                Log.logWarningMessage(WarningMessage.FIELD_TYPE_IS_NOT_INVALID);
                return null;
        }
    }

}
