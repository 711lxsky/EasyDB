package top.lxsky711.easydb.common.data;

import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;

import java.util.List;

/**
 * @Author: 711lxsky
 * @Description: 数据解析转换器
 */

public class DataParser {

    /**
     * @Author: 711lxsky
     * @Description: 根据数据类型将字符串转换为对应的数据
     */
    public static Object parseStringToData(String str, String dataType) throws WarningException {
        if(StringUtil.stringIsBlank(str) || StringUtil.stringIsBlank(dataType)){
            Log.logWarningMessage(WarningMessage.STRING_IS_INVALID);
            return null;
        }
        switch (dataType){
            case DataSetting.DATA_INT32:
                return Integer.parseInt(str);
            case DataSetting.DATA_INT64:
                return Long.parseLong(str);
            case DataSetting.DATA_STRING:
                return str;
            default:
                Log.logWarningMessage(WarningMessage.DATA_TYPE_IS_INVALID);
                return null;
        }
    }

    public static long parseStringToLong(String str){
        long seed = DataSetting.DATA_SEED;
        long result = 0;
        byte[] bytes = str.getBytes();
        for(byte oneByte : bytes){
            result = result * seed + oneByte;
        }
        return result;
    }

    public static long parseDataToLong(Object data, String dataType) throws WarningException{
        switch (dataType){
            case DataSetting.DATA_INT32:
                return (long)(int)data;
            case DataSetting.DATA_INT64:
                return (long)data;
            case DataSetting.DATA_STRING:
                return parseStringToLong((String)data);
            default:
                Log.logWarningMessage(WarningMessage.DATA_TYPE_IS_INVALID);
                return 0;
        }
    }

    public static String parseDataToString(Object data, String dataType) throws WarningException{
        switch (dataType){
            case DataSetting.DATA_INT32:
                return String.valueOf((int)data);
            case DataSetting.DATA_INT64:
                return String.valueOf((long)data);
            case DataSetting.DATA_STRING:
                return (String)data;
            default:
                Log.logWarningMessage(WarningMessage.DATA_TYPE_IS_INVALID);
                return null;
        }
    }

    public static byte[] parseDataToBytes(Object data, String dataType) throws WarningException{
        switch (dataType){
            case DataSetting.DATA_INT32:
                return ByteParser.intToBytes((int)data);
            case DataSetting.DATA_INT64:
                return ByteParser.longToBytes((long)data);
            case DataSetting.DATA_STRING:
                return StringUtil.stringToBytes((String)data);
            default:
                Log.logWarningMessage(WarningMessage.DATA_TYPE_IS_INVALID);
                return null;
        }
    }

    public static <T> List<T> analyzeTwoListWithLogic(String logic, List<T> list1, List<T> list2) throws WarningException{
        switch (logic){
            case DataSetting.LOGIC_AND:
                return CollectionUtil.getIntersectionForTwoList(list1, list2);
            case DataSetting.LOGIC_OR:
                return CollectionUtil.getUnionForTwoList(list1, list2);
            default:
                Log.logWarningMessage(WarningMessage.LOGIC_OPERATOR_IS_INVALID);
                return null;
        }
    }

    public static boolean judgeTypeSame(Object typeInstance, String value) {
        if (typeInstance instanceof Integer) {
            try {
                Integer.parseInt(value);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        } else if (typeInstance instanceof Long) {
            try {
                Long.parseLong(value);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        } else if (typeInstance instanceof Double) {
            try {
                Double.parseDouble(value);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        } else if (typeInstance instanceof Float) {
            try {
                Float.parseFloat(value);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        } else if (typeInstance instanceof Boolean) {
            // 通常认为任何字符串都可以转换为Boolean，因为非"true"的字符串都被解释为false
            return true;
        } else {
            // 对于不支持的类型，返回false
            return false;
        }
    }

    public static Object parseDataTypeToFormat(String dataType) throws WarningException{
        if(StringUtil.stringIsBlank(dataType)){
            Log.logWarningMessage(WarningMessage.STRING_IS_INVALID);
            return null;
        }
        switch (dataType){
            case DataSetting.DATA_INT32:
                return Integer.valueOf(dataType);
            case DataSetting.DATA_INT64:
                return Long.valueOf(dataType);
            case DataSetting.DATA_STRING:
                return dataType;
            default:
                Log.logWarningMessage(WarningMessage.DATA_TYPE_IS_INVALID);
                return null;
        }
    }

}
