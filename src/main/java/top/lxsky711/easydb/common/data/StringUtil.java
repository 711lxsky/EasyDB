package top.lxsky711.easydb.common.data;

import com.google.common.primitives.Bytes;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;

import java.util.Arrays;
import java.util.Objects;

/**
 * @Author: 711lxsky
 * @Description: 字符工具类
 */

public class StringUtil {

    public static boolean byteIsLegalSymbol(Byte oneByte){
        return oneByte == '>' || oneByte == '<' || oneByte == '='
                || oneByte == '*'
                || oneByte == ','
                || oneByte == '(' || oneByte == ')';
    }

    public static boolean byteIsBlank(Byte oneByte){
        return oneByte == ' ' || oneByte == '\t' || oneByte == '\n';
    }

    public static boolean byteIsDigit(Byte oneByte){
        return oneByte >= '0' && oneByte <= '9';
    }

    public static boolean byteIsLetter(Byte oneByte){
        return (oneByte >= 'a' && oneByte <= 'z') || (oneByte >= 'A' && oneByte <= 'Z');
    }

    public static boolean byteIsLegalQuote(Byte oneByte){
        return oneByte == '\'' || oneByte == '"';
    }

    public static boolean byteIsLegalToken(Byte oneByte){
        return byteIsDigit(oneByte) || byteIsLetter(oneByte) || oneByte == '_';
    }

    public static boolean stringEqual(String str1, String str2){
        return str1.equals(str2);
    }

    public static boolean stringIsBlank(String str){
        return Objects.isNull(str) || str.isEmpty();
    }

    public static boolean stringEqualIgnoreCase(String str1, String str2){
        return str1.equalsIgnoreCase(str2);
    }

    public static String parseStringToLowerCase(String str){
        return str.toLowerCase();
    }

    public static boolean nameIsLegal(String name) throws WarningException {
        if(Objects.isNull(name) || name.isEmpty() ){
            Log.logWarningMessage(WarningMessage.NAME_IS_NULL);
        }
        if(name.length() > DataSetting.NAME_MAX_LENGTH){
            Log.logWarningMessage(WarningMessage.NAME_TOO_LONG);
            return false;
        }
        if(! Character.isLetter(name.charAt(0))){
            Log.logWarningMessage(WarningMessage.NAME_FIRST_CHAR_IS_NOT_LETTER);
            return false;
        }
        int nameLength = name.length();
        for(int i = 1; i < nameLength; i++){
            char ch = name.charAt(i);
            if(! (Character.isLetterOrDigit(ch) || ch == '_')){
                Log.logWarningMessage(WarningMessage.NAME_CONTAIN_SPECIAL_CHAR);
                return false;
            }
        }
        return true;
    }

    public static boolean isLegalLeftParenthesis(String str){
        return str.equals("(");
    }

    public static boolean isLegalRightParenthesis(String str){
        return str.equals(")");
    }

    public static boolean isLegalComma(String str){
        return str.equals(",");
    }

    public static boolean dataTypeIsLegal(String type){
        for(String dataType : DataSetting.DATA_TYPES_DEFAULT){
            if(dataType.equals(type)){
                return true;
            }
        }
        return false;
    }

    public static boolean isLegalWildcard(String str){
        return str.equals("*");
    }

    public static boolean isLegalCompareOperator(String str){
        for(String compare : DataSetting.COMPARE_DEFAULT){
            if(compare.equals(str)){
                return true;
            }
        }
        return false;
    }

    public static boolean isEqualsOperator(String str){
        return str.equals("=");
    }

    public static boolean isLegalLogicOperator(String str){
        for(String logic : DataSetting.LOGIC_DEFAULT){
            if(logic.equals(str)){
                return true;
            }
        }
        return false;
    }

    public static byte[] stringToBytes(String str){
        byte[] strLengthBytes = ByteParser.intToBytes(str.length());
        return Bytes.concat(strLengthBytes, str.getBytes());
    }

    public static DataSetting.StringBytes parseBytesToString(byte[] bytes) {
        DataSetting.StringBytes stringBytes = new DataSetting.StringBytes();
        stringBytes.strLength = ByteParser.parseBytesToInt(Arrays.copyOf(bytes, stringBytes.strLengthSize));
        stringBytes.str = new String(Arrays.copyOfRange(bytes, stringBytes.strLengthSize, stringBytes.strLengthSize + stringBytes.strLength));
        return stringBytes;
    }

}
