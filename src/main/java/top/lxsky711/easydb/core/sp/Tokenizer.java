package top.lxsky711.easydb.core.sp;

import top.lxsky711.easydb.common.data.StringUtil;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;

import java.util.Objects;

/**
 * @Author: 711lxsky
 * @Description: 分词器，用以解析分割语句
 */

public class Tokenizer {

    // 语句
    private byte[] statement;

    // 当前在语句中的分析定位
    private int curAnalysisPos;

     // 当前解析出的token
    private String curToken;

    // 是否需要刷新token,即将下一个token解析出来
    private boolean needFlushToken;

    public Tokenizer(byte[] statement) {
        this.statement = statement;
        this.curAnalysisPos = 0;
        this.curToken = SPSetting.TOKEN_END_DEFAULT;
        this.needFlushToken = true;
    }

    /**
     * @Author: 711lxsky
     * @Description: 获取当前解析出的token
     */
    public String peek() throws WarningException {
        if(this.needFlushToken){
            this.curToken = this.nextMetaState();
            this.needFlushToken = false;
        }
        return this.curToken;
    }

    /**
     * @Author: 711lxsky
     * @Description: 强制要求刷新token
     */
    public void pop(){
        this.needFlushToken = true;
    }

    /**
     * @Author: 711lxsky
     * @Description: 解析下一个token
     */
    private String nextMetaState() throws WarningException {
        while(true){
            Byte b = this.peekByte();
            if(b == null){
                return SPSetting.TOKEN_END_DEFAULT;
            }
            if(! StringUtil.byteIsBlank(b)){
                break;
            }
            this.popByte();
        }
        Byte curByte = this.peekByte();
        // 符号
        if(StringUtil.byteIsLegalSymbol(curByte)){
            this.popByte();
            return String.valueOf(curByte);
        }
        // 引号
        else if(StringUtil.byteIsLegalQuote(curByte)){
            return nextQuoteState();
        }
        // 普通
        else if(StringUtil.byteIsDigit(curByte) || StringUtil.byteIsLetter(curByte)){
            return nextTokenState();
        }
        else {
            return StateAnalysisWrong();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description:  获取下一个 \ 和 " 包含的token
     */
    private String nextQuoteState() throws WarningException {
        Byte quote = this.peekByte();
        this.popByte();
        StringBuilder sb = new StringBuilder();
        while(true){
            Byte curByte = this.peekByte();
            if(curByte == null){
                return StateAnalysisWrong();
            }
            if(curByte.equals(quote)){
                this.popByte();
                break;
            }
            sb.append(curByte);
            this.popByte();
        }
        return sb.toString();
    }

    /**
     * @Author: 711lxsky
     * @Description: 获取下一个只含字母、数字、下划线的token
     */
    private String nextTokenState(){
        StringBuilder sb = new StringBuilder();
        while(true){
            Byte curByte = this.peekByte();
            if(curByte == null || ! StringUtil.byteIsLegalToken(curByte)){
                if(Objects.nonNull(curByte) && StringUtil.byteIsBlank(curByte)){
                    this.popByte();
                }
                return sb.toString();
            }
            sb.append(curByte);
            this.popByte();
        }
    }


    /**
     * @Author: 711lxsky
     * @Description: 获取当前分析位置的字符
     */
    private Byte peekByte(){
        // 到达语句末尾
        if(this.curAnalysisPos == this.statement.length){
            return null;
        }
        return this.statement[this.curAnalysisPos];
    }

    /**
     * @Author: 711lxsky
     * @Description:  将当前分析位置向后移动一位
     */
    private void popByte(){
        this.curAnalysisPos = Math.min(this.curAnalysisPos + 1, this.statement.length);
    }

    /**
     * @Author: 711lxsky
     * @Description: 警告解析错误
     */
    public String StateAnalysisWrong() throws WarningException {
        Log.logWarningMessage(WarningMessage.STATEMENT_NOT_SUPPORT);
        return null;
    }

    /**
     * @Author: 711lxsky
     * @Description: 获取完整解析语句
     */
    public byte[] getStatement() {
        return statement;
    }
}
