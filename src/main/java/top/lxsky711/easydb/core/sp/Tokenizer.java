package top.lxsky711.easydb.core.sp;

import top.lxsky711.easydb.common.data.StringUtil;
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

    //
    private boolean needFlushToken;

    public Tokenizer(byte[] statement) {
        this.statement = statement;
        this.curAnalysisPos = 0;
        this.curToken = SPSetting.TOKEN_END_DEFAULT;
        this.needFlushToken = true;
    }

    public String peek(){
        if(this.needFlushToken){
            this.curToken = this.nextMetaState();
            this.needFlushToken = false;
        }
        return this.curToken;
    }

    public void pop(){
        this.needFlushToken = true;
    }

    private String nextMetaState(){
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
        if(StringUtil.byteIsLegalSymbol(curByte)){
            this.popByte();
            return String.valueOf(curByte);
        }
        else if(StringUtil.byteIsLegalQuote(curByte)){
            return nextQuoteState();
        }
        else if(StringUtil.byteIsDigit(curByte) || StringUtil.byteIsLetter(curByte)){
            return nextTokenState();
        }
        else {
            return StateAnalysisWrong();
        }
    }

    private String nextQuoteState(){
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


    private Byte peekByte(){
        if(this.curAnalysisPos == this.statement.length){
            return null;
        }
        return this.statement[this.curAnalysisPos];
    }

    private void popByte(){
        this.curAnalysisPos = Math.min(this.curAnalysisPos + 1, this.statement.length);
    }

    public String StateAnalysisWrong(){
        Log.logWarningMessage(WarningMessage.STATEMENT_NOT_SUPPORT);
        return null;
    }

    public byte[] getStatement() {
        return statement;
    }
}
