package top.lxsky711.easydb.core.sp;

import top.lxsky711.easydb.common.data.CollectionUtil;
import top.lxsky711.easydb.common.data.StringUtil;
import top.lxsky711.easydb.common.log.InfoMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.core.vm.VMSetting;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: 711lxsky
 * @Description: 语句解析器
 */

public class StatementParser {

    public static Object Parse(byte[] statement){
        Tokenizer tokenizer = new Tokenizer(statement);
        String tokenHead = StringUtil.parseStringToLowerCase(tokenizer.peek());
        tokenizer.pop();
        switch (tokenHead) {
            case SPSetting.TOKEN_BEGIN_DEFAULT:
                return parseBegin(tokenizer);
            case SPSetting.TOKEN_COMMIT_DEFAULT:
                return parseCommit(tokenizer);
            case SPSetting.TOKEN_ABORT_DEFAULT:
                return parseAbort(tokenizer);
            case SPSetting.TOKEN_CREATE_DEFAULT:
                return parseCreate(tokenizer);
                // 未完待续...
        }
        return parseStatementWrong(tokenizer);
    }

    private static SPSetting.Create parseCreate(Tokenizer tokenizer){
        String table = StringUtil.parseStringToLowerCase(tokenizer.peek());
        if(! StringUtil.stringEqual(SPSetting.TOKEN_TABLE_DEFAULT, table)){
            return (SPSetting.Create) parseStatementWrong(tokenizer);
        }
        tokenizer.pop();
        SPSetting.Create create = new SPSetting.Create();
        String tableName = tokenizer.peek();
        if(! StringUtil.nameIsLegal(tableName)){
            return (SPSetting.Create) parseStatementWrong(tokenizer);
        }
        create.tableName = tableName;
        List<String> fieldNames = new ArrayList<>();
        List<String> fieldTypes = new ArrayList<>();
        while(true){
            tokenizer.pop();
            String fieldName = tokenizer.peek();
            if(StringUtil.isLegalLeftParenthesis(fieldName)){
                break;
            }
            if(! StringUtil.nameIsLegal(fieldName)){
                return (SPSetting.Create) parseStatementWrong(tokenizer);
            }
            tokenizer.pop();
            String fieldType = StringUtil.parseStringToLowerCase(tokenizer.peek());
            if(! StringUtil.dataTypeIsLegal(fieldType)){
                return (SPSetting.Create) parseStatementWrong(tokenizer);
            }
            fieldNames.add(fieldName);
            fieldTypes.add(fieldType);
            tokenizer.pop();
            String nextToken = tokenizer.peek();
            if(StringUtil.isLegalLeftParenthesis(nextToken)){
                break;
            }
            else if(! StringUtil.isLegalComma(nextToken)){
                return (SPSetting.Create) parseStatementWrong(tokenizer);
            }
        }
        create.fieldsName = fieldNames;
        create.fieldsType = fieldTypes;
        tokenizer.pop();
        String index = StringUtil.parseStringToLowerCase(tokenizer.peek());
        if(!StringUtil.stringEqual(SPSetting.TOKEN_INDEX_DEFAULT, index)){
            return (SPSetting.Create) parseStatementWrong(tokenizer);
        }
        List<String> indexList = new ArrayList<>();
        while(true){
            tokenizer.pop();
            String indexName = tokenizer.peek();
            if(StringUtil.isLegalRightParenthesis(indexName)){
                break;
            }
            if(! (StringUtil.nameIsLegal(indexName) && CollectionUtil.judgeElementInList(fieldNames, indexName))){
                return (SPSetting.Create) parseStatementWrong(tokenizer);
            }
            indexList.add(indexName);
        }
        if(indexList.isEmpty()){
            return (SPSetting.Create) parseStatementWrong(tokenizer);
        }
        return create;
    }

    private static SPSetting.Abort parseAbort(Tokenizer tokenizer){
        if(isStatementAnalyseEnd(tokenizer)){
            return new SPSetting.Abort();
        }
        return (SPSetting.Abort) parseStatementWrong(tokenizer);
    }

    private static SPSetting.Commit parseCommit(Tokenizer tokenizer){
        if(isStatementAnalyseEnd(tokenizer)){
            return new SPSetting.Commit();
        }
        return (SPSetting.Commit) parseStatementWrong(tokenizer);
    }

    private static SPSetting.Begin parseBegin(Tokenizer tokenizer){
        String isolation = StringUtil.parseStringToLowerCase(tokenizer.peek());
        SPSetting.Begin begin = new SPSetting.Begin();
        if(StringUtil.stringEqual(SPSetting.TOKEN_END_DEFAULT, isolation)){
            // 如果 begin 后面没有跟东西，就指定隔离级别为默认级别
            begin.TRANSACTION_ISOLATION_LEVEL = VMSetting.TRANSACTION_ISOLATION_LEVEL_DEFAULT;
            return begin;
        }
        // 检查有无 isolation 字段
        if(! StringUtil.stringEqual(SPSetting.TOKEN_ISOLATION_DEFAULT, isolation)){
            return (SPSetting.Begin) parseStatementWrong(tokenizer);
        }
        tokenizer.pop();
        String level = StringUtil.parseStringToLowerCase(tokenizer.peek());
        // 检查有无 level 字段
        if(! StringUtil.stringEqual(SPSetting.TOKEN_LEVEL_DEFAULT, level)){
            return (SPSetting.Begin) parseStatementWrong(tokenizer);
        }
        tokenizer.pop();
        String isolationLevelStr1 = StringUtil.parseStringToLowerCase(tokenizer.peek());
        // 先查看级别前半部分
        if(StringUtil.stringEqual(SPSetting.TOKEN_READ_DEFAULT, isolationLevelStr1)){
            // read
            tokenizer.pop();
            String isolationLevelStr2 = StringUtil.parseStringToLowerCase(tokenizer.peek());
            if(StringUtil.stringEqual(SPSetting.TOKEN_COMMITTED_DEFAULT, isolationLevelStr2)){
                // committed
                tokenizer.pop();
                begin.TRANSACTION_ISOLATION_LEVEL = VMSetting.TRANSACTION_ISOLATION_LEVEL_READ_COMMITTED;
            }
            else {
                return (SPSetting.Begin) parseStatementWrong(tokenizer);
            }
        }
        else if(StringUtil.stringEqual(SPSetting.TOKEN_REPEATABLE_DEFAULT, isolationLevelStr1)) {
            // repeatable
            tokenizer.pop();
            String isolationLevelStr2 = StringUtil.parseStringToLowerCase(tokenizer.peek());
            if(StringUtil.stringEqual(SPSetting.TOKEN_READ_DEFAULT, isolationLevelStr2)){
                // read
                tokenizer.pop();
                begin.TRANSACTION_ISOLATION_LEVEL = VMSetting.TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ;
            }
            else {
                return (SPSetting.Begin) parseStatementWrong(tokenizer);
            }
        }
        else {
            return (SPSetting.Begin) parseStatementWrong(tokenizer);
        }
        if(isStatementAnalyseEnd(tokenizer)){
            return begin;
        }
        return (SPSetting.Begin) parseStatementWrong(tokenizer);
    }

    private static Object parseStatementWrong(Tokenizer tokenizer){
        String fullStatement = new String(tokenizer.getStatement());
        String errorToken = tokenizer.peek();
        Log.logInfo(Log.concatMessage(InfoMessage.STATEMENT_SYNTAX_ERROR, fullStatement, errorToken));
        return null;
    }

    private static boolean isStatementAnalyseEnd(Tokenizer tokenizer){
        return StringUtil.stringEqual(SPSetting.TOKEN_END_DEFAULT, tokenizer.peek());
    }

}
