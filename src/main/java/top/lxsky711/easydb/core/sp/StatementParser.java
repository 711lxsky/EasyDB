package top.lxsky711.easydb.core.sp;

import top.lxsky711.easydb.common.data.CollectionUtil;
import top.lxsky711.easydb.common.data.StringUtil;
import top.lxsky711.easydb.common.exception.WarningException;
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

    public static Object Parse(byte[] statement) throws WarningException {
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
            case SPSetting.TOKEN_DROP_DEFAULT:
                return parseDrop(tokenizer);
            case SPSetting.TOKEN_SELECT_DEFAULT:
                return parseSelect(tokenizer);
            case SPSetting.TOKEN_INSERT_DEFAULT:
                return parseInsert(tokenizer);
            case SPSetting.TOKEN_DELETE_DEFAULT:
                return parseDelete(tokenizer);
            case SPSetting.TOKEN_UPDATE_DEFAULT:
                return parseUpdate(tokenizer);
            case SPSetting.TOKEN_SHOW_DEFAULT:
                return parseShow(tokenizer);
            default:
                return parseStatementWrong(tokenizer);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 解析 begin 语句
     * 如果 begin 后面没有跟东西，就指定隔离级别为默认级别
     * 结构: begin [isolation level] [read committed | repeatable read]
     * 例子: begin isolation level read committed
     */
    private static SPSetting.Begin parseBegin(Tokenizer tokenizer) throws WarningException {
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

    /**
     * @Author: 711lxsky
     * @Description: 解析commit语句
     * commit语句后面不应该有其他内容
     * 结构: commit
     */
    private static SPSetting.Commit parseCommit(Tokenizer tokenizer) throws WarningException {
        if(isStatementAnalyseEnd(tokenizer)){
            return new SPSetting.Commit();
        }
        return (SPSetting.Commit) parseStatementWrong(tokenizer);
    }

    /**
     * @Author: 711lxsky
     * @Description: 解析abort语句
     * abort语句后面不应该有其他内容
     * 结构: abort
     */
    private static SPSetting.Abort parseAbort(Tokenizer tokenizer) throws WarningException {
        if(isStatementAnalyseEnd(tokenizer)){
            return new SPSetting.Abort();
        }
        return (SPSetting.Abort) parseStatementWrong(tokenizer);
    }

    /**
     * @Author: 711lxsky
     * @Description: 解析create语句
     * create语句后面应该是表名，然后是字段名和字段类型，然后是索引
     * 结构: create table [tableName]
     *                   [fieldName1][fieldType1],
     *                   ...
     *                   [fieldNameN][fieldTypeN]
     *                   (index [indexName1]...[indexNameM])
     * 例子: create table test
     *                   id int,
     *                   name string,
     *                   (index id)
     */
    private static SPSetting.Create parseCreate(Tokenizer tokenizer) throws WarningException {
        String table = StringUtil.parseStringToLowerCase(tokenizer.peek());
        if(! StringUtil.stringEqual(SPSetting.TOKEN_TABLE_DEFAULT, table)){
            return (SPSetting.Create) parseStatementWrong(tokenizer);
        }
        tokenizer.pop();
        SPSetting.Create create = new SPSetting.Create();
        String tableName = tokenizer.peek();
        // 检查表名是否合法
        if(! StringUtil.nameIsLegal(tableName)){
            return (SPSetting.Create) parseStatementWrong(tokenizer);
        }
        create.tableName = tableName;
        List<String> fieldNames = new ArrayList<>();
        List<String> fieldTypes = new ArrayList<>();
        while(true){
            tokenizer.pop();
            String fieldName = tokenizer.peek();
            // 如果是左括号，说明字段定义结束
            if(StringUtil.isLegalLeftParenthesis(fieldName)){
                break;
            }
            // 检查字段名是否合法
            if(! StringUtil.nameIsLegal(fieldName)){
                return (SPSetting.Create) parseStatementWrong(tokenizer);
            }
            tokenizer.pop();
            String fieldType = StringUtil.parseStringToLowerCase(tokenizer.peek());
            // 检查字段类型是否合法
            if(! StringUtil.dataTypeIsLegal(fieldType)){
                return (SPSetting.Create) parseStatementWrong(tokenizer);
            }
            // 添加字段名和字段类型
            fieldNames.add(fieldName);
            fieldTypes.add(fieldType);
            tokenizer.pop();
            String nextToken = tokenizer.peek();
            if(StringUtil.isLegalLeftParenthesis(nextToken)){
                // 左括号，跳出
                break;
            }
            else if(! StringUtil.isLegalComma(nextToken)){
                // 不是都好， 有问题
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
            // 如果是右括号，说明索引定义结束
            if(StringUtil.isLegalRightParenthesis(indexName)){
                break;
            }
            // 检查索引是否在字段中
            if(! CollectionUtil.judgeElementInList(fieldNames, indexName)){
                return (SPSetting.Create) parseStatementWrong(tokenizer);
            }
            indexList.add(indexName);
        }
        // 暂时不支持无索引的全表扫描
        if(indexList.isEmpty()){
            return (SPSetting.Create) parseStatementWrong(tokenizer);
        }
        create.indexs = indexList;
        return create;
    }

    /**
     * @Author: 711lxsky
     * @Description: 解析drop语句
     * 结构: drop table [tableName]
     * 例子: drop table test
     */
    private static SPSetting.Drop parseDrop(Tokenizer tokenizer) throws WarningException {
        String table = StringUtil.parseStringToLowerCase(tokenizer.peek());
        if(! StringUtil.stringEqual(SPSetting.TOKEN_TABLE_DEFAULT, table)){
            return (SPSetting.Drop) parseStatementWrong(tokenizer);
        }
        tokenizer.pop();
        String tableName = tokenizer.peek();
        if(! StringUtil.nameIsLegal(tableName)){
            return (SPSetting.Drop) parseStatementWrong(tokenizer);
        }
        tokenizer.pop();
        if(! isStatementAnalyseEnd(tokenizer)){
            return (SPSetting.Drop) parseStatementWrong(tokenizer);
        }
        SPSetting.Drop drop = new SPSetting.Drop();
        drop.tableName = tableName;
        return drop;
    }

    /**
     * @Author: 711lxsky
     * @Description: 解析select语句
     * where语句是可选的
     * 结构: select * | [fieldName1], [fieldName2], ...
     *             from [tableName]
     *             where ...
     * 例子: select id, name from test where id < 5
     */
    private static SPSetting.Select parseSelect(Tokenizer tokenizer) throws WarningException {
        SPSetting.Select select = new SPSetting.Select();
        List<String> fieldNameList = new ArrayList<>();
        String next = tokenizer.peek();
        if(StringUtil.isLegalWildcard(next)){
            // 通配符
            fieldNameList.add(next);
            tokenizer.pop();
        }
        else {
            // 具体列
            while(true){
                String fieldName = tokenizer.peek();
                if(! StringUtil.nameIsLegal(fieldName)){
                    return (SPSetting.Select) parseStatementWrong(tokenizer);
                }
                fieldNameList.add(fieldName);
                tokenizer.pop();
                if(StringUtil.isLegalComma(tokenizer.peek())){
                    tokenizer.pop();
                }
                else {
                    break;
                }
            }
        }
        select.fieldsName = fieldNameList;
        String from = StringUtil.parseStringToLowerCase(tokenizer.peek());
        if(! StringUtil.stringEqual(SPSetting.TOKEN_FROM_DEFAULT, from)){
            return (SPSetting.Select) parseStatementWrong(tokenizer);
        }
        tokenizer.pop();
        String tableName = tokenizer.peek();
        if(! StringUtil.nameIsLegal(tableName)){
            return (SPSetting.Select) parseStatementWrong(tokenizer);
        }
        select.tableName = tableName;
        tokenizer.pop();
        if(isStatementAnalyseEnd(tokenizer)){
            select.where = null;
        }
        else {
            select.where = parseWhere(tokenizer);
        }
        return select;
    }

    /**
     * @Author: 711lxsky
     * @Description: 解析insert语句
     * 结构: insert into [tableName] values [value1], [value2], ...
     * 例子: insert into test values 1, 'test'
     */
    private static SPSetting.Insert parseInsert(Tokenizer tokenizer) throws WarningException {
        SPSetting.Insert insert = new SPSetting.Insert();
        String into = StringUtil.parseStringToLowerCase(tokenizer.peek());
        if(! StringUtil.stringEqual(SPSetting.TOKEN_INTO_DEFAULT, into)){
            return (SPSetting.Insert) parseStatementWrong(tokenizer);
        }
        tokenizer.pop();
        String tableName = tokenizer.peek();
        if(! StringUtil.nameIsLegal(tableName)){
            return (SPSetting.Insert) parseStatementWrong(tokenizer);
        }
        insert.tableName = tableName;
        tokenizer.pop();
        String values = StringUtil.parseStringToLowerCase(tokenizer.peek());
        if(! StringUtil.stringEqual(SPSetting.TOKEN_VALUES_DEFAULT, values)){
            return (SPSetting.Insert) parseStatementWrong(tokenizer);
        }
        List<String> valueList = new ArrayList<>();
        while(true){
            tokenizer.pop();
            if(isStatementAnalyseEnd(tokenizer)){
                break;
            }
            // 这里不做过多校验，交给表管理层
            String value = tokenizer.peek();
            valueList.add(value);
        }
        insert.values = valueList;
        return insert;
    }

    /**
     * @Author: 711lxsky
     * @Description: 解析delete语句
     * 结构: delete from [tableName] where ...
     * 例子: delete from test where id < 5
     */
    public static SPSetting.Delete parseDelete(Tokenizer tokenizer) throws WarningException {
        SPSetting.Delete delete = new SPSetting.Delete();
        String from = StringUtil.parseStringToLowerCase(tokenizer.peek());
        if(! StringUtil.stringEqual(SPSetting.TOKEN_FROM_DEFAULT, from)){
            return (SPSetting.Delete) parseStatementWrong(tokenizer);
        }
        tokenizer.pop();
        String tableName = tokenizer.peek();
        if(! StringUtil.nameIsLegal(tableName)){
            return (SPSetting.Delete) parseStatementWrong(tokenizer);
        }
        delete.tableName = tableName;
        tokenizer.pop();
        if(isStatementAnalyseEnd(tokenizer)){
            delete.where = null;
        }
        else {
            delete.where = parseWhere(tokenizer);
        }
        return delete;
    }

    /**
     * @Author: 711lxsky
     * @Description: 解析update语句
     * 结构: update [tableName] set [fieldName] = [value] where ...
     * 例子: update test set id = 5 where name = 'test'
     */
    private static SPSetting.Update parseUpdate(Tokenizer tokenizer) throws WarningException {
        SPSetting.Update update = new SPSetting.Update();
        String tableName = tokenizer.peek();
        if(! StringUtil.nameIsLegal(tableName)){
            return (SPSetting.Update) parseStatementWrong(tokenizer);
        }
        update.tableName = tableName;
        tokenizer.pop();
        String set = StringUtil.parseStringToLowerCase(tokenizer.peek());
        if(! StringUtil.stringEqual(SPSetting.TOKEN_SET_DEFAULT, set)){
            return (SPSetting.Update) parseStatementWrong(tokenizer);
        }
        tokenizer.pop();
        String fieldName = tokenizer.peek();
        if(! StringUtil.nameIsLegal(fieldName)){
            return (SPSetting.Update) parseStatementWrong(tokenizer);
        }
        update.fieldName = fieldName;
        tokenizer.pop();
        if(! StringUtil.isEqualsOperator(tokenizer.peek())){
            return (SPSetting.Update) parseStatementWrong(tokenizer);
        }
        tokenizer.pop();
        update.value = tokenizer.peek();
        tokenizer.pop();
        if(isStatementAnalyseEnd(tokenizer)){
            update.where = null;
        }
        else {
            update.where = parseWhere(tokenizer);
        }
        return update;
    }

    /**
     * @Author: 711lxsky
     * @Description: 解析show语句
     */
    private static SPSetting.Show parseShow(Tokenizer tokenizer) throws WarningException {
        SPSetting.Show show = new SPSetting.Show();
        if(! isStatementAnalyseEnd(tokenizer)){
            return (SPSetting.Show) parseStatementWrong(tokenizer);
        }
        return show;
    }

    /**
     * @Author: 711lxsky
     * @Description: 解析where语句
     * 目前只支持两个条件表示式
     */
    private static SPSetting.Where parseWhere(Tokenizer tokenizer) throws WarningException {
        SPSetting.Where where = new SPSetting.Where();
        String whereStr = StringUtil.parseStringToLowerCase(tokenizer.peek());
        if(! StringUtil.stringEqual(SPSetting.TOKEN_WHERE_DEFAULT, whereStr)){
            return (SPSetting.Where) parseStatementWrong(tokenizer);
        }
        tokenizer.pop();
        // 解析第一个
        where.expression1 = parseOneExpression(tokenizer);
        if(isStatementAnalyseEnd(tokenizer)){
            return where;
        }
        // 逻辑运算符
        String logic = StringUtil.parseStringToLowerCase(tokenizer.peek());
        if(! StringUtil.isLegalLogicOperator(logic)){
            return (SPSetting.Where) parseStatementWrong(tokenizer);
        }
        where.logic = logic;
        tokenizer.pop();
        // 解析第二个
        where.expression2 = parseOneExpression(tokenizer);
        if(! isStatementAnalyseEnd(tokenizer)){
            return (SPSetting.Where) parseStatementWrong(tokenizer);
        }
        return where;
    }

    /**
     * @Author: 711lxsky
     * @Description: 解析一个逻辑表达式
     */
    private static SPSetting.Expression parseOneExpression(Tokenizer tokenizer) throws WarningException {
        SPSetting.Expression expression = new SPSetting.Expression();
        String fieldName = tokenizer.peek();
        if(! StringUtil.nameIsLegal(fieldName)){
            return (SPSetting.Expression) parseStatementWrong(tokenizer);
        }
        expression.fieldName = fieldName;
        tokenizer.pop();
        String compare = tokenizer.peek();
        if(! StringUtil.isLegalCompareOperator(compare)){
            return (SPSetting.Expression) parseStatementWrong(tokenizer);
        }
        expression.compare = compare;
        tokenizer.pop();
        String value = tokenizer.peek();
        if(isStatementAnalyseEnd(tokenizer)){
            return (SPSetting.Expression) parseStatementWrong(tokenizer);
        }
        expression.value = value;
        tokenizer.pop();
        return expression;
    }

    /**
     * @Author: 711lxsky
     * @Description: 打印语法错误警告信息
     */
    private static Object parseStatementWrong(Tokenizer tokenizer) throws WarningException {
        String fullStatement = new String(tokenizer.getStatement());
        String errorToken = tokenizer.peek();
        Log.logInfo(Log.concatMessage(InfoMessage.STATEMENT_SYNTAX_ERROR, fullStatement, errorToken));
        return null;
    }

    /**
     * @Author: 711lxsky
     * @Description: 判断是否解析完毕
     */
    private static boolean isStatementAnalyseEnd(Tokenizer tokenizer) throws WarningException {
        return StringUtil.stringEqual(SPSetting.TOKEN_END_DEFAULT, tokenizer.peek());
    }

}
