package top.lxsky711.easydb.server;

import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.log.InfoMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;
import top.lxsky711.easydb.core.sp.SPSetting;
import top.lxsky711.easydb.core.sp.StatementParser;
import top.lxsky711.easydb.core.tbm.TBMSetting;
import top.lxsky711.easydb.core.tbm.TableManager;
import top.lxsky711.easydb.core.tm.TMSetting;

/**
 * @Author: 711lxsky
 * @Description: 服务端SQL语句解析执行器实现类
 */

public class Executor {

    // 当前事务XID
    private long transactionXid;

    // 表管理器
    private TableManager tbm;

    public Executor(TableManager tbm){
        this.tbm = tbm;
        this.transactionXid = TMSetting.SUPER_TRANSACTION_XID;
    }

    /**
     * @Author: 711lxsky
     * @Description: 控制执行SQL语句
     */
    public byte[] execute(byte[] SQLStatement) throws WarningException, ErrorException {
        Log.logInfo(InfoMessage.TRY_TO_PARSE_SQL_STATEMENT);
        // 拿到初步解析结果
        Object parseResult = StatementParser.Parse(SQLStatement);
        if(parseResult instanceof SPSetting.Begin){
            // 执行 begin
            if(this.transactionXid != ServerSetting.EXECUTOR_TRANSACTION_XID_DEFAULT){
                // 不能重复 begin 嵌套
                Log.logWarningMessage(WarningMessage.NESTED_TRANSACTION_NOT_SUPPORT);
            }
            TBMSetting.BeginResult beginResult = this.tbm.begin((SPSetting.Begin) parseResult);
            this.transactionXid = beginResult.transactionXid;
            return beginResult.result;
        }
        else if (parseResult instanceof SPSetting.Commit){
            // 执行 commit
            if(this.transactionXid == ServerSetting.EXECUTOR_TRANSACTION_XID_DEFAULT){
                // 事务未正式开启
                Log.logWarningMessage(WarningMessage.NONE_TRANSACTION);
            }
            return this.tbm.commit(this.transactionXid);
        }else if (parseResult instanceof SPSetting.Abort){
            // 执行 abort
            if(this.transactionXid == ServerSetting.EXECUTOR_TRANSACTION_XID_DEFAULT){
                Log.logWarningMessage(WarningMessage.NONE_TRANSACTION);
            }
            return this.tbm.abort(this.transactionXid);
        }
        else {
            // 其余的数据操作
            return this.dataOperateExecute(parseResult);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 数据操作执行
     */
    private byte[] dataOperateExecute(Object parseSQLResult) throws WarningException, ErrorException {
        boolean isImplicitTransaction = false;
        boolean executeSuccess = true;
        if(this.transactionXid == ServerSetting.EXECUTOR_TRANSACTION_XID_DEFAULT){
            // 隐式事务， 先去开启
            isImplicitTransaction = true;
            TBMSetting.BeginResult implicitBeginResult = this.tbm.begin((SPSetting.Begin) parseSQLResult);
            this.transactionXid  = implicitBeginResult.transactionXid;
        }
        try {
            if(parseSQLResult instanceof SPSetting.Create){
                // 执行 create
                return this.tbm.create(this.transactionXid, (SPSetting.Create) parseSQLResult);
            }
            else if(parseSQLResult instanceof SPSetting.Drop){
                // 执行 drop
                return this.tbm.drop(this.transactionXid, (SPSetting.Drop) parseSQLResult);
            }
            else if(parseSQLResult instanceof SPSetting.Select){
                // 执行 select
                return this.tbm.select(this.transactionXid, (SPSetting.Select) parseSQLResult);
            }
            else if(parseSQLResult instanceof SPSetting.Insert){
                // 执行 insert
                return this.tbm.insert(this.transactionXid, (SPSetting.Insert) parseSQLResult);
            }else if(parseSQLResult instanceof SPSetting.Delete){
                // 执行 delete
                return this.tbm.delete(this.transactionXid, (SPSetting.Delete) parseSQLResult);
            }else if(parseSQLResult instanceof SPSetting.Update){
                // 执行 update
                return this.tbm.update(this.transactionXid, (SPSetting.Update) parseSQLResult);
            }
            else {
                Log.logWarningMessage(WarningMessage.STATEMENT_NOT_SUPPORT);
                return null;
            }
        }
        catch (WarningException | ErrorException e){
            // 中途出错， 标记一下
            executeSuccess = false;
            throw e;
        }
        finally {
            // 如果是隐式事务， 则需要提交或回滚
            if(isImplicitTransaction){
                if(executeSuccess){
                    this.tbm.commit(this.transactionXid);
                }else {
                    this.tbm.abort(this.transactionXid);
                }
            }
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 关闭执行器
     */
    public void close() throws WarningException, ErrorException {
        if(this.transactionXid != ServerSetting.EXECUTOR_TRANSACTION_XID_DEFAULT){
            // 如果当前事务没有提交， 则强制回滚
            String abortInfo = Log.concatMessage(InfoMessage.TRANSACTION_ABORTED, String.valueOf(this.transactionXid));
            this.tbm.abort(this.transactionXid);
            Log.logInfo(abortInfo);
        }
    }

}
