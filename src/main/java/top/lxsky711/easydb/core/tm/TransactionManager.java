package top.lxsky711.easydb.core.tm;

/*
  @Author: 711lxsky
 */

import top.lxsky711.easydb.common.file.FileSetting;
import top.lxsky711.easydb.common.log.ErrorMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * 每个事务都有一个 XID，这个XID唯一标识此事务，且XID从1开始自增，不可重复
 * 事务状态有3种： 0 -> active 正在执行，尚未结束   1 -> committed 事务已经提交   2 -> aborted 事务已经撤销回滚
 * 另外规定， XID = 0 是一个超级事务，可以在没有申请的事务的情况下执行某些操作。且超级事务状态永远是committed
 *
 * <p>
 *
 * TransactionManager 是事务管理器，提供接口供其他模块调用，创建事务、查询事务状态
 * 其维护一个 XID 格式的文件，用以记录事务状态
 * 文件结构： [t_cnt 事务个数(8字节)][t_status 事务状态(1字节)]...
 * 比如： [t_cnt=3][t_status1=1][t_status2=0][t_status3=2] 表示有3个事务，其中XID=1事务已经提交，XID=2事务正在执行，XID=3事务已经回滚
 * 所以，某个XID = x_id 的事务状态存储在 (x_id - 1) + 8 字节位置(XID=0的超级事务不需记录)
 */



public interface TransactionManager {


    /*
      注意，这里的事务管理器只是维护事务状态
      真正的数据提交、回滚另外有数据管理器维护
     */


    /**
     * @Author: 711lxsky
     * @Description: 开启新事务
     */
    long begin();

    /**
     * @Author: 711lxsky
     * @Description: 提交新事务
     */
    void commit(long xid);

    /**
     * @Author: 711lxsky
     * @Description: 取消事务
     */
    void abort(long xid);

    /**
     * @Author: 711lxsky
     * @Description: 查询某个事务状态是否为活动状态
     */
    boolean isActive(long xid);

    /**
     * @Author: 711lxsky
     * @Description: 查询某个事务状态是否为已提交状态
     */
    boolean isCommitted(long xid);

    /**
     * @Author: 711lxsky
     * @Description: 查询某个事务状态是否为已回滚状态
     */
    boolean isAborted(long xid);

    /**
     * @Author: 711lxsky
     * @Description: 关闭事务管理器
     */
    void close();


    /**
     * @Author: 711lxsky
     * @Description: 根据某个路径创建一个新的事务管理器
     */
     static TransactionManagerImpl create(String xidFileName){
        // 创建XID文件
        File xidFile = new File(xidFileName + TMSetting.XID_FILE_SUFFIX);
        try {
            if(! xidFile.createNewFile()){
                Log.logWarningMessage(WarningMessage.FILE_CREATE_ERROR);
                return null;
            }
        }catch (IOException e){
            Log.logException(e);
        }
        RandomAccessFile newFile = buildFile(xidFile);
        if (newFile != null) {
             TransactionManagerImpl newTM = new TransactionManagerImpl(newFile, false);
             // 从零创建 XID 文件时需要写一个空的 XID 文件头，即设置 xidCounter=0，否则后续在校验时会不合法
             newTM.init();
             return newTM;
        }
        Log.logErrorMessage(ErrorMessage.BAD_XID_FILE);
        return null;
    }


    /**
     * @Author: 711lxsky
     * @Description: 根据某个路径打开一个事务管理器
     */
    static TransactionManagerImpl open(String xidFileName){
        File xidFile = new File(xidFileName + TMSetting.XID_FILE_SUFFIX);
        RandomAccessFile newFile = buildFile(xidFile);
        if (newFile != null) {
            return new TransactionManagerImpl(newFile, true);
        }
        Log.logErrorMessage(ErrorMessage.BAD_XID_FILE);
        return null;
    }


    /**
     * @Author: 711lxsky
     * @Description: 获取XID文件需要的 RandomAccessFile
     */
    private static RandomAccessFile buildFile(File xidFile){
        if(! xidFile.exists()){
            Log.logWarningMessage(WarningMessage.FILE_NOT_EXIST);
            return null;
        }
        if(! xidFile.canRead() || ! xidFile.canWrite()){
            Log.logWarningMessage(WarningMessage.FILE_USE_ERROR);
            return null;
        }
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(xidFile, FileSetting.FILE_MODE_READ_AND_WRITE);
        }
        catch (FileNotFoundException e) {
            Log.logException(e);
        }
        return raf;
    }
}
