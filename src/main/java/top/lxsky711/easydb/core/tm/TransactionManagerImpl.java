package top.lxsky711.easydb.core.tm;

import top.lxsky711.easydb.common.log.ErrorMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.LogSetting;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;

/**
 * @Author: 711lxsky
 */

public class TransactionManagerImpl implements TransactionManager{

    // 记录事务状态的XID文件，直接操作文件内部数据结构，可以跳跃至文件不同位置
    private RandomAccessFile xidFile;

    // 文件读写都基于此
    // 提供了对文件的随机访问和顺序访问能力，允许读取、写入、映射到内存以及在不同通道之间传输数据
    private FileChannel xidFileChannel;

    // 事务数量统计，位于XID文件头部，占8字节
    private long transactionCounter;

    // 事务计数器锁，防止其他线程同时修改计数器造成数据不一致
    private Lock counterLock;

    @Override
    public long begin() {

    }

    @Override
    public void commit(long xid) {

    }

    @Override
    public void abort(long xid) {

    }

    @Override
    public boolean isActive(long xid) {

    }

    @Override
    public boolean isCommitted(long xid) {

    }

    @Override
    public boolean isAborted(long xid) {

    }

    @Override
    public void close() {

    }

    private void checkXIDCounter() {
        long xidFileLength = 0L;
        try {
            xidFileLength = xidFile.length();
        }
        catch (IOException e){
            Log.logErrorMessage(ErrorMessage.BAD_XID_FILE + LogSetting.LOG_MASSAGE_CONNECTOR + e.getMessage());
        }
        if(xidFileLength < TMSetting.XID_FILE_HEADER_LENGTH){
            Log.logErrorMessage(ErrorMessage.BAD_XID_FILE_HEADER);
        }
    }

}
