package top.lxsky711.easydb.core.tm;

import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.common.log.ErrorMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.LogSetting;
import top.lxsky711.easydb.common.log.WarningMessage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: 711lxsky
 */

public class TransactionManagerImpl implements TransactionManager{

    // 记录事务状态的XID文件，直接操作文件内部数据结构，可以跳跃至文件不同位置
    @SuppressWarnings("FieldMayBeFinal")
    private RandomAccessFile xidFile;

    // 文件读写都基于此
    // 提供了对文件的随机访问和顺序访问能力，允许读取、写入、映射到内存以及在不同通道之间传输数据
    @SuppressWarnings("FieldMayBeFinal")
    private FileChannel xidFileChannel;

    // 事务数量统计，位于XID文件头部，占8字节
    private long transactionCounter;

    // 事务计数器锁，防止其他线程同时修改计数器造成数据不一致
    @SuppressWarnings("FieldMayBeFinal")
    private Lock counterLock;

    public TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.xidFile = raf;
        this.xidFileChannel = fc;
        this.counterLock = new ReentrantLock();
        this.checkXIDCounter();
    }

    public void init(){
        ByteBuffer xidFileHeaderBuf = ByteBuffer.wrap(new byte[TMSetting.XID_FILE_HEADER_LENGTH]);
        this.writeXIDFile(TMSetting.XID_FILE_HEADER_OFFSET, xidFileHeaderBuf);
    }

    /**
     * @Author: 711lxsky
     * @Description: 开始事务，文件头计数器增1，返回事务XID
     */
    @Override
    public long begin() {
        this.counterLock.lock();
        try {
            long newXid = this.transactionCounter + 1;
            this.updateXIDStatus(newXid, TMSetting.TRANSACTION_ACTIVE);
            this.addOneForXIDCounter();
            return newXid;
        }
        finally {
            this.counterLock.unlock();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description:
     */
    @Override
    public void commit(long xid) {
        this.updateXIDStatus(xid, TMSetting.TRANSACTION_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        this.updateXIDStatus(xid, TMSetting.TRANSACTION_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if(xid == TMSetting.SUPER_TRANSACTION_XID) {
            return false;
        }
        return this.checkXIDStatus(xid, TMSetting.TRANSACTION_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if(xid == TMSetting.SUPER_TRANSACTION_XID) {
            return true;
        }
        return this.checkXIDStatus(xid, TMSetting.TRANSACTION_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if(xid == TMSetting.SUPER_TRANSACTION_XID) {
            return false;
        }
        return this.checkXIDStatus(xid, TMSetting.TRANSACTION_ABORTED);
    }

    @Override
    public void close() {
        try {
            this.xidFileChannel.close();
            this.xidFile.close();
        }catch (IOException e){
            Log.logErrorMessage(e.getMessage() + LogSetting.LOG_MASSAGE_CONNECTOR + ErrorMessage.FILE_CLOSE_ERROR);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 更新计数器并写入文件刷盘
     */
    private void addOneForXIDCounter(){
        this.transactionCounter ++;
        ByteBuffer counterBuffer = ByteBuffer.wrap(ByteParser.longToBytes(this.transactionCounter));
        this.writeXIDFile(TMSetting.XID_FILE_HEADER_OFFSET, counterBuffer);
        this.forceFileChannel(false);
    }

    /**
     * @Author: 711lxsky
     * @Description: 将数据读取操作封装
     */
    private void readXIDFile(long pos, ByteBuffer readBuffer){
        try {
            this.xidFileChannel.position(pos);
            this.xidFileChannel.read(readBuffer);
        }
        catch (IOException e){
            Log.logErrorMessage(e.getMessage() + LogSetting.LOG_MASSAGE_CONNECTOR + ErrorMessage.FILE_CHANNEL_ERROR);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 将数据写入操作封装，这里会判断写入数据的大小是否足够，作为警告日志
     */
    private void writeXIDFile(long pos, ByteBuffer writeBuffer){
        try {
            this.xidFileChannel.position(pos);
            int writeCapacity = this.xidFileChannel.write(writeBuffer);
            if(writeCapacity < writeBuffer.capacity()){
                Log.logWarningMessage(WarningMessage.FILE_CHANNEL_WRITE_NOT_ENOUGH);
            }
        }
        catch (IOException e){
            Log.logErrorMessage(e.getMessage() + LogSetting.LOG_MASSAGE_CONNECTOR + ErrorMessage.FILE_CHANNEL_ERROR);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 强制FileChannel数据刷新到磁盘，参数是选择是否刷新元数据
     */
    private void forceFileChannel(boolean metaDataForce){
        try {
            this.xidFileChannel.force(metaDataForce);
        }
        catch (IOException e){
            Log.logWarningMessage(e.getMessage() + LogSetting.LOG_MASSAGE_CONNECTOR + WarningMessage.FILE_CHANNEL_DATA_REFRESH_ERROR);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 检查当前事务计数器以及XID文件是否合法
     */
    private void checkXIDCounter() {
        long xidFileLength = 0L;
        // 检查文件合法
        try {
            xidFileLength = xidFile.length();
        }
        catch (IOException e){
            Log.logErrorMessage(ErrorMessage.BAD_XID_FILE + LogSetting.LOG_MASSAGE_CONNECTOR + e.getMessage());
        }
        if(xidFileLength < TMSetting.XID_FILE_HEADER_LENGTH){
            Log.logErrorMessage(ErrorMessage.BAD_XID_FILE_HEADER);
        }

        // 创建一个ByteBuffer用以处理字节数据
        ByteBuffer bf = ByteBuffer.allocate(TMSetting.XID_FILE_HEADER_LENGTH);
        this.readXIDFile(TMSetting.XID_FILE_HEADER_OFFSET, bf);
        this.transactionCounter = ByteParser.parseBytesToLong(bf.array());
        // 这里加上1是因为XID从1开始
        long fileEndPos = this.getXIDStatusPos(this.transactionCounter + 1);
        if(fileEndPos != xidFileLength){
            Log.logErrorMessage(ErrorMessage.BAD_XID_FILE);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 获取某个事务状态所处的文件位置
     */
    private long getXIDStatusPos(long xid){
        return TMSetting.XID_FILE_HEADER_LENGTH + (xid - 1) * TMSetting.TRANSACTION_STATUS_SIZE;
    }

    /**
     * @Author: 711lxsky
     * @Description: 检查某个XID状态是否是目标状态
     */
    private boolean checkXIDStatus(long xid, byte status){
        long xidOffset = this.getXIDStatusPos(xid);
        ByteBuffer xidStatus = ByteBuffer.wrap(new byte[TMSetting.TRANSACTION_STATUS_SIZE]);
        this.readXIDFile(xidOffset, xidStatus);
        return xidStatus.get(0) == status;
    }


    /**
     * @Author: 711lxsky
     * @Description: 更新某个XID为指定状态，并刷盘
     */
    private void updateXIDStatus(long xid, byte status){
        long xidOffset = this.getXIDStatusPos(xid);
        byte[] xidStatus = this.getBytesWithXIDStatus(status);
        ByteBuffer newStatus = ByteBuffer.wrap(xidStatus);
        this.writeXIDFile(xidOffset, newStatus);
        this.forceFileChannel(false);
    }

    /**
     * @Author: 711lxsky
     * @Description: 将状态转换为字节数组
     */
    private byte[] getBytesWithXIDStatus(byte status){
        byte[] xidStatus = new byte[TMSetting.TRANSACTION_STATUS_SIZE];
        xidStatus[0] = status;
//       如果XID状态不止一个字节，可以用下面方法复制
//        System.arraycopy(status, 0, xidStatus, 0, TMSetting.TRANSACTION_STATUS_SIZE);
        return xidStatus;
    }

}
