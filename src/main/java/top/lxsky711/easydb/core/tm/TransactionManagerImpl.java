package top.lxsky711.easydb.core.tm;

import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.common.file.FileManager;
import top.lxsky711.easydb.common.log.ErrorMessage;
import top.lxsky711.easydb.common.log.Log;

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

    public TransactionManagerImpl(RandomAccessFile raf) {
        this.xidFile = raf;
        this.xidFileChannel = raf.getChannel();
        this.counterLock = new ReentrantLock();
    }

    public void initCreate(){
        ByteBuffer xidFileHeaderBuf = ByteBuffer.wrap(new byte[TMSetting.XID_FILE_HEADER_LENGTH]);
        FileManager.writeByteDataIntoFileChannel(this.xidFileChannel, TMSetting.XID_FILE_HEADER_OFFSET, xidFileHeaderBuf);
        FileManager.forceRefreshFileChannel(this.xidFileChannel, false);
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
        FileManager.closeFileAndChannel(this.xidFileChannel, this.xidFile);
    }

    /**
     * @Author: 711lxsky
     * @Description: 更新计数器并写入文件刷盘
     */
    private void addOneForXIDCounter(){
        this.transactionCounter ++;
        ByteBuffer counterBuffer = ByteBuffer.wrap(ByteParser.longToBytes(this.transactionCounter));
        FileManager.writeByteDataIntoFileChannel(this.xidFileChannel, TMSetting.XID_FILE_HEADER_OFFSET, counterBuffer);
        FileManager.forceRefreshFileChannel(this.xidFileChannel,false);
    }

    /**
     * @Author: 711lxsky
     * @Description: 检查当前事务计数器以及XID文件是否合法
     */
    protected void checkXIDCounter() {
        // 检查文件合法
        long xidFileLength = FileManager.getRAFileLength(this.xidFile);
        if(xidFileLength < TMSetting.XID_FILE_HEADER_LENGTH){
            Log.logErrorMessage(ErrorMessage.BAD_XID_FILE_HEADER);
        }

        // 创建一个ByteBuffer用以处理字节数据
        ByteBuffer bf = ByteBuffer.allocate(TMSetting.XID_FILE_HEADER_LENGTH);
        FileManager.readByteDataIntoFileChannel(this.xidFileChannel, TMSetting.XID_FILE_HEADER_OFFSET, bf);
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
        FileManager.readByteDataIntoFileChannel(this.xidFileChannel, xidOffset, xidStatus);
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
        FileManager.writeByteDataIntoFileChannel(this.xidFileChannel, xidOffset, newStatus);
        FileManager.forceRefreshFileChannel(this.xidFileChannel, false);
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
