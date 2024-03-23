package top.lxsky711.easydb.common.file;

import top.lxsky711.easydb.common.log.ErrorMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.LogSetting;
import top.lxsky711.easydb.common.log.WarningMessage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @Author: 711lxsky
 * @Description: 因为全局对文件的操作非常频繁，尝试将文件操作封装
 */

public class FileManager {

    /**
     * @Author: 711lxsky
     * @Description: 数据读取操作封装
     */
    public static void readByteDataIntoFileChannel(FileChannel fc, long pos, ByteBuffer readBuffer){
        try {
            fc.position(pos);
            fc.read(readBuffer);
        }
        catch (IOException e){
            Log.logErrorMessage(e.getMessage()
                    + LogSetting.LOG_MASSAGE_CONNECTOR
                    + ErrorMessage.FILE_CHANNEL_USE_ERROR);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 将数据写入操作封装，这里会判断写入数据的大小是否足够，作为警告日志
     */
    public static void writeByteDataIntoFileChannel(FileChannel fc, long pos, ByteBuffer writeBuffer){
        try {
            fc.position(pos);
            int writeCapacity = fc.write(writeBuffer);
            if(writeCapacity < writeBuffer.capacity()){
                Log.logWarningMessage(WarningMessage.FILE_CHANNEL_WRITE_NOT_ENOUGH);
            }
        }
        catch (IOException e){
            Log.logErrorMessage(e.getMessage()
                    + LogSetting.LOG_MASSAGE_CONNECTOR
                    + ErrorMessage.FILE_CHANNEL_USE_ERROR);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 强制FileChannel数据刷新到磁盘封装，参数是选择是否刷新元数据
     */
    public static void forceRefreshFileChannel(FileChannel fc, boolean metaDataForce){
        try {
            fc.force(metaDataForce);
        }
        catch (IOException e){
            Log.logWarningMessage(e.getMessage()
                    + LogSetting.LOG_MASSAGE_CONNECTOR
                    + WarningMessage.FILE_CHANNEL_DATA_REFRESH_ERROR);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 获取RAF文件长度封装
     */
    public static long getRAFileLength(RandomAccessFile file){
        long fileLength = -1L;
        try {
            fileLength = file.length();
        }
        catch (IOException e) {
            Log.logErrorMessage(e.getMessage() + LogSetting.LOG_MASSAGE_CONNECTOR + ErrorMessage.BAD_RANDOM_ACCESS_FILE);
        }
        return fileLength;
    }


    /**
     * @Author: 711lxsky
     * @Description: 设置文件新长度封装
     * 如果新长度大于文件的当前长度，则文件将被扩展，并且扩展的部分将由零填充
     * 如果新长度小于文件的当前长度，则文件将被截断，超出部分的数据将被丢失
     */
    public static void setFileNewLength(RandomAccessFile file, long newSize){
        try {
            file.setLength(newSize);
        }catch (IOException e){
            Log.logWarningMessage(e.getMessage()
                    + LogSetting.LOG_MASSAGE_CONNECTOR
                    + WarningMessage.FILE_LENGTH_SET_ERROR);
        }
    }


    /**
     * @Author: 711lxsky
     * @Description: 关闭文件和文件通道封装
     */
    public static void closeFileAndChannel(FileChannel fc, RandomAccessFile file){
        try {
            fc.close();
            file.close();
        }
        catch (IOException e){
            Log.logErrorMessage(e.getMessage() + LogSetting.LOG_MASSAGE_CONNECTOR + ErrorMessage.FILE_CLOSE_ERROR);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 打开文件封装
     */
    public static File openFile(String fileFullName){
        File newFile = new File(fileFullName);
        if(! newFile.exists()){
            Log.logWarningMessage(WarningMessage.FILE_NOT_EXIST);
            return null;
        }
        return newFile;
    }

    /**
     * @Author: 711lxsky
     * @Description: 基于文件全名创建一个新的文件封装
     */
    public static File createFile(String fileFullName){
        File newFile = new File(fileFullName);
        try {
            if(! newFile.createNewFile()){
                Log.logWarningMessage(WarningMessage.FILE_CREATE_ERROR);
                return null;
            }
        }catch (IOException e){
            Log.logException(e);
        }
        return newFile;
    }


    /**
     * @Author: 711lxsky
     * @Description: 基于File创建一个新的RandomAccessFile操作封装
     */
    public static RandomAccessFile buildRAFile(File file){
        if(! file.canRead() || ! file.canWrite()){
            Log.logWarningMessage(WarningMessage.FILE_USE_ERROR);
            return null;
        }
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, FileSetting.FILE_MODE_READ_AND_WRITE);
        }
        catch (FileNotFoundException e) {
            Log.logException(e);
        }
        return raf;
    }

}
