package top.lxsky711.easydb.core.tbm;

import top.lxsky711.easydb.common.file.FileManager;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * @Author: 711lxsky
 * @Description: 表管理的启动类， 存储第一个表(因为是链表形式，头表)信息
 */

public class Booter {

    private String booterFileFullName;

    private File booterFile;

    private Booter(String booterFileFullName, File booterFile){
        this.booterFileFullName = booterFileFullName;
        this.booterFile = booterFile;
    }

    public static Booter createBooter(String booterFileFullName) {
        removeBooterTmpFile(booterFileFullName);
        File booterFile = FileManager.createFile(booterFileFullName + TBMSetting.BOOTER_SUFFIX);
        return new Booter(booterFileFullName, booterFile);
    }

    public static Booter openBooter(String booterFileFullName) {
        removeBooterTmpFile(booterFileFullName);
        File booterFile = FileManager.openFile(booterFileFullName + TBMSetting.BOOTER_SUFFIX);
        return new Booter(booterFileFullName, booterFile);
    }

    private static void removeBooterTmpFile(String booterFileFullName) {
        boolean delete = new File(booterFileFullName + TBMSetting.BOOTER_TMP_SUFFIX).delete();
    }

    public byte[] readAllBytesDataInBooterFile(){
        return FileManager.readAllBytesWithFilePath(this.booterFile.toPath());
    }

    public void updateBytesDataInBooterFile(byte[] dataBytes){
        File booterTmpFile = new File(this.booterFileFullName + TBMSetting.BOOTER_TMP_SUFFIX);
        try {
            boolean createRes = booterTmpFile.createNewFile();
        }catch (IOException e){
            Log.logException(e);
        }
        if(! booterTmpFile.canRead() || ! booterTmpFile.canWrite()){
            Log.logWarningMessage(WarningMessage.FILE_USE_ERROR);
            return;
        }
        try(FileOutputStream fos = new FileOutputStream(booterTmpFile)) {
            fos.write(dataBytes);
            fos.flush();
        }
        catch (IOException e){
            Log.logException(e);
        }
        try {
            Files.move(booterTmpFile.toPath(), new File(this.booterFileFullName + TBMSetting.BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e){
            Log.logException(e);
        }
        this.booterFile = new File(this.booterFileFullName + TBMSetting.BOOTER_SUFFIX);
        if(! booterTmpFile.canRead() || ! booterTmpFile.canWrite()){
            Log.logWarningMessage(WarningMessage.FILE_USE_ERROR);
        }
    }
}
