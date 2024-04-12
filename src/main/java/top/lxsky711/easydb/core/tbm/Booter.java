package top.lxsky711.easydb.core.tbm;

import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.file.FileManager;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * @Author: 711lxsky
 * @Description: 表管理的启动类， 存储第一个表(因为是链表形式，头表)信息，目前只需要存储第一个表的uid即可
 *
 * 更新的时候，会先写入一个临时文件，然后再将临时文件移动到原文件，保证数据的完整性
 */

public class Booter {

    // 记录文件的全名，不包含后缀
    private String booterFileFullName;

    // 记录文件
    private File booterFile;

    private Booter(String booterFileFullName, File booterFile){
        this.booterFileFullName = booterFileFullName;
        this.booterFile = booterFile;
    }

    // 创建一个新的booter文件
    public static Booter createBooter(String booterFileFullName) throws WarningException, ErrorException {
        removeBooterTmpFile(booterFileFullName);
        File booterFile = FileManager.createFile(booterFileFullName + TBMSetting.BOOTER_SUFFIX);
        return new Booter(booterFileFullName, booterFile);
    }

    // 打开一个已经存在的booter文件
    public static Booter openBooter(String booterFileFullName) throws WarningException {
        removeBooterTmpFile(booterFileFullName);
        File booterFile = FileManager.openFile(booterFileFullName + TBMSetting.BOOTER_SUFFIX);
        return new Booter(booterFileFullName, booterFile);
    }

    // 删除临时的booter文件
    private static void removeBooterTmpFile(String booterFileFullName) {
        boolean delete = new File(booterFileFullName + TBMSetting.BOOTER_TMP_SUFFIX).delete();
    }

    // 读取booter文件中的所有数据(uid信息)
    public byte[] readAllBytesDataInBooterFile() throws ErrorException {
        return FileManager.readAllBytesWithFilePath(this.booterFile.toPath());
    }

    // 更新booter文件中的数据(uid信息)
    public void updateBytesDataInBooterFile(byte[] dataBytes) throws ErrorException, WarningException {
        // 先写入一个临时文件
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
        // 再将临时文件移动到原文件路径，文件名会改变为.bt后缀
        try {
            Path sourcePath = booterTmpFile.toPath();
            Path targetPath = new File(this.booterFileFullName + TBMSetting.BOOTER_SUFFIX).toPath();

            // 移动文件并替换已存在的同名文件
            Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);

            // 移动成功后，更新文件引用
            this.booterFile = targetPath.toFile();

            // 检查新位置文件的权限（如果需要）
            if (!this.booterFile.canRead() || !this.booterFile.canWrite()) {
                Log.logWarningMessage(this.booterFile.getAbsolutePath() + WarningMessage.FILE_USE_ERROR);
            }
        } catch (IOException e) {
            // 记录异常详细信息
            Log.logException(e);

            // 根据实际情况考虑是否需要添加额外的错误处理逻辑，如重试、回滚、通知等
        }

    }
}
