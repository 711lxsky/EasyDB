package top.lxsky711.easydb.core.tbm;

import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.core.dm.DataManager;
import top.lxsky711.easydb.core.sp.SPSetting;
import top.lxsky711.easydb.core.vm.VersionManager;

/**
 * @Author: 711lxsky
 * @Description: 表管理器接口
 */

public interface TableManager {

    /**
     * @Author: 711lxsky
     * @Description: 开启事务
     */
    TBMSetting.BeginResult begin(SPSetting.Begin begin) throws WarningException, ErrorException;

    /**
     * @Author: 711lxsky
     * @Description: 提交事务
     */
    byte[] commit(long xid) throws WarningException, ErrorException;

    /**
     * @Author: 711lxsky
     * @Description: 撤销事务
     */
    byte[] abort(long xid) throws WarningException, ErrorException;

    /**
     * @Author: 711lxsky
     * @Description: 创建表
     */
    byte[] create(long xid, SPSetting.Create create) throws WarningException, ErrorException;

    /**
     * @Author: 711lxsky
     * @Description: 插入数据
     */
    byte[] insert(long xid, SPSetting.Insert insert) throws WarningException, ErrorException;

    /**
     * @Author: 711lxsky
     * @Description: 查询数据
     */
    byte[] select(long xid, SPSetting.Select select) throws WarningException, ErrorException;

    /**
     * @Author: 711lxsky
     * @Description: 删除数据
     */
    byte[] delete(long xid, SPSetting.Delete delete) throws WarningException, ErrorException;

    /**
     * @Author: 711lxsky
     * @Description: 更新数据
     */
    byte[] update(long xid, SPSetting.Update update) throws WarningException, ErrorException;

    /**
     * @Author: 711lxsky
     * @Description: 显示表信息
     */
    byte[] show(long xid);

    DataManager getDM();

    VersionManager getVM();

    /**
     * @Author: 711lxsky
     * @Description: 以创建的形式拿到表管理器
     */
    static TableManager create(String fullFileName, VersionManager vm, DataManager dm) throws WarningException, ErrorException {
        Booter booter = Booter.createBooter(fullFileName);
        booter.updateBytesDataInBooterFile(ByteParser.longToBytes(TBMSetting.TABLE_UID_DEFAULT));
        return new TableManagerImpl(vm, dm, booter);
    }

    /**
     * @Author: 711lxsky
     * @Description: 以打开的形式拿到表管理器
     */
    static TableManager load(String fullFileName, VersionManager vm, DataManager dm) throws WarningException, ErrorException {
       Booter booter = Booter.openBooter(fullFileName);
       return new TableManagerImpl(vm, dm, booter);
    }
}