package top.lxsky711.easydb.core.tbm;

import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.core.dm.DataManager;
import top.lxsky711.easydb.core.sp.SPSetting;
import top.lxsky711.easydb.core.vm.VersionManager;

/**
 * @Author: 711lxsky
 * @Description: 表管理器接口
 */

public interface TableManager {

    TBMSetting.BeginResult begin(SPSetting.Begin begin);

    byte[] commit(long xid);

    byte[] abort(long xid);

    byte[] create(long xid, SPSetting.Create create);

    byte[] insert(long xid, SPSetting.Insert insert);

    byte[] select(long xid, SPSetting.Select select);

    byte[] delete(long xid, SPSetting.Delete delete);

    byte[] update(long xid, SPSetting.Update update);

    byte[] show(long xid);

    DataManager getDM();

    VersionManager getVM();

    static TableManager create(String fullFileName, VersionManager vm, DataManager dm){
        Booter booter = Booter.createBooter(fullFileName);
        booter.updateBytesDataInBooterFile(ByteParser.longToBytes(TBMSetting.TABLE_UID_DEFAULT));
        return new TableManagerImpl(vm, dm, booter);
    }

    static TableManager load(String fullFileName, VersionManager vm, DataManager dm){
       Booter booter = Booter.openBooter(fullFileName);
       return new TableManagerImpl(vm, dm, booter);
    }
}