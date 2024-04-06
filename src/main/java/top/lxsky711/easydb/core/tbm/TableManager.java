package top.lxsky711.easydb.core.tbm;

import top.lxsky711.easydb.core.sp.SPSetting;

/**
 * @Author: 711lxsky
 * @Description: 表管理器接口
 */

public interface TableManager {

    TBMSetting.BeginResult begin(SPSetting.Begin begin);

    byte[] read(long xid);

    byte[] commit(long xid);

    byte[] abort(long xid);

    byte[] create(long xid, SPSetting.Create create);

    byte[] insert(long xid, SPSetting.Insert insert);

    byte[] select(long xid, SPSetting.Select select);

    byte[] delete(long xid, SPSetting.Delete delete);

    byte[] update(long xid, SPSetting.Update update);

    byte[] show(long xid);
}
