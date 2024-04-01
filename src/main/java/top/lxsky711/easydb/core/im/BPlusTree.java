package top.lxsky711.easydb.core.im;

import top.lxsky711.easydb.core.dm.DataItem;
import top.lxsky711.easydb.core.dm.DataManager;

import java.util.concurrent.locks.Lock;

/**
 * @Author: 711lxsky
 * @Description: B+树实现类
 * 索引的数据直接被插入数据库文件中，不需要经过版本管理
 */

public class BPlusTree {

    private DataManager dm;

    private DataItem rootDataItem;

    private long rootUid;

    private Lock selfLock;

    public DataManager getDm() {
        return dm;
    }

    public void setDm(DataManager dm) {
        this.dm = dm;
    }
}
