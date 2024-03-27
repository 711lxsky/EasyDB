package top.lxsky711.easydb.core.dm;

import top.lxsky711.easydb.core.dm.logger.Logger;
import top.lxsky711.easydb.core.dm.page.PageOne;
import top.lxsky711.easydb.core.dm.pageCache.PageCache;
import top.lxsky711.easydb.core.tm.TransactionManager;

/**
 * @Author: 711lxsky
 * @Description: 数据管理器，直接对外提供方法，同时也实现成DataItem对象的缓存
 * <p>
 * 对于向外提供方法，有: 读(以DataItem形式返回)、插入
 * </p>
 * 对于缓存，其存储的key是由页号和偏移量组成的 8 字节无符号整形， 两者各占 4 字节(但是实际上偏移量offset是一个短整形)
 *               value是一个DataItem对象
 */

public interface DataManager {

    DataItem readData(long uid);

    long insertData(long xid, byte[] data);

    void writeLog(byte[] log);

    void releaseOneDataItem(long uid);

    void close();

    static DataManager create(String dataFileFullName, long memory, TransactionManager tm){
        PageCache pageCache = PageCache.create(dataFileFullName, memory);
        Logger logger = Logger.create(dataFileFullName);
        DataManagerImpl dm = new DataManagerImpl(tm, pageCache, logger);
        dm.initPageOne();
        return dm;
    }

    static DataManager open(String dataFileFullName, long memory, TransactionManager tm){
        PageCache pageCache = PageCache.open(dataFileFullName, memory);
        Logger logger = Logger.open(dataFileFullName);
        DataManagerImpl dm = new DataManagerImpl(tm, pageCache, logger);
        if(! dm.loadAndCheckPageOne()){
            Recover.recover(tm, logger, pageCache);
        }
        dm.initPageIndex();
        dm.setPageOneVCOpen();
        dm.flushPageOne();
        return dm;
    }

}
