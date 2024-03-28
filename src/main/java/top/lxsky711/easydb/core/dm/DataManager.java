package top.lxsky711.easydb.core.dm;

import top.lxsky711.easydb.core.dm.logger.Logger;
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

    /**
     * @Author: 711lxsky
     * @Description: 以DataItem形式读取并返回数据
     */
    DataItem readDataItem(long uid);

    /**
     * @Author: 711lxsky
     * @Description: 插入数据，先包裹成DataRecord格式，然后再借助页面索引插入到相应的页中，返回uid
     */
    long insertData(long xid, byte[] data);

    /**
     * @Author: 711lxsky
     * @Description: 调用Logger的writeLog方法，将日志写入到日志文件中
     */
    void writeLog(byte[] log);

    /**
     * @Author: 711lxsky
     * @Description: 释放一个DataItem对象
     */
    void releaseOneDataItem(long uid);

    /**
     * @Author: 711lxsky
     * @Description: 关闭相应的资源
     */
    void close();

    /**
     * @Author: 711lxsky
     * @Description: 处于上层调用方法创建其他小模块
     */
    static DataManager create(String dataFileFullName, long memory, TransactionManager tm){
        PageCache pageCache = PageCache.create(dataFileFullName, memory);
        Logger logger = Logger.create(dataFileFullName);
        DataManagerImpl dm = new DataManagerImpl(pageCache, logger);
        dm.initPageOne();
        return dm;
    }

    /**
     * @Author: 711lxsky
     * @Description: 处于上层调用方法打开其他小模块资源并作校验
     */
    static DataManager open(String dataFileFullName, long memory, TransactionManager tm){
        PageCache pageCache = PageCache.open(dataFileFullName, memory);
        Logger logger = Logger.open(dataFileFullName);
        DataManagerImpl dm = new DataManagerImpl(pageCache, logger);
        // 校验第一页， 未通过则进行数据恢复
        if(! dm.loadAndCheckPageOne()){
            Recover.recover(tm, logger, pageCache);
        }
        dm.initPageIndex();
        dm.setPageOneVCOpen();
        dm.flushPageOne();
        return dm;
    }

}
