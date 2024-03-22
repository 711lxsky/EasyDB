package top.lxsky711.easydb.core.common;

import top.lxsky711.easydb.common.log.ErrorMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;
import top.lxsky711.easydb.common.thread.ThreadSetting;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: 711lxsky
 * @Description: 引用技术缓存框架，抽象类
 */

public abstract class AbstractCache<T> {

    // 缓存数据
    private HashMap<Long, T> cacheData;

    // 缓存中资源引用个数标记
    private HashMap<Long, Integer> referenceRecord;

    // 资源获取情况记录
    private HashMap<Long, Boolean> acquisitionSituation;

    // 缓存最大资源数
    private int maxResourceNum;

    // 现有缓存资源数量
    private int cacheCounter;

    private Lock lock;

    public AbstractCache(int maxResourceNum) {
        if(maxResourceNum <= 0){
            Log.logErrorMessage(ErrorMessage.CACHE_RESOURCE_NUMBER_ERROR);
        }
        this.maxResourceNum = maxResourceNum;
        this.cacheData = new HashMap<>();
        this.referenceRecord = new HashMap<>();
        this.acquisitionSituation = new HashMap<>();
        this.lock = new ReentrantLock();
        this.cacheCounter = 0;
    }

    /**
     * @Author: 711lxsky
     * @Description: 获取某个资源，可能是从缓存中拿到，也可能是去获取，然后放入缓存
     * 所以这个方法本身也就是在获取缓存数据
     */
    protected T getResource(long key) {
        while(true){
            this.lock.lock();
            // 如果请求的资源一直在被其他线程获取，就反复等待尝试
            if(this.acquisitionSituation.containsKey(key)){
                this.lock.unlock();
                try {
                    Thread.sleep(ThreadSetting.CACHE_GET_SLEEP_TIME);
                } catch (InterruptedException e) {
                    Log.logWarningMessage(e.getMessage());
                    continue;
                }
                continue;
            }
            // 如果资源在缓存中，直接返回
            if(this.cacheData.containsKey(key)){
                T object = this.cacheData.get(key);
                // 拿到资源，给引用数+1
                this.referenceRecord.put(key, this.referenceRecord.get(key) + 1);
                this.lock.unlock();
                return object;
            }
            // 尝试获取该资源并放入缓存
            if(this.maxResourceNum > 0 && this.cacheCounter == this.maxResourceNum){
                // 缓存已经满了
                this.lock.unlock();
                Log.logWarningMessage(WarningMessage.CACHE_FULL);
                return null;
            }
            // 缓存没满，在资源获取中注册一下，准备从数据源获取资源
            this.acquisitionSituation.put(key, true);
            this.lock.unlock();
            break;
        }
        this.lock.lock();
        T object;
        try {
            object = this.getCacheFromDataSourceByKey(key);
            this.cacheData.put(key, object);
            this.cacheCounter ++;
            this.referenceRecord.put(key, 1);
            this.acquisitionSituation.remove(key);
        }
        finally {
            this.lock.unlock();
        }
        return object;
    }

    /**
     * @Author: 711lxsky
     * @Description: 释放一个资源引用
     */
    protected void releaseOneReference(long key){
        this.lock.lock();
        try {
            // 把引用数 - 1
            int referenceNum = this.referenceRecord.get(key) - 1;
            // 如果接下来资源没有被引用，就释放写回
            if(referenceNum == 0){
                T obj = cacheData.get(key);
                this.releaseCacheForObject(obj);
                this.referenceRecord.remove(key);
                this.cacheData.remove(key);
                this.cacheCounter --;
            }
            else {
                this.referenceRecord.put(key, referenceNum);
            }
        }
        finally {
            this.lock.unlock();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 安全关闭缓存，并将资源数据写回
     */
    protected void close(){
        this.lock.lock();
        try{
            Set<Long> keys = this.cacheData.keySet();
            for(long key: keys){
                T obj = this.cacheData.get(key);
                this.releaseCacheForObject(obj);
                this.cacheData.remove(key);
                this.referenceRecord.remove(key);
            }
        }
        finally {
            this.lock.unlock();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 当资源不在缓存中时，从数据源加载获取
     */
    protected abstract T getCacheFromDataSourceByKey(long cacheKey);

    /**
     * @Author: 711lxsky
     * @Description: 释放缓存，并写回文件/磁盘
     * 写回一般指的是将脏页数据写入到对应硬盘或者其他持久化存储设备中
     */
    protected abstract void releaseCacheForObject(T Object);

}
