package top.lxsky711.easydb.core.dm.pageIndex;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: 711lxsky
 * @Description: 页面索引
 * <p>
 * 工作机制：
 * 页面空间在页面索引管理视角下，是被分割成40个(默认)小区间的，
 * 初始时空闲区间数量就是40, 然后用着用着就会减少空间，空闲区间数量会减少
 * 所以页面索引就以空闲区间的数量为基准，管理页面，每次写一个页面时，就会按照需要的空间大小去找合适的页面
 * </p>
 * <p>
 * 在启动时，就会遍历所有的页面信息，获取页面的空闲空间，安排到这 40 个区间中
 * insert 在请求一个页时，会首先将所需的空间向上取整，映射到某一个区间，随后取出这个区间的任何一页，都可以满足需求
 * 实现的逻辑是在能满足空间要求的情况下，优先去找空闲空间更小的页面
 * </P>
 */

public class PageIndex {

    // 资源锁
    private final Lock lock;

    // 页面信息列表
    private final List<PageInfo> [] pageInfoLists;

    /**
     * @Author: 711lxsky
     * @Description: 初始化时调用，注意这里申请的是INTERVAL_NUMBER + 1个区间，因为后续是向上取整
     */
    public PageIndex() {
        this.lock = new ReentrantLock();
        this.pageInfoLists = new List[PageIndexSetting.PAGE_INTERVAL_NUMBER + 1];
        for(int i = 0; i <= PageIndexSetting.PAGE_INTERVAL_NUMBER; i++){
            pageInfoLists[i] = new java.util.ArrayList<>();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 添加一个页面的空闲空间信息
     */
    public void addFreeSpaceForPage(int pageNumber, int freeSpace){
        this.lock.lock();
        try {
            int index = freeSpace / PageIndexSetting.PAGE_INTERVAL_SIZE;
            this.pageInfoLists[index].add(new PageInfo(pageNumber, freeSpace));
        }
        finally {
            this.lock.unlock();
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 拿到满足插入要求的页面信息
     * 注意这里是remove()方法，意味着页面信息拿到之后会被暂时移除，后续重新加入
     */
    public PageInfo selectOnePage(int needSpaceSize){
        this.lock.lock();
        try {
            int index = needSpaceSize / PageIndexSetting.PAGE_INTERVAL_SIZE;
            if(index < PageIndexSetting.PAGE_INTERVAL_NUMBER){
                // 向上取整
                index ++;
            }
            while(index <= PageIndexSetting.PAGE_INTERVAL_NUMBER){
                if(this.pageInfoLists[index].isEmpty()){
                    index ++;
                    continue;
                }
                return this.pageInfoLists[index].remove(0);
            }
            return null;

        }
        finally {
            this.lock.unlock();
        }
    }

}
