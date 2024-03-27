package top.lxsky711.easydb.core.dm.pageIndex;

/**
 * @Author: 711lxsky
 * @Description: 页面信息
 */

public class PageInfo {

    // 页号
    public int pageNumber;

    // 空闲空间大小
    public int freeSpace;

    public PageInfo(int pageNumber, int freeSpace) {
        this.pageNumber = pageNumber;
        this.freeSpace = freeSpace;
    }
}
