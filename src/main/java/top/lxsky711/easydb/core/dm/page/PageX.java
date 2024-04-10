package top.lxsky711.easydb.core.dm.page;

import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;

import java.util.Arrays;

/**
 * @Author: 711lxsky
 * @Description: 普通页面
 * 结构： [页头][存储数据]
 * 页头是个2字节的无符号整形，记录了当前页的空闲空间的偏移量
 */

public class PageX {

    /**
     * @Author: 711lxsky
     * @Description: 初始化普通页面
     */
    public static byte[] init(){
        byte[] newData = new byte[PageSetting.PAGE_SIZE];
        setFreeSpaceOffsetIntoPage(newData, PageSetting.PAGE_X_HEADER_LENGTH);
        return newData;
    }

    /**
     * @Author: 711lxsky
     * @Description: 将数据插入到页面中，并返回数据的偏移量
     */
    public static short insertDataIntoPage(Page page, byte[] data) throws WarningException {
        if(data.length > getFreeSpaceForPage(page)){
            Log.logWarningMessage(WarningMessage.PAGE_FREE_SPACE_NOT_ENOUGH);
        }
        page.setDirtyStatus(true);
        short offset = getFreeSpaceOffsetFromPage(page);
        System.arraycopy(data, 0,
                page.getPageData(), offset,
                data.length);
        setFreeSpaceOffsetIntoPage(page.getPageData(), (short)(offset + data.length));
        return offset;
    }

    /**
     * @Author: 711lxsky
     * @Description: 设置页面的空闲空间偏移量
     */
    private static void setFreeSpaceOffsetIntoPage(byte[] pageData, short freeSpaceOffset){
        byte[] fso = ByteParser.shortToBytes(freeSpaceOffset);
        System.arraycopy(fso, 0,
                pageData, PageSetting.PAGE_X_HEADER_OFFSET,
                PageSetting.PAGE_X_HEADER_LENGTH);
    }

    /**
     * @Author: 711lxsky
     * @Description: 获取页面的空闲空间大小
     */
    public static int getFreeSpaceForPage(Page page){
        return PageSetting.PAGE_SIZE - (int)getFreeSpaceOffsetFromPage(page);
    }

    /**
     * @Author: 711lxsky
     * @Description: 从页面中获取空闲空间偏移量
     */
    public static short getFreeSpaceOffsetFromPage(Page page){
        return getFreeSpaceOffsetFromPageData(page.getPageData());
    }

    /**
     * @Author: 711lxsky
     * @Description: 从页面数据中获取空闲空间偏移量
     */
    private static short getFreeSpaceOffsetFromPageData(byte[] pageData){
        return ByteParser.parseBytesToShort(
                Arrays.copyOfRange(pageData, PageSetting.PAGE_X_HEADER_OFFSET,
                        PageSetting.PAGE_X_HEADER_OFFSET + PageSetting.PAGE_X_HEADER_LENGTH));
    }


    /*
     * 下面两个方法用于在数据库崩溃后重新打开时，恢复插入数据以及修改数据
     */

    /**
     * @Author: 711lxsky
     * @Description: 恢复页面中的插入数据操作
     */
    public static void recoverInsert(Page page, byte[] insertData, short offset){
        page.setDirtyStatus(true);
        System.arraycopy(insertData, 0, page.getPageData(), offset, insertData.length);
        short oldFreeSpaceOffset = getFreeSpaceOffsetFromPage(page);
        // 这里可能因为数据库发生过崩溃，导致数据没有插入，所以需要更新空闲空间偏移量
        if(oldFreeSpaceOffset < offset + insertData.length){
            setFreeSpaceOffsetIntoPage(page.getPageData(), (short)(offset + insertData.length));
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 恢复页面中的更新数据操作
     */
    public static void recoverUpdate(Page page, byte[] updateData, short offset){
        page.setDirtyStatus(true);
        System.arraycopy(updateData, 0, page.getPageData(), offset, updateData.length);
    }

}
