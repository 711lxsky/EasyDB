package top.lxsky711.easydb.core.dm.pageIndex;

import org.junit.Test;
import top.lxsky711.easydb.core.dm.page.PageSetting;

public class PageIndexTest {
    @Test
    public void testPageIndex() {
        PageIndex pIndex = new PageIndex();
        int threshold = PageSetting.PAGE_SIZE / 20;
        for(int i = 0; i < 20; i ++) {
            pIndex.addFreeSpaceForPage(i, i*threshold);
            pIndex.addFreeSpaceForPage(i, i*threshold);
            pIndex.addFreeSpaceForPage(i, i*threshold);
        }

        for(int k = 0; k < 3; k ++) {
            for(int i = 0; i < 19; i ++) {
                PageInfo pi = pIndex.selectOnePage(i * threshold);
                assert pi != null;
                assert pi.pageNumber == i+1;
            }
        }
    }
}
