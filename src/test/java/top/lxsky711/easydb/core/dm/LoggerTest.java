package top.lxsky711.easydb.core.dm;

import org.junit.Test;
import top.lxsky711.easydb.core.dm.logger.Logger;

import static org.junit.Assert.assertEquals;

public class LoggerTest {
    
    @Test
    public void testParsePageNumberAndOffsetToUid() {
        int pageNumber = 5;
        short offset = 10;
        long expectedUid = (((long)pageNumber) << Integer.SIZE) | (long)offset ;
        
        long actualUid = Logger.parsePageNumberAndOffsetToUid(pageNumber, offset);
        
        assertEquals(expectedUid, actualUid);
    }
    
    @Test
    public void testGetPageNumberFromUid() {
        long uid = 5000L;
        int expectedPageNumber = (int)(uid >> Integer.SIZE);
        
        int actualPageNumber = Logger.getPageNumberFromUid(uid);
        
        assertEquals(expectedPageNumber, actualPageNumber);
    }
    
    @Test
    public void testGetOffsetFromUid() {
        long uid = 5000L;
        short expectedOffset = (short)(uid & ((1 << Short.SIZE) - 1));
        
        short actualOffset = Logger.getOffsetFromUid(uid);
        
        assertEquals(expectedOffset, actualOffset);
    }
}
