package top.lxsky711.easydb.core.dm.logger;

import org.junit.Test;
import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;

import java.io.File;

public class LoggerTest {
    @Test
    public void testLogger() throws WarningException, ErrorException {
        Logger lg = Logger.create("/tmp/logger_test1");
        lg.writeLog("aaa".getBytes());
        lg.writeLog("bbb".getBytes());
        lg.writeLog("ccc".getBytes());
        lg.writeLog("ddd".getBytes());
        lg.writeLog("eee".getBytes());
        lg.close();

        lg = Logger.open("/tmp/logger_test1");
        lg.rewind();

        byte[] log = lg.readNextLogData();
        assert log != null;
        assert "aaa".equals(new String(log));

        log = lg.readNextLogData();
        assert log != null;
        assert "bbb".equals(new String(log));

        log = lg.readNextLogData();
        assert log != null;
        assert "ccc".equals(new String(log));

        log = lg.readNextLogData();
        assert log != null;
        assert "ddd".equals(new String(log));

        log = lg.readNextLogData();
        assert log != null;
        assert "eee".equals(new String(log));

        log = lg.readNextLogData();
        assert log == null;

        lg.close();

        assert new File("/tmp/logger_test1.log").delete();
    }
}
