package top.lxsky711.easydb.common.log;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @Author: 711lxsky
 * @Date: 2024-03-20
 */


public class Log {

    private final static Logger logger = LogManager.getLogger(Log.class);


    public static void logErrorMessage(String err){
        logger.error(err);
        System.exit(LogSetting.SYSTEM_EXIT_CODE);
    }
}
