package top.lxsky711.easydb.common.log;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @Author: 711lxsky
 */


public class Log {

    private final static Logger logger = LogManager.getLogger(Log.class);

    public static void logErrorMessage(String err){
        logger.error(err);
        System.exit(LogSetting.SYSTEM_EXIT_CODE);
    }

    public static void logException(Exception e){
        logger.error(e + e.getMessage());
        System.exit(LogSetting.SYSTEM_EXIT_CODE);
    }

    public static void logWarningMessage(String warning){
        logger.warn(warning);
    }

    public static void logInfo(String message){
        logger.info(message);
    }

    public static String concatMessage(String mainMessage, String...additionalMessages){
        return String.format(mainMessage, (Object) additionalMessages);
    }

    public static Exception buildException(String message){
        return new Exception(message);
    }
}
