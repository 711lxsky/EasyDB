package top.lxsky711.easydb.common.log;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;


/**
 * @Author: 711lxsky
 */


public class Log {

    private final static Logger logger = LogManager.getLogger(Log.class);

    public static void logErrorMessage(String err) throws ErrorException {
        logger.error(err);
        System.exit(LogSetting.SYSTEM_EXIT_CODE);
        throw new ErrorException(err);
    }

    public static void logException(Exception e) throws ErrorException{
        logger.error(e + e.getMessage());
        System.exit(LogSetting.SYSTEM_EXIT_CODE);
        throw new ErrorException(e.getMessage(), e.getCause());
    }

    public static void logWarningMessage(String warning) throws WarningException {
        logger.warn(warning);
        throw new WarningException(warning);
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
