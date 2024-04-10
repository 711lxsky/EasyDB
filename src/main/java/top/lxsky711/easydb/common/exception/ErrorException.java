package top.lxsky711.easydb.common.exception;

/**
 * @Author: 711lxsky
 * @Description:
 */

public class ErrorException extends Exception{

        private static final long serialVersionUID = 1L;

        public ErrorException(){
            super(ExceptionSetting.ERROR_EXCEPTION_HAPPENED_EN);
        }

        public ErrorException(String message){
            super(
                    ExceptionSetting.ERROR_EXCEPTION_HAPPENED_EN + "\n" +
                    ExceptionSetting.EXCEPTION_MASSAGE_CONNECTOR_1 + message + ExceptionSetting.EXCEPTION_MASSAGE_CONNECTOR_2 + "\n" +
                    ExceptionSetting.DATABASE_SYSTEM_STOP_EN
            );
        }

        public ErrorException(String message, Throwable cause){
            super(
                    ExceptionSetting.ERROR_EXCEPTION_HAPPENED_EN + "\n" +
                            ExceptionSetting.EXCEPTION_MASSAGE_CONNECTOR_1 + message + ExceptionSetting.EXCEPTION_MASSAGE_CONNECTOR_2 + "\n" +
                            ExceptionSetting.DATABASE_SYSTEM_STOP_EN,
                    cause);
        }
}
