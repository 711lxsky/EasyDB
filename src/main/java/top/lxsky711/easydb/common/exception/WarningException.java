package top.lxsky711.easydb.common.exception;

/**
 * @Author: 711lxsky
 * @Description: 警告日志对应的异常
 */

public class WarningException extends Exception{

    private static final long serialVersionUID = 1L;

    public WarningException(){
        super(ExceptionSetting.WARNING_EXCEPTION_HAPPENED_EN);
    }

    public WarningException(String message){
        super(
                ExceptionSetting.WARNING_EXCEPTION_HAPPENED_EN + "\n" +
                ExceptionSetting.EXCEPTION_MASSAGE_CONNECTOR_1 + message + ExceptionSetting.EXCEPTION_MASSAGE_CONNECTOR_2 + "\n" +
                ExceptionSetting.EXCEPTION_MASSAGE_CONNECTOR_3
        );
    }

    public WarningException(String message, Throwable cause){
        super(
                ExceptionSetting.WARNING_EXCEPTION_HAPPENED_EN + "\n" +
                        ExceptionSetting.EXCEPTION_MASSAGE_CONNECTOR_1 + message + ExceptionSetting.EXCEPTION_MASSAGE_CONNECTOR_2 + "\n" +
                        ExceptionSetting.EXCEPTION_MASSAGE_CONNECTOR_3,

                cause
        );
    }

}
