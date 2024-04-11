package top.lxsky711.easydb.transport;

import com.google.common.primitives.Bytes;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;

import java.util.Arrays;
import java.util.Objects;

/**
 * @Author: 711lxsky
 * @Description: 传输编码实现类
 */

public class Encoder {

    /**
     * @Author: 711lxsky
     * @Description: 数据编码，将数据包转换成字节数组
     */
    public static byte[] dataEncode(DataPackage dataPackage){
        Exception dataException = dataPackage.getException();
        if(Objects.nonNull(dataException)){
            return Bytes.concat(new byte[]{TransportSetting.DATA_IS_EXCEPTION_TRUE}, dataException.getMessage().getBytes());
        }
        else {
            return Bytes.concat(new byte[]{TransportSetting.DATA_IS_EXCEPTION_FALSE}, dataPackage.getData());
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 数据解码，将字节数组转换成数据包
     */
    public static DataPackage dataDecode(byte[] data) throws WarningException {
        if(data.length < TransportSetting.DATA_MIN_LENGTH_DEFAULT){
            Log.logWarningMessage(WarningMessage.DATA_ERROR);
        }
        // 正常数据
        if(data[0] == TransportSetting.DATA_IS_EXCEPTION_FALSE){
            return new DataPackage(Arrays.copyOfRange(data, TransportSetting.DATA_EXCEPTION_MARK_OFFSET, data.length), null);
        }
        // 异常信息
        else if(data[0] == TransportSetting.DATA_IS_EXCEPTION_TRUE){
            return new DataPackage(null, new RuntimeException(new String(Arrays.copyOfRange(data, TransportSetting.DATA_EXCEPTION_MARK_OFFSET, data.length))));
        }
        else {
            Log.logWarningMessage(WarningMessage.DATA_ERROR);
            return null;
        }
    }

}
