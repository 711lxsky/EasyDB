package top.lxsky711.easydb.transport;

import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;

/**
 * @Author: 711lxsky
 * @Description: 数据包装类
 */

public class Packager {

    private Transporter transporter;

    public Packager(Transporter transporter) {
        this.transporter = transporter;
    }

    /**
     * @Author: 711lxsky
     * @Description: 发送数据包
     */
    public void send(DataPackage dataPackage) throws ErrorException {
        byte[] data = Encoder.dataEncode(dataPackage);
        this.transporter.sendData(data);
    }

    /**
     * @Author: 711lxsky
     * @Description: 接收数据包
     */
    public DataPackage receive() throws ErrorException, WarningException {
        byte[] data = this.transporter.receiveData();
        return Encoder.dataDecode(data);
    }

    /**
     * @Author: 711lxsky
     * @Description: 关闭
     */
    public void close() throws ErrorException {
        this.transporter.close();
    }
}
