package top.lxsky711.easydb.transport;

import org.apache.commons.codec.binary.Hex;
import top.lxsky711.easydb.common.data.StringUtil;
import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.log.Log;

import java.io.*;
import java.net.Socket;

/**
 * @Author: 711lxsky
 * @Description: 传输器实现类
 */

public class Transporter {

    private final Socket socket;

    private BufferedReader reader;

    private BufferedWriter writer;

    public Transporter(Socket socket) throws ErrorException {
        this.socket = socket;
        try {
            // 通过传入的 Socket 获取输入输出流
            this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        }
        catch (IOException e) {
            Log.logException(e);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 将字节数组转换为十六进制字符串
     */
    private String hexEncode(byte[] data){
        return Hex.encodeHexString(data, true) + TransportSetting.LINE_FEED;
    }

    /**
     * @Author: 711lxsky
     * @Description: 发送，写入数据
     */
    public void sendData(byte[] data) throws ErrorException {
        try {
            String hexData = this.hexEncode(data);
            this.writer.write(hexData);
            this.writer.flush();
        }
        catch (IOException e) {
            Log.logException(e);
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 将十六进制字符串转换为字节数组
     */
    private byte[] hexDecode(String hexData) throws ErrorException {
        try {
            return Hex.decodeHex(hexData);
        }
        catch (Exception e) {
            Log.logException(e);
            return null;
        }

    }

    /**
     * @Author: 711lxsky
     * @Description: 接收读取数据
     */
    public byte[] receiveData() throws ErrorException {
        try {
            String hexData = this.reader.readLine();
            if(StringUtil.stringIsBlank(hexData)){
                this.close();
            }
            return this.hexDecode(hexData);
        }
        catch (IOException e) {
            Log.logException(e);
            return null;
        }
    }

    /**
     * @Author: 711lxsky
     * @Description: 关闭传输器
     */
    public void close() throws ErrorException {
        try {
            this.writer.close();
            this.reader.close();
            this.socket.close();
        }
        catch (IOException e) {
            Log.logException(e);
        }
    }
}
