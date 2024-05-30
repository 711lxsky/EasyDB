package top.lxsky711.easydb.server;

import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.log.InfoMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.core.tbm.TableManager;
import top.lxsky711.easydb.transport.DataPackage;
import top.lxsky711.easydb.transport.Packager;
import top.lxsky711.easydb.transport.Transporter;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;

/**
 * @Author: 711lxsky
 * @Description: 服务端 Socket 处理器
 */

public class SocketHandler implements Runnable{

    private final Socket socket;

    private final TableManager tbm;

    public SocketHandler(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    @Override
    public void run() {
        // 获取客户端地址
        InetSocketAddress address = (InetSocketAddress)this.socket.getRemoteSocketAddress();
        String connectionInfo = Log.concatMessage(InfoMessage.SOCKET_CONNECTION_ESTABLISHED,
                address.getAddress().getHostAddress(),
                String.valueOf(address.getPort()));
        // 打印客户端连接信息
        Log.logInfo(connectionInfo);
        Packager packager;
        try {
            // 创建传输器和打包器
            Transporter transporter = new Transporter(this.socket);
            packager = new Packager(transporter);
        } catch (ErrorException e) {
            try {
                this.socket.close();
            }
            catch (Exception e1){
                Log.logInfo(e1.getMessage() + "    " + Arrays.toString(e1.getStackTrace()));
            }
            return;
        }
        Executor executor = new Executor(this.tbm);
        while(true){
            DataPackage dataPackage;
            try {
                dataPackage = packager.receive();
            }
            catch (Exception e){
                break;
            }
            // 获取 SQL 语句
            byte[] SQLStatement = dataPackage.getData();
            byte[] result = null;
            Exception e = null;
            try {
                // 执行 SQL 语句
                result = executor.execute(SQLStatement);
            }
            catch (Exception e1){
                e = e1;
                Log.logInfo(e1.getMessage() + "    " + Arrays.toString(e1.getStackTrace()));
            }
            // 封装结果并发送
            dataPackage = new DataPackage(result, e);
            try {
                packager.send(dataPackage);
            }
            catch (Exception e1){
                Log.logInfo(e1.getMessage() + "    " + Arrays.toString(e1.getStackTrace()));
                break;
            }
        }
        try {
            executor.close();
            packager.close();
        } catch (WarningException | ErrorException e) {
            Log.logInfo(e.getMessage() + "    " + Arrays.toString(e.getStackTrace()));
        }
    }

}
