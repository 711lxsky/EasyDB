package top.lxsky711.easydb.server;

import top.lxsky711.easydb.common.log.InfoMessage;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;
import top.lxsky711.easydb.common.thread.ThreadSetting;
import top.lxsky711.easydb.core.tbm.TableManager;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author: 711lxsky
 * @Description: 服务端
 */

public class Server {

    // 监听/运行端口
    private int operatingPort;

    private TableManager tbm;

    public Server(int operatingPort, TableManager tbm) {
        this.operatingPort = operatingPort;
        this.tbm = tbm;
    }

    /**
     * @Author: 711lxsky
     * @Description: 启动服务
     */
    public void start() {
        ServerSocket serverSocket = null;
        try {
            // 先尝创建一个 Socket
            serverSocket = new ServerSocket(operatingPort);
        }
        catch (Exception e){
            Log.logInfo(e.getMessage() + "    " + Arrays.toString(e.getStackTrace()));
        }
        // 构建 Socket 成功
        String serverPortInfo = Log.concatMessage(InfoMessage.SERVER_IS_LISTENING, String.valueOf(operatingPort));
        Log.logInfo(serverPortInfo);
        // 创建线程池
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(
                ThreadSetting.THREAD_POOL_CORE_SIZE,
                ThreadSetting.THREAD_POOL_MAX_SIZE,
                ThreadSetting.THREAD_POOL_KEEP_ALIVE_TIME_SECONDS,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(ThreadSetting.THREAD_POOL_QUEUE_CAPACITY),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        if(Objects.isNull(serverSocket)){
            Log.logInfo(WarningMessage.SERVER_SOCKET_BUILD_ERROR);
            return;
        }
        try {
            while(true){
                // 处理消息
                Socket socket = serverSocket.accept();
                Runnable worker = new SocketHandler(socket, tbm);
                tpe.execute(worker);
            }
        }
        catch (Exception e){
            Log.logInfo(e.getMessage() + "    " + Arrays.toString(e.getStackTrace()));
        }
        finally {
            try {
                serverSocket.close();
            }
            catch (Exception e){
                Log.logInfo(e.getMessage() + "    " + Arrays.toString(e.getStackTrace()));
            }
        }
    }
}
