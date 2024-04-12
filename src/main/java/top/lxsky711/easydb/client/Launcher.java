package top.lxsky711.easydb.client;

import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.transport.Encoder;
import top.lxsky711.easydb.transport.Packager;
import top.lxsky711.easydb.transport.Transporter;

import java.io.IOException;
import java.net.Socket;

/**
 * @Author: 711lxsky
 * @Description: 客户端启动类
 */

public class Launcher {

    public static void main(String[] args) throws IOException, ErrorException {
        Socket clientSocket = new Socket(ClientSetting.CLIENT_SOCKET_ADDRESS, ClientSetting.CLIENT_SOCKET_PORT);
        Transporter transporter = new Transporter(clientSocket);
        Packager packager = new Packager(transporter);
        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
