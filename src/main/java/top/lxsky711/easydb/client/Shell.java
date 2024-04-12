package top.lxsky711.easydb.client;

import top.lxsky711.easydb.common.data.StringUtil;

import java.util.Arrays;
import java.util.Scanner;

/**
 * @Author: 711lxsky
 * @Description: 客户端 Shell 命令行
 */

public class Shell {

    private Client client;

    public Shell(Client client) {
        this.client = client;
    }

    public void run(){
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                System.out.print(ClientSetting.CLIENT_RUN_MARK);
                String command = scanner.nextLine();
                String commandLower = StringUtil.parseStringToLowerCase(command);
                if (StringUtil.stringEqual(commandLower, ClientSetting.EXIT_COMMAND) || StringUtil.stringEqual(commandLower, ClientSetting.QUIT_COMMAND)) {
                    break;
                }
                try {
                    byte[] SQLResult = client.execute(command.getBytes());
                    System.out.println(new String(SQLResult));
                } catch (Exception e) {
                    System.out.println(e.getMessage() + "      " + Arrays.toString(e.getStackTrace()));
                }
            }
        } finally {
            client.close();
        }
    }
}
