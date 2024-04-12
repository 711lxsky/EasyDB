package top.lxsky711.easydb.server;

import org.apache.commons.cli.*;
import top.lxsky711.easydb.common.data.StringUtil;
import top.lxsky711.easydb.common.exception.ErrorException;
import top.lxsky711.easydb.common.exception.WarningException;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;
import top.lxsky711.easydb.core.dm.DataManager;
import top.lxsky711.easydb.core.tbm.TableManager;
import top.lxsky711.easydb.core.tm.TransactionManager;
import top.lxsky711.easydb.core.vm.VersionManager;

/**
 * @Author: 711lxsky
 * @Description: 服务端启动器实现类
 */

public class Launcher {

    public static void main(String[] args) throws ParseException, WarningException, ErrorException {
        Options options = new Options();
        options.addOption(ServerSetting.OPTION_OPEN, true, ServerSetting.OPTION_OPEN_DESCRIPTION);
        options.addOption(ServerSetting.OPTION_CREATE, true, ServerSetting.OPTION_CREATE_DESCRIPTION);
        options.addOption(ServerSetting.OPTION_MEMORY, true, ServerSetting.OPTION_MEMORY_DESCRIPTION);
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        if(cmd.hasOption(ServerSetting.OPTION_CREATE)){
            createDB(cmd.getOptionValue(ServerSetting.OPTION_CREATE));
            return;
        }
        else if(cmd.hasOption(ServerSetting.OPTION_OPEN)){
            openDB(cmd.getOptionValue(ServerSetting.OPTION_OPEN), parseMemory(cmd.getOptionValue(ServerSetting.OPTION_MEMORY)));
            return;
        }
        System.out.println(ServerSetting.OPTION_USAGE);
    }

    private static void createDB(String dbPath) throws WarningException, ErrorException {
        TransactionManager tm = TransactionManager.create(dbPath);
        DataManager dm = DataManager.create(dbPath, ServerSetting.MEMORY_SIZE_DEFAULT, tm);
        VersionManager vm = VersionManager.buildVersionManager(tm, dm);
        TableManager.create(dbPath, vm, dm);
        tm.close();
        dm.close();
    }

    private static void openDB(String dbPath, long memorySize) throws WarningException, ErrorException {
        TransactionManager tm = TransactionManager.open(dbPath);
        DataManager dm = DataManager.open(dbPath, memorySize, tm);
        VersionManager vm = VersionManager.buildVersionManager(tm, dm);
        TableManager tbm = TableManager.open(dbPath, vm, dm);
        new Server(ServerSetting.SERVER_SOCKET_PORT, tbm).start();
    }

    private static long parseMemory(String memorySize) throws WarningException {
        if(StringUtil.stringIsBlank(memorySize)){
            return ServerSetting.MEMORY_SIZE_DEFAULT;
        }
        if(memorySize.length() < ServerSetting.OPTION_MEMORY_LENGTH_MIN){
            Log.logWarningMessage(WarningMessage.MEMORY_INVALID);
        }
        String memoryValueStr = memorySize.substring(0, memorySize.length() - ServerSetting.OPTION_MEMORY_LENGTH_MIN);
        long memoryValue = Long.parseLong(memoryValueStr);
        String memoryUnit = memorySize.substring(memorySize.length() - ServerSetting.OPTION_MEMORY_LENGTH_MIN);
        switch(memoryUnit) {
            case ServerSetting.KB_UNIT:
                return memoryValue * ServerSetting.KB;
            case ServerSetting.MB_UNIT:
                return memoryValue * ServerSetting.MB;
            case ServerSetting.GB_UNIT:
                return memoryValue * ServerSetting.GB;
            default:
                Log.logWarningMessage(WarningMessage.MEMORY_INVALID);
        }
        return ServerSetting.MEMORY_SIZE_DEFAULT;
    }

}
