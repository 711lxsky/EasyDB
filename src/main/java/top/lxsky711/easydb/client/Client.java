package top.lxsky711.easydb.client;

import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.transport.DataPackage;
import top.lxsky711.easydb.transport.Packager;

import java.util.Arrays;
import java.util.Objects;

/**
 * @Author: 711lxsky
 * @Description: 客户端实现类
 */

public class Client {

    private final Packager packager;

    public Client(Packager packager){
        this.packager = packager;
    }

    public byte[] execute(byte[] data) throws Exception {
        DataPackage dataPackage = new DataPackage(data, null);
        this.packager.send(dataPackage);
        DataPackage response = this.packager.receive();
        if(Objects.nonNull(response.getException())){
            throw response.getException();
        }
        return response.getData();
    }

    public void close() {
        try {
            this.packager.close();
        }
        catch (Exception e){
            Log.logInfo(e.getMessage() + "      " + Arrays.toString(e.getStackTrace()));
        }
    }

}
