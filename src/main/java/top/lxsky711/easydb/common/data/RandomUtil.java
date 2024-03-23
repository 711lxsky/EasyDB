package top.lxsky711.easydb.common.data;

import java.security.SecureRandom;
import java.util.Random;

/**
 * @Author: 711lxsky
 * @Description: 随机生成器
 */

public class RandomUtil {

    /**
     * @Author: 711lxsky
     * @Description: 随机生成一个长度为 length 的字节数组
     */
    public static byte[] randomBytes(int length){
        Random random = new SecureRandom();
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }
}
