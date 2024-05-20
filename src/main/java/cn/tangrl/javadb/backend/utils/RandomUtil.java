package cn.tangrl.javadb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * 随机方法工具类
 */
public class RandomUtil {
    /**
     * 随机生成传参length大小的byte数组
     * @param length
     * @return
     */
    public static byte[] randomBytes(int length) {
        Random r = new SecureRandom();
        byte[] buf = new byte[length];
        r.nextBytes(buf);
        return buf;
    }
}
