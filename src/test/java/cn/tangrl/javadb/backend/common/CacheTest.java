package cn.tangrl.javadb.backend.common;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import cn.tangrl.javadb.backend.utils.Panic;
import cn.tangrl.javadb.common.Error;

/**
 * Cache测试类
 */
public class CacheTest {

    static Random random = new SecureRandom();

    private CountDownLatch cdl;
    private MockCache cache;

    /**
     * 测试多线程下操作Cache
     */
    @Test
    public void testCache() {
        cache = new MockCache();
        cdl = new CountDownLatch(200);
        for(int i = 0; i < 200; i ++) {
            Runnable r = () -> work();
            new Thread(r).run();
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 操作方法
     * 循环1000次，get后比较数值是否正确，然后release
     */
    private void work() {
        for(int i = 0; i < 1000; i++) {
            long uid = random.nextInt();
            long h = 0;
            try {
                h = cache.get(uid);
            } catch (Exception e) {
                if(e == Error.CacheFullException) continue;
                Panic.panic(e);
            }
            assert h == uid;
            cache.release(h);
        }
        cdl.countDown();
    }
}
