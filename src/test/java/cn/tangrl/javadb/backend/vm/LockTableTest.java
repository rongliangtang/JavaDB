package cn.tangrl.javadb.backend.vm;

import static org.junit.Assert.assertThrows;

import java.util.concurrent.locks.Lock;

import org.junit.Test;

import cn.tangrl.javadb.backend.utils.Panic;

/**
 * 依赖图测试类
 */
public class LockTableTest {
    /**
     * 单线程测试方法，测试基本功能
     */
    @Test
    public void testLockTable() {
        LockTable lt = new LockTable();
        try {
            lt.add(1, 1);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            lt.add(2, 2);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            lt.add(2, 1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        
        assertThrows(RuntimeException.class, ()->lt.add(1, 2));
    }

    /**
     * 多线程测试方法
     */
    @Test
    public void testLockTable2() {
        LockTable lt = new LockTable();
        for(long i = 1; i <= 100; i ++) {
            try {
                // 添加边到图中，不会产生依赖关系
                Lock o = lt.add(i, i);
                if(o != null) {
                    Runnable r = () -> {
                        // 尝试上锁
                        o.lock();
                        // 解锁
                        o.unlock();
                    };
                    new Thread(r).run();
                }
            } catch (Exception e) {
                Panic.panic(e);
            }
        }

        for(long i = 1; i <= 99; i ++) {
            try {
                // 添加边到图中，会产生依赖
                Lock o = lt.add(i, i+1);
                if(o != null) {
                    Runnable r = () -> {
                        o.lock();
                        o.unlock();
                    };
                    new Thread(r).run();
                }
            } catch (Exception e) {
                Panic.panic(e);
            }
        }
        // 尝试形成个环，不会形成环
        assertThrows(RuntimeException.class, ()->lt.add(100, 1));
        // 移除一个，分配资源
        lt.remove(23);

        // 尝试形成个环，不会形成环
        try {
            lt.add(100, 1);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }
}
