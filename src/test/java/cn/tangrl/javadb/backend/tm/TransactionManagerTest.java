package cn.tangrl.javadb.backend.tm;

import java.io.File;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

/**
 * TM测试类
 * 测试目的: 该测试通过模拟多线程环境中的事务操作，验证 TransactionManager 的并发处理能力。
 * 事务操作: 工作线程随机执行事务开始、提交、中止和状态查询操作。
 * 同步机制: 使用显式锁 ReentrantLock 和倒计时锁存器 CountDownLatch 保证线程安全和同步。
 * 资源清理: 测试完成后，删除测试文件以清理环境。
 */
public class TransactionManagerTest {
    /**
     * 随机数生成器
     */
    static Random random = new SecureRandom();
    /**
     * 事务数量
     */
    private int transCnt = 0;
    /**
     * 工作线程数
     */
    private int noWorkers = 50;
    /**
     * 每个线程执行的工作数
     */
    private int noWorks = 3000;
    /**
     * 可重入锁
     */
    private Lock lock = new ReentrantLock();
    /**
     * 事务管理器实例
     */
    private TransactionManager tmger;
    /**
     * 用于存储事务ID及其状态的哈希表
     * status为0 则事务为active
     * status为1 则事务为commit
     * status为2 则事务为abort
     */
    private Map<Long, Byte> transMap;
    /**
     * 倒计时锁存器，用于协调线程的执行
     */
    private CountDownLatch cdl;

    /**
     * 测试方法
     */
    @Test
    public void testMultiThread() {
        // 初始化事务管理器
        tmger = TransactionManager.create("/tmp/tranmger_test");
        // 初始化事务映射哈希表，使用线程安全的实现
        transMap = new ConcurrentHashMap<>();
        // 初始化倒计时锁存器，计数为工作线程数
        cdl = new CountDownLatch(noWorkers);
        // 创建并启动工作线程
        for(int i = 0; i < noWorkers; i ++) {
            // 每个线程执行 worker 方法
            Runnable r = () -> worker();
            //  启动线程
            new Thread(r).run();
        }
        try {
            // 主线程等待所有工作线程完成
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 断言删除测试文件
        assert new File("/tmp/tranmger_test.xid").delete();
    }

    /**
     * 辅助方法
     */
    private void worker() {
        // 标识是否在事务中
        boolean inTrans = false;
        // 存储当前事务ID
        long transXID = 0;
        // 循环执行 noWorks 次
        for(int i = 0; i < noWorks; i ++) {
            // 生成一个随机操作代码
            int op = Math.abs(random.nextInt(6));
            // 如果 op == 0，尝试开始或结束事务。
            if(op == 0) {
                // 加锁 lock.lock() 保证对map及相关操作的原子性
                lock.lock();
                // 如果未在事务中，开始新事务并记录状态。
                if(inTrans == false) {
                    long xid = tmger.begin();
                    transMap.put(xid, (byte)0);
                    transCnt ++;
                    transXID = xid;
                    inTrans = true;
                } else {
                    // 如果在事务中，随机选择提交或中止事务，更新事务状态。
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch(status) {
                        case 1:
                            tmger.commit(transXID);
                            break;
                        case 2:
                            tmger.abort(transXID);
                            break;
                    }
                    transMap.put(transXID, (byte)status);
                    inTrans = false;
                }
                // 解锁
                lock.unlock();
            } else {
                // 如果 op != 0，查询事务状态
                // 加锁 lock.lock() 保证操作的原子性。
                lock.lock();
                // 如果有事务存在，随机选择一个事务ID，查询其状态并断言状态正确
                if(transCnt > 0) {
                    long xid = (long)((random.nextInt(Integer.MAX_VALUE) % transCnt) + 1);
                    byte status = transMap.get(xid);
                    boolean ok = false;
                    switch (status) {
                        case 0:
                            ok = tmger.isActive(xid);
                            break;
                        case 1:
                            ok = tmger.isCommitted(xid);
                            break;
                        case 2:
                            ok = tmger.isAborted(xid);
                            break;
                    }
                    assert ok;
                }
                // 解锁 lock.unlock()
                lock.unlock();
            }
        }
        // 工作线程完成后，计数减一
        cdl.countDown();
    }
}
