package cn.tangrl.javadb.backend.vm;

import java.util.HashMap;
import java.util.Map;

import cn.tangrl.javadb.backend.tm.TransactionManagerImpl;

/**
 * vm对一个事务的抽象类，用于保存快照数据
 * 这个快照存放这个事务t当前activa的事务
 * 在事务 t 开始时，记录下当前活跃的所有事务，用于可重复读隔离级别。
 * 在可重复读隔离级别中，如果记录的某个版本，XMIN 在 t的快照中，也应当对 t 不可见。
 */
public class Transaction {
    /**
     * 事务的xid
     */
    public long xid;
    /**
     * 事务的隔离级别
     * 0 表示 读已提交
     * 1 表示 可重复读
     */
    public int level;
    /**
     * 快照哈希表，为true则表明这个事务还是active
     */
    public Map<Long, Boolean> snapshot;
    /**
     * 异常
     */
    public Exception err;
    /**
     * 是否自动回滚？
     */
    public boolean autoAborted;

    /**
     * 创建新的事务，传入xid、level和active的事务哈希表
     * @param xid
     * @param level
     * @param active
     * @return
     */
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        // 如果level不为0，将active的事务在snapshot哈希表中设置为true
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    /**
     * 判断是否在snapshot哈希表中
     * @param xid
     * @return
     */
    public boolean isInSnapshot(long xid) {
        // 如果xid是超级事务，直接返回true
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
