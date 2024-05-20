package cn.tangrl.javadb.backend.vm;

import cn.tangrl.javadb.backend.tm.TransactionManager;

/**
 * 可见性类
 * 判断Entry的可见性工具类
 * 用处：判断某个版本的entry在某个隔离级别下对某个事务的可见性
 *
 * 如果一个记录的最新版本被加锁，当另一个事务想要修改或读取这条记录时，MYDB 就会返回一个较旧的版本的数据。
 * 这时就可以认为，最新的被加锁的版本，对于另一个事务来说，是不可见的。于是版本可见性的概念就诞生了。
 */
public class Visibility {

    /**
     * 判断事务 t 想要操作的版本entry是否存在版本跳跃问题
     * 用于解决版本跳跃问题
     * 可重复读隔离级别下存在版本跳跃问题，指在MVCC中，一个事务要修改某个数据项时，可能会出现跳过中间版本直接修改最新版本的情况。
     * 当存在版本跳跃问题，需要对事务t进行回滚
     * @param tm
     * @param t
     * @param e
     * @return
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        // 删除版本entry的事务xmax
        long xmax = e.getXmax();
        // 如果事务t的隔离级别是 读已提交
        // 返回false，表示不存在版本跳跃问题
        if(t.level == 0) {
            return false;
        } else {
            // 如果事务t的隔离级别是 可重复读
            // 如果 事务xmax已提交 且 （事务t在事务xmax之前 或 事务xmax在事务t的活跃快照中）
            // 返回true，表示存在版本跳跃问题
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    /**
     * 某个版本记录 e 对事务 t 是否可见
     * 如果事务的level=0，则隔离级别是读已提交级
     * 如果事务的level=0，则隔离级别是可重复读
     * @param tm
     * @param t
     * @param e
     * @return
     */
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 实现 读已提交 下的可见性判断：某个版本记录 e 对事务 t 是否可见
     * 读已提交 隔离级别：事务只能读取到已提交事务产生的数据。
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        // 事务xid
        long xid = t.xid;
        // 获取 xmin修改entru的事务xid 和 xmax删除entry的事务xid
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        // 1. 如果 记录e是由事务t创建且还未删除，则返回true
        if(xmin == xid && xmax == 0) return true;
        // 2. 如果 记录e是由一个已提交的事务创建且尚未删除 或 由一个未提交的事务删除，则返回true
        if(tm.isCommitted(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        // 否则返回false
        return false;
    }

    // TODO 这一块判断可见性的逻辑比较绕，用流程图画出来
    /**
     * 实现 可重复读 下的可见性判断：某个版本记录 e 对事务 t 是否可见
     * 可重复读 隔离级别：事务只能读取它开始时, 就已经结束的那些事务产生的数据版本
     * 所以需要利用到事务 t 的快照，这个快照记录下在事务 t 开始时，活跃的所有事务。
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        // 事务xid
        long xid = t.xid;
        // 获取 xmin修改entry的事务xid 和 xmax删除entry的事务xid
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        // 1. 如果 这个entry是由t修改的，并且没有删除，则是可见的
        if(xmin == xid && xmax == 0) return true;
        // 2. 如果 修改这个entry的事务xmin已提交 且 xmin在事务t之前 且  xmin不在当前t这个事务active事务的快照中
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            // 2.1 且 entry没有删除，则可见
            if(xmax == 0) return true;
            // 2.2 且 删除entry的事务不是t
            if(xmax != xid) {
                // 2.2.1 且 （删除entry的事务没有提交 或 xmax在xid之后 或  xmax在当前t这个事务active事务的快照中），则返回true
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
