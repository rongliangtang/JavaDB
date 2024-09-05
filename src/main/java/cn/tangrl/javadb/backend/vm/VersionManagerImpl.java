package cn.tangrl.javadb.backend.vm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import cn.tangrl.javadb.backend.common.AbstractCache;
import cn.tangrl.javadb.backend.dm.DataManager;
import cn.tangrl.javadb.backend.tm.TransactionManager;
import cn.tangrl.javadb.backend.tm.TransactionManagerImpl;
import cn.tangrl.javadb.backend.utils.Panic;
import cn.tangrl.javadb.common.Error;

/**
 * VM模块的实现类
 * 继承AbstractCache<Entry>，会对entry进行缓存
 * 实现VersionManager接口
 * VM 层通过 VersionManager 接口，向上层提供功能
 */
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {
    /**
     * TM对象引用
     */
    TransactionManager tm;
    /**
     * DM对象引用
     */
    DataManager dm;
    /**
     * activeTransaction哈希表
     */
    Map<Long, Transaction> activeTransaction;
    /**
     * 对属性操作的锁
     */
    Lock lock;
    /**
     * 用于检测死锁的依赖等待图
     */
    LockTable lt;

    /**
     * 构造函数
     * @param tm
     * @param dm
     */
    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        // 将超级事务放进activeTransaction哈希表中
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    /**
     * 读取一个 entry，传入xid和entry的uid
     * 注意可见性的判断
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            // 调用AbstratCache的get，cache没有的话会调用getforcache从硬盘文件中取
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            // 如果enrty对t是可见的，返回数据，否则返回null
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    /**
     * 将数据包裹成 Entry，无脑交给 DM 插入，返回 DataItem 的 uid
     * @param xid
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    /**
     * 删除entry，传入xid和uid，返回true表示成功删除。
     * 删除和更新操作的时候回执行这个方法。
     * 1. 可见性判断
     * 2. 获取资源的锁
     * 3. 版本跳跃判断
     * 4. 删除的操作只有一个设置 XMAX
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            // 如果不可见，返回false
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            // 获取资源的锁
            Lock l = null;
            try {
                // 往locktable中添加变，如果抛出异常，说明有死锁。
                // 这步的目的是判断是否有别的线程在用这个资源
                // TODO add后会不会没有remove把边去掉？delete后这个entry，这条记录就是旧版本了，会有新版本的entry出现。
                l = lt.add(xid, uid);
            } catch(Exception e) {
                // 进行自动回滚
                // 设置t的err属性并抛出异常
                t.err = Error.ConcurrentUpdateException;
                // 回滚操作会执行locktable的remove方法
                internAbort(xid, true);
                // 设置t的autoAborted属性为true
                t.autoAborted = true;
                throw t.err;
            }
            // 如果可以获得锁，说明需要等待
            if(l != null) {
                // 上锁，如果l被其他线程持有则会阻塞当前线程
                // 这一步的目的是看是否有其他线程在用锁
                l.lock();
                // 解锁
                l.unlock();
            }

            if(entry.getXmax() == xid) {
                return false;
            }
            // 如果存在版本跳跃问题，进行事务回滚
            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            // 设置这条记录的xmax字段为xid
            entry.setXmax(xid);
            return true;

        } finally {
            // 一定会执行的释放entry缓存方法
            entry.release();
        }
    }

    /**
     * 开启一个事务，并初始化事务的结构，将其存放在 activeTransaction 中，用于检查和快照使用
     * @param level
     * @return
     */
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 提交一个事务，主要就是 free 掉相关的结构，并且释放持有的锁，并修改 TM 状态
     * @param xid
     * @throws Exception
     */
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        // 从activeTransaction哈希表中获取事务t
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            // 如果事务t的err非空，抛出异常
            if(t.err != null) {
                throw t.err;
            }
        } catch(NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock();
        // 从activeTransaction哈希表中移除事务t
        activeTransaction.remove(xid);
        lock.unlock();
        // LockTable依赖等待图对象中移除xid
        lt.remove(xid);
        // 调用tm模块的comit提交事务xid设置状态
        tm.commit(xid);
    }

    /**
     * 手动回滚
     * abort 事务的方法则有两种，手动和自动。
     * 手动指的是调用 abort() 方法，而自动则是在事务被检测出出现死锁时，会自动撤销回滚事务；或者出现版本跳跃时，也会自动回滚。
     * @param xid
     */
    @Override
    public void abort(long xid) {
        // 传入false
        internAbort(xid, false);
    }

    /**
     * 回滚操作的具体实现
     * 手动回滚，传入false
     * 自动回滚，传入true
     * 传入的autoAborted为false，说明正在进行手动回滚，activeTransaction哈希表中存在记录，需要去除
     * @param xid
     * @param autoAborted
     */
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        // 如果autoAborted为false，则是进行手动回滚，此时activeTransaction哈希表中存在记录，需要移除
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();
        // 如果事务t里面的autoAborted为true，则return。说明事务t进行自动回滚了。
        if(t.autoAborted) return;
        // remove()可以释放所有locktable中它持有的锁，并将自身从等待图中删除，会进行资源分配。
        lt.remove(xid);
        // 调用tm模块的abort回滚事务xid设置状态
        tm.abort(xid);
    }

    /**
     * 强行释放一个缓存，调用AbstractCache中的release
     * @param entry
     */
    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    /**
     * cache中没有时，获取entry的方法
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    /**
     * 当资源被驱逐时的写回行为，写回数据源（硬盘）中
     * @param entry
     */
    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }
    
}
