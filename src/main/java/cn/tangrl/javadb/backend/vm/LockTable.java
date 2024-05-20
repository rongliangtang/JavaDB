package cn.tangrl.javadb.backend.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import cn.tangrl.javadb.common.Error;

/**
 * LockTable类
 * 设计目的：
 * 2PL 会阻塞事务，直至持有锁的线程释放锁。可以将这种等待关系抽象成有向边，例如 Tj 在等待 Ti，就可以表示为 Tj –> Ti。
 * 这样，无数有向边就可以形成一个图（不一定是连通图）。
 * 检测死锁也就简单了，只需要查看这个图中是否有环即可。
 * 作用：
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {
    /**
     * 某个XID已经获得的资源的UID列表，UID为entry（dataitem）的uid
     * 一个事务可能操作多个uid entry。
     */
    private Map<Long, List<Long>> x2u;
    /**
     * UID被某个XID持有
     */
    private Map<Long, Long> u2x;
    /**
     * 正在等待UID的XID列表
     */
    private Map<Long, List<Long>> wait;
    /**
     * 正在等待资源的XID的lock
     * 当某个事务 xid 需要等待资源 uid 时，通过锁 l 来管理它的等待状态。通过 l.lock() 将当前线程置于等待状态，当资源可用时，通过 l.unlock() 来唤醒等待的线程。
     */
    private Map<Long, Lock> waitLock;
    /**
     * XID正在等待的UID
     */
    private Map<Long, Long> waitU;
    /**
     * 用于实现互斥访问上面属性的ReentrantLock
     */
    private Lock lock;

    /**
     * 无参构造函数
     */
    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 将xid和uid登记到对应的表中：
     * 如果不需要等待则返回null。
     * 如果会造成死锁则抛出异常，后面的代码不会执行。
     * 如果需要等待且不会造成死锁，会返回一个上了锁的 Lock 对象。
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    public Lock add(long xid, long uid) throws Exception {
        // 对这个类属性的操作上锁
        lock.lock();
        try {
            // 如果事务xid已经获取到uid entry了
            // 则返回null，表示不需要等待
            if(isInList(x2u, xid, uid)) {
                return null;
            }
            // 如果uid entry没有被事务持有，将其登记到x2u和u2x中
            // 则返回null，表示不需要等待
            if(!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            // 此时enrty uid被事务持有，将登记到waitU表中
            waitU.put(xid, uid);
            // putIntoList(wait, xid, uid);
            // 将xid：uid登记到 正在等待UID的XID列表
            putIntoList(wait, uid, xid);
            // 检测是否有死锁发生
            if(hasDeadLock()) {
                // 如果有死锁发生
                // 撤销刚刚waitU和wait的登记
                // 抛出异常，后面的代码不会执行
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            // 创建一个重入锁l
            Lock l = new ReentrantLock();
            // l上锁
            l.lock();
            // 将l添加到waitLock表中
            waitLock.put(xid, l);
            return l;

        } finally {
            // 对这个类属性的操作解锁
            lock.unlock();
        }
    }

    /**
     * 在一个事务 commit 或者 abort 时，remove()可以释放所有它持有的锁，并将自身从等待图中删除，会进行资源分配。
     * @param xid
     */
    public void remove(long xid) {
        lock.lock();
        try {
            // 获取到xid拥有的uid entry列表
            List<Long> l = x2u.get(xid);
            if(l != null) {
                // 将uid entry列表里的uid移除
                // 并调用selectNewXID(uid)将uid分配给需要的事务
                while(l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            // 从等待图中删除这个事务的有关信息
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);

        } finally {
            lock.unlock();
        }
    }

    /**
     * 从等待队列中选择一个xid来占用uid
     * 当一个事务释放资源的时候（remove()），会调用这个函数，将资源给需要的事务。
     * @param uid
     */
    private void selectNewXID(long uid) {
        // 将uid从u2x表中移除（u2x表示UID被某个XID持有）
        u2x.remove(uid);
        // 获取正在等待uid的xid列表（wait表示正在等待UID的XID列表）
        List<Long> l = wait.get(uid);
        // 从等待队列中选择一个xid来占用uid
        if(l == null) return;
        assert l.size() > 0;

        while(l.size() > 0) {
            long xid = l.remove(0);
            // 这个等待的xid应该是有锁的
            if(!waitLock.containsKey(xid)) {
                continue;
            } else {
                // 进行选择，并进行相关的表设置
                u2x.put(uid, xid);
                // 获取锁并进行解锁
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                // 解锁，唤醒需要这个资源的线程。因为线程会调用lo.lock来等待资源的释放。注意：在一个线程中lock和unlock应该配对。
                lo.unlock();
                break;
            }
        }

        if(l.size() == 0) wait.remove(uid);
    }

    /**
     * 查找图中是否有环的算法也非常简单，就是一个深搜，只是需要注意这个图不一定是连通图。
     * 思路就是为每个节点设置一个访问戳，都初始化为 0，随后遍历所有节点，以每个非 0 的节点作为根进行深搜。
     * 并将深搜该连通图中遇到的所有节点都设置为同一个数字，不同的连通图数字不同。
     * 这样，如果在遍历某个图时，遇到了之前遍历过的节点，说明出现了环。
     */
    /**
     * xid：stamp表，每个事务的xid和访问戳表。
     */
    private Map<Long, Integer> xidStamp;
    /**
     * 当前访问戳
     */
    private int stamp;

    /**
     * 检测当前图中是否有死锁
     * @return
     */
    private boolean hasDeadLock() {
        // 初始化表和当前访问戳
        xidStamp = new HashMap<>();
        stamp = 1;
        // 取出x2u（事务xid拥有的uid列表）中的每一个xid进行操作
        for(long xid : x2u.keySet()) {
            // 从xidStamp表中取出stamp
            // 如果stamp非空且>0，表示这个xid遍历过，则处理下一个
            Integer s = xidStamp.get(xid);
            if(s != null && s > 0) {
                continue;
            }
            // 将stamp+1
            stamp ++;
            // 从这个xid出发进行dfs，如果dfs返回true则表示有死锁
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 从xid出发进行dfs
     * 如果深搜的时候，发现某个节点以前遍历过，则有环
     * @param xid
     * @return
     */
    private boolean dfs(long xid) {
        // 从xidStamp表中取出stamp
        // 如果stamp非空且==当前时间戳，表示这个xid在这个dfs中遍历过，说明有环，返回true
        Integer stp = xidStamp.get(xid);
        if(stp != null && stp == stamp) {
            return true;
        }
        // 如果stamp非空且<当前时间戳，表示这个xid在之前dfs遍历过，说明无环，返回false。例如A->B<-C，这里的B。
        if(stp != null && stp < stamp) {
            return false;
        }
        // 将xid和当前时间戳放入到xidStamp表中
        xidStamp.put(xid, stamp);
        // 获取xid等待的uid
        Long uid = waitU.get(xid);
        // 如果没有等待的uid，说明遍历结束，返回false
        if(uid == null) return false;
        // 获取持有uid的xid
        Long x = u2x.get(uid);
        // 如果xid为null会抛出异常
        assert x != null;
        // 对xid调用dfs
        return dfs(x);
    }

    /**
     * 将uid1从uid0的列表中去除
     * Map<uid0, List<uid1>> listMap
     * @param listMap
     * @param uid0
     * @param uid1
     */
    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    /**
     * 将uid1放入到uid0拥有的entry表中
     * Map<uid0, List<uid1>> listMap表示uid0拥有的entry表
     * @param listMap
     * @param uid0
     * @param uid1
     */
    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    /**
     * 判断entry uid1是否是事务uid0已获取的entry
     * Map<uid0, List<uid1>> listMap表示uid0拥有的entry表
     * @param listMap
     * @param uid0
     * @param uid1
     * @return
     */
    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }

}
