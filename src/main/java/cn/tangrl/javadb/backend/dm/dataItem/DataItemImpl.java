package cn.tangrl.javadb.backend.dm.dataItem;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import cn.tangrl.javadb.backend.common.SubArray;
import cn.tangrl.javadb.backend.dm.DataManagerImpl;
import cn.tangrl.javadb.backend.dm.page.Page;

/**
 * DataItem 实现类
 * 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem {
    /**
     * ValidFlag 数据开始的起点
     * ValidFlag 占用 1 字节，标识了该 DataItem 是否有效。删除一个 DataItem，只需要简单地将其有效位设置为 0。
     */
    static final int OF_VALID = 0;
    /**
     * DataSize 数据开始的起点
     * DataSize 占用 2 字节，标识了后面 Data 的长度。
     */
    static final int OF_SIZE = 1;
    /**
     * Data 数据开始的起点
     */
    static final int OF_DATA = 3;
    /**
     * 存放DataItem的对象
     * 使用SubArray是为了取出共享数据方便
     */
    private SubArray raw;
    /**
     * 用于存放就数据的数据，大小与DataItem一样
     */
    private byte[] oldRaw;
    /**
     * ReentrantReadWriteLock的读锁，允许多个线程读，会阻塞写进程
     */
    private Lock rLock;
    /**
     * ReentrantReadWriteLock的写锁，一个线程写时会阻塞所有读写进程
     */
    private Lock wLock;
    /**
     * 保存一个 dm 的引用是因为其释放依赖 dm 的释放（dm 同时实现了缓存接口，用于缓存 DataItem），以及修改数据时落日志
     */
    private DataManagerImpl dm;
    /**
     * 唯一id，用于AbstractCache中的key
     */
    private long uid;
    /**
     * 页面对象引用
     */
    private Page pg;

    /**
     * 构造函数
     * @param raw
     * @param oldRaw
     * @param pg
     * @param uid
     * @param dm
     */
    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    /**
     * 判断DataItem是否合法
     * @return
     */
    public boolean isValid() {
        return raw.raw[raw.start+OF_VALID] == (byte)0;
    }

    /**
     * 上层模块在获取到 DataItem 后，可以通过 data() 方法，该方法返回的数组是数据共享的，而不是拷贝实现的，所以使用了 SubArray。
     * @return
     */
    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start+OF_DATA, raw.end);
    }

    /**
     * 在上层模块试图对 DataItem 进行修改时，需要遵循一定的流程
     * 1.在修改之前需要调用 before() 方法
     * 2.想要撤销修改时，调用 unBefore() 方法
     * 3.在修改完成后，调用 after() 方法
     * 整个流程，主要是为了保存旧数据，并及时落日志。DM 会保证对 DataItem 的修改是原子性的。
     */

    /**
     * 在修改之前需要调用 before() 方法
     */
    @Override
    public void before() {
        // 对写锁上锁
        wLock.lock();
        // 设置对应的page为脏
        pg.setDirty(true);
        // 将旧数据复制到oldRaw数组中
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    /**
     * 想要撤销修改时，调用 unBefore() 方法
     */
    @Override
    public void unBefore() {
        // 将旧数据复制回现数据中
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        // 对写锁解锁
        wLock.unlock();
    }

    /**
     * 在修改完成后，调用 after() 方法
     * 调用 dm 中的一个方法，对修改操作落日志
     * @param xid
     */
    @Override
    public void after(long xid) {
        // 对修改操作落日志
        dm.logDataItem(xid, this);
        // 对写锁解锁
        wLock.unlock();
    }

    /**
     * 使用完 DataItem 后，也应当及时调用 release() 方法，释放掉 DataItem 的缓存（由 DM 缓存的 DataItem，基础AbstractCache实现）。
     */
    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    /**
     * 上写锁
     */
    @Override
    public void lock() {
        wLock.lock();
    }

    /**
     * 释放写锁
     */
    @Override
    public void unlock() {
        wLock.unlock();
    }

    /**
     * 上读锁
     */
    @Override
    public void rLock() {
        rLock.lock();
    }

    /**
     * 释放读锁
     */
    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    /**
     * 获取Page对象
     * @return
     */
    @Override
    public Page page() {
        return pg;
    }

    /**
     * 获取uid
     * @return
     */
    @Override
    public long getUid() {
        return uid;
    }

    /**
     * 获取旧数据
     * @return
     */
    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    /**
     * 获得DataItem的数据
     * @return
     */
    @Override
    public SubArray getRaw() {
        return raw;
    }
    
}
