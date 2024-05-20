package cn.tangrl.javadb.backend.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import cn.tangrl.javadb.backend.dm.pageCache.PageCache;

/**
 * 存储在内存中的页面，实现页面接口
 */
public class PageImpl implements Page {
    /**
     * 页号，从1开始
     * 即使在文件中的页号
     */
    private int pageNumber;
    /**
     * 这个页实际包含的字节数据
     */
    private byte[] data;
    /**
     * 这个页面是否脏
     */
    private boolean dirty;
    /**
     * 互斥锁
     */
    private Lock lock;
    /**
     * 页面缓存对象引用
     * 方便在拿到 Page 的引用时可以快速对这个页面的缓存进行释放操作。
     */
    private PageCache pc;

    /**
     * 构造函数
     * @param pageNumber
     * @param data
     * @param pc
     */
    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    /**
     * 上锁 方法
     */
    public void lock() {
        lock.lock();
    }

    /**
     * 解锁 方法
     */
    public void unlock() {
        lock.unlock();
    }

    /**
     * 释放页面缓存 方法
     */
    public void release() {
        pc.release(this);
    }

    /**
     * 设置为脏页面 方法
     * @param dirty
     */
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    /**
     * 判断页面是否脏 方法
     * @return
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * 获取页号方法
     * @return
     */
    public int getPageNumber() {
        return pageNumber;
    }

    /**
     * 获取数据方法
     * @return
     */
    public byte[] getData() {
        return data;
    }

}
