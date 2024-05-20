package cn.tangrl.javadb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import cn.tangrl.javadb.common.Error;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 * LRU资源驱逐不可控，上层模块无法感知，难以决定回源操作。
 */
public abstract class AbstractCache<T> {
    /**
     * 实际缓存的数据
     */
    private HashMap<Long, T> cache;
    /**
     * 元素的引用个数
     */
    private HashMap<Long, Integer> references;
    /**
     * 正在被其他线程从数据源（硬盘）中获取的缓存
     */
    private HashMap<Long, Boolean> getting;
    /**
     * 缓存的最大缓存资源数
     */
    private int maxResource;
    /**
     * 缓存中元素的个数，即key的数量
     */
    private int count = 0;
    /**
     * 互斥锁
     */
    private Lock lock;

    /**
     * 构造函数，传入参数 maxResource 表示最大缓存数量限制
     * 初始化属性
     * @param maxResource
     */
    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 从缓存中获取资源
     * 涉及到哈希表的操作都要上锁
     * @param key
     * @return
     * @throws Exception
     */
    protected T get(long key) throws Exception {
        while(true) {
            // 上锁
            lock.lock();
            // 如果请求的资源正在被其他线程从数据源（硬盘）中获取，延迟1毫秒
            if(getting.containsKey(key)) {
                // 解锁
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            // 如果资源在缓存中，从缓存中获取返回
            if(cache.containsKey(key)) {

                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                // 解锁
                lock.unlock();
                return obj;
            }

            // 缓存装满了，抛出异常
            if(maxResource > 0 && count == maxResource) {
                // 解锁
                lock.unlock();
                throw Error.CacheFullException;
            }

            // 不在缓存中，标注有线程准备在从资源中获取这个缓存，并退出循环。
            count ++;
            getting.put(key, true);
            // 解锁
            lock.unlock();
            break;
        }

        // 从数据源（硬盘）中获取资源，并加入到缓存中
        T obj = null;
        try {
            obj = getForCache(key);
        } catch(Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        // 上锁
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        // 解锁
        lock.unlock();
        
        return obj;
    }

    /**
     * 强行释放一个缓存
     * 直接从 references 中减 1，如果已经减到 0 了，就可以回源，并且删除缓存中所有相关的结构
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key)-1;
            if(ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count --;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 抽象方法：当资源不在缓存时的获取行为，从数据源（硬盘）中读
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 抽象方法：当资源被驱逐时的写回行为，写回数据源（硬盘）中
     */
    protected abstract void releaseForCache(T obj);
}
