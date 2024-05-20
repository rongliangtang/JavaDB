package cn.tangrl.javadb.backend.dm;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import cn.tangrl.javadb.backend.common.SubArray;
import cn.tangrl.javadb.backend.dm.dataItem.DataItem;
import cn.tangrl.javadb.backend.dm.dataItem.MockDataItem;

/**
 * 模拟DM
 * 用于模拟数据管理器的功能，主要包括数据的读取和插入操作
 * 使用了一个简单的缓存机制（HashMap）来存储DataItem，并使用显式锁（ReentrantLock）来确保线程安全
 * 通过这种方式，可以在不涉及实际复杂数据管理操作的情况下测试或模拟DM的行为
 */
public class MockDataManager implements DataManager {

    private Map<Long, DataItem> cache;
    private Lock lock;

    public static MockDataManager newMockDataManager() {
        MockDataManager dm = new MockDataManager();
        dm.cache = new HashMap<>();
        dm.lock = new ReentrantLock();
        return dm;
    }

    @Override
    public DataItem read(long uid) throws Exception {
        lock.lock();
        try {
            return cache.get(uid);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        try {
            long uid = 0;
            while(true) {
                uid = Math.abs(new Random(System.nanoTime()).nextInt(Integer.MAX_VALUE));
                if(uid == 0) continue;
                if(cache.containsKey(uid)) continue;
                break;
            }
            DataItem di = MockDataItem.newMockDataItem(uid, new SubArray(data, 0, data.length));
            cache.put(uid, di);
            return uid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {}
    
}
