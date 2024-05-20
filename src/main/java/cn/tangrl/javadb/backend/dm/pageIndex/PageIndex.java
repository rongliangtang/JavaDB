package cn.tangrl.javadb.backend.dm.pageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import cn.tangrl.javadb.backend.dm.pageCache.PageCache;

/**
 * 页面索引类
 * 页面索引的设计用于提高在数据库中进行插入操作时的效率
 * 它缓存了每一页的空闲空间信息，以便在进行插入操作时能够快速找到合适的页面，而无需遍历磁盘或者缓存中的所有页面
 * 在数据库启动时，会遍历所有页面，将每个页面的空闲空间信息分配到这些区间中
 */
public class PageIndex {
    /**
     * 每页划分的区间数量
     */
    private static final int INTERVALS_NO = 40;
    /**
     * 每个区间的大小
     */
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;
    /**
     * list操作的互斥锁
     */
    private Lock lock;
    /**
     * 存放[[PageInfo，...],[]]
     * 下标表示空闲区块的数量
     * [PageInfo，...]表示拥有下标空闲区块数量的页的信息
     */
    private List<PageInfo>[] lists;

    /**
     * 构造函数
     * 忽略类型检查警告
     */
    @SuppressWarnings("unchecked")
    public PageIndex() {
        // 创建锁
        lock = new ReentrantLock();
        // 创建lists
        // +1的原因是空闲区块数量为0-40
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 将pgno和freeSpace包装成对象添加到list中
     * @param pgno
     * @param freeSpace
     */
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    /**
     * PageIndex 中获取页面
     * 算出需要的区块数量，因为/向下取整，所以+1
     * 从列表中找出>=需要区块数量的第一个元素
     * 注意，被选择的页，会直接从 PageIndex 中移除，这意味着，同一个页面是不允许并发写的。在上层模块使用完这个页面后，需要将其重新插入 PageIndex。
     * @param spaceSize
     * @return
     */
    public PageInfo select(int spaceSize) {
        // 对list操作要上锁
        lock.lock();
        try {
            // 需要的区块数量
            int number = spaceSize / THRESHOLD;
            if(number < INTERVALS_NO) number ++;
            while(number <= INTERVALS_NO) {
                if(lists[number].size() == 0) {
                    number ++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

}
