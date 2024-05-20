package cn.tangrl.javadb.backend.dm.pageCache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import cn.tangrl.javadb.backend.common.AbstractCache;
import cn.tangrl.javadb.backend.dm.page.Page;
import cn.tangrl.javadb.backend.dm.page.PageImpl;
import cn.tangrl.javadb.backend.utils.Panic;
import cn.tangrl.javadb.common.Error;

/**
 * 页面缓存实现类，继承抽象缓存框架类（泛型为Page类），实现页面缓存接口，
 * Page类是存在内存中的页，有数据
 * 需要继承抽象缓存框架 AbstractCache，并且实现 getForCache() 和 releaseForCache() 两个抽象方法
 * 会定义File对象，以便从文件中读取数据到缓存，和将缓存写入到文件
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    /**
     * 缓存中的资源数量下限值
     */
    private static final int MEM_MIN_LIM = 10;
    /**
     * 数据库文件后缀名
     */
    public static final String DB_SUFFIX = ".db";
    /**
     * 数据库文件的RandomAccessFile对象
     */
    private RandomAccessFile file;
    /**
     * RandomAccessFile对象的FileChannel对象
     */
    private FileChannel fc;
    /**
     * 互斥锁
     */
    private Lock fileLock;
    /**
     * 数据库文件页数
     * 这个数字在数据库文件被打开时就会被计算，并在新建页面时自增
     * 原子整型
     */
    private AtomicInteger pageNumbers;

    /**
     * 页面缓存实现类的构造函数
     * @param file
     * @param fileChannel
     * @param maxResource
     */
    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    /**
     * 创建一个新页面，传入initData byte数组，返回数据库文件页数量
     * @param initData
     * @return
     */
    public int newPage(byte[] initData) {
        // 增加数据库文件页数，原子操作
        int pgno = pageNumbers.incrementAndGet();
        // 创建一个新页
        Page pg = new PageImpl(pgno, initData, null);
        // 新建的页面需要立刻写回到磁盘文件中
        flush(pg);
        return pgno;
    }

    /**
     * 获取数据库文件的页数
     * @param pgno
     * @return
     * @throws Exception
     */
    public Page getPage(int pgno) throws Exception {
        return get((long)pgno);
    }

    /**
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page对象返回
     * key即页号
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = PageCacheImpl.pageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        // 读取db文件的操作需要上锁
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno, buf.array(), this);
    }

    /**
     * 将缓存写回到db文件中
     * 如果是脏页面，则调用这个类中的flunsh方法
     * @param pg
     */
    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }

    /**
     * 释放页，会缓存中对应的计数，传参为Page对象
     * 调用Abstract类的release方法 强行释放一个缓存，传参为页号
     * @param page
     */
    public void release(Page page) {
        release((long)page.getPageNumber());
    }

    /**
     * 将Page写回到磁盘文件中
     * 调用下面的flush方法
     * @param pg
     */
    public void flushPage(Page pg) {
        flush(pg);
    }

    /**
     * 将页面写回到磁盘文件中
     * @param pg
     */
    private void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        // 将页面写入到磁盘到操作需要上锁
        // force 立即刷新到磁盘中
        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * 将文件进行截断，传参数为maxPgno 最大的页号
     * 获取偏移量，将数据进行截断，并将文件的页数设置为maxPgno
     * @param maxPgno
     */
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    /**
     * 调用父类的close方法，并关闭这个对象中定义的文件资源
     */
    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 获取当前数据库文件的页数
     * @return
     */
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    /**
     * 获取当前页在文件中的偏移量
     * @param pgno
     * @return
     */
    private static long pageOffset(int pgno) {
        return (pgno-1) * PAGE_SIZE;
    }
    
}
