package cn.tangrl.javadb.backend.dm.pageCache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import cn.tangrl.javadb.backend.dm.page.Page;
import cn.tangrl.javadb.backend.utils.Panic;
import cn.tangrl.javadb.common.Error;

/**
 * 页面缓存接口
 */
public interface PageCache {

    /**
     * 页的大小，8KB
     */
    public static final int PAGE_SIZE = 1 << 13;

    /**
     * 创建一个新页面，传入initData byte数组，返回数据库文件页数量
     * @param initData
     * @return
     */
    int newPage(byte[] initData);

    /**
     * 获取数据库文件的页数
     * @param pgno
     * @return
     * @throws Exception
     */
    Page getPage(int pgno) throws Exception;

    /**
     * 关闭资源
     */
    void close();

    /**
     * 释放页，会缓存中对应的计数，传参为Page对象
     * @param page
     */
    void release(Page page);

    /**
     * 将文件进行截断，传参数为maxPgno 最大的页号
     * @param maxPgno
     */
    void truncateByBgno(int maxPgno);

    /**
     * 获取文件的页数
     * @return
     */
    int getPageNumber();

    /**
     * 将Page写回到磁盘文件中
     * @param pg
     */
    void flushPage(Page pg);

    /**
     * 创建db文件的静态工厂类，返回PageCacheImpl对象
     * 实现过程与TM模块一样
     * @param path
     * @param memory
     * @return
     */
    public static PageCacheImpl create(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

    /**
     * 打开db文件的静态工厂类，返回PageCacheImpl对象
     * 实现过程与TM模块一样
     * @param path
     * @param memory
     * @return
     */
    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }
}
