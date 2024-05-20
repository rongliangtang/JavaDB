package cn.tangrl.javadb.backend.dm;

import cn.tangrl.javadb.backend.dm.dataItem.DataItem;
import cn.tangrl.javadb.backend.dm.logger.Logger;
import cn.tangrl.javadb.backend.dm.page.PageOne;
import cn.tangrl.javadb.backend.dm.pageCache.PageCache;
import cn.tangrl.javadb.backend.tm.TransactionManager;

/**
 * DM 模块的接口
 * DM的功能1:上层模块和文件系统之间的一个抽象层，向下直接读写文件，向上提供数据的包装
 * DM的功能2:日志功能
 * 需要注意的是，无论是向上还是向下，DM 都提供了一个缓存的功能，用内存操作来保证效率
 */
public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    /**
     * 创建日志文件和db文件的工厂静态方法，返回 DataManagerImpl 对象
     * @param path
     * @param mem PageCache可用的内存数量，单位为byte
     * @param tm
     * @return
     */
    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);
        // 创建 DataManagerImpl 对象
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        // 创建第一页
        dm.initPageOne();
        return dm;
    }

    /**
     * 打开日志文件和db文件的工厂静态方法，返回 DataManagerImpl 对象
     * @param path
     * @param mem
     * @param tm
     * @return
     */
    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        // 检查db文件的正确性，如果不正确则进行日志恢复
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        // 初始化pageIndex
        // 获取所有页面并填充 PageIndex
        // open的时候才执行
        // create的时候不执行这步，因为初始化为0，没有页
        dm.fillPageIndex();
        // 设置第一页的初始字节，并写回到磁盘文件中
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
