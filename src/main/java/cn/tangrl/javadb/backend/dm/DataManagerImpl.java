package cn.tangrl.javadb.backend.dm;

import cn.tangrl.javadb.backend.common.AbstractCache;
import cn.tangrl.javadb.backend.dm.dataItem.DataItem;
import cn.tangrl.javadb.backend.dm.dataItem.DataItemImpl;
import cn.tangrl.javadb.backend.dm.logger.Logger;
import cn.tangrl.javadb.backend.dm.page.Page;
import cn.tangrl.javadb.backend.dm.page.PageOne;
import cn.tangrl.javadb.backend.dm.page.PageX;
import cn.tangrl.javadb.backend.dm.pageCache.PageCache;
import cn.tangrl.javadb.backend.dm.pageIndex.PageIndex;
import cn.tangrl.javadb.backend.dm.pageIndex.PageInfo;
import cn.tangrl.javadb.backend.tm.TransactionManager;
import cn.tangrl.javadb.backend.utils.Panic;
import cn.tangrl.javadb.backend.utils.Types;
import cn.tangrl.javadb.common.Error;

/**
 * DM模块实现类
 * 继承AbstractCache类和实现DataManager接口
 * DataManager 是 DM 层直接对外提供方法的类，同时，也实现成 DataItem 对象的缓存。
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {
    /**
     * TM模块对象，事务管理，与xid文件相关
     */
    TransactionManager tm;
    /**
     * PageCache对象，缓存页面用，与db文件相关
     */
    PageCache pc;
    /**
     * Logger对象，日志功能用
     */
    Logger logger;
    /**
     * PageIndex对象，快速查找插入位置用
     */
    PageIndex pIndex;
    /**
     * 磁盘db文件的第一页对象
     */
    Page pageOne;

    /**
     * 构造函数
     * @param pc
     * @param logger
     * @param tm
     */
    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    /**
     * 读取DataItem，从DataItem cache中读取，key为uid
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        // 如果读取的DataItem非法，则释放
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    // TODO 为什么insert不上锁？
    /**
     * 将有效数据包裹成DataItem并插入到页中
     * 1. 在 pageIndex 中获取一个足以存储插入内容的页面的页号
     * 2. 获取页面后，首先需要写入插入日志
     * 3. 通过 pageX 插入数据，并返回插入位置的偏移
     * 4. 最后需要将页面信息重新插入 pageIndex
     * @param xid
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        // 将有效数据包裹成DataItem
        byte[] raw = DataItem.wrapDataItemRaw(data);
        // 如果DataItem超过了一页可以放入的大小，抛出异常
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }
        // 1.在 pageIndex 中获取一个足以存储插入内容的页面的页号
        // for循环的作用是用于找不到时，创建新页。
        // 循环5次是为了避免一创建就被别的线程抢用的情况，若抢用了再创建。若五次都这样，则报错。
        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                // 如果找不到则创建一个新页，并将新页的数据添加到pageIndex中
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }
        // 2.获取页面后，首先需要写入插入日志
        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);
            // 3. 通过 pageX 插入数据，并返回插入位置的偏移
            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 4. 最后需要将页面信息重新插入 pageIndex
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    /**
     * 关闭资源
     * 注意super的调用
     */
    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    /**
     * 为xid生成update日志
     * @param xid
     * @param di
     */
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    /**
     * 从PageCache中解析处DataItem
     * 从 key（uid） 中解析出页号，从 pageCache 中获取到页面，再根据偏移，解析出 DataItem
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    /**
     * DataItem 缓存释放，需要将 DataItem 写回数据源，由于对文件的读写是以页为单位进行的，只需要将 DataItem 所在的页 release 即可：
     * @param di
     */
    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    /**
     * 初始化第一页
     */
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    /**
     * 在打开已有文件时时读入PageOne，并验证正确性
     * @return
     */
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /**
     * 初始化pageIndex对象
     * 读取每页，利用页号和页的空闲空间来构建
     */
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }
    
}
