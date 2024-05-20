package cn.tangrl.javadb.backend.vm;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import cn.tangrl.javadb.backend.common.SubArray;
import cn.tangrl.javadb.backend.dm.dataItem.DataItem;
import cn.tangrl.javadb.backend.utils.Parser;

/**
 * Entry类
 * 作用：VM向上层抽象出entry
 * 虽然理论上，MVCC 实现了多版本，但是在实现中，VM 并没有提供 Update 操作，对于字段的更新操作由后面的表和字段管理（TBM）实现。
 * 所以在 VM 的实现中，一条记录只有一个版本。
 * entry结构：
 * [XMIN] [XMAX] [data]
 */
public class Entry {
    /**
     * XMIN数据的起始位置，占8byte，表示创建该条记录（版本）的事务编号
     */
    private static final int OF_XMIN = 0;
    /**
     * XMAX数据的起始位置，占8byte，表示删除该条记录（版本）的事务编号
     */
    private static final int OF_XMAX = OF_XMIN+8;
    /**
     * data数据的起始位置
     */
    private static final int OF_DATA = OF_XMAX+8;
    /**
     * DataItem在cache中的key，根据pgno和offset生成的，也可以表示在硬盘文件中的位置
     */
    private long uid;
    /**
     * DataItem引用
     */
    private DataItem dataItem;
    /**
     * VM引用
     */
    private VersionManager vm;

    /**
     * 创建一条新entry，返回Entry对象
     * @param vm
     * @param dataItem
     * @param uid
     * @return
     */
    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        if (dataItem == null) {
            return null;
        }
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    /**
     * 加载Entry，返回Entry对象
     * 根据uid从dm模块中读取出dataitem，然后调用newEntry()
     * @param vm
     * @param uid
     * @return
     * @throws Exception
     */
    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    /**
     * 包装记录时调用的方法，将xid和data包裹成entry，返回字节数组
     * @param xid
     * @param data
     * @return
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    /**
     * 调用vm的releaseEntry，释放掉entry缓存
     * VersionManagerImpl继承了类AbstractCache
     */
    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    /**
     * 释放dataItem缓存
     */
    public void remove() {
        dataItem.release();
    }

    /**
     * 以拷贝的形式返回data
     * dataitem就是entry，取出entry中的data
     * @return
     */
    public byte[] data() {
        // 对dataItem上读锁
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw, sa.start+OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 获取Xmin
     * 从dataItem中读出
     * @return
     */
    public long getXmin() {
        // 对dataItem上读锁
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMIN, sa.start+OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 获取Xmax
     * 从dataItem中读出
     * @return
     */
    public long getXmax() {
        // 对dataItem上读锁
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMAX, sa.start+OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 设置Xmax
     * 设置dataItem
     * @param xid
     */
    public void setXmax(long xid) {
        // 调用修改dataItem的before()
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start+OF_XMAX, 8);
        } finally {
            // 调用修改dataItem的after()
            dataItem.after(xid);
        }
    }

    /**
     * 获取uid
     * @return
     */
    public long getUid() {
        return uid;
    }
}
