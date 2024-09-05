package cn.tangrl.javadb.backend.dm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.primitives.Bytes;

import cn.tangrl.javadb.backend.common.SubArray;
import cn.tangrl.javadb.backend.dm.dataItem.DataItem;
import cn.tangrl.javadb.backend.dm.logger.Logger;
import cn.tangrl.javadb.backend.dm.page.Page;
import cn.tangrl.javadb.backend.dm.page.PageX;
import cn.tangrl.javadb.backend.dm.pageCache.PageCache;
import cn.tangrl.javadb.backend.tm.TransactionManager;
import cn.tangrl.javadb.backend.utils.Panic;
import cn.tangrl.javadb.backend.utils.Parser;

/**
 * 根据日志恢复数据库类
 * DM 为上层模块，提供了两种操作，分别是插入新数据（I）和更新现有数据（U）。
 * 在进行 I 和 U 操作之前，必须先进行对应的日志操作，在保证日志写入磁盘后，才进行数据操作。
 * 注意单线程和多线程的恢复策略区别，多线程下要满足什么规定，及策略。
 */
public class Recover {
    /**
     * 插入日志的格式
     */
    private static final byte LOG_TYPE_INSERT = 0;
    /**
     * 更新日志的格式
     */
    private static final byte LOG_TYPE_UPDATE = 1;
    /**
     * 重做标志
     */
    private static final int REDO = 0;
    /**
     * 撤销标志
     */
    private static final int UNDO = 1;

    /**
     * 插入日志格式的对象，静态类
     * [LogType] [XID] [Pgno] [Offset] [Raw]
     */
    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    /**
     * 更新日志格式的对象，静态类
     * [LogType] [XID] [UID] [OldRaw] [NewRaw]
     */
    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    /**
     * 进行日志恢复操作
     * @param tm
     * @param lg
     * @param pc
     */
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");
        // 取出最大有效页号，将BadTail截断掉
        lg.rewind();
        int maxPgno = 0;
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            int pgno;
            // 如果日志是插入日志，进行解析
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                // 如果日志是更新日志，进行解析
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if(maxPgno == 0) {
            maxPgno = 1;
        }
        pc.truncateByBgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        // 重做所有崩溃时已完成（committed 或 aborted）的事务
        // 保证持久性
        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        // 撤销所有崩溃时未完成（active）的事务
        // 保证原子性
        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    /**
     * 重做事务所有已经完成（committed、abort）事务
     * @param tm
     * @param lg
     * @param pc
     */
    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        // 将postion移动到第一条日志的起点
        lg.rewind();
        // 按顺序读取每条日志并处理
        while(true) {
            // 读取日志，如果为空则结束
            byte[] log = lg.next();
            if(log == null) break;
            // 如果这条日志是插入日志，进行解析
            // 如果这条日志对应的事务是完成的则重做
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                // 如果这条日志是更新日志，进行解析
                // 如果这条日志对应的事务是完成的则重做
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    /**
     * 撤销未完成（activate）事务
     * @param tm
     * @param lg
     * @param pc
     */
    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        // 创建一个哈希表来存放日志缓存
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        // 将postion移动到第一条日志的起点
        lg.rewind();
        // 按顺序读取每条日志并处理
        while(true) {
            // 读取日志，如果为空则结束
            byte[] log = lg.next();
            if(log == null) break;
            // 如果这条日志是插入日志，进行解析
            // 如果这条日志对应的事务是未完成则将其以xid：log的格式放入到logCache中
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                // 如果这条日志是更新日志，进行解析
                // 如果这条日志对应的事务是未完成则将其以xid：log的格式放入到logCache中
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 在logCache中取出所有未完成事务的日志
        // 将未完成事务按日志倒序进行undo
        // 事务是不按顺序的
        for(Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            // 在xid文件中设置对应xid事务的状态回滚
            tm.abort(entry.getKey());
        }
    }

    /**
     * 取出log有效数据中第一个字节，判断类型是否为insert
     * @param log
     * @return
     */
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    /**
     * [LogType] [XID] [UID] [OldRaw] [NewRaw]
     * 下面的属性为update日志对应数据的起始位置
     */
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE+1;
    private static final int OF_UPDATE_UID = OF_XID+8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8;

    /**
     * 将xid和DataItem包裹成updateLog的字节数组
     * @param xid
     * @param di
     * @return
     */
    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    /**
     * 将字节数组解析成UpdateLogInfo对象
     * @param log
     * @return
     */
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }

    /**
     * 更新日志的操作
     * 1. 获取写入的数据
     * 当flag为REDO时，进行不同的解析 raw = xi.newRaw;
     * 当flag为UNDO时，进行不同的解析 raw = xi.oldRaw;
     * 2. 获取对应的page
     * 3. 将数据写入到page对应的位置上（数据即DataItem的数据）
     * 4. 释放Page缓存
     * @param pc
     * @param log
     * @param flag
     */
    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        // 获取写入的数据
        if(flag == REDO) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        // 获取对应的page
        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        // 将数据写入到page对应的位置上（数据即DataItem的数据）
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    /**
     * [LogType] [XID] [Pgno] [Offset] [Raw]
     * 下面的属性为insert日志对应数据的起始位置
     */
    private static final int OF_INSERT_PGNO = OF_XID+8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2;

    /**
     * 将一个insert操作按inset日志的格式组织成一个byte数组
     * @param xid
     * @param pg
     * @param raw
     * @return
     */
    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    /**
     * 将log有效数据包装成InsertLogInfo对象
     * @param log
     * @return
     */
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    /**
     * 对insert log执行 flag 类型的操作
     * flag为redo or undo
     * @param pc
     * @param log
     * @param flag
     */
    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        // 解析对象
        InsertLogInfo li = parseInsertLog(log);
        // 从PageCache中获取Page对象
        Page pg = null;
        try {
            pg = pc.getPage(li.pgno);
        } catch(Exception e) {
            Panic.panic(e);
        }
        try {
            // 如果flag为UNDO，则插入的数据
            if(flag == UNDO) {
                // 将该条 DataItem 的有效位设置为无效，来进行逻辑删除。
                DataItem.setDataItemRawInvalid(li.raw);
            }
            // 在对应page的offset中插入数据
            // 为什么undo和redo都执行？
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }
}
