package cn.tangrl.javadb.backend.tbm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import cn.tangrl.javadb.backend.dm.DataManager;
import cn.tangrl.javadb.backend.parser.statement.Begin;
import cn.tangrl.javadb.backend.parser.statement.Create;
import cn.tangrl.javadb.backend.parser.statement.Delete;
import cn.tangrl.javadb.backend.parser.statement.Insert;
import cn.tangrl.javadb.backend.parser.statement.Select;
import cn.tangrl.javadb.backend.parser.statement.Update;
import cn.tangrl.javadb.backend.utils.Parser;
import cn.tangrl.javadb.backend.vm.VersionManager;
import cn.tangrl.javadb.common.Error;

/**
 * TBM模块的实现类
 */
public class TableManagerImpl implements TableManager {
    VersionManager vm;
    DataManager dm;
    /**
     * booter对象对应一个文件，存放了第一个表的 UID，并提供了针对这个文件的一些操作方法。
     * 由于 TBM 的表管理，使用的是链表串起的 Table 结构，所以就必须保存一个链表的头节点，即第一个表的 UID，这样在 MYDB 启动时，才能快速找到表信息。
     */
    private Booter booter;
    /**
     * 表的缓存
     */
    private Map<String, Table> tableCache;
    private Map<Long, List<Table>> xidTableCache;
    private Lock lock;

    /**
     * 构造函数
     * @param vm
     * @param dm
     * @param booter
     */
    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }

    /**
     * 加载表
     */
    private void loadTables() {
        long uid = firstTableUid();
        while(uid != 0) {
            Table tb = Table.loadTable(this, uid);
            uid = tb.nextUid;
            tableCache.put(tb.name, tb);
        }
    }

    /**
     * 获取第一个表的uid
     * @return
     */
    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    /**
     * 更新第一个表的uid
     * @param uid
     */
    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }

    /**
     * 执行begin语句
     * @param begin
     * @return
     */
    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        int level = begin.isRepeatableRead?1:0;
        res.xid = vm.begin(level);
        res.result = "begin".getBytes();
        return res;
    }

    /**
     * 执行commit语句
     * @param xid
     * @return
     * @throws Exception
     */
    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }

    /**
     * 执行abort语句
     * @param xid
     * @return
     */
    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }

    /**
     * 执行show语句
     * @param xid
     * @return
     */
    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
            List<Table> t = xidTableCache.get(xid);
            if(t == null) {
                return "\n".getBytes();
            }
            for (Table tb : t) {
                sb.append(tb.toString()).append("\n");
            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 执行create语句
     * @param xid
     * @param create
     * @return
     * @throws Exception
     */
    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            if(tableCache.containsKey(create.tableName)) {
                throw Error.DuplicatedTableException;
            }
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            if(!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 执行insert语句
     * @param xid
     * @param insert
     * @return
     * @throws Exception
     */
    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        table.insert(xid, insert);
        return "insert".getBytes();
    }

    /**
     * 执行read语句
     * @param xid
     * @param read
     * @return
     * @throws Exception
     */
    @Override
    public byte[] read(long xid, Select read) throws Exception {
        lock.lock();
        Table table = tableCache.get(read.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        return table.read(xid, read).getBytes();
    }

    /**
     * 执行update语句
     * @param xid
     * @param update
     * @return
     * @throws Exception
     */
    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.update(xid, update);
        return ("update " + count).getBytes();
    }

    /**
     * 执行delete语句
     * @param xid
     * @param delete
     * @return
     * @throws Exception
     */
    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes();
    }
}
