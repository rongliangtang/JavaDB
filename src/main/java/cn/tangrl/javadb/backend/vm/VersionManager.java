package cn.tangrl.javadb.backend.vm;

import cn.tangrl.javadb.backend.dm.DataManager;
import cn.tangrl.javadb.backend.tm.TransactionManager;

/**
 * VM 模块 接口
 * VM 基于两段锁协议实现了调度序列的可串行化，并实现了 MVCC 以消除读写阻塞。同时实现了两种隔离级别。
 * 类似于 Data Manager 是 MYDB 的数据管理核心，Version Manager 是 MYDB 的事务和数据版本的管理核心。
 * DM 层向上层提供了数据项（Data Item）的概念，VM 通过管理所有的数据项，向上层提供了记录（Entry）的概念。上层模块通过 VM 操作数据的最小单位，就是记录。
 * VM 则在其内部，为每个记录，维护了多个版本（Version）。每当上层模块对某个记录进行修改时，VM 就会为这个记录创建一个新的版本。
 */
public interface VersionManager {
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);

    /**
     * 创建VM对象的工厂方法，返回 VersionManagerImpl 对象
     * @param tm
     * @param dm
     * @return
     */
    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }

}
