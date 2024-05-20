package cn.tangrl.javadb.backend.tm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import cn.tangrl.javadb.backend.utils.Panic;
import cn.tangrl.javadb.backend.utils.Parser;
import cn.tangrl.javadb.common.Error;

/**
 * 事务管理实现类，实现 TransactionManager 接口
 * 每个事务由XID来标识，数据库刚创建时创建xid文件，XID初始化为0，事务从1开始标记。
 * 每个事务占用1字节，存放事务状态
 */
public class TransactionManagerImpl implements TransactionManager {

    /**
     * XID文件头 字节长度（文件头存放了long类型的数据，即xid（事务）的数量，XID从1开始计数）
     */
    static final int LEN_XID_HEADER_LENGTH = 8;
    /**
     * 每个事务的占用字节长度
     */
    private static final int XID_FIELD_SIZE = 1;
    /**
     * 事务的三种状态常量：
     * FIELD_TRAN_ACTIVE        正在进行，尚未结束
     * FIELD_TRAN_COMMITTED     已提交
     * FIELD_TRAN_ABORTED       已撤销（回滚）
     */
    private static final byte FIELD_TRAN_ACTIVE   = 0;
	private static final byte FIELD_TRAN_COMMITTED = 1;
	private static final byte FIELD_TRAN_ABORTED  = 2;
    /**
     * 超级事务状态常量，永远为commited状态
     */
    public static final long SUPER_XID = 0;
    /**
     * 文件后缀常量
     */
    static final String XID_SUFFIX = ".xid";
    /**
     * 私有属性，随机访问文件对象
     */
    private RandomAccessFile file;
    /**
     * 私有属性，随机访问文件对象对应的文件通道
     */
    private FileChannel fc;
    /**
     * XID计数器，即header中存放的数据，从1开始，表示事务的数量。
     * 创建文件时写入的xid是0。
     */
    private long xidCounter;
    /**
     * 计算锁，用于XID的读写并发操作
     */
    private Lock counterLock;

    /**
     * 构造函数
     *
     * @param raf RandomAccessFile对象
     * @param fc FileChannel对象，RandomAccessFile类中getChannel()获得
     */
    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     * 对于校验没有通过的，会直接通过 panic 方法，强制退出程序。
     * 在一些基础模块中出现错误都会如此处理，无法恢复的错误只能直接退出程序。
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            Panic.panic(Error.BadXIDFileException);
        }
        if(fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // Parser.parseLong将byte array解析成long类型
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if(end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    /**
     * 根据事务xid取得其在xid文件中对应的位置，即左边的起始位置，xid从1开始计数
     * @param xid
     * @return
     */
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid-1)*XID_FIELD_SIZE;
    }

    /**
     * 更新xid事务的状态为 传参status
     * status写入到XID_FIELD_SIZE字节大小的数组前一个字节，XID_FIELD_SIZE为1
     * @param xid
     * @param status
     */
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            // 注意，这里的所有文件操作，在执行后都需要立刻刷入文件中，防止在崩溃后文件丢失数据，fileChannel 的 force() 方法，强制同步缓存内容到文件中，类似于 BIO 中的 flush() 方法。
            // force 方法的参数是一个布尔，表示是否同步文件的元数据（例如最后修改时间等）。
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将XID加一，并更新XID Header中的XID
     * 注意XID从1开始计数，会存储在header，long类型六个字节
     */
    private void incrXIDCounter() {
        xidCounter ++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 开始一个新事务，创建新事物的xid，将新事务设置为active状态并写入到xid文件中，并返回XID
     * 会使用重入锁来限制不同线程的并发
     * @return
     */
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    /**
     * 提交XID事务，更改这个事务的状态，将新状态写入到文件中
     * @param xid
     */
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    /**
     * 回滚XID事务，更改这个事务的状态，将新状态写入到文件中
     * @param xid
     */
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    /**
     * 检测XID事务是否处于 传参status 这个状态
     * @param xid
     * @param status
     * @return
     */
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    /**
     * 检测XID事务是否处于active
     * @param xid
     * @return
     */
    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    /**
     * 检测XID事务是否处于committed
     * @param xid
     * @return
     */
    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    /**
     * 检测XID事务是否处于aborted
     * @param xid
     * @return
     */
    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    /**
     * 关闭文件对象
     */
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
