package cn.tangrl.javadb.backend.tm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import cn.tangrl.javadb.backend.utils.Panic;
import cn.tangrl.javadb.common.Error;

/**
 * TM 模块的接口
 * 该接口定义了需要维护事务的所有方法
 * TM 模块通过维护 XID 文件来维护事务的状态，并提供接口供其他模块来查询某个事务的状态。
 */
public interface TransactionManager {
    /**
     * 开启一个新事务
     * @return
     */
    long begin();

    /**
     * 提交一个事务
     * @param xid
     */
    void commit(long xid);

    /**
     * 取消一个事务
     * @param xid
     */
    void abort(long xid);

    /**
     * 查询一个事务的状态是否是“正在进行”的状态
     * @param xid
     * @return
     */
    boolean isActive(long xid);

    /**
     * 查询一个事务的状态是否是“已提交”的状态
     * @param xid
     * @return
     */
    boolean isCommitted(long xid);

    /**
     * 查询一个事务的状态是否是“已取消“的状态
     * @param xid
     * @return
     */
    boolean isAborted(long xid);

    /**
     * 关闭事务管理对象
     */
    void close();

    /**
     * 创建xid文件的静态工厂方法，会返回一个 TransactionManagerImpl 对象
     * 创建数据库的时候调用
     * 定义在接口中的好处是可以通过接口直接创建实例，提高了代码的灵活性和可维护性。
     * @param path
     * @return
     */
    public static TransactionManagerImpl create(String path) {
        // 创建一个文件对象
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        // 尝试创建文件，会进行异常处理
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        // 如果文件不可读或不可写，会进行异常处理
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        // channel是nio中的
        // nio相比于io在阻塞（异步）、缓冲、支持map、多线程、文件定位等方面有优势
        // FileChannel只支持阻塞模式
        FileChannel fc = null;
        RandomAccessFile raf = null;
        // 创建一个可以随机访问的文件对象，并获取其文件通道，会进行异常处理
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }

        // 写入空的XID文件头，值为0
        // 因为创建新数组时，每个元素数值初始化为0
        // 创建一个字节缓冲区对象
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            // 将通道位置设置为文件开头，并写入缓冲区内容。
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 返回 TransactionManagerImpl 对象
        return new TransactionManagerImpl(raf, fc);
    }

    /**
     * 打开xid文件的静态工厂方法，返回一个 TransactionManagerImpl 对象
     * 与 create 的区别是没有写入xid头，此时fc的位置为文件起点
     * 打开数据库的时候调用
     * @param path
     * @return
     */
    public static TransactionManagerImpl open(String path) {
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
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

        return new TransactionManagerImpl(raf, fc);
    }
}
