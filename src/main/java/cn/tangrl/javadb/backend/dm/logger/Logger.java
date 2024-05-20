package cn.tangrl.javadb.backend.dm.logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import cn.tangrl.javadb.backend.utils.Panic;
import cn.tangrl.javadb.backend.utils.Parser;
import cn.tangrl.javadb.common.Error;

/**
 * 日志功能接口
 * MYDB 提供了崩溃后的数据恢复功能。
 * DM 层在每次对底层数据操作时，都会记录一条日志到磁盘上。
 * 在数据库奔溃之后，再次启动时，可以根据日志的内容，恢复数据文件，保证其一致性。
 */
public interface Logger {
    /**
     * 存储日志，传入有效数据
     * @param data
     */
    void log(byte[] data);

    /**
     * 截断文件，在x的位置
     * @param x
     * @throws Exception
     */
    void truncate(long x) throws Exception;

    /**
     * 取出下一条log记录
     * @return
     */
    byte[] next();

    /**
     * 退回postion到第一条log的起始位置
     */
    void rewind();

    /**
     * 关闭资源
     */
    void close();

    /**
     * 创建日志文件的静态工厂方法，返回Logger对象
     * 实现过程与TM模块一样
     * @param path
     * @return
     */
    public static Logger create(String path) {
        File f = new File(path+LoggerImpl.LOG_SUFFIX);
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

        // 初始化xCheckSum为0
        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 起始的xCheckSum 为0
        return new LoggerImpl(raf, fc, 0);
    }

    /**
     * 打开日志文件的静态工厂方法，返回Logger对象
     * 实现过程与TM模块一样
     * @param path
     * @return
     */
    public static Logger open(String path) {
        File f = new File(path+LoggerImpl.LOG_SUFFIX);
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

        LoggerImpl lg = new LoggerImpl(raf, fc);
        // 需要调用Logger的init()方法，读取文件的xChecksum并校验
        lg.init();

        return lg;
    }
}
