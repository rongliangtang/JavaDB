package cn.tangrl.javadb.backend.dm.logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.primitives.Bytes;

import cn.tangrl.javadb.backend.utils.Panic;
import cn.tangrl.javadb.backend.utils.Parser;
import cn.tangrl.javadb.common.Error;

/**
 * 日志文件功能实现类
 * 
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 * 
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 */
public class LoggerImpl implements Logger {
    /**
     * 用于计算单条日志校验和的种子
     */
    private static final int SEED = 13331;
    /**
     * 每条日志的size数据相对起始位置
     */
    private static final int OF_SIZE = 0;
    /**
     * 每条日志的CHECKSUM相对起始位置
     */
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    /**
     * 每条日志的数据相对起始位置
     */
    private static final int OF_DATA = OF_CHECKSUM + 4;
    /**
     * 日志文件的后缀名
     */
    public static final String LOG_SUFFIX = ".log";
    /**
     * 日志文件对应的RandomAccessFile对象
     */
    private RandomAccessFile file;
    /**
     * RandomAccessFile对应的FileChannel对象
     */
    private FileChannel fc;
    /**
     * 文件读写互斥锁
     */
    private Lock lock;
    /**
     * 当前日志指针的位置
     */
    private long position;
    /**
     * 文件大小
     */
    private long fileSize;
    /**
     * 整个文件的校验和
     */
    private int xChecksum;

    /**
     * 构造函数
     * 打开文件时调用
     * @param raf
     * @param fc
     */
    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    /**
     * 构造函数，需要传入xChecksum
     * 创建文件时调用
     * @param raf
     * @param fc
     * @param xChecksum
     */
    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    /**
     * 初始化
     * 读取文件的xChecksum并校验
     */
    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    /**
     * 检查并移除bad tail
     *
     */
    private void checkAndRemoveTail() {
        // 退回postion，调用internNext()回从第一条日志开始
        rewind();
        // 计算xCheckSum
        int xCheck = 0;
        // 直至log为null时退出，此时position为bad tail（不一定存在）的起始位置
        while(true) {
            // 读取下一条log，
            byte[] log = internNext();
            if(log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        // 校验xCheckSum
        if(xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }

        // 截断文件，将postion后的bad tail进行移除
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 退回postion
        rewind();
    }

    /**
     * 计算日志的校验和
     * 单条 calChecksum(0, byte[] log) 对单条日志计算
     * 文件 while（calChecksum(0, byte[] log)） 对所有日志计算
     * @param xCheck
     * @param log
     * @return
     */
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 将有效数据包装写入到日志文件中
     * 传参数data byte数据为有效数据
     * 包装logo，写入内存，并更新XChecksum
     * @param data
     */
    @Override
    public void log(byte[] data) {
        // 包装
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        // 涉及文件的读写都要加锁
        // 写入到文件中
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        // 更新XChecksum
        updateXChecksum(log);
    }

    /**
     * 更新xChecksum，并写入文件
     * 在当前的xChecksum基础上进行更新，因为xChecksum是累加log数据计算的
     * @param log
     */
    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            // false表示不强制将元数据写回，强制将fc有关的缓存内容写回到磁盘文件中
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将有效数据包装成一条log的格式
     * @param data
     * @return
     */
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    /**
     * 截断文件，在x的位置
     * @param x
     * @throws Exception
     */
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 读取下一条log，利用position实现
     * @return
     */
    private byte[] internNext() {
        // 如果都存不下真实数据了，返回null
        if(position + OF_DATA >= fileSize) {
            return null;
        }
        // 读取size数据
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch(IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        // 如果丢失了数据，返回null
        if(position + size + OF_DATA > fileSize) {
            return null;
        }
        // 读取整条log数据
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();
        // 利用log数据中的有效数据计算Checksum
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        // 获取这条日志的Checksum
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        // 进行校验
        if(checkSum1 != checkSum2) {
            return null;
        }
        // 操作成功，则对position进行偏移
        position += log.length;
        return log;
    }

    /**
     * 读取下一条日志
     * Logger 被实现成迭代器模式，通过 next() 方法，不断地从文件中读取下一条日志，并将其中的 Data 解析出来并返回
     * next() 方法的实现主要依靠 internNext()
     * @return
     */
    @Override
    public byte[] next() {
        // 涉及日志文件的读写都要上锁
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 将postion退回到第一条log数据开始的位置
     */
    @Override
    public void rewind() {
        position = 4;
    }

    /**
     * 关闭资源
     */
    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
    
}
