package cn.tangrl.javadb.backend.utils;

/**
 *  Types工具类
 */
public class Types {
    /**
     * 根据传入的页号和偏移量，生成唯一id
     * 页号和页内偏移组成的一个 8 字节无符号整数，页号和偏移各占 4 字节。
     * @param pgno
     * @param offset
     * @return
     */
    public static long addressToUid(int pgno, short offset) {
        long u0 = (long)pgno;
        long u1 = (long)offset;
        return u0 << 32 | u1;
    }
}
