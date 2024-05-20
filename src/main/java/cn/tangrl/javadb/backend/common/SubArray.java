package cn.tangrl.javadb.backend.common;

// TODO 实现更好的共享内存子数组
/**
 * 可以共享部分数组内存的子数组类
 */
public class SubArray {
    /**
     * 源数组
     */
    public byte[] raw;
    /**
     * 共享起点
     */
    public int start;
    /**
     * 共享结尾
     */
    public int end;

    /**
     * 构造函数
     * @param raw
     * @param start
     * @param end
     */
    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
