package cn.tangrl.javadb.backend.dm.pageIndex;

/**
 * 每页的信息对象
 */
public class PageInfo {
    /**
     * 页号
     */
    public int pgno;
    /**
     * 空闲空间
     */
    public int freeSpace;

    /**
     * 构造函数
     * @param pgno
     * @param freeSpace
     */
    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
