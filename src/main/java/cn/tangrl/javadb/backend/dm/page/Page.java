package cn.tangrl.javadb.backend.dm.page;

/**
 * 页面接口
 */
public interface Page {
    /**
     * 上锁
     */
    void lock();

    /**
     * 解锁
     */
    void unlock();

    /**
     * 释放页面缓存
     */
    void release();

    /**
     * 设置为脏页面
     * @param dirty
     */
    void setDirty(boolean dirty);

    /**
     * 是否脏
     * @return
     */
    boolean isDirty();

    /**
     * 获取页号
     * @return
     */
    int getPageNumber();

    /**
     * 获取数据
     * @return
     */
    byte[] getData();
}
