package cn.tangrl.javadb.backend.dm.page;

import java.util.Arrays;

import cn.tangrl.javadb.backend.dm.pageCache.PageCache;
import cn.tangrl.javadb.backend.utils.Parser;

/**
 * 存储在硬盘文件中的 普通页 管理类
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲数据开始的位置
 * FSO表示空闲位置的起点
 */
public class PageX {
    /**
     * FSO数据在页中的起点位置
     */
    private static final short OF_FREE = 0;
    /**
     * FSO占用的大小为2字节
     */
    private static final short OF_DATA = 2;
    /**
     * 最大的页面空闲空间大小
     */
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    /**
     * 初始化页
     * 设置FSO为FSO的大小
     * @return
     */
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

    /**
     * 更新页的FSO
     * 将ofData写入到传参raw byte数组中的前两个字节中，即写入到pg的前两个字节中
     * @param raw
     * @param ofData
     */
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    /**
     * 获取pg的FSO，传参是page对象
     * 调用下面一个函数，传参是data
     * @param pg
     * @return
     */
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    /**
     * 获取pg的FSO，传参是是页面数据
     * 取出页面的前两个字节数据，即FSO
     * @param raw
     * @return
     */
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 将raw插入pg中，返回插入位置
     * 在写入之前获取 FSO，来确定写入的位置，并在写入之后更新 FSO。
     * @param pg
     * @param raw
     * @return
     */
    public static short insert(Page pg, byte[] raw) {
        // 设置页面为脏
        pg.setDirty(true);
        //
        short offset = getFSO(pg.getData());
        // 将传入的raw byte数组，插入到pg数据FSO开始的空闲位置
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        // 更新这个页的FSO
        setFSO(pg.getData(), (short)(offset + raw.length));
        return offset;
    }

    /**
     * 利用FSO获取这个页的空闲空间
     * @param pg
     * @return
     */
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }

    /**
     * 将raw插入pg中的offset位置，并更新offset为较大的值
     * 因为在日志恢复数据操作中，传入的offset可能小于真实的空闲起点offset
     * 用于在数据库崩溃后重新打开时，恢复例程直接插入数据使用
     * @param pg
     * @param raw
     * @param offset
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset+raw.length));
        }
    }

    /**
     * 将raw插入pg中的offset位置，不更新offset
     * 用于在数据库崩溃后重新打开时，恢复例程直接修改数据使用
     * @param pg
     * @param raw
     * @param offset
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
