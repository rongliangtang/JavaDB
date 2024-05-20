package cn.tangrl.javadb.backend.dm.dataItem;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import cn.tangrl.javadb.backend.common.SubArray;
import cn.tangrl.javadb.backend.dm.DataManagerImpl;
import cn.tangrl.javadb.backend.dm.page.Page;
import cn.tangrl.javadb.backend.utils.Parser;
import cn.tangrl.javadb.backend.utils.Types;

/**
 * DataItem 接口
 * DataItem 是 DM 层向上层提供的数据抽象
 * 上层模块通过地址，向 DM 请求到对应的 DataItem，再获取到其中的数据。
 */
public interface DataItem {
    /**
     * 上层模块在获取到 DataItem 后，可以通过 data() 方法，该方法返回的数组是数据共享的，而不是拷贝实现的，所以使用了 SubArray。
     * @return
     */
    SubArray data();

    /**
     * 修改DataItem的前置步骤，复制旧数据
     */
    void before();

    /**
     * 撤销修改DataItem操作的前置步骤，将旧数据拷贝回元数据
     */
    void unBefore();

    /**
     * 修改DataItem的后置步骤，释放缓存引用
     * @param xid
     */
    void after(long xid);

    /**
     * 使用完 DataItem 后，也应当及时调用 release() 方法，释放掉 DataItem 的缓存
     */
    void release();

    /**
     * 上写锁
     */
    void lock();

    /**
     * 释放写锁
     */
    void unlock();

    /**
     * 上读锁
     */
    void rLock();

    /**
     * 释放读锁
     */
    void rUnLock();

    /**
     * 获取Page对象
     * @return
     */
    Page page();

    /**
     * 获取uid
     * @return
     */
    long getUid();

    /**
     * 获取旧数据
     * @return
     */
    byte[] getOldRaw();

    /**
     * 获取DataItem数据
     * @return
     */
    SubArray getRaw();

    /**
     * 将传入的raw数组包裹DataItem的格式，raw为data
     * @param raw
     * @return
     */
    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short)raw.length);
        return Bytes.concat(valid, size, raw);
    }

    /**
     * 从页面的offset处解析出dataitem，返回 DataItem 对象，实际是DataItemImpl对象
     * @param pg
     * @param offset
     * @param dm
     * @return
     */
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        // 获取页面数据
        byte[] raw = pg.getData();
        // 从offset处开始为dataitem
        // dataitem格式是[ValidFlag] [DataSize] [Data]
        // 取出dataitem的DataSize
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset+DataItemImpl.OF_SIZE, offset+DataItemImpl.OF_DATA));
        // 取出这个dataitem中的Data的结尾的相对位置
        short length = (short)(size + DataItemImpl.OF_DATA);
        // 生成uid
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        // new SubArray(raw, offset, offset+length)表示DataItem的数据
        return new DataItemImpl(new SubArray(raw, offset, offset+length), new byte[length], pg, uid, dm);
    }

    /**
     * 设置DataItem中的ValidFlag为Invalid
     * @param raw
     */
    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }
}
