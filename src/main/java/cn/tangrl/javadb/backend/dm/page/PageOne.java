package cn.tangrl.javadb.backend.dm.page;

import java.util.Arrays;

import cn.tangrl.javadb.backend.dm.pageCache.PageCache;
import cn.tangrl.javadb.backend.utils.RandomUtil;

/**
 * 数据库文件的第一页，用来做启动检查。（第一页的管理类）
 * 在每次数据库启动时，会生成一串随机字节，存储在 100 ~ 107 字节。
 * 在数据库正常关闭时，会将这串字节，拷贝到第一页的 108 ~ 115 字节。
 * 数据库在每次启动时，就会检查第一页两处的字节是否相同，以此来判断上一次是否正常关闭。
 * 如果是异常关闭，就需要执行数据的恢复流程。
 */
public class PageOne {
    /**
     * 起始写入起点 静态常量
     */
    private static final int OF_VC = 100;
    /**
     * 写入校验数据的长度
     */
    private static final int LEN_VC = 8;

    /**
     * 初始化第一页
     * 创建一个页面数据大小的数组，并调用setVcOpen设置初始字节
     * @return
     */
    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    /**
     * 启动时设置初始字节
     * 设置为脏页，并调用下面一个setVcOpen
     * @param pg
     */
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    /**
     * 将随机生成的LEN_VC byte数组，从传参raw byte数组中的100开始拷贝LEN_VC大小的字节
     * @param raw
     */
    private static void setVcOpen(byte[] raw) {
        // 在不创建新的数组的情况下，进行数组复制
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    /**
     * 关闭时拷贝字节
     * 设置为脏页，并调用下面一个setVcClose
     * @param pg
     */
    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    /**
     * 将传参raw byte数组的100开始的数据，从传参raw byte数组的OF_VC+LEN_VC位置开始写，LEN_VC大小的字节
     * @param raw
     */
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }

    /**
     * 校验字节
     * 调研下面一个私有方法checkVc
     * @param pg
     * @return
     */
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    /**
     * 判断传参raw byte数组的 100 ~ 107 字节 和  108 ~ 115 字节 是否相等
     * @param raw
     * @return
     */
    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC+LEN_VC), Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC));
    }
}
