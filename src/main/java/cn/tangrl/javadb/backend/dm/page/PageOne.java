package cn.tangrl.javadb.backend.dm.page;

import java.util.Arrays;

import cn.tangrl.javadb.backend.dm.pageCache.PageCache;
import cn.tangrl.javadb.backend.utils.RandomUtil;

/**
 * 存储在硬盘文件中的 第一页 管理类（是需要特殊管理的，所以单独定义一个类）
 * ValidCheck 启动检查，检查数据库是否正常关闭，如果不是，则要执行数据恢复的流程。
 * 原理：db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 每次启动时，就会检查第一页两处的字节是否相同，以此来判断上一次是否正常关闭
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
     * 创建一个页大小的数组，并调用setVcOpen设置初始字节
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
