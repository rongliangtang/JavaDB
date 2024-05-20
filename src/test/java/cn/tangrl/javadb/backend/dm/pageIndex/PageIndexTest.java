package cn.tangrl.javadb.backend.dm.pageIndex;

import org.junit.Test;

import cn.tangrl.javadb.backend.dm.pageCache.PageCache;

/**
 * PageIndex测试类
 */
public class PageIndexTest {
    /**
     * 测试基本方法，add和select
     */
    @Test
    public void testPageIndex() {
        PageIndex pIndex = new PageIndex();
        int threshold = PageCache.PAGE_SIZE / 20;
        for(int i = 0; i < 20; i ++) {
            pIndex.add(i, i*threshold);
            pIndex.add(i, i*threshold);
            pIndex.add(i, i*threshold);
        }

        for(int k = 0; k < 3; k ++) {
            for(int i = 0; i < 19; i ++) {
                PageInfo pi = pIndex.select(i * threshold);
                assert pi != null;
                assert pi.pgno == i+1;
            }
        }
    }
}
