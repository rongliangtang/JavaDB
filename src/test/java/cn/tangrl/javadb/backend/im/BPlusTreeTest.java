package cn.tangrl.javadb.backend.im;

import java.io.File;
import java.util.List;

import org.junit.Test;

import cn.tangrl.javadb.backend.dm.DataManager;
import cn.tangrl.javadb.backend.dm.pageCache.PageCache;
import cn.tangrl.javadb.backend.tm.MockTransactionManager;
import cn.tangrl.javadb.backend.tm.TransactionManager;

/**
 * b+树的测试类
 */
public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        // 创建模拟tm模块
        TransactionManager tm = new MockTransactionManager();
        // 创建dm模块
        DataManager dm = DataManager.create("/tmp/TestTreeSingle", PageCache.PAGE_SIZE*10, tm);
        // 创建新的b树，空的根结点
        long root = BPlusTree.create(dm);
        // 加载根节点
        BPlusTree tree = BPlusTree.load(root, dm);
        // 倒序插入一万个节点
        int lim = 10000;
        for(int i = lim-1; i >= 0; i --) {
            tree.insert(i, i);
        }
        // 顺序搜索一万个节点
        for(int i = 0; i < lim; i ++) {
            List<Long> uids = tree.search(i);
            // 判断搜索结果是否正确
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }
        // 判断能否正常删除
        assert new File("/tmp/TestTreeSingle.db").delete();
        assert new File("/tmp/TestTreeSingle.log").delete();
    }
}
