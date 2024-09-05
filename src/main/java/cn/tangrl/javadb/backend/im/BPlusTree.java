package cn.tangrl.javadb.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import cn.tangrl.javadb.backend.common.SubArray;
import cn.tangrl.javadb.backend.dm.DataManager;
import cn.tangrl.javadb.backend.dm.dataItem.DataItem;
import cn.tangrl.javadb.backend.im.Node.InsertAndSplitRes;
import cn.tangrl.javadb.backend.im.Node.LeafSearchRangeRes;
import cn.tangrl.javadb.backend.im.Node.SearchNextRes;
import cn.tangrl.javadb.backend.tm.TransactionManagerImpl;
import cn.tangrl.javadb.backend.utils.Parser;

// TODO 没有仔细看懂，大概过了一遍
/**
 * B+树类 实现索引管理
 */
public class BPlusTree {
    /**
     * dm引用
     */
    DataManager dm;
    /**
     * bootDataItem的uid
     */
    long bootUid;
    /**
     * 由于 B+ 树在插入删除时，会动态调整，根节点不是固定节点，于是设置一个 bootDataItem，该 DataItem 中存储了根节点的 UID。
     * 这个bootDataItem只存放了根节点的 UID
     */
    DataItem bootDataItem;
    /**
     * 操作属性的锁
     */
    Lock bootLock;

    /**
     * 创建新的b树，空的根结点
     * @param dm
     * @return
     * @throws Exception
     */
    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    /**
     * 根据根结点的uid，加载根结点
     * @param bootUid
     * @param dm
     * @return
     * @throws Exception
     */
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    /**
     * 获取根结点的uid
     * bootDataItem专门存放bootuid
     * @return
     */
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 更新 bootDataItem中的bootuid
     * @param left
     * @param right
     * @param rightKey
     * @throws Exception
     */
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            // rootRaw是一个node，是dataitem的data
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            // 会找到一个页面插入
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 在 nodeUid 的node中，搜索key关键字，找到叶节点上的uid（即关键字为key的uid dataitem）
     * @param nodeUid
     * @param key
     * @return
     * @throws Exception
     */
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if(isLeaf) {
            return nodeUid;
        } else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    /**
     * 在 nodeUid 的node中，搜索key关键字，返回下一个节点ui
     * @param nodeUid
     * @param key
     * @return
     * @throws Exception
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            SearchNextRes res = node.searchNext(key);
            node.release();
            if(res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }

    /**
     * 单个搜索
     * @param key
     * @return
     * @throws Exception
     */
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    /**
     * 范围搜索，返回满足的uids，即 DataItem 列表
     * @param leftKey
     * @param rightKey
     * @return
     * @throws Exception
     */
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        // 找到叶子节点uid
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        // 在叶子节点找范围内的uids
        while(true) {
            Node leaf = Node.loadNode(this, leafUid);
            LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    /**
     * 插入
     * @param key
     * @param uid
     * @throws Exception
     */
    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        // 插到根节点上
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if(res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    /**
     * 存放插入的结果
     */
    class InsertRes {
        long newNode, newKey;
    }

    /**
     * 插入key，uid到nodeUid节点上
     * @param nodeUid
     * @param uid
     * @param key
     * @return
     * @throws Exception
     */
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        // 如果当前节点是叶子节点，调用 insertAndSplit 方法。
        if(isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            // 如果当前节点是内部节点，递归地插入键值对，并在需要时分裂节点。
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if(ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    /**
     * 插入并分割
     * @param nodeUid
     * @param uid
     * @param key
     * @return
     * @throws Exception
     */
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if(iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    /**
     * 关闭 B+ 树，释放资源。
     */
    public void close() {
        bootDataItem.release();
    }
}
