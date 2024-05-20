package cn.tangrl.javadb.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cn.tangrl.javadb.backend.common.SubArray;
import cn.tangrl.javadb.backend.dm.dataItem.DataItem;
import cn.tangrl.javadb.backend.tm.TransactionManagerImpl;
import cn.tangrl.javadb.backend.utils.Parser;
/**
 * IM模块，即 Index Manager，索引管理器，为 MYDB 提供了基于 B+ 树的聚簇索引。
 * 目前 MYDB 只支持基于索引查找数据，不支持全表扫描。
 * 在依赖关系图中可以看到，IM 直接基于 DM，而没有基于 VM。索引的数据被直接插入数据库文件中，而不需要经过版本管理。
 * 所以在im中dataitem的data为node，而在vm中dataitem的data为entry。
 */

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 * Son和Key都是8个字节。
 * LeafFlag 标记了该节点是否是个叶子节点；KeyNumber 为该节点中 key 的个数；SiblingUid 是其兄弟节点存储在 DM 中的 UID。
 * 后续是穿插的子节点（SonN）和 KeyN。最后的一个 KeyN 始终为 MAX_VALUE，以此方便查找。
 *
 * 定义有有关node的插入key，插入后的分割等方法。
 * IM 在操作 DM 时，使用的事务都是 SUPER_XID。
 */
public class Node {
    /**
     * LeafFlag数据的起始位置，占1个字节
     */
    static final int IS_LEAF_OFFSET = 0;
    /**
     * KeyNumber数据的起始位置，占2个字节
     */
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1;
    /**
     * SiblingUid数据的起始位置，占8个字节
     */
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2;
    /**
     * 节点头部的大小，即[LeafFlag][KeyNumber][SiblingUid]，占1+2+8个字节
     */
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8;
    /**
     * 节点的平衡因子的常量，一个节点最多可以包含32*2个key
     */
    static final int BALANCE_NUMBER = 32;
    /**
     * 节点的大小
     */
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);
    /**
     * B+树对象的索引
     */
    BPlusTree tree;
    /**
     * DataItem 的引用（二叉树由一个个 Node 组成，每个 Node 都存储在一条 DataItem的data 中）
     */
    DataItem dataItem;
    /**
     * SubArray 的引用
     */
    SubArray raw;
    /**
     * DataItem的uid
     */
    long uid;

    /**
     * 在SubArray中设置LeafFlag数据
     * @param raw
     * @param isLeaf
     */
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if(isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)0;
        }
    }

    /**
     * 在SubArray中获取LeafFlag数据
     * @param raw
     * @return
     */
    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }

    /**
     * 在SubArray中设置KeyNumber数据
     * @param raw
     * @param noKeys
     */
    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short)noKeys), 0, raw.raw, raw.start+NO_KEYS_OFFSET, 2);
    }

    /**
     * 在SubArray中获取KeyNumber数据
     * @param raw
     * @return
     */
    static int getRawNoKeys(SubArray raw) {
        return (int)Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));
    }

    /**
     * 在SubArray中设置SiblingUid数据
     * @param raw
     * @param sibling
     */
    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start+SIBLING_OFFSET, 8);
    }

    /**
     * 在SubArray中获取SiblingUid数据
     * @param raw
     * @return
     */
    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start+SIBLING_OFFSET, raw.start+SIBLING_OFFSET+8));
    }

    /**
     * 在SubArray中设置第kth个son节点
     * @param raw
     * @param uid
     * @param kth
     */
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    /**
     * 在SubArray中获取第kth个son节点
     * @param raw
     * @param kth
     * @return
     */
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    /**
     * 在SubArray中设置第kth个key节点
     * @param raw
     * @param key
     * @param kth
     */
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    /**
     * 在SubArray中获取第kth个key节点
     * @param raw
     * @param kth
     * @return
     */
    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    /**
     * 将 from 中 kth 中开始的数据复制到 to 中header后的位置
     * @param from
     * @param to
     * @param kth
     */
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }

    /**
     * 将kth后开始的数据往后移一个位置
     * @param raw
     * @param kth
     */
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        int end = raw.start+NODE_SIZE-1;
        for(int i = end; i >= begin; i --) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }

    /**
     * 生成一个根节点，传入数据
     * @param left
     * @param right
     * @param key
     * @return
     */
    static byte[] newRootRaw(long left, long right, long key)  {
        // 创建存储节点数据的SubArray
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        // 设置LeafFlag数据为false
        setRawIsLeaf(raw, false);
        // 设置KeyNumber数据，为2
        setRawNoKeys(raw, 2);
        // 设置SiblingUid数据，为0
        setRawSibling(raw, 0);
        // 设置son数据，为left
        setRawKthSon(raw, left, 0);
        // 设置key数据，为key
        setRawKthKey(raw, key, 0);
        // 设置son数据，为right
        setRawKthSon(raw, right, 1);
        // 设置key数据，为Long.MAX_VALUE
        setRawKthKey(raw, Long.MAX_VALUE, 1);
        // 返回byte数组
        return raw.raw;
    }

    /**
     * 生成一个空的根节点
     * @return
     */
    static byte[] newNilRootRaw()  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    /**
     *
     * @param bTree
     * @param uid
     * @return
     * @throws Exception
     */
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    /**
     * 释放dataItem缓存
     */
    public void release() {
        dataItem.release();
    }

    /**
     *
     * @return
     */
    public boolean isLeaf() {
        // 上dataItem读锁
        dataItem.rLock();
        try {
            // 在SubArray中获取LeafFlag数据
            return getRawIfLeaf(raw);
        } finally {
            // 解dataItem读锁
            dataItem.rUnLock();
        }
    }

    /**
     * 存放key的搜索结果，用于searchNext()
     */
    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    /**
     * 寻找对应 单个key 的 UID, 如果找不到, 则返回兄弟节点的 UID。
     * 返回SearchNextRes对象。
     * @param key
     * @return
     */
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            // 获取key的数量
            int noKeys = getRawNoKeys(raw);
            for(int i = 0; i < noKeys; i ++) {
                // 获取第i个key
                long ik = getRawKthKey(raw, i);
                // 如果key小于keyi，则返回第i个son
                if(key < ik) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            // 找不到则返回兄弟节点的uid
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;

        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 存放范围key的搜索节点，用于leafSearchRange()
     */
    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    /**
     * 在当前节点进行范围查找，范围是 [leftKey, rightKey]
     * 这里约定如果 rightKey 大于等于该节点的最大的 key, 则还同时返回兄弟节点的 UID，方便继续搜索下一个节点。
     * @param leftKey
     * @param rightKey
     * @return
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik >= leftKey) {
                    break;
                }
                kth ++;
            }
            List<Long> uids = new ArrayList<>();
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth ++;
                } else {
                    break;
                }
            }
            long siblingUid = 0;
            if(kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 传入插入和分裂的结果
     */
    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    /**
     * 在B+树的节点中插入一个键值对，并在需要时分裂节点。
     * @param uid
     * @param key
     * @return
     * @throws Exception
     */
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        // 创建一个标志位，用于标记插入操作是否成功
        boolean success = false;
        // 创建一个异常对象，用于存储在插入或分裂节点时发生的异常
        Exception err = null;
        // 创建一个InsertAndSplitRes对象，用于存储插入和分裂节点的结果
        InsertAndSplitRes res = new InsertAndSplitRes();

        // 执行dataItem的before()，保存数据副本
        dataItem.before();
        try {
            // 如果插入失败，设置兄弟节点的UID，并返回结果
            success = insert(uid, key);
            if(!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            // 如果需要分割
            if(needSplit()) {
                try {
                    // 分裂节点，并获取分裂结果
                    SplitRes r = split();
                    // 设置新节点的UID和新键，并返回结果
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch(Exception e) {
                    // 如果在分裂节点时发生错误，保存异常并抛出
                    err = e;
                    throw e;
                }
            } else {
                // 如果不需要分裂节点，直接返回结果
                return res;
            }
        } finally {
            // 如果没有发生错误并且插入成功，提交数据项的修改
            if(err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                // 如果发生错误或插入失败，回滚数据项的修改
                dataItem.unBefore();
            }
        }
    }

    /**
     * 在B+树的节点中插入一个键值对的方法
     * @param uid
     * @param key
     * @return
     */
    private boolean insert(long uid, long key) {
        // 获取节点中的键的数量
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        // 尝试找插入的位置
        while(kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if(ik < key) {
                kth ++;
            } else {
                break;
            }
        }
        // 如果所有的键都被遍历过，并且存在兄弟节点，插入失败
        if(kth == noKeys && getRawSibling(raw) != 0) return false;

        // 如果节点是叶子节点
        if(getRawIfLeaf(raw)) {
            // 在插入位置后的所有键和子节点向后移动一位
            shiftRawKth(raw, kth);
            // 在插入位置插入新的键和子节点的UID
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            // 更新节点中的键的数量
            setRawNoKeys(raw, noKeys+1);
        } else {
            // 如果节点是非叶子节点
            // 获取插入位置的键
            long kk = getRawKthKey(raw, kth);
            // 在插入位置插入新的键
            setRawKthKey(raw, key, kth);
            // 在插入位置后的所有键和子节点向后移动一位
            shiftRawKth(raw, kth+1);
            // 在插入位置的下一个位置插入原来的键和新的子节点的UID
            setRawKthKey(raw, kk, kth+1);
            setRawKthSon(raw, uid, kth+1);
            // 更新节点中的键的数量
            setRawNoKeys(raw, noKeys+1);
        }
        // 插入成功
        return true;
    }

    /**
     * 判断是否需要分割，如果key的数量==BALANCE_NUMBER*2则需要分割
     * @return
     */
    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }

    /**
     * 存放分割结果
     */
    class SplitRes {
        long newSon, newKey;
    }

    /**
     * 分裂B+树的节点。
     * 当一个节点的键的数量达到 `BALANCE_NUMBER * 2` 时，就意味着这个节点已经满了，需要进行分裂操作。
     * 分裂操作的目的是将一个满的节点分裂成两个节点，每个节点包含一半的键。
     * @return
     * @throws Exception
     */
    private SplitRes split() throws Exception {
        // 创建一个新的字节数组，用于存储新节点的原始数据
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        // 设置新节点的叶子节点标志，与原节点相同
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        // 设置新节点的键的数量为BALANCE_NUMBER
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        // 设置新节点的兄弟节点的UID，与原节点的兄弟节点的UID相同
        setRawSibling(nodeRaw, getRawSibling(raw));
        // 从原节点的原始字节数组中复制后面32个数据到新节点的原始字节数组中
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        // 在数据管理器中插入新节点的原始数据，并获取新节点的UID
        // xid为super_xid
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        // 更新原节点的键的数量为BALANCE_NUMBER
        setRawNoKeys(raw, BALANCE_NUMBER);
        // 更新原节点的兄弟节点的UID为新节点的UID
        setRawSibling(raw, son);

        // 创建一个SplitRes对象，用于存储分裂结果
        SplitRes res = new SplitRes();
        // 设置新节点的UID
        res.newSon = son;
        // 设置新键为新节点的第一个键的值
        res.newKey = getRawKthKey(nodeRaw, 0);
        // 返回分裂结果
        return res;
    }

    /**
     * 将raw即node打印出来
     * @return
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }

}
