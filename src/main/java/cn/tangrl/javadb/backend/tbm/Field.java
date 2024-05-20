package cn.tangrl.javadb.backend.tbm;

import java.util.Arrays;
import java.util.List;

import com.google.common.primitives.Bytes;

import cn.tangrl.javadb.backend.im.BPlusTree;
import cn.tangrl.javadb.backend.parser.statement.SingleExpression;
import cn.tangrl.javadb.backend.tm.TransactionManagerImpl;
import cn.tangrl.javadb.backend.utils.Panic;
import cn.tangrl.javadb.backend.utils.ParseStringRes;
import cn.tangrl.javadb.backend.utils.Parser;
import cn.tangrl.javadb.common.Error;

/**
 * field 表示字段信息，单个字段信息和表信息都是直接保存在 Entry 中。
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * FieldName 和 TypeName，以及后面的表明，存储的都是字节形式的字符串。这里规定一个字符串的存储方式：[StringLength][StringData]。
 * TypeName 为字段的类型，限定为 int32、int64 和 string 类型。
 * 如果这个字段有索引，那个 IndexUID 指向了索引二叉树的根，否则该字段为 0。
 */
public class Field {
    /**
     * field（entry）的uid
     */
    long uid;
    /**
     * Table引用，维护表的结构
     */
    private Table tb;
    /**
     * 字段名
     */
    String fieldName;
    /**
     * 字段类型
     */
    String fieldType;
    /**
     * IndexUid
     */
    private long index;
    /**
     * b+树引用
     */
    private BPlusTree bt;

    /**
     * 通过uid从vm中取并解析字段
     * @param tb
     * @param uid
     * @return
     */
    public static Field loadField(Table tb, long uid) {
        // 根据uid读取对应的entry，entry是dataitem的data。
        byte[] raw = null;
        try {
            // 注意读取的事务是超级事务，超级事务永远是已提交状态。
            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        // 返回Field对象，属性会在parseSelf进行赋值
        return new Field(uid, tb).parseSelf(raw);
    }

    /**
     * 构造函数
     * @param uid
     * @param tb
     */
    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    /**
     * 构造函数
     * @param tb
     * @param fieldName
     * @param fieldType
     * @param index
     */
    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    /**
     * 传入的raw是entry，从entry中解析出，entry的格式是[FieldName][TypeName][IndexUid]
     * @param raw
     * @return
     */
    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        fieldName = res.str;
        position += res.next;
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;
        position += res.next;
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        // 如果IndexUid存在，则从b+树中读出对应的b+树对象
        if(index != 0) {
            try {
                bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            } catch(Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    /**
     * 创建一个字段，返回Field对象
     * @param tb
     * @param xid
     * @param fieldName
     * @param fieldType
     * @param indexed 是否需要索引
     * @return
     * @throws Exception
     */
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        // 类型检查，检查是否为int32、int64和string
        typeCheck(fieldType);
        // 创建field对象
        Field f = new Field(tb, fieldName, fieldType, 0);
        // 如果这个字段需要索引，创建对应的b+树，并对field对象的属性进行复制
        if(indexed) {
            // index是uid
            long index = BPlusTree.create(((TableManagerImpl)tb.tbm).dm);
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            f.index = index;
            f.bt = bt;
        }
        // 将相关的信息通过 VM 持久化
        f.persistSelf(xid);
        // 返回field对象
        return f;
    }

    /**
     * 将相关的信息通过 VM 持久化
     * @param xid
     * @throws Exception
     */
    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);
        // 通过vm将数据持久化
        this.uid = ((TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    /**
     * 类型检查，检查是否为int32、int64和string
     * @param fieldType
     * @throws Exception
     */
    private static void typeCheck(String fieldType) throws Exception {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }

    /**
     * 根据indexUID判断这个字段是否被索引
     * @return
     */
    public boolean isIndexed() {
        return index != 0;
    }

    /**
     * 将key：uid插入到b+树中
     * 将key是字段的值，uid即dataitem的uid（猜测是一条记录所在的dataitem的uid）
     * @param key
     * @param uid
     * @throws Exception
     */
    public void insert(Object key, long uid) throws Exception {
        // 获取key的ukey
        long uKey = value2Uid(key);
        // 将ukey和uid插入到节点中
        bt.insert(uKey, uid);
    }

    /**
     * 从b+树中搜索范围内的uids
     * @param left
     * @param right
     * @return
     * @throws Exception
     */
    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }

    /**
     * 将字符串转化为对应类型的值
     * 传入的str是字段的值（字符串类型）
     * @param str
     * @return
     */
    public Object string2Value(String str) {
        switch(fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    /**
     * 根据key和fieldType，输出key的uid
     * key应该是这个字段的值
     * @param key
     * @return
     */
    public long value2Uid(Object key) {
        long uid = 0;
        switch(fieldType) {
            case "string":
                uid = Parser.str2Uid((String)key);
                break;
            case "int32":
                int uint = (int)key;
                return (long)uint;
            case "int64":
                uid = (long)key;
                break;
        }
        return uid;
    }

    /**
     * 将value转化为byte数组
     * @param v
     * @return
     */
    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch(fieldType) {
            case "int32":
                raw = Parser.int2Byte((int)v);
                break;
            case "int64":
                raw = Parser.long2Byte((long)v);
                break;
            case "string":
                raw = Parser.string2Byte((String)v);
                break;
        }
        return raw;
    }

    /**
     * 字段的值结果类
     */
    class ParseValueRes {
        Object v;
        // 占用字节大小
        int shift;
    }

    /**
     * 将byte数组解析为ParseValueRes对象
     * @param raw
     * @return
     */
    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch(fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }

    /**
     * 打印出字段的值
     * @param v
     * @return
     */
    public String printValue(Object v) {
        String str = null;
        switch(fieldType) {
            case "int32":
                str = String.valueOf((int)v);
                break;
            case "int64":
                str = String.valueOf((long)v);
                break;
            case "string":
                str = (String)v;
                break;
        }
        return str;
    }

    /**
     * 将字段输出成字符串
     * @return
     */
    @Override
    public String toString() {
        return new StringBuilder("(")
            .append(fieldName)
            .append(", ")
            .append(fieldType)
            .append(index!=0?", Index":", NoIndex")
            .append(")")
            .toString();
    }

    /**
     * 根据传入的SingleExpression对象（例如id > 5），计算出FieldCalRes对象（即表达式的结果）
     * @param exp
     * @return
     * @throws Exception
     */
    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch(exp.compareOp) {
            case "<":
                res.left = 0;
                v = string2Value(exp.value);
                // value2Uid返回的是数值
                res.right = value2Uid(v);
                if(res.right > 0) {
                    res.right --;
                }
                break;
            case "=":
                v = string2Value(exp.value);
                res.left = value2Uid(v);
                res.right = res.left;
                break;
            case ">":
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                res.left = value2Uid(v) + 1;
                break;
        }
        return res;
    }
}
