package cn.tangrl.javadb.backend.tbm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Bytes;

import cn.tangrl.javadb.backend.parser.statement.Create;
import cn.tangrl.javadb.backend.parser.statement.Delete;
import cn.tangrl.javadb.backend.parser.statement.Insert;
import cn.tangrl.javadb.backend.parser.statement.Select;
import cn.tangrl.javadb.backend.parser.statement.Update;
import cn.tangrl.javadb.backend.parser.statement.Where;
import cn.tangrl.javadb.backend.tbm.Field.ParseValueRes;
import cn.tangrl.javadb.backend.tm.TransactionManagerImpl;
import cn.tangrl.javadb.backend.utils.Panic;
import cn.tangrl.javadb.backend.utils.ParseStringRes;
import cn.tangrl.javadb.backend.utils.Parser;
import cn.tangrl.javadb.common.Error;

/**
 * Table 维护了表结构
 * 一个数据库中存在多张表，TBM 使用链表的形式将其组织起来，每一张表都保存一个指向下一张表的 UID。
 * 表的二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 * 与field一样，以entry的形式存储
 *
 * 记录是以字段的顺序，作为一个entry存储的。
 * TODO id一定有索引吗？查找是以id为索引进行的吗？
 */
public class Table {
    /**
     * tbm模块 表管理器，用于管理数据库表
     */
    TableManager tbm;
    /**
     * 表的唯一标识符，即对应dataitem的uid
     */
    long uid;
    /**
     * 表的名称
     */
    String name;
    /**
     *  表的状态，未用到
     */
    byte status;
    /**
     * 下一个表的uid，即对应dataitem的uid
     */
    long nextUid;
    /**
     * 表的字段对象列表
     */
    List<Field> fields = new ArrayList<>();

    /**
     * 从数据库中加载一个表，即通过uid读取对应entry
     * @param tbm
     * @param uid
     * @return
     */
    public static Table loadTable(TableManager tbm, long uid) {
        // 初始化一个字节数组用于存储从数据库中读取的原始数据
        byte[] raw = null;
        // 使用表管理器的版本管理器从数据库中读取指定uid的表的原始数据
        try {
            raw = ((TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        // 断言原始数据不为空
        assert raw != null;
        // 创建一个新的表对象
        Table tb = new Table(tbm, uid);
        // 使用原始数据解析表对象，并返回这个表对象
        return tb.parseSelf(raw);
    }

    /**
     * 创建一个新的数据库表
     * 唯一值得注意的一个小点是，在创建新表时，采用的时头插法，所以每次创建表都需要更新 Booter 文件。
     * @param tbm
     * @param nextUid
     * @param xid
     * @param create
     * @return
     * @throws Exception
     */
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        // 创建一个新的表对象
        Table tb = new Table(tbm, create.tableName, nextUid);
        // 遍历创建表语句中的所有字段
        for(int i = 0; i < create.fieldName.length; i ++) {
            // 获取字段名和字段类型
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            // 判断该字段是否需要建立索引
            boolean indexed = false;
            for(int j = 0; j < create.index.length; j ++) {
                if(fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            // 创建字段对象，并添加到表对象中
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }
        // 将表对象的状态持久化到存储系统中，并返回表对象
        return tb.persistSelf(xid);
    }

    /**
     * 构造函数
     * @param tbm
     * @param uid
     */
    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    /**
     * 构造函数
     * @param tbm
     * @param tableName
     * @param nextUid
     */
    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    /**
     * 解析表对象，传入byte数组，返回表对象
     * @param raw
     * @return
     */
    private Table parseSelf(byte[] raw) {
        // 初始化位置变量
        int position = 0;
        // 使用Parser.parseString方法解析原始数据中的字符串
        ParseStringRes res = Parser.parseString(raw);
        // 将解析出的字符串赋值给表的名称
        name = res.str;
        // 更新位置变量
        position += res.next;
        // 使用Parser.parseLong方法解析原始数据中的长整数，并赋值给下一个uid
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        // 更新位置变量
        position += 8;

        // 当位置变量小于原始数据的长度时，继续循环
        while(position < raw.length) {
            // 使用Parser.parseLong方法解析原始数据中的长整数，并赋值给uid
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
            // 更新位置变量
            position += 8;
            // 使用Field.loadField方法加载字段，并添加到表的字段对象列表中
            fields.add(Field.loadField(this, uid));
        }
        // 返回当前表对象
        return this;
    }

    /**
     * 将Table对象的状态持久化到存储系统中。
     * @param xid
     * @return
     * @throws Exception
     */
    private Table persistSelf(long xid) throws Exception {
        // 将表名转换为字节数组
        byte[] nameRaw = Parser.string2Byte(name);
        // 将下一个uid转换为字节数组
        byte[] nextRaw = Parser.long2Byte(nextUid);
        // 创建一个空的字节数组，用于存储字段的uid
        byte[] fieldRaw = new byte[0];
        // 遍历所有的字段
        for(Field field : fields) {
            // 将字段的uid转换为字节数组，并添加到fieldRaw中
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        // 将表名、下一个uid和所有字段的uid插入到存储系统中，返回插入的uid。即对应dataitem的uid。
        uid = ((TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        // 返回当前Table对象
        return this;
    }

    /**
     * 删除表中的数据，返回删除的数量
     * @param xid
     * @param delete
     * @return
     * @throws Exception
     */
    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for (Long uid : uids) {
            if(((TableManagerImpl)tbm).vm.delete(xid, uid)) {
                count ++;
            }
        }
        return count;
    }

    /**
     * 更新表中的记录，返回更新的记录
     * update
     * @param xid
     * @param update
     * @return
     * @throws Exception
     */
    public int update(long xid, Update update) throws Exception {
        List<Long> uids = parseWhere(update.where);
        Field fd = null;
        for (Field f : fields) {
            if(f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        if(fd == null) {
            throw Error.FieldNotFoundException;
        }
        Object value = fd.string2Value(update.value);
        int count = 0;
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;

            ((TableManagerImpl)tbm).vm.delete(xid, uid);

            Map<String, Object> entry = parseEntry(raw);
            entry.put(fd.fieldName, value);
            raw = entry2Raw(entry);
            long uuid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
            
            count ++;

            for (Field field : fields) {
                if(field.isIndexed()) {
                    field.insert(entry.get(field.fieldName), uuid);
                }
            }
        }
        return count;
    }

    /**
     * select读取表中的记录
     * @param xid
     * @param read
     * @return
     * @throws Exception
     */
    public String read(long xid, Select read) throws Exception {
        List<Long> uids = parseWhere(read.where);
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }

    /**
     * 插入记录
      * @param xid
     * @param insert
     * @throws Exception
     */
    public void insert(long xid, Insert insert) throws Exception {
        // 获取 value 按顺序 拼接成的byte数组 raw
        Map<String, Object> entry = string2Entry(insert.values);
        byte[] raw = entry2Raw(entry);
        // 将 raw 以entry插入到数据库中，获取对应dataitem的uid
        long uid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
        // 将uid索引到对应的需要索引的字段上
        for (Field field : fields) {
            if(field.isIndexed()) {
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }

    /**
     * 将values[]，即一条记录的值，转换成一个Map<String, Object>
     * 这个Map是fieldName:value的映射
     * @param values
     * @return
     * @throws Exception
     */
    private Map<String, Object> string2Entry(String[] values) throws Exception {
        if(values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        return entry;
    }

    /**
     * 解析 Where，返回存储记录的 uids，即 DataItem
     * 目前 Where 只支持两个条件的与和或。
     * @param where
     * @return
     * @throws Exception
     */
    private List<Long> parseWhere(Where where) throws Exception {
        long l0=0, r0=0, l1=0, r1=0;
        boolean single = false;
        Field fd = null;
        if(where == null) {
            for (Field field : fields) {
                if(field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            for (Field field : fields) {
                if(field.fieldName.equals(where.singleExp1.field)) {
                    if(!field.isIndexed()) {
                        throw Error.FieldNotIndexedException;
                    }
                    fd = field;
                    break;
                }
            }
            if(fd == null) {
                throw Error.FieldNotFoundException;
            }
            CalWhereRes res = calWhere(fd, where);
            l0 = res.l0; r0 = res.r0;
            l1 = res.l1; r1 = res.r1;
            single = res.single;
        }
        List<Long> uids = fd.search(l0, r0);
        if(!single) {
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }

    /**
     * where的范围对象
     */
    class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }

    /**
     * 计算where的范围，返回CalWhereRes对象
     * @param fd
     * @param where
     * @return
     * @throws Exception
     */
    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch(where.logicOp) {
            case "":
                res.single = true;
                FieldCalRes r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                break;
            case "or":
                res.single = false;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                break;
            case "and":
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                if(res.l1 > res.l0) res.l0 = res.l1;
                if(res.r1 < res.r0) res.r0 = res.r1;
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return res;
    }

    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if(i == fields.size()-1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        for (Field field : fields) {
            ParseValueRes r = field.parserValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, r.v);
            pos += r.shift;
        }
        return entry;
    }

    /**
     * 将Map<fieldname,value>中的value拼接成一个byte数组
     * @param entry
     * @return
     */
    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for(Field field : fields) {
            sb.append(field.toString());
            if(field == fields.get(fields.size()-1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
