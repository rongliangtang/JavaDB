package cn.tangrl.javadb.backend.tbm;

import cn.tangrl.javadb.backend.dm.DataManager;
import cn.tangrl.javadb.backend.parser.statement.Begin;
import cn.tangrl.javadb.backend.parser.statement.Create;
import cn.tangrl.javadb.backend.parser.statement.Delete;
import cn.tangrl.javadb.backend.parser.statement.Insert;
import cn.tangrl.javadb.backend.parser.statement.Select;
import cn.tangrl.javadb.backend.parser.statement.Update;
import cn.tangrl.javadb.backend.utils.Parser;
import cn.tangrl.javadb.backend.vm.VersionManager;

/**
 * 接口
 * TBM表管理器 实现了对字段结构和表结构的管理。
 * TBM 基于 VM，单个字段信息和表信息都是直接保存在 Entry 中。
 * 也基于 IM，会使用到索引。
 * 字段的二进制表示：[FieldName][TypeName][IndexUid]。
 *  FieldName 和 TypeName，以及后面的表明，存储的都是字节形式的字符串。这里规定一个字符串的存储方式：[StringLength][StringData]。
 *
 *  TypeName 为字段的类型，限定为 int32、int64 和 string 类型。
 *  如果这个字段有索引，那个 IndexUID 指向了索引二叉树的根，否则该字段为 0。
 *
 *  由于 TableManager 已经是直接被最外层 Server 调用（MYDB 是 C/S 结构），这些方法直接返回执行的结果，例如错误信息或者结果信息的字节数组（可读）。
 */
public interface TableManager {
    BeginRes begin(Begin begin);
    byte[] commit(long xid) throws Exception;
    byte[] abort(long xid);

    byte[] show(long xid);
    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;
    byte[] read(long xid, Select select) throws Exception;
    byte[] update(long xid, Update update) throws Exception;
    byte[] delete(long xid, Delete delete) throws Exception;

    /**
     * 根据path创建TableManagerImpl对象的静态工厂方法
     * @param path
     * @param vm
     * @param dm
     * @return
     */
    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        // 初始化为0
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    /**
     * 根据path打开TableManagerImpl对象的静态工厂方法
     * @param path
     * @param vm
     * @param dm
     * @return
     */
    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
