package cn.tangrl.javadb.backend.server;

import cn.tangrl.javadb.backend.parser.Parser;
import cn.tangrl.javadb.backend.parser.statement.Abort;
import cn.tangrl.javadb.backend.parser.statement.Begin;
import cn.tangrl.javadb.backend.parser.statement.Commit;
import cn.tangrl.javadb.backend.parser.statement.Create;
import cn.tangrl.javadb.backend.parser.statement.Delete;
import cn.tangrl.javadb.backend.parser.statement.Insert;
import cn.tangrl.javadb.backend.parser.statement.Select;
import cn.tangrl.javadb.backend.parser.statement.Show;
import cn.tangrl.javadb.backend.parser.statement.Update;
import cn.tangrl.javadb.backend.tbm.BeginRes;
import cn.tangrl.javadb.backend.tbm.TableManager;
import cn.tangrl.javadb.common.Error;

/**
 * Executor 类负责解析和执行 SQL 语句。
 * 它与 TableManager 交互，处理事务并执行各种 SQL 操作。
 */
public class Executor {
    /**
     * 事务xid
     * xid =0 ，表示此时没有事务在执行
     */
    private long xid;
    /**
     * tbm模块对象
     */
    TableManager tbm;

    /**
     * 构造函数
     * @param tbm
     */
    public Executor(TableManager tbm) {
        this.tbm = tbm;
        // xid为0表示没有事务在执行
        this.xid = 0;
    }

    /**
     * 关闭执行器
     * 如果此时有事务正在执行，回滚事务
     */
    public void close() {
        if(xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }
    }

    /**
     * 执行传入的sql字节数组
     * @param sql
     * @return
     * @throws Exception
     */
    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        // 解析出命令对象
        Object stat = Parser.Parse(sql);
        // 判断是什么命令，并执行
        if(Begin.class.isInstance(stat)) {
            // 如果xid不为0，说明此时有事务正在执行，不可以begin新的
            if(xid != 0) {
                throw Error.NestedTransactionException;
            }
            // 如果xid=0，表示。此时没有事务正在执行，开始一个事务。
            BeginRes r = tbm.begin((Begin)stat);
            // 对xid进行赋值
            xid = r.xid;
            return r.result;
        } else if(Commit.class.isInstance(stat)) {
            // 如果xid为0，说明没有事务在执行，无法commit，抛出异常
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        } else if(Abort.class.isInstance(stat)) {
            // 如果xid为0，说明没有事务在执行，无法abort，抛出异常
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else {
            // 执行非事务操作命令
            return execute2(stat);
        }
    }

    /**
     * 执行非事务操作命令
     * 当执行单条语句的时候，也要开始一个事务
     * @param stat
     * @return
     * @throws Exception
     */
    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        // 如果xid == 0，begin一个事务
        if(xid == 0) {
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }
        // 执行相对应的命令操作
        try {
            byte[] res = null;
            if(Show.class.isInstance(stat)) {
                res = tbm.show(xid);
            } else if(Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create)stat);
            } else if(Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select)stat);
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert)stat);
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete)stat);
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update)stat);
            }
            return res;
        } catch(Exception e1) {
            e = e1;
            throw e;
        } finally {
            // 提交一个事务，有异常就回滚
            if(tmpTransaction) {
                if(e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                // 结束设置xid=0，表示此时没有事务在执行
                xid = 0;
            }
        }
    }
}
