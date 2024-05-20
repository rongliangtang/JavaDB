package cn.tangrl.javadb.backend.tm;

/**
 * 模拟TM
 * 以便在不需要真正的事务管理器实现时提供一个简单的替代品
 * 测试其他模块的时候会用到这个模拟TM。例如DM，测试DM的时候不需要用到真实的TM
 */
public class MockTransactionManager implements TransactionManager {

    @Override
    public long begin() {
        return 0;
    }

    @Override
    public void commit(long xid) {}

    @Override
    public void abort(long xid) {}

    @Override
    public boolean isActive(long xid) {
        return false;
    }

    @Override
    public boolean isCommitted(long xid) {
        return false;
    }

    @Override
    public boolean isAborted(long xid) {
        return false;
    }

    @Override
    public void close() {}
    
}
