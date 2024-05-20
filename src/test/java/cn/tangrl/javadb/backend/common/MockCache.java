package cn.tangrl.javadb.backend.common;

/**
 * 模拟Cache类，继承AbstractCache类
 * 实现抽象方法，用于测试
 */
public class MockCache extends AbstractCache<Long> {

    public MockCache() {
        super(50);
    }

    @Override
    protected Long getForCache(long key) throws Exception {
        return key;
    }

    @Override
    protected void releaseForCache(Long obj) {}
    
}
