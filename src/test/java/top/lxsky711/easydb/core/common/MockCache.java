package top.lxsky711.easydb.core.common;

public class MockCache extends AbstractCache<Long> {

    public MockCache() {
        super(50);
    }

    @Override
    protected Long getCacheByKey(long key) throws Exception {
        return key;
    }

    @Override
    protected void releaseCacheForObject(Long obj) {}
    
}
