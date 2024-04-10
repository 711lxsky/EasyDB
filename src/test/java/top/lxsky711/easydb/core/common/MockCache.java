package top.lxsky711.easydb.core.common;

import top.lxsky711.easydb.common.exception.ErrorException;

public class MockCache extends AbstractCache<Long> {

    public MockCache() throws ErrorException {
        super(50);
    }

    @Override
    protected Long getCacheFromDataSourceByKey(long key) {
        return key;
    }

    @Override
    protected void releaseCacheForObject(Long obj) {}
    
}
