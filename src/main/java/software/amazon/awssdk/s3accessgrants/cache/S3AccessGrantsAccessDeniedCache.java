/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.s3accessgrants.cache;

import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.ACCESS_DENIED_CACHE_SIZE;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.services.s3control.model.S3ControlException;

public class S3AccessGrantsAccessDeniedCache {
    private Cache<CacheKey, S3ControlException> cache;
    private int maxCacheSize;

    private S3AccessGrantsAccessDeniedCache () {
        this.maxCacheSize = ACCESS_DENIED_CACHE_SIZE;
    }

    public static S3AccessGrantsAccessDeniedCache.Builder builder() {
        return new S3AccessGrantsAccessDeniedCache.BuilderImpl();
    }

    public interface Builder {
        S3AccessGrantsAccessDeniedCache build();
        S3AccessGrantsAccessDeniedCache.Builder maxCacheSize(int maxCacheSize);
    }

    static final class BuilderImpl implements S3AccessGrantsAccessDeniedCache.Builder {

        private int maxCacheSize = ACCESS_DENIED_CACHE_SIZE;
        private BuilderImpl() {
        }

        @Override
        public S3AccessGrantsAccessDeniedCache build() {
            S3AccessGrantsAccessDeniedCache s3AccessGrantsAccessDeniedCache = new S3AccessGrantsAccessDeniedCache();
            s3AccessGrantsAccessDeniedCache.maxCacheSize = maxCacheSize();
            s3AccessGrantsAccessDeniedCache.cache = Caffeine.newBuilder()
                                                            .maximumSize(maxCacheSize)
                                                            .expireAfterWrite(5, TimeUnit.MINUTES)
                                                            .recordStats()
                                                            .build();

            return s3AccessGrantsAccessDeniedCache;
        }

        @Override
        public Builder maxCacheSize(int maxCacheSize) {
            if (maxCacheSize <= 0 || maxCacheSize > MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE) {
                throw new IllegalArgumentException(String.format("maxCacheSize needs to be in range (0, %d]",
                                                                 MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE));
            }
            this.maxCacheSize = maxCacheSize;
            return this;
        }

        public int maxCacheSize() {
            return maxCacheSize;
        }
    }

    /**
     * This method throws an exception when there is a cache hit.
     * @param cacheKey CacheKey consists of AwsCredentialsIdentity, Permission, and S3Prefix.
     * @return null
     * @throws S3ControlException when it's a cache hit.
     */
    protected S3ControlException getValueFromCache (CacheKey cacheKey) {
        return cache.getIfPresent(cacheKey);
    }

    /**
     * This method puts an entry in cache.
     * @param cacheKey CacheKey consists of AwsCredentialsIdentity, Permission, and S3Prefix.
     * @param exception The cache value is an Access Denied Exception.
     */
    protected void putValueInCache(CacheKey cacheKey, S3ControlException exception) {
        cache.put(cacheKey, exception);
    }

    /**
     * Invalidates the cache.
     */
    void invalidateCache() {
        cache.invalidateAll();
    }

    /***
     * @return metrics captured by the cache
     */
    protected CacheStats getCacheStats() { return cache.stats(); }

}
