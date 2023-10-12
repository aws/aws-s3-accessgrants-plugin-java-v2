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

import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.assertj.core.util.VisibleForTesting;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.services.s3control.S3ControlClient;
import software.amazon.awssdk.services.s3control.model.Credentials;
import software.amazon.awssdk.services.s3control.model.GetDataAccessRequest;
import software.amazon.awssdk.services.s3control.model.GetDataAccessResponse;
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.services.s3control.model.Privilege;
import software.amazon.awssdk.services.s3control.model.S3ControlException;

/**
 * This class caches the credentials returned by Access Grants.
 */
public class S3AccessGrantsCache {

    Cache<CacheKey, AwsCredentialsIdentity> cache;
    private final S3ControlClient s3ControlClient;
    private int maxCacheSize;
    private final S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver;


    private S3AccessGrantsCache (@NotNull S3ControlClient s3ControlClient) {
        if (s3ControlClient == null) {
            throw new IllegalArgumentException("s3ControlClient is required");
        }
        this.s3ControlClient = s3ControlClient;
        this.maxCacheSize = DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
        this.s3AccessGrantsCachedAccountIdResolver = new S3AccessGrantsCachedAccountIdResolver(s3ControlClient);
    }

    private S3AccessGrantsCache (@NotNull S3ControlClient s3ControlClient, S3AccessGrantsCachedAccountIdResolver resolver) {
        if (s3ControlClient == null) {
            throw new IllegalArgumentException("s3ControlClient is required");
        }
        this.s3ControlClient = s3ControlClient;
        this.maxCacheSize = DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
        this.s3AccessGrantsCachedAccountIdResolver = resolver;

        this.maxCacheSize = maxCacheSize();
        this.cache = Caffeine.newBuilder()
                                          .maximumSize(maxCacheSize)
                                          .expireAfter(new CustomExpiry<>())
                                          .recordStats()
                                          .build();
    }

    public int maxCacheSize() {
        return maxCacheSize;
    }

    public static S3AccessGrantsCache.Builder builder() {
        return new S3AccessGrantsCache.BuilderImpl();
    }

    public interface Builder {
        S3AccessGrantsCache build();
        S3AccessGrantsCache buildWithAccountIdResolver();
        S3AccessGrantsCache.Builder s3ControlClient(S3ControlClient s3ControlClient);
        S3AccessGrantsCache.Builder maxCacheSize(int maxCacheSize);
        S3AccessGrantsCache.Builder s3AccessGrantsCachedAccountIdResolver(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver);
    }

    static final class BuilderImpl implements S3AccessGrantsCache.Builder {
        private S3ControlClient s3ControlClient;
        private int maxCacheSize = DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
        private S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver;

        private BuilderImpl() {
        }

        @Override
        public S3AccessGrantsCache build() {
            S3AccessGrantsCache accessGrantsCache = new S3AccessGrantsCache(s3ControlClient);
            accessGrantsCache.maxCacheSize = maxCacheSize();
            accessGrantsCache.cache = Caffeine.newBuilder()
                                              .maximumSize(maxCacheSize)
                                              .expireAfter(new CustomExpiry<>())
                                              .recordStats()
                                              .build();
            return accessGrantsCache;
        }

        @Override
        public S3AccessGrantsCache buildWithAccountIdResolver() {
                    S3AccessGrantsCache accessGrantsCache = new S3AccessGrantsCache(s3ControlClient,
                                                                                    s3AccessGrantsCachedAccountIdResolver);
                    accessGrantsCache.maxCacheSize = maxCacheSize();
                    accessGrantsCache.cache = Caffeine.newBuilder()
                                                      .maximumSize(maxCacheSize)
                                                      .expireAfter(new CustomExpiry<>())
                                                      .recordStats()
                                                      .build();
                    return accessGrantsCache;
                }

        @Override
        public Builder s3ControlClient(S3ControlClient s3ControlClient) {
            this.s3ControlClient = s3ControlClient;
            return this;
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

        @Override
        public Builder s3AccessGrantsCachedAccountIdResolver(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver) {
            this.s3AccessGrantsCachedAccountIdResolver = s3AccessGrantsCachedAccountIdResolver;
            return this;
        }

        public int maxCacheSize() {
            return maxCacheSize;
        }
    }

    /**
     * This method searches for the cacheKey in the cache. It will also search for a cache key with broader permission than
     * requested.
     * @param cacheKey CacheKey consists of AwsCredentialsIdentity, Permission, and S3Prefix.
     * @return cached Access Grants credentials.
     */
    public AwsCredentialsIdentity getValueFromCache (CacheKey cacheKey, String accountId) {

        AwsCredentialsIdentity credentials = searchKeyInCache(cacheKey);
        if (credentials == null &&
            (cacheKey.permission == Permission.READ ||
             cacheKey.permission == Permission.WRITE)) {
            credentials = searchKeyInCache(cacheKey.toBuilder().permission(Permission.READWRITE).build());
        }

        if (credentials == null) {
            credentials = getCredentialsFromService(cacheKey, accountId);
        }
        return credentials;
    }

    @VisibleForTesting
    public AwsCredentialsIdentity getCredentialsFromService(CacheKey cacheKey, String account) throws S3ControlException{
        String accountId = s3AccessGrantsCachedAccountIdResolver.resolve(account, cacheKey.s3Prefix);
        AwsSessionCredentials sessionCredentials;
        long duration;

        try {
            GetDataAccessRequest dataAccessRequest = GetDataAccessRequest.builder()
                                                                         .accountId(accountId)
                                                                         .target(cacheKey.s3Prefix)
                                                                         .permission(cacheKey.permission)
                                                                         .privilege(Privilege.DEFAULT)
                                                                         .build();

            GetDataAccessResponse getDataAccessResponse = s3ControlClient.getDataAccess(dataAccessRequest);
            Credentials credentials = getDataAccessResponse.credentials();
            sessionCredentials = AwsSessionCredentials.builder().accessKeyId(credentials.accessKeyId())
                                                                            .secretAccessKey(credentials.secretAccessKey())
                                                                            .sessionToken(credentials.sessionToken()).build();
            Instant ttl = getDataAccessResponse.credentials().expiration();
            Instant now = Instant.now();
            // cache 90% of ttl to prevent Access Denied because of credentials expiry.
            duration = (long)(now.getEpochSecond() - ttl.getEpochSecond() * (90.0f / 100.0f));
        }
        catch (S3ControlException e) {
            // Todo: cache in Access Denied cache and return
            throw e;
        }
        putValueInCache(cacheKey, sessionCredentials, duration);
        return sessionCredentials;
    }

    /**
     * This method searches for the cacheKey in the cache. It will also search for a cache key with higher S3 prefix than
     * requested.
     * @param cacheKey CacheKey consists of AwsCredentialsIdentity, Permission, and S3Prefix.
     * @return cached Access Grants credentials.
     */
    private AwsCredentialsIdentity searchKeyInCache (CacheKey cacheKey) {

        AwsCredentialsIdentity cacheValue;
        String prefix = cacheKey.s3Prefix;
        while (!prefix.equals("s3:/")) {
            cacheValue = cache.getIfPresent(cacheKey.toBuilder().s3Prefix(prefix).build());
            if (cacheValue != null){
                return cacheValue;
            }
            prefix = getNextPrefix(prefix);
        }
        return null;

    }

    /**
     * This method puts an entry in cache.
     * @param cacheKey CacheKey consists of AwsCredentialsIdentity, Permission, and S3Prefix.
     * @param credentials The cache value credentials returned by Access Grants.
     * @param duration TTL for the cache entry.
     */
    public void putValueInCache(CacheKey cacheKey, AwsCredentialsIdentity credentials, long duration) {

        cache.put(cacheKey, credentials);
        cache.policy().expireVariably().ifPresent(ev -> ev.setExpiresAfter(cacheKey, duration, TimeUnit.SECONDS));
    }

    /**
     * This method splits S3Prefix on last "/" and returns the first part.
     */
    private String getNextPrefix(String prefix){

        return prefix.substring(0, prefix.lastIndexOf("/"));
    }

    /**
     * Invalidates the cache.
     */
    public void invalidateCache() {
        cache.invalidateAll();
    }

    private static class CustomExpiry<K, V> implements Expiry<K, V> {

        @Override
        public long expireAfterCreate(K key, V value, long currentTime) {
            return Long.MIN_VALUE;  // Keep min by default
        }

        @Override
        public long expireAfterUpdate(K key, V value, long currentTime, long currentDuration) {
            return currentDuration;  // Retain original expiration time if updated
        }

        @Override
        public long expireAfterRead(K key, V value, long currentTime, long currentDuration) {
            return currentDuration;  // Retain original expiration time if read
        }
    }

}

