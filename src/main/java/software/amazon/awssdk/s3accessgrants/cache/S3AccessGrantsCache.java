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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.assertj.core.util.VisibleForTesting;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
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
    private final S3ControlAsyncClient S3ControlAsyncClient;
    private int maxCacheSize;
    private final S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver;
    private final int cacheExpirationTimePercentage;


    private S3AccessGrantsCache (@NotNull S3ControlAsyncClient S3ControlAsyncClient, int maxCacheSize, int cacheExpirationTimePrecentage) {
        this(S3ControlAsyncClient, new S3AccessGrantsCachedAccountIdResolver(S3ControlAsyncClient), maxCacheSize,
             cacheExpirationTimePrecentage);
    }

    private S3AccessGrantsCache (@NotNull S3ControlAsyncClient S3ControlAsyncClient,
                                 S3AccessGrantsCachedAccountIdResolver resolver, int maxCacheSize, int cacheExpirationTimePrecentage) {
        if (S3ControlAsyncClient == null) {
            throw new IllegalArgumentException("S3ControlAsyncClient is required");
        }
        this.S3ControlAsyncClient = S3ControlAsyncClient;
        this.maxCacheSize = DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
        this.s3AccessGrantsCachedAccountIdResolver = resolver;
        this.cacheExpirationTimePercentage = cacheExpirationTimePrecentage;
        this.maxCacheSize = maxCacheSize();
        this.cache = Caffeine.newBuilder()
                                          .maximumSize(maxCacheSize)
                                          .expireAfter(new CustomCacheExpiry<>())
                                          .recordStats()
                                          .build();
    }

    protected int maxCacheSize() {
        return maxCacheSize;
    }

    protected static S3AccessGrantsCache.Builder builder() {
        return new S3AccessGrantsCache.BuilderImpl();
    }

    protected interface Builder {
        S3AccessGrantsCache build();
        S3AccessGrantsCache buildWithAccountIdResolver();
        S3AccessGrantsCache.Builder S3ControlAsyncClient(S3ControlAsyncClient S3ControlAsyncClient);
        S3AccessGrantsCache.Builder maxCacheSize(int maxCacheSize);
        S3AccessGrantsCache.Builder cacheExpirationTimePercentage(int cacheExpirationTimePrecentage);
        S3AccessGrantsCache.Builder s3AccessGrantsCachedAccountIdResolver(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver);
    }

    static final class BuilderImpl implements S3AccessGrantsCache.Builder {
        private S3ControlAsyncClient S3ControlAsyncClient;
        private int maxCacheSize;
        private S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver;
        private int cacheExpirationTimePercentage;

        private BuilderImpl() {
        }

        @Override
        public S3AccessGrantsCache build() {
            return new S3AccessGrantsCache(S3ControlAsyncClient, maxCacheSize, cacheExpirationTimePercentage);
        }

        @Override
        public S3AccessGrantsCache buildWithAccountIdResolver() {
                    return new S3AccessGrantsCache(S3ControlAsyncClient, s3AccessGrantsCachedAccountIdResolver, maxCacheSize, cacheExpirationTimePercentage);
                }

        @Override
        public Builder S3ControlAsyncClient(S3ControlAsyncClient S3ControlAsyncClient) {
            this.S3ControlAsyncClient = S3ControlAsyncClient;
            return this;
        }

        @Override
        public Builder maxCacheSize(int maxCacheSize) {
            this.maxCacheSize = maxCacheSize;
            return this;
        }

        @Override
        public Builder cacheExpirationTimePercentage(int cacheExpirationTimePrecentage) {
            this.cacheExpirationTimePercentage = cacheExpirationTimePrecentage;
            return this;
        }

        @Override
        public Builder s3AccessGrantsCachedAccountIdResolver(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver) {
            this.s3AccessGrantsCachedAccountIdResolver = s3AccessGrantsCachedAccountIdResolver;
            return this;
        }
    }

    /**
     * This method searches for the cacheKey in the cache. It will also search for a cache key with broader permission than
     * requested.
     * @param cacheKey CacheKey consists of AwsCredentialsIdentity, Permission, and S3Prefix.
     * @return cached Access Grants credentials.
     */
    protected AwsCredentialsIdentity getCredentials (CacheKey cacheKey, String accountId,
                                                  S3AccessGrantsAccessDeniedCache s3AccessGrantsAccessDeniedCache) throws S3ControlException {

        AwsCredentialsIdentity credentials = searchKeyInCache(cacheKey);
        if (credentials == null &&
            (cacheKey.permission == Permission.READ ||
             cacheKey.permission == Permission.WRITE)) {
            credentials = searchKeyInCache(cacheKey.toBuilder().permission(Permission.READWRITE).build());
        }

        if (credentials == null) {
            credentials = getCredentialsFromService(cacheKey, accountId, s3AccessGrantsAccessDeniedCache);
        }
        return credentials;
    }

    protected AwsCredentialsIdentity getCredentialsFromService(CacheKey cacheKey, String accountId,
                                                            S3AccessGrantsAccessDeniedCache s3AccessGrantsAccessDeniedCache) throws S3ControlException{
        String resolvedAccountId = s3AccessGrantsCachedAccountIdResolver.resolve(accountId, cacheKey.s3Prefix);
        AwsSessionCredentials sessionCredentials;
        long duration;

        try {
            GetDataAccessRequest dataAccessRequest = GetDataAccessRequest.builder()
                                                                         .accountId(resolvedAccountId)
                                                                         .target(cacheKey.s3Prefix)
                                                                         .permission(cacheKey.permission)
                                                                         .privilege(Privilege.DEFAULT)
                                                                         .build();

            CompletableFuture<GetDataAccessResponse> getDataAccessResponse =
                S3ControlAsyncClient.getDataAccess(dataAccessRequest);
            Credentials credentials = getDataAccessResponse.join().credentials();
            sessionCredentials = AwsSessionCredentials.builder().accessKeyId(credentials.accessKeyId())
                                                                            .secretAccessKey(credentials.secretAccessKey())
                                                                            .sessionToken(credentials.sessionToken()).build();
            Instant ttl = getDataAccessResponse.join().credentials().expiration();
            Instant now = Instant.now();
            duration = (long)(now.getEpochSecond() - ttl.getEpochSecond() * (cacheExpirationTimePercentage / 100.0f));
        }
        catch (S3ControlException s3ControlException) {
            if (s3ControlException.statusCode() == 403) {
                s3AccessGrantsAccessDeniedCache.putValueInCache(cacheKey, s3ControlException);
            }
            throw s3ControlException;
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
    @VisibleForTesting
    void putValueInCache(CacheKey cacheKey, AwsCredentialsIdentity credentials, long duration) {

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
    @VisibleForTesting
    void invalidateCache() {
        cache.invalidateAll();
    }

    private static class CustomCacheExpiry<K, V> implements Expiry<K, V> {

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

