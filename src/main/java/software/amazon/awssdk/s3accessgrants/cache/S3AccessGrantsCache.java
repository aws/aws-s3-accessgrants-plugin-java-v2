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

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
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
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.services.s3control.model.Privilege;
import software.amazon.awssdk.services.s3control.model.S3ControlException;

/**
 * This class caches the credentials returned by Access Grants.
 */
public class S3AccessGrantsCache {

    private AsyncCache<CacheKey, AwsCredentialsIdentity> cache;
    private final S3ControlAsyncClient s3ControlAsyncClient;
    private int maxCacheSize;
    private final S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver;
    private final int cacheExpirationTimePercentage;

    private S3AccessGrantsCache (@NotNull S3ControlAsyncClient s3ControlAsyncClient,
                                 S3AccessGrantsCachedAccountIdResolver resolver, int maxCacheSize, int cacheExpirationTimePercentage) {
        if (s3ControlAsyncClient == null) {
            throw new IllegalArgumentException("S3ControlAsyncClient is required");
        }
        this.s3ControlAsyncClient = s3ControlAsyncClient;
        this.s3AccessGrantsCachedAccountIdResolver = resolver;
        this.cacheExpirationTimePercentage = cacheExpirationTimePercentage;
        this.maxCacheSize = maxCacheSize;
        this.cache = Caffeine.newBuilder()
                                          .maximumSize(maxCacheSize)
                                          .expireAfter(new CustomCacheExpiry<>())
                                          .recordStats()
                                          .buildAsync();
    }

    protected S3AccessGrantsCachedAccountIdResolver getS3AccessGrantsCachedAccountIdResolver() {
        return this.s3AccessGrantsCachedAccountIdResolver;
    }

    protected static S3AccessGrantsCache.Builder builder() {
        return new S3AccessGrantsCache.BuilderImpl();
    }

    public interface Builder {
        S3AccessGrantsCache build();
        S3AccessGrantsCache buildWithAccountIdResolver();
        S3AccessGrantsCache.Builder s3ControlAsyncClient(S3ControlAsyncClient s3ControlAsyncClient);
        S3AccessGrantsCache.Builder maxCacheSize(int maxCacheSize);
        S3AccessGrantsCache.Builder cacheExpirationTimePercentage(int cacheExpirationTimePercentage);
        S3AccessGrantsCache.Builder s3AccessGrantsCachedAccountIdResolver(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver);
    }

    static final class BuilderImpl implements S3AccessGrantsCache.Builder {
        private S3ControlAsyncClient s3ControlAsyncClient;
        private int maxCacheSize = DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
        private S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver;
        private int cacheExpirationTimePercentage;

        private BuilderImpl() {
        }

        @Override
        public S3AccessGrantsCache build() {
            S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver =
                S3AccessGrantsCachedAccountIdResolver.builder().S3ControlAsyncClient(s3ControlAsyncClient).build();
            return new S3AccessGrantsCache(s3ControlAsyncClient, s3AccessGrantsCachedAccountIdResolver, maxCacheSize, cacheExpirationTimePercentage);
        }

        @Override
        public S3AccessGrantsCache buildWithAccountIdResolver() {
                    return new S3AccessGrantsCache(s3ControlAsyncClient, s3AccessGrantsCachedAccountIdResolver, maxCacheSize,
                                                   cacheExpirationTimePercentage);
                }

        @Override
        public Builder s3ControlAsyncClient(S3ControlAsyncClient s3ControlAsyncClient) {
            this.s3ControlAsyncClient = s3ControlAsyncClient;
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
     * @param accountId Account Id of the requester
     * @param s3AccessGrantsAccessDeniedCache instance of S3AccessGrantsAccessDeniedCache
     * @return cached Access Grants credentials.
     */
    protected CompletableFuture<AwsCredentialsIdentity> getCredentials (CacheKey cacheKey, String accountId,
                                                  S3AccessGrantsAccessDeniedCache s3AccessGrantsAccessDeniedCache) throws S3ControlException {

        CompletableFuture<AwsCredentialsIdentity> credentials = searchKeyInCache(cacheKey);
        if (credentials == null &&
            (cacheKey.permission == Permission.READ ||
             cacheKey.permission == Permission.WRITE)) {
            credentials = searchKeyInCache(cacheKey.toBuilder().permission(Permission.READWRITE).build());
        }

        if (credentials == null) {
            try {
                credentials = getCredentialsFromService(cacheKey,accountId).thenApply(accessGrantsCredentials -> {
                    Instant expirationTime = accessGrantsCredentials.expiration();
                    Instant now = Instant.now();
                    long duration =
                        (long) (now.getEpochSecond() - expirationTime.getEpochSecond() * (cacheExpirationTimePercentage / 100.0f));
                    AwsSessionCredentials sessionCredentials = AwsSessionCredentials.builder().accessKeyId(accessGrantsCredentials.accessKeyId())
                                                                                    .secretAccessKey(accessGrantsCredentials.secretAccessKey())
                                                                                    .sessionToken(accessGrantsCredentials.sessionToken()).build();

                    putValueInCache(cacheKey, CompletableFuture.supplyAsync(()-> sessionCredentials), duration);
                    return sessionCredentials;
                });
            } catch (S3ControlException s3ControlException) {
                if (s3ControlException.statusCode() == 403) {
                    s3AccessGrantsAccessDeniedCache.putValueInCache(cacheKey, s3ControlException);
                }
                throw s3ControlException;
            }
        }
        return credentials;
    }

    /**
     * This method calls Access Grants service to get the credentials.
     * @param cacheKey CacheKey consists of AwsCredentialsIdentity, Permission, and S3Prefix.
     * @param accountId Account Id of the requester.
     * @return Access Grants Credentials.
     * @throws S3ControlException throws Exception received from service.
     */
    private CompletableFuture<Credentials> getCredentialsFromService(CacheKey cacheKey, String accountId) throws S3ControlException{
        String resolvedAccountId = s3AccessGrantsCachedAccountIdResolver.resolve(accountId, cacheKey.s3Prefix);
        GetDataAccessRequest dataAccessRequest = GetDataAccessRequest.builder()
                                                                     .accountId(resolvedAccountId)
                                                                     .target(cacheKey.s3Prefix)
                                                                     .permission(cacheKey.permission)
                                                                     .privilege(Privilege.DEFAULT)
                                                                     .build();

        return s3ControlAsyncClient.getDataAccess(dataAccessRequest).thenApply(dataAccessResponse -> dataAccessResponse.credentials());
    }

    /**
     * This method searches for the cacheKey in the cache. It will also search for a cache key with higher S3 prefix than
     * requested.
     * @param cacheKey CacheKey consists of AwsCredentialsIdentity, Permission, and S3Prefix.
     * @return cached Access Grants credentials.
     */
    private CompletableFuture<AwsCredentialsIdentity> searchKeyInCache (CacheKey cacheKey) {

        CompletableFuture<AwsCredentialsIdentity> cacheValue;
        String prefix = cacheKey.s3Prefix;
        while (!prefix.equals("s3:/")){
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
    void putValueInCache(CacheKey cacheKey, CompletableFuture<AwsCredentialsIdentity> credentials, long duration) {

        cache.put(cacheKey, credentials);
        cache.synchronous().policy().expireVariably().ifPresent(ev -> ev.setExpiresAfter(cacheKey, duration, TimeUnit.SECONDS));
    }

    /**
     * This method splits S3Prefix on last "/" and returns the first part.
     */
    private String getNextPrefix(String prefix){

        return prefix.substring(0, prefix.lastIndexOf("/"));
    }

    /***
     * @return metrics captured by the cache
     */
    protected CacheStats getCacheStats() { return cache.synchronous().stats();}

    /**
     * Invalidates the cache.
     */
    @VisibleForTesting
    void invalidateCache() {
        cache.synchronous().invalidateAll();
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

