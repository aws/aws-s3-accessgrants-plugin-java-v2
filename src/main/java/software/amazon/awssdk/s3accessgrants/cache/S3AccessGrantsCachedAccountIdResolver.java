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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.assertj.core.util.VisibleForTesting;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.services.s3control.endpoints.internal.Arn;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixRequest;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixResponse;
import software.amazon.awssdk.services.s3control.model.S3ControlException;
import software.amazon.awssdk.utils.Logger;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.DEFAULT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.DEFAULT_ACCOUNT_ID_MAX_CACHE_SIZE;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.MAX_LIMIT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.MAX_LIMIT_ACCOUNT_ID_MAX_CACHE_SIZE;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsUtil.getBucketName;

/**
 * A loading cache S3 Access Grants AccountId Resolver
 */
public class S3AccessGrantsCachedAccountIdResolver implements S3AccessGrantsAccountIdResolver {

    private final S3ControlAsyncClient S3ControlAsyncClient;
    private int maxCacheSize;
    private int expireCacheAfterWriteSeconds;
    private static final Logger logger = Logger.loggerFor(S3AccessGrantsCachedAccountIdResolver.class);

    private Cache<String, String> cache;

    public S3ControlAsyncClient S3ControlAsyncClient() {
        return S3ControlAsyncClient;
    }

    public int maxCacheSize() {
        return maxCacheSize;
    }

    public int expireCacheAfterWriteSeconds() {
        return expireCacheAfterWriteSeconds;
    }

    protected CacheStats getCacheStats() { return cache.stats(); }

    @VisibleForTesting
    S3AccessGrantsCachedAccountIdResolver(@NotNull S3ControlAsyncClient S3ControlAsyncClient) {
        if (S3ControlAsyncClient == null) {
            throw new IllegalArgumentException("S3ControlAsyncClient is required");
        }
        this.S3ControlAsyncClient = S3ControlAsyncClient;
        this.maxCacheSize = DEFAULT_ACCOUNT_ID_MAX_CACHE_SIZE;
        this.expireCacheAfterWriteSeconds = DEFAULT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
    }


    public Builder toBuilder() {
        return new BuilderImpl(this);
    }

    public static Builder builder() {
        return new BuilderImpl();
    }

    @Override
    public String resolve(String accountId, String s3Prefix) {
        String bucketName = getBucketName(s3Prefix);
        String s3PrefixAccountId = cache.getIfPresent(bucketName);
        if (s3PrefixAccountId == null) {
            logger.debug(()->"Account Id not available in the cache. Fetching account from server.");
            s3PrefixAccountId = resolveFromService(accountId, s3Prefix);
            cache.put(bucketName, s3PrefixAccountId);
        }
        return s3PrefixAccountId;
    }

    /**
     * @param accountId AWS AccountId from the request context parameter
     * @param s3Prefix e.g., s3://bucket-name/path/to/helloworld.txt
     * @return accountId from the service response
     */
    private String resolveFromService(String accountId, String s3Prefix) {
        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse> accessGrantsInstanceForPrefix =
            S3ControlAsyncClient.getAccessGrantsInstanceForPrefix(GetAccessGrantsInstanceForPrefixRequest
                                                                 .builder()
                                                                 .accountId(accountId)
                                                                 .s3Prefix(s3Prefix)
                                                                 .build());
        String accessGrantsInstanceArn = accessGrantsInstanceForPrefix.join().accessGrantsInstanceArn();
        Optional<Arn> optionalArn = Arn.parse(accessGrantsInstanceArn);
        if (!optionalArn.isPresent()) {
            logger.error(()->"accessGrantsInstanceArn is empty");
            throw S3ControlException.builder().message("accessGrantsInstanceArn is empty").build();
        }
        return optionalArn.get().accountId();
    }

    public interface Builder {
        S3AccessGrantsCachedAccountIdResolver build();

        Builder S3ControlAsyncClient(S3ControlAsyncClient S3ControlAsyncClient);

        Builder maxCacheSize(int maxCacheSize);

        Builder expireCacheAfterWriteSeconds(int expireCacheAfterWriteSeconds);
    }

    static final class BuilderImpl implements Builder {
        private S3ControlAsyncClient S3ControlAsyncClient;
        private int maxCacheSize = DEFAULT_ACCOUNT_ID_MAX_CACHE_SIZE;
        private int expireCacheAfterWriteSeconds = DEFAULT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS;

        private BuilderImpl() {
        }

        public BuilderImpl(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver) {
            S3ControlAsyncClient(s3AccessGrantsCachedAccountIdResolver.S3ControlAsyncClient);
            maxCacheSize(s3AccessGrantsCachedAccountIdResolver.maxCacheSize);
            expireCacheAfterWriteSeconds(s3AccessGrantsCachedAccountIdResolver.expireCacheAfterWriteSeconds);
        }

        @Override
        public Builder S3ControlAsyncClient(S3ControlAsyncClient S3ControlAsyncClient) {
            this.S3ControlAsyncClient = S3ControlAsyncClient;
            return this;
        }

        public int maxCacheSize() {
            return maxCacheSize;
        }

        @Override
        public Builder maxCacheSize(int maxCacheSize) {
            if (maxCacheSize <= 0 || maxCacheSize > MAX_LIMIT_ACCOUNT_ID_MAX_CACHE_SIZE) {
                throw new IllegalArgumentException(String.format("maxCacheSize needs to be in range (0, %d]",
                                                                 MAX_LIMIT_ACCOUNT_ID_MAX_CACHE_SIZE));
            }
            this.maxCacheSize = maxCacheSize;
            return this;
        }

        public int expireCAcheAfterWriteSeconds() {
            return expireCacheAfterWriteSeconds;
        }

        @Override
        public Builder expireCacheAfterWriteSeconds(int expireCacheAfterWriteSeconds) {
            if (expireCacheAfterWriteSeconds <= 0 || expireCacheAfterWriteSeconds > MAX_LIMIT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS) {
                throw new IllegalArgumentException(String.format("expireCacheAfterWriteSeconds needs to be in range (0, %d]",
                                                                 MAX_LIMIT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS));
            }
            this.expireCacheAfterWriteSeconds = expireCacheAfterWriteSeconds;
            return this;
        }

        @Override
        public S3AccessGrantsCachedAccountIdResolver build() {
            S3AccessGrantsCachedAccountIdResolver resolver = new S3AccessGrantsCachedAccountIdResolver(S3ControlAsyncClient);
            resolver.maxCacheSize = maxCacheSize();
            resolver.expireCacheAfterWriteSeconds = expireCAcheAfterWriteSeconds();
            resolver.cache = Caffeine.newBuilder()
                                     .maximumSize(maxCacheSize)
                                     .expireAfterWrite(Duration.ofSeconds(expireCacheAfterWriteSeconds))
                                     .build();
            return resolver;
        }
    }
}
