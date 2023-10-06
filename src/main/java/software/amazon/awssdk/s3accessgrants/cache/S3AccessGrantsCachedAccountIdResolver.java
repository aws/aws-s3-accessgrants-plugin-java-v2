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

import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.DEFAULT_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.DEFAULT_MAX_CACHE_SIZE;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.MAX_LIMIT_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.MAX_LIMIT_MAX_CACHE_SIZE;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsUtil.getBucketName;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import java.util.Optional;
import org.assertj.core.util.VisibleForTesting;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.services.s3control.S3ControlClient;
import software.amazon.awssdk.services.s3control.endpoints.internal.Arn;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixRequest;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixResponse;
import software.amazon.awssdk.services.s3control.model.S3ControlException;

/**
 * A loading cache S3 Access Grants AccountId Resolver
 */
public class S3AccessGrantsCachedAccountIdResolver implements S3AccessGrantsAccountIdResolver {

    private final S3ControlClient s3ControlClient;
    private final String accountId;
    private int maxCacheSize;
    private int expireCacheAfterWriteSeconds;

    private Cache<String, String> cache;

    public S3ControlClient s3ControlClient() {
        return s3ControlClient;
    }

    public String accountId() {
        return accountId;
    }

    public int maxCacheSize() {
        return maxCacheSize;
    }

    public int expireCacheAfterWriteSeconds() {
        return expireCacheAfterWriteSeconds;
    }

    @VisibleForTesting
    S3AccessGrantsCachedAccountIdResolver(@NotNull String accountId, @NotNull S3ControlClient s3ControlClient) {
        if (accountId == null) {
            throw new IllegalArgumentException("accountId is required");
        }
        if (s3ControlClient == null) {
            throw new IllegalArgumentException("s3ControlClient is required");
        }
        this.accountId = accountId;
        this.s3ControlClient = s3ControlClient;
        this.maxCacheSize = DEFAULT_MAX_CACHE_SIZE;
        this.expireCacheAfterWriteSeconds = DEFAULT_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
    }


    public Builder toBuilder() {
        return new BuilderImpl(this);
    }

    public static Builder builder() {
        return new BuilderImpl();
    }

    @Override
    public String resolve(String s3Prefix) {
        String bucketName = getBucketName(s3Prefix);
        String s3PrefixAccountId = cache.getIfPresent(bucketName);
        if (s3PrefixAccountId == null) {
            s3PrefixAccountId = resolveFromService(s3Prefix);
            cache.put(bucketName, s3PrefixAccountId);
        }
        return s3PrefixAccountId;
    }

    /**
     * @param s3Prefix e.g., s3://bucket-name/path/to/helloworld.txt
     * @return accountId from the service response
     */
    private String resolveFromService(String s3Prefix) {
        GetAccessGrantsInstanceForPrefixResponse accessGrantsInstanceForPrefix =
            s3ControlClient.getAccessGrantsInstanceForPrefix(GetAccessGrantsInstanceForPrefixRequest
                                                                 .builder()
                                                                 .accountId(accountId())
                                                                 .s3Prefix(s3Prefix)
                                                                 .build());
        String accessGrantsInstanceArn = accessGrantsInstanceForPrefix.accessGrantsInstanceArn();
        Optional<Arn> optionalArn = Arn.parse(accessGrantsInstanceArn);
        if (!optionalArn.isPresent()) {
            throw S3ControlException.builder().message("accessGrantsInstanceArn is empty").build();
        }
        return optionalArn.get().accountId();
    }

    public interface Builder {
        S3AccessGrantsCachedAccountIdResolver build();

        Builder accountId(String accountId);

        Builder s3ControlClient(S3ControlClient s3ControlClient);

        Builder maxCacheSize(int maxCacheSize);

        Builder expireCacheAfterWriteSeconds(int expireCacheAfterWriteSeconds);
    }

    static final class BuilderImpl implements Builder {
        private String accountId;
        private S3ControlClient s3ControlClient;
        private int maxCacheSize = DEFAULT_MAX_CACHE_SIZE;
        private int expireCacheAfterWriteSeconds = DEFAULT_EXPIRE_CACHE_AFTER_WRITE_SECONDS;

        private BuilderImpl() {
        }

        public BuilderImpl(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver) {
            accountId(s3AccessGrantsCachedAccountIdResolver.accountId);
            s3ControlClient(s3AccessGrantsCachedAccountIdResolver.s3ControlClient);
            maxCacheSize(s3AccessGrantsCachedAccountIdResolver.maxCacheSize);
            expireCacheAfterWriteSeconds(s3AccessGrantsCachedAccountIdResolver.expireCacheAfterWriteSeconds);
        }

        @Override
        public Builder s3ControlClient(S3ControlClient s3ControlClient) {
            this.s3ControlClient = s3ControlClient;
            return this;
        }

        public int maxCacheSize() {
            return maxCacheSize;
        }

        @Override
        public Builder maxCacheSize(int maxCacheSize) {
            if (maxCacheSize <= 0 || maxCacheSize > MAX_LIMIT_MAX_CACHE_SIZE) {
                throw new IllegalArgumentException(String.format("maxCacheSize needs to be in range (0, %d]",
                                                                 MAX_LIMIT_MAX_CACHE_SIZE));
            }
            this.maxCacheSize = maxCacheSize;
            return this;
        }

        public int expireCAcheAfterWriteSeconds() {
            return expireCacheAfterWriteSeconds;
        }

        @Override
        public Builder expireCacheAfterWriteSeconds(int expireCacheAfterWriteSeconds) {
            if (expireCacheAfterWriteSeconds <= 0 || expireCacheAfterWriteSeconds > MAX_LIMIT_EXPIRE_CACHE_AFTER_WRITE_SECONDS) {
                throw new IllegalArgumentException(String.format("expireCacheAfterWriteSeconds needs to be in range (0, %d]",
                                                                 MAX_LIMIT_EXPIRE_CACHE_AFTER_WRITE_SECONDS));
            }
            this.expireCacheAfterWriteSeconds = expireCacheAfterWriteSeconds;
            return this;
        }

        public String accountId() {
            return accountId;
        }

        @Override
        public Builder accountId(String accountId) {
            if (accountId == null) {
                throw new IllegalArgumentException("accountId is required");
            }
            this.accountId = accountId;
            return this;
        }

        @Override
        public S3AccessGrantsCachedAccountIdResolver build() {
            S3AccessGrantsCachedAccountIdResolver resolver = new S3AccessGrantsCachedAccountIdResolver(accountId,
                                                                                                       s3ControlClient);
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
