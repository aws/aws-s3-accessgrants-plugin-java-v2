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

import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.services.s3control.S3ControlClient;
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.services.s3control.model.S3ControlException;

import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;

public class S3AccessGrantsCachedCredentialsProviderImpl implements S3AccessGrantsCachedCredentialsProvider{

    private final String accountId;
    private final S3AccessGrantsCache accessGrantsCache;

    private S3AccessGrantsCachedCredentialsProviderImpl(S3ControlClient s3ControlClient, String accountId) {

        this.accountId = accountId;
        accessGrantsCache = S3AccessGrantsCache.builder()
                                               .s3ControlClient(s3ControlClient)
                                               .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).build();
    }

    private S3AccessGrantsCachedCredentialsProviderImpl(S3ControlClient s3ControlClient, String accountId, S3AccessGrantsCachedAccountIdResolver resolver) {

        this.accountId = accountId;
        accessGrantsCache = S3AccessGrantsCache.builder()
                                               .s3ControlClient(s3ControlClient)
                                               .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE)
                                               .s3AccessGrantsCachedAccountIdResolver(resolver)
                                               .buildWithAccountIdResolver();
    }

    public String accountId(){ return accountId; }

    public static S3AccessGrantsCachedCredentialsProviderImpl.Builder builder() {
        return new S3AccessGrantsCachedCredentialsProviderImpl.BuilderImpl();
    }

    public interface Builder {
        S3AccessGrantsCachedCredentialsProviderImpl build();
        S3AccessGrantsCachedCredentialsProviderImpl buildWithAccountIdResolver();
        S3AccessGrantsCachedCredentialsProviderImpl.Builder accountId(String accountId);
        S3AccessGrantsCachedCredentialsProviderImpl.Builder s3ControlClient(S3ControlClient s3ControlClient);
        S3AccessGrantsCachedCredentialsProviderImpl.Builder s3AccessGrantsCachedAccountIdResolver(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver);

    }

    static final class BuilderImpl implements S3AccessGrantsCachedCredentialsProviderImpl.Builder {
        private S3ControlClient s3ControlClient;
        private String accountId;
        private S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver;

        private BuilderImpl() {
        }

        @Override
        public S3AccessGrantsCachedCredentialsProviderImpl build() {
            return new S3AccessGrantsCachedCredentialsProviderImpl(s3ControlClient, accountId);
        }

        @Override
        public S3AccessGrantsCachedCredentialsProviderImpl buildWithAccountIdResolver() {
            return new S3AccessGrantsCachedCredentialsProviderImpl(s3ControlClient, accountId, s3AccessGrantsCachedAccountIdResolver);
        }

        @Override
        public S3AccessGrantsCachedCredentialsProviderImpl.Builder s3ControlClient(S3ControlClient s3ControlClient) {
            this.s3ControlClient = s3ControlClient;
            return this;
        }

        @Override
        public S3AccessGrantsCachedCredentialsProviderImpl.Builder accountId(String accountId) {
            this.accountId = accountId;
            return this;
        }

        @Override
        public S3AccessGrantsCachedCredentialsProviderImpl.Builder s3AccessGrantsCachedAccountIdResolver(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver) {
            this.s3AccessGrantsCachedAccountIdResolver = s3AccessGrantsCachedAccountIdResolver;
            return this;
        }
    }

    @Override
    public AwsCredentialsIdentity getDataAccess (AwsCredentialsIdentity credentials, Permission permission,
                                                     String s3Prefix) throws S3ControlException {
        CacheKey cacheKey = CacheKey.builder()
                                    .credentials(credentials)
                                    .permission(permission)
                                    .s3Prefix(s3Prefix).build();
        return accessGrantsCache.getValueFromCache(cacheKey, accountId());
    }

    public void invalidateCache() {
        accessGrantsCache.invalidateCache();
    }

}
