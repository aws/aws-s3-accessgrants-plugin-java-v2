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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.assertj.core.util.VisibleForTesting;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.metrics.internal.DefaultMetricCollector;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.services.s3control.model.S3ControlException;
import software.amazon.awssdk.utils.Logger;

import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.CACHE_EXPIRATION_TIME_PERCENTAGE;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE;

public class S3AccessGrantsCachedCredentialsProviderImpl implements S3AccessGrantsCachedCredentialsProvider{

    private final S3AccessGrantsCache accessGrantsCache;
    private final S3AccessGrantsAccessDeniedCache s3AccessGrantsAccessDeniedCache;
    DefaultMetricCollector collector = new DefaultMetricCollector("AccessGrantsMetrics");
    public static final Logger logger =
        Logger.loggerFor(software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProviderImpl.class);

    private S3AccessGrantsCachedCredentialsProviderImpl(S3ControlAsyncClient S3ControlAsyncClient, int maxCacheSize, int cacheExpirationTimePercentage) {

        accessGrantsCache = S3AccessGrantsCache.builder()
                                               .s3ControlAsyncClient(S3ControlAsyncClient)
                                               .maxCacheSize(maxCacheSize)
                                               .cacheExpirationTimePercentage(cacheExpirationTimePercentage).build();

        s3AccessGrantsAccessDeniedCache = S3AccessGrantsAccessDeniedCache.builder()
                                                .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).build();
    }

    @VisibleForTesting
    S3AccessGrantsCachedCredentialsProviderImpl(S3ControlAsyncClient s3ControlAsyncClient,
                                                        S3AccessGrantsCachedAccountIdResolver resolver,int maxCacheSize, int cacheExpirationTimePercentage) {

        accessGrantsCache = S3AccessGrantsCache.builder()
                                               .s3ControlAsyncClient(s3ControlAsyncClient)
                                               .maxCacheSize(maxCacheSize)
                                               .cacheExpirationTimePercentage(cacheExpirationTimePercentage)
                                               .s3AccessGrantsCachedAccountIdResolver(resolver)
                                               .buildWithAccountIdResolver();
        s3AccessGrantsAccessDeniedCache = S3AccessGrantsAccessDeniedCache.builder()
                                                                         .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).build();
    }

    public static S3AccessGrantsCachedCredentialsProviderImpl.Builder builder() {
        return new S3AccessGrantsCachedCredentialsProviderImpl.BuilderImpl();
    }

    public interface Builder {
        S3AccessGrantsCachedCredentialsProviderImpl build();
        S3AccessGrantsCachedCredentialsProviderImpl buildWithAccountIdResolver();
        S3AccessGrantsCachedCredentialsProviderImpl.Builder S3ControlAsyncClient(S3ControlAsyncClient S3ControlAsyncClient);
        S3AccessGrantsCachedCredentialsProviderImpl.Builder s3AccessGrantsCachedAccountIdResolver(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver);
        S3AccessGrantsCachedCredentialsProviderImpl.Builder maxCacheSize(int maxCacheSize);
        S3AccessGrantsCachedCredentialsProviderImpl.Builder cacheExpirationTimePercentage(int cacheExpirationTimePercentage);
    }

    static final class BuilderImpl implements S3AccessGrantsCachedCredentialsProviderImpl.Builder {
        private S3ControlAsyncClient S3ControlAsyncClient;
        private S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver;
        private int maxCacheSize = DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
        private int cacheExpirationTimePercentage = CACHE_EXPIRATION_TIME_PERCENTAGE;

        private BuilderImpl() {
        }

        @Override
        public S3AccessGrantsCachedCredentialsProviderImpl build() {
            return new S3AccessGrantsCachedCredentialsProviderImpl(S3ControlAsyncClient, maxCacheSize, cacheExpirationTimePercentage);
        }

        @Override
        public S3AccessGrantsCachedCredentialsProviderImpl buildWithAccountIdResolver() {
            return new S3AccessGrantsCachedCredentialsProviderImpl(S3ControlAsyncClient, s3AccessGrantsCachedAccountIdResolver, maxCacheSize, cacheExpirationTimePercentage);
        }

        @Override
        public S3AccessGrantsCachedCredentialsProviderImpl.Builder S3ControlAsyncClient(S3ControlAsyncClient S3ControlAsyncClient) {
            this.S3ControlAsyncClient = S3ControlAsyncClient;
            return this;
        }

        @Override
        public S3AccessGrantsCachedCredentialsProviderImpl.Builder s3AccessGrantsCachedAccountIdResolver(S3AccessGrantsCachedAccountIdResolver s3AccessGrantsCachedAccountIdResolver) {
            this.s3AccessGrantsCachedAccountIdResolver = s3AccessGrantsCachedAccountIdResolver;
            return this;
        }

        @Override
        public Builder maxCacheSize(int maxCacheSize) {
            if (maxCacheSize <= 0 || maxCacheSize > MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE) {
                throw new IllegalArgumentException(String.format("maxCacheSize needs to be in range (0, %d]",
                                                                 MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE));
            }
            return this;
        }

        @Override
        public Builder cacheExpirationTimePercentage(int cacheExpirationTimePercentage) {
            if (cacheExpirationTimePercentage <= 0 || (float) cacheExpirationTimePercentage > DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE) {
                throw new IllegalArgumentException(String.format("maxExpirationTimePercentage needs to be in range (0, %d]",
                                                                 DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE));
            }
            this.cacheExpirationTimePercentage = cacheExpirationTimePercentage;
            return this;
        }

    }

    @Override
    public CompletableFuture<AwsCredentialsIdentity> getDataAccess (AwsCredentialsIdentity credentials, Permission permission,
                                                                    String s3Prefix, @NotNull String accountId) throws S3ControlException {

        Instant start = Instant.now();
        CacheKey cacheKey = CacheKey.builder()
                                    .credentials(credentials)
                                    .permission(permission)
                                    .s3Prefix(s3Prefix).build();

        S3ControlException s3ControlException = s3AccessGrantsAccessDeniedCache.getValueFromCache(cacheKey);
        if (s3ControlException != null) {
            logger.debug(()->"Found a matching request in the cache which was denied.");
            logger.error(()->"Exception occurred while fetching the credentials: " + s3ControlException);
            throw s3ControlException;
        }

        CompletableFuture<AwsCredentialsIdentity> accessGrantsCredentials;
        try {
            accessGrantsCredentials = accessGrantsCache.getCredentials(cacheKey, accountId, s3AccessGrantsAccessDeniedCache);
        }catch (S3ControlException e) {
            collector.reportMetric(MetricsCollector.ERROR_COUNT,1);
            throw e;
        }
        collector.reportMetric(MetricsCollector.LATENCY, Duration.between(start, Instant.now()));
        collector.reportMetric(MetricsCollector.CALL_COUNT, 1);
        return accessGrantsCredentials;
    }

    public void invalidateCache() {
        accessGrantsCache.invalidateCache();
    }

    private void collectMetrics() {
        collector.reportMetric(CoreMetric.SERVICE_ID, "AccessGrants");
        collector.reportMetric(CoreMetric.OPERATION_NAME, "Metrics");
        MetricsCollector.getMetricsForAccessGrantsCache(accessGrantsCache.getCacheStats(), collector);
        MetricsCollector.getMetricsForAccessDeniedCache(s3AccessGrantsAccessDeniedCache.getCacheStats(), collector);
        MetricsCollector.getMetricsForAccountIdResolverCache(accessGrantsCache.getS3AccessGrantsCachedAccountIdResolver().getCacheStats(), collector);
    }

    @Override
    public MetricCollector getAccessGrantsMetrics() {
        collectMetrics();
        return collector;

    }

}
