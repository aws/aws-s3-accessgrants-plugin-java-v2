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

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.time.Duration;
import software.amazon.awssdk.metrics.MetricCategory;
import software.amazon.awssdk.metrics.MetricLevel;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.metrics.internal.DefaultMetricCollector;

public class MetricsCollector {
    private static final SdkMetric<Double>
        ACCESS_GRANT_CACHE_METRIC_HIT_RATE = SdkMetric.create("AccessGrantsCacheHitRate", Double.class, MetricLevel.INFO, MetricCategory.CUSTOM),
        ACCESS_DENIED_CACHE_METRIC_HIT_RATE = SdkMetric.create("AccessDeniedCacheHitRate", Double.class, MetricLevel.INFO,
                                                               MetricCategory.CUSTOM),
        ACCOUNT_ID_RESOLVER_CACHE_METRIC_HIT_RATE = SdkMetric.create("AccountIdResolverCacheHitRate", Double.class, MetricLevel.INFO,
                                                                    MetricCategory.CUSTOM);
    private static final SdkMetric<Long>
        ACCESS_GRANT_CACHE_METRIC_HIT_COUNT = SdkMetric.create("AccessGrantsCacheHitCount", Long.class, MetricLevel.INFO,
                                                               MetricCategory.CUSTOM),
        ACCESS_GRANT_CACHE_METRIC_MISS_COUNT = SdkMetric.create("AccessGrantsCacheMissCount", Long.class, MetricLevel.INFO, MetricCategory.CUSTOM),
        ACCESS_GRANT_CACHE_METRIC_LOAD_COUNT = SdkMetric.create("AccessGrantsCacheLoadCount", Long.class, MetricLevel.INFO,
                                                                MetricCategory.CUSTOM),
        ACCESS_GRANT_CACHE_METRIC_LOAD_SUCCESS_COUNT = SdkMetric.create("AccessGrantsCacheLoadSuccessCount", Long.class, MetricLevel.INFO, MetricCategory.CUSTOM),
        ACCESS_GRANT_CACHE_METRIC_EVICTION_COUNT = SdkMetric.create("AccessGrantsCacheEvictionCount", Long.class,
                                                                    MetricLevel.INFO, MetricCategory.CUSTOM),

        ACCESS_DENIED_CACHE_METRIC_HIT_COUNT = SdkMetric.create("AccessDeniedCacheHitCount", Long.class, MetricLevel.INFO,
                                                            MetricCategory.CUSTOM),
        ACCESS_DENIED_CACHE_METRIC_MISS_COUNT = SdkMetric.create("AccessDeniedCacheMissCount", Long.class, MetricLevel.INFO, MetricCategory.CUSTOM),
        ACCESS_DENIED_CACHE_METRIC_LOAD_COUNT = SdkMetric.create("AccessDeniedCacheLoadCount", Long.class, MetricLevel.INFO,
                                                                 MetricCategory.CUSTOM),
        ACCESS_DENIED_CACHE_METRIC_LOAD_SUCCESS_COUNT = SdkMetric.create("AccessDeniedCacheLoadSuccessCount", Long.class, MetricLevel.INFO, MetricCategory.CUSTOM),
        ACCESS_DENIED_CACHE_METRIC_EVICTION_COUNT = SdkMetric.create("AccessDeniedCacheEvictionCount", Long.class,
                                                                     MetricLevel.INFO, MetricCategory.CUSTOM),

        ACCOUNT_ID_RESOLVER_CACHE_METRIC_HIT_COUNT = SdkMetric.create("AccountIdResolverCacheHitCount", Long.class, MetricLevel.INFO,
        MetricCategory.CUSTOM),
        ACCOUNT_ID_RESOLVER_CACHE_METRIC_MISS_COUNT = SdkMetric.create("AccountIdResolverCacheMissCount", Long.class, MetricLevel.INFO, MetricCategory.CUSTOM),
        ACCOUNT_ID_RESOLVER_CACHE_METRIC_LOAD_COUNT = SdkMetric.create("AccountIdResolverCacheLoadCount", Long.class, MetricLevel.INFO,
        MetricCategory.CUSTOM),
        ACCOUNT_ID_RESOLVER_CACHE_METRIC_LOAD_SUCCESS_COUNT = SdkMetric.create("AccountIdResolverCacheLoadSuccessCount", Long.class, MetricLevel.INFO, MetricCategory.CUSTOM),
        ACCOUNT_ID_RESOLVER_CACHE_METRIC_EVICTION_COUNT = SdkMetric.create("AccountIdResolverCacheEvictionCount", Long.class, MetricLevel.INFO, MetricCategory.CUSTOM);

    public static final SdkMetric<Integer> CALL_COUNT =
        SdkMetric.create("CallCount", Integer.class, MetricLevel.INFO, MetricCategory.CUSTOM);

    public static final SdkMetric<Integer> ERROR_COUNT =
        SdkMetric.create("ErrorCount", Integer.class, MetricLevel.ERROR, MetricCategory.CUSTOM);

    public static final SdkMetric<Duration> LATENCY =
        SdkMetric.create("Latency", Duration.class, MetricLevel.INFO, MetricCategory.CUSTOM);

    public static final SdkMetric<Duration> searchKeyInCacheAtCharacterLevel_CacheHit_Latency =
        SdkMetric.create("searchKeyInCacheAtCharacterLevel_CacheHit_Latency", Duration.class, MetricLevel.INFO, MetricCategory.CUSTOM);

    public static final SdkMetric<Duration> searchKeyInCacheAtCharacterLevel_CacheMiss_Latency =
        SdkMetric.create("searchKeyInCacheAtCharacterLevel_CacheMiss_Latency", Duration.class, MetricLevel.INFO, MetricCategory.CUSTOM);

    public static void getMetricsForAccessGrantsCache (CacheStats stats, DefaultMetricCollector collector) {
        collector.reportMetric(ACCESS_GRANT_CACHE_METRIC_HIT_RATE, stats.hitRate());
        collector.reportMetric(ACCESS_GRANT_CACHE_METRIC_HIT_COUNT, stats.hitCount());
        collector.reportMetric(ACCESS_GRANT_CACHE_METRIC_MISS_COUNT, stats.missCount());
        collector.reportMetric(ACCESS_GRANT_CACHE_METRIC_LOAD_COUNT, stats.loadCount());
        collector.reportMetric(ACCESS_GRANT_CACHE_METRIC_LOAD_SUCCESS_COUNT, stats.loadSuccessCount());
        collector.reportMetric(ACCESS_GRANT_CACHE_METRIC_EVICTION_COUNT, stats.evictionCount());
    }

    public static void getMetricsForAccessDeniedCache (CacheStats stats, DefaultMetricCollector collector) {
        collector.reportMetric(ACCESS_DENIED_CACHE_METRIC_HIT_RATE, stats.hitRate());
        collector.reportMetric(ACCESS_DENIED_CACHE_METRIC_HIT_COUNT, stats.hitCount());
        collector.reportMetric(ACCESS_DENIED_CACHE_METRIC_MISS_COUNT, stats.missCount());
        collector.reportMetric(ACCESS_DENIED_CACHE_METRIC_LOAD_COUNT, stats.loadCount());
        collector.reportMetric(ACCESS_DENIED_CACHE_METRIC_LOAD_SUCCESS_COUNT, stats.loadSuccessCount());
        collector.reportMetric(ACCESS_DENIED_CACHE_METRIC_EVICTION_COUNT, stats.evictionCount());
    }

    public static void getMetricsForAccountIdResolverCache (CacheStats stats, DefaultMetricCollector collector) {

        collector.reportMetric(ACCOUNT_ID_RESOLVER_CACHE_METRIC_HIT_RATE, stats.hitRate());
        collector.reportMetric(ACCOUNT_ID_RESOLVER_CACHE_METRIC_HIT_COUNT, stats.hitCount());
        collector.reportMetric(ACCOUNT_ID_RESOLVER_CACHE_METRIC_MISS_COUNT, stats.missCount());
        collector.reportMetric(ACCOUNT_ID_RESOLVER_CACHE_METRIC_LOAD_COUNT, stats.loadCount());
        collector.reportMetric(ACCOUNT_ID_RESOLVER_CACHE_METRIC_LOAD_SUCCESS_COUNT, stats.loadSuccessCount());
        collector.reportMetric(ACCOUNT_ID_RESOLVER_CACHE_METRIC_EVICTION_COUNT, stats.evictionCount());
    }

}
