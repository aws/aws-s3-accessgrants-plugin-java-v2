package software.amazon.awssdk.s3accessgrants.cache;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.*;

public class S3AccessGrantsBucketRegionResolverCreationTest {

    private static int TEST_BUCKET_REGION_CACHE_SIZE = 5_000;
    private static int TEST_CACHE_EXPIRATION_DURATION = 6_0;

    @Test
    public void create_bucket_region_cache_with_default_settings() {
        S3AccessGrantsCachedBucketRegionResolver cachedBucketRegionResolver = new S3AccessGrantsCachedBucketRegionResolver();
        Assert.assertEquals(BUCKET_REGION_CACHE_SIZE, cachedBucketRegionResolver.getMaxCacheSize());
        Assert.assertEquals(BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS, cachedBucketRegionResolver.getExpireCacheAfterWriteSeconds());
    }

    @Test
    public void create_bucket_region_cache_with_custom_settings() {
        S3AccessGrantsCachedBucketRegionResolver cachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver
                .builder()
                .maxCacheSize(TEST_BUCKET_REGION_CACHE_SIZE)
                .expireCacheAfterWriteSeconds(TEST_CACHE_EXPIRATION_DURATION)
                .build();
        Assert.assertEquals(TEST_BUCKET_REGION_CACHE_SIZE, cachedBucketRegionResolver.getMaxCacheSize());
        Assert.assertEquals(TEST_CACHE_EXPIRATION_DURATION, cachedBucketRegionResolver.getExpireCacheAfterWriteSeconds());
    }

    @Test
    public void create_bucket_region_cache_with_builder_default_settings() {
        S3AccessGrantsCachedBucketRegionResolver cachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().build();
        Assert.assertEquals(BUCKET_REGION_CACHE_SIZE, cachedBucketRegionResolver.getMaxCacheSize());
        Assert.assertEquals(BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS, cachedBucketRegionResolver.getExpireCacheAfterWriteSeconds());
    }

    @Test
    public void create_bucket_region_cache_with_invalid_max_cache_size() {

        Assertions.assertThatThrownBy(() -> S3AccessGrantsCachedBucketRegionResolver
                .builder()
                .maxCacheSize(MAX_BUCKET_REGION_CACHE_SIZE+1)
                .expireCacheAfterWriteSeconds(BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS)
                .build()).isInstanceOf(IllegalArgumentException.class);

        Assertions.assertThatThrownBy(() -> S3AccessGrantsCachedBucketRegionResolver
                .builder()
                .maxCacheSize(0)
                .expireCacheAfterWriteSeconds(BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS)
                .build()).isInstanceOf(IllegalArgumentException.class);

    }

    @Test
    public void create_bucket_region_cache_with_invalid_expiration_duration() {

        Assertions.assertThatThrownBy(() -> S3AccessGrantsCachedBucketRegionResolver
                .builder()
                .maxCacheSize(BUCKET_REGION_CACHE_SIZE)
                .expireCacheAfterWriteSeconds(MAX_BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS+10)
                .build()).isInstanceOf(IllegalArgumentException.class);

        Assertions.assertThatThrownBy(() -> S3AccessGrantsCachedBucketRegionResolver
                .builder()
                .maxCacheSize(BUCKET_REGION_CACHE_SIZE)
                .expireCacheAfterWriteSeconds(0)
                .build()).isInstanceOf(IllegalArgumentException.class);

    }

    @Test
    public void copy_Resolver() {

        S3AccessGrantsCachedAccountIdResolver cachedBucketRegionResolver = S3AccessGrantsCachedAccountIdResolver
                .builder()
                .maxCacheSize(TEST_BUCKET_REGION_CACHE_SIZE)
                .expireCacheAfterWriteSeconds(TEST_CACHE_EXPIRATION_DURATION)
                .build();
        S3AccessGrantsCachedAccountIdResolver copy = cachedBucketRegionResolver.toBuilder().build();

        Assert.assertEquals(TEST_BUCKET_REGION_CACHE_SIZE, copy.maxCacheSize());
        Assert.assertEquals(TEST_CACHE_EXPIRATION_DURATION, copy.expireCacheAfterWriteSeconds());

    }

}


