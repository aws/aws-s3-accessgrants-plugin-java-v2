package software.amazon.awssdk.s3accessgrants.cache;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

import static org.mockito.Mockito.mock;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.BUCKET_REGION_CACHE_SIZE;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.MAX_BUCKET_REGION_CACHE_SIZE;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.MAX_BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS;

public class S3AccessGrantsBucketRegionResolverCreationTest {

    private static final int TEST_BUCKET_REGION_CACHE_SIZE = 5_000;
    private static final int TEST_CACHE_EXPIRATION_DURATION = 6_0;

    private static S3Client s3Client;

    @BeforeClass
    public static void setUp() {
        s3Client = mock(S3Client.class);
    }

    @Test
    public void create_bucket_region_cache_with_default_settings() {
        S3AccessGrantsCachedBucketRegionResolver cachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().s3Client(s3Client).build();
        Assert.assertEquals(BUCKET_REGION_CACHE_SIZE, cachedBucketRegionResolver.getMaxCacheSize());
        Assert.assertEquals(BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS, cachedBucketRegionResolver.getExpireCacheAfterWriteSeconds());
    }

    @Test
    public void create_bucket_region_cache_with_custom_settings() {
        S3AccessGrantsCachedBucketRegionResolver cachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver
                .builder()
                .maxCacheSize(TEST_BUCKET_REGION_CACHE_SIZE)
                .expireCacheAfterWriteSeconds(TEST_CACHE_EXPIRATION_DURATION)
                .s3Client(s3Client)
                .build();
        Assert.assertEquals(TEST_BUCKET_REGION_CACHE_SIZE, cachedBucketRegionResolver.getMaxCacheSize());
        Assert.assertEquals(TEST_CACHE_EXPIRATION_DURATION, cachedBucketRegionResolver.getExpireCacheAfterWriteSeconds());
    }

    @Test
    public void create_bucket_region_cache_with_builder_default_settings() {
        S3AccessGrantsCachedBucketRegionResolver cachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().s3Client(s3Client).build();
        Assert.assertEquals(BUCKET_REGION_CACHE_SIZE, cachedBucketRegionResolver.getMaxCacheSize());
        Assert.assertEquals(BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS, cachedBucketRegionResolver.getExpireCacheAfterWriteSeconds());
    }

    @Test
    public void create_bucket_region_cache_with_invalid_max_cache_size() {

        Assertions.assertThatThrownBy(() -> S3AccessGrantsCachedBucketRegionResolver
                .builder()
                .maxCacheSize(MAX_BUCKET_REGION_CACHE_SIZE+1)
                .expireCacheAfterWriteSeconds(BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS)
                .s3Client(s3Client)
                .build()).isInstanceOf(IllegalArgumentException.class);

        Assertions.assertThatThrownBy(() -> S3AccessGrantsCachedBucketRegionResolver
                .builder()
                .maxCacheSize(0)
                .expireCacheAfterWriteSeconds(BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS)
                .s3Client(s3Client)
                .build()).isInstanceOf(IllegalArgumentException.class);

    }

    @Test
    public void create_bucket_region_cache_with_invalid_expiration_duration() {

        Assertions.assertThatThrownBy(() -> S3AccessGrantsCachedBucketRegionResolver
                .builder()
                .maxCacheSize(BUCKET_REGION_CACHE_SIZE)
                .expireCacheAfterWriteSeconds(MAX_BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS+10)
                .s3Client(s3Client)
                .build()).isInstanceOf(IllegalArgumentException.class);

        Assertions.assertThatThrownBy(() -> S3AccessGrantsCachedBucketRegionResolver
                .builder()
                .maxCacheSize(BUCKET_REGION_CACHE_SIZE)
                .expireCacheAfterWriteSeconds(0)
                .s3Client(s3Client)
                .build()).isInstanceOf(IllegalArgumentException.class);

    }

    @Test
    public void copy_Resolver() {

        S3AccessGrantsCachedBucketRegionResolver cachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver
                .builder()
                .maxCacheSize(TEST_BUCKET_REGION_CACHE_SIZE)
                .expireCacheAfterWriteSeconds(TEST_CACHE_EXPIRATION_DURATION)
                .s3Client(s3Client)
                .build();
        S3AccessGrantsCachedBucketRegionResolver copy = cachedBucketRegionResolver.toBuilder().build();

        Assert.assertEquals(TEST_BUCKET_REGION_CACHE_SIZE, copy.maxCacheSize());
        Assert.assertEquals(TEST_CACHE_EXPIRATION_DURATION, copy.expireCacheAfterWriteSeconds());

    }

    @Test
    public void test_invalidS3Client() {
       Assertions.assertThatThrownBy(() -> S3AccessGrantsCachedBucketRegionResolver
                .builder()
                .maxCacheSize(TEST_BUCKET_REGION_CACHE_SIZE)
                .s3Client(null)
                .expireCacheAfterWriteSeconds(TEST_CACHE_EXPIRATION_DURATION)
                .build()).isInstanceOf(IllegalArgumentException.class);
    }

}


