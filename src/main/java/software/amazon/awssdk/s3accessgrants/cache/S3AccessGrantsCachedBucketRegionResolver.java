package software.amazon.awssdk.s3accessgrants.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.Logger;

import java.time.Duration;

import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.BUCKET_REGION_CACHE_SIZE;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.MAX_BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.MAX_BUCKET_REGION_CACHE_SIZE;

public class S3AccessGrantsCachedBucketRegionResolver implements S3AccessGrantsBucketRegionResolver{


    private int maxCacheSize;

    private int expireCacheAfterWriteSeconds;

    private static Cache<String, Region> cache;

    public int getMaxCacheSize() {
        return maxCacheSize;
    }


    public int getExpireCacheAfterWriteSeconds() {
        return expireCacheAfterWriteSeconds;
    }

    private S3Client s3Client;

    private static final Logger logger = Logger.loggerFor(S3AccessGrantsCachedBucketRegionResolver.class);

    public int maxCacheSize() {
        return maxCacheSize;
    }

    public int expireCacheAfterWriteSeconds() {
        return expireCacheAfterWriteSeconds;
    }

    protected CacheStats getCacheStats() { return cache.stats(); }

    S3AccessGrantsCachedBucketRegionResolver() {
        this.maxCacheSize = BUCKET_REGION_CACHE_SIZE;
        this.expireCacheAfterWriteSeconds = BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
    }


    public S3AccessGrantsCachedBucketRegionResolver.Builder toBuilder() {
        return new S3AccessGrantsCachedBucketRegionResolver.BuilderImpl(this);
    }

    public static S3AccessGrantsCachedBucketRegionResolver.Builder builder() {
        return new S3AccessGrantsCachedBucketRegionResolver.BuilderImpl();
    }

    @Override
    public Region resolve(String bucket, S3Client s3Client) throws S3Exception {

        this.s3Client = s3Client;
        Region bucketRegion = cache.getIfPresent(bucket);
        if(bucketRegion == null) {
            logger.debug(() -> "bucket region not available in cache, fetching the region from the service!");
            if (s3Client == null) {
                throw new IllegalArgumentException("S3Client is required for the bucket region resolver!");
            }
            bucketRegion = resolveFromService(bucket);
            if(bucketRegion != null) {
                cache.put(bucket, bucketRegion);
            }
        } else {
            logger.debug(() -> "bucket region available in cache!");
        }
        return bucketRegion;

    }

    private Region resolveFromService(String bucket) {
        String resolvedRegion = null;
        try {
                logger.info(() -> "making a call to S3 for determining the bucket region!");
                HeadBucketRequest bucketLocationRequest = HeadBucketRequest.builder().bucket(bucket).build();
                HeadBucketResponse headBucketResponse = s3Client.headBucket(bucketLocationRequest);
                resolvedRegion = headBucketResponse.bucketRegion();
        } catch (S3Exception e) {
            if (e.statusCode() == 301) {
                // A fallback in case S3 Clients are not able to re-direct the head bucket requests to the correct region.
                String bucketRegion = e.awsErrorDetails().sdkHttpResponse().headers().get("x-amz-bucket-region").get(0);
                resolvedRegion = bucketRegion;
            } else {
                throw e;
            }
        }
        if(resolvedRegion == null) throw SdkServiceException.builder().message("S3 error! region cannot be determined for the specified bucket!").build();
        return Region.of(resolvedRegion);
    }


    public interface Builder {
        S3AccessGrantsCachedBucketRegionResolver build();

        S3AccessGrantsCachedBucketRegionResolver.Builder maxCacheSize(int maxCacheSize);

        S3AccessGrantsCachedBucketRegionResolver.Builder expireCacheAfterWriteSeconds(int expireCacheAfterWriteSeconds);
    }

    static final class BuilderImpl implements S3AccessGrantsCachedBucketRegionResolver.Builder {
        private int maxCacheSize = BUCKET_REGION_CACHE_SIZE;
        private int expireCacheAfterWriteSeconds = BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS;

        private BuilderImpl() {
        }

        public BuilderImpl(S3AccessGrantsCachedBucketRegionResolver s3AccessGrantsCachedBucketRegionResolver) {
            maxCacheSize(s3AccessGrantsCachedBucketRegionResolver.maxCacheSize);
            expireCacheAfterWriteSeconds(s3AccessGrantsCachedBucketRegionResolver.expireCacheAfterWriteSeconds);
        }

        public int maxCacheSize() {
            return maxCacheSize;
        }

        @Override
        public S3AccessGrantsCachedBucketRegionResolver.Builder maxCacheSize(int maxCacheSize) {
            if (maxCacheSize <= 0 || maxCacheSize > MAX_BUCKET_REGION_CACHE_SIZE) {
                throw new IllegalArgumentException(String.format("maxCacheSize needs to be in range (0, %d]",
                        MAX_BUCKET_REGION_CACHE_SIZE));
            }
            this.maxCacheSize = maxCacheSize;
            return this;
        }

        public int expireCacheAfterWriteSeconds() {
            return expireCacheAfterWriteSeconds;
        }

        @Override
        public S3AccessGrantsCachedBucketRegionResolver.Builder expireCacheAfterWriteSeconds(int expireCacheAfterWriteSeconds) {
            if (expireCacheAfterWriteSeconds <= 0 || expireCacheAfterWriteSeconds > MAX_BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS) {
                throw new IllegalArgumentException(String.format("expireCacheAfterWriteSeconds needs to be in range (0, %d]",
                        MAX_BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS));
            }
            this.expireCacheAfterWriteSeconds = expireCacheAfterWriteSeconds;
            return this;
        }

        @Override
        public S3AccessGrantsCachedBucketRegionResolver build() {
            S3AccessGrantsCachedBucketRegionResolver resolver = new S3AccessGrantsCachedBucketRegionResolver();
            resolver.maxCacheSize = maxCacheSize();
            resolver.expireCacheAfterWriteSeconds = expireCacheAfterWriteSeconds();
            resolver.cache = Caffeine.newBuilder()
                    .maximumSize(maxCacheSize)
                    .expireAfterWrite(Duration.ofSeconds(expireCacheAfterWriteSeconds))
                    .build();
            return resolver;
        }
    }

}
