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

public class S3AccessGrantsCachedBucketRegionResolver implements S3AccessGrantsBucketRegionResolver {


    private int maxCacheSize;

    private int expireCacheAfterWriteSeconds;

    private Cache<String, Region> cache;

    private S3Client s3Client;

    private static final Logger logger = Logger.loggerFor(S3AccessGrantsCachedBucketRegionResolver.class);

    public int getMaxCacheSize() {
        return maxCacheSize;
    }


    public int getExpireCacheAfterWriteSeconds() {
        return expireCacheAfterWriteSeconds;
    }

    public int expireCacheAfterWriteSeconds() {
        return expireCacheAfterWriteSeconds;
    }

    public int maxCacheSize() {
        return maxCacheSize;
    }

    protected CacheStats getCacheStats() { return cache.stats(); }

    public S3AccessGrantsCachedBucketRegionResolver.Builder toBuilder() {
        return new S3AccessGrantsCachedBucketRegionResolver.BuilderImpl(this);
    }

    public static S3AccessGrantsCachedBucketRegionResolver.Builder builder() {
        return new S3AccessGrantsCachedBucketRegionResolver.BuilderImpl();
    }

    private S3AccessGrantsCachedBucketRegionResolver() {
        this.maxCacheSize = BUCKET_REGION_CACHE_SIZE;
        this.expireCacheAfterWriteSeconds = BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
    }

    @Override
    public Region resolve(String bucket) throws S3Exception {
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

        S3AccessGrantsCachedBucketRegionResolver.Builder s3Client(S3Client s3Client);

        S3AccessGrantsCachedBucketRegionResolver.Builder expireCacheAfterWriteSeconds(int expireCacheAfterWriteSeconds);
    }

    static final class BuilderImpl implements S3AccessGrantsCachedBucketRegionResolver.Builder {
        private int maxCacheSize = BUCKET_REGION_CACHE_SIZE;
        private int expireCacheAfterWriteSeconds = BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS;

        private S3Client s3Client;

        private BuilderImpl() {
        }

        public BuilderImpl(S3AccessGrantsCachedBucketRegionResolver s3AccessGrantsCachedBucketRegionResolver) {
            maxCacheSize(s3AccessGrantsCachedBucketRegionResolver.maxCacheSize);
            expireCacheAfterWriteSeconds(s3AccessGrantsCachedBucketRegionResolver.expireCacheAfterWriteSeconds);
            s3Client(s3AccessGrantsCachedBucketRegionResolver.s3Client);
        }

        public int maxCacheSize() {
            return maxCacheSize;
        }

        public S3Client s3Client() {
            if(s3Client == null) throw new IllegalArgumentException("S3 Client is required while configuring the S3 Bucket Region resolver!");
            return s3Client;
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

        @Override
        public Builder s3Client(S3Client s3Client) {
            if(s3Client == null) throw new IllegalArgumentException("S3 Client is required while configuring the S3 Bucket Region resolver!");
            this.s3Client = s3Client;
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
            resolver.s3Client = s3Client();
            resolver.cache = Caffeine.newBuilder()
                    .maximumSize(maxCacheSize)
                    .expireAfterWrite(Duration.ofSeconds(expireCacheAfterWriteSeconds))
                    .build();
            return resolver;
        }
    }

}
