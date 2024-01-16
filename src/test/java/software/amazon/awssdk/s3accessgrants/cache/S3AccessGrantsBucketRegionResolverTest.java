package software.amazon.awssdk.s3accessgrants.cache;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

public class S3AccessGrantsBucketRegionResolverTest {

    private S3Client s3Client;
    private S3AccessGrantsCachedBucketRegionResolver s3AccessGrantsCachedBucketRegionResolver;
    private String TEST_BUCKET_NAME;

    @Before
    public void setUp() {
        s3Client = mock(S3Client.class);
        TEST_BUCKET_NAME = "test-bucket";
        s3AccessGrantsCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().s3Client(s3Client).build();
        HeadBucketResponse headBucketResponse = HeadBucketResponse.builder().bucketRegion(Region.US_EAST_1.toString()).build();
        when(s3Client.headBucket(any(HeadBucketRequest.class))).thenReturn(headBucketResponse);
    }

    @Test
    public void call_resolve_should_cache_the_bucket_region() {
        Assert.assertEquals(s3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME), Region.US_EAST_1);
        // initial request should be made to the service
        verify(s3Client, times(1)).headBucket(any(HeadBucketRequest.class));
        Assert.assertEquals(s3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME), Region.US_EAST_1);
        // No call should be made to the service as the region is already cached
        verify(s3Client, times(1)).headBucket(any(HeadBucketRequest.class));
    }

    @Test
    public void call_resolve_should_not_cache_the_bucket_region() {
        S3Client localS3Client = mock(S3Client.class);
        S3AccessGrantsCachedBucketRegionResolver localS3AccessGrantsCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().s3Client(localS3Client).build();
        HeadBucketResponse headBucketResponse = HeadBucketResponse.builder().bucketRegion(null).build();
        when(localS3Client.headBucket(any(HeadBucketRequest.class))).thenReturn(headBucketResponse);
        Assertions.assertThatThrownBy(() -> localS3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME)).isInstanceOf(SdkServiceException.class);
        verify(localS3Client, times(1)).headBucket(any(HeadBucketRequest.class));
        // since bucket region is null, cache will not store the entry
        Assertions.assertThatThrownBy(() -> localS3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME)).isInstanceOf(SdkServiceException.class);
        verify(localS3Client, times(2)).headBucket(any(HeadBucketRequest.class));
    }

    @Test
    public void verify_bucket_region_cache_expiration() throws InterruptedException {

        S3AccessGrantsCachedBucketRegionResolver localCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver
                .builder()
                .s3Client(s3Client)
                .expireCacheAfterWriteSeconds(1)
                .build();

        Assert.assertEquals(Region.US_EAST_1, localCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME));
        verify(s3Client, times(1)).headBucket(any(HeadBucketRequest.class));
        Thread.sleep(2000);
        // should evict the entry and the subsequent request should call the service
        Assert.assertEquals(Region.US_EAST_1, localCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME));
        verify(s3Client, times(2)).headBucket(any(HeadBucketRequest.class));

    }

    @Test
    public void call_bucket_region_cache_with_non_existent_bucket() throws InterruptedException {

        S3Client localS3Client = mock(S3Client.class);
        S3AccessGrantsCachedBucketRegionResolver localS3AccessGrantsCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().s3Client(localS3Client).build();
        when(localS3Client.headBucket(any(HeadBucketRequest.class))).thenThrow(S3Exception.builder().message("Bucket does not exist").statusCode(404).build());
        Assertions.assertThatThrownBy(() ->  localS3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME)).isInstanceOf(SdkServiceException.class);
        verify(localS3Client, times(1)).headBucket(any(HeadBucketRequest.class));

    }

    @Test
    public void call_bucket_region_cache_resolve_returns_redirect() throws InterruptedException {

        S3Client localS3Client = mock(S3Client.class);
        AwsServiceException s3Exception = mock(S3Exception.class);
        S3AccessGrantsCachedBucketRegionResolver localS3AccessGrantsCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().s3Client(localS3Client).build();
        List<String> regionList = new ArrayList<>();
        regionList.add(Region.US_EAST_1.toString());
        SdkHttpResponse sdkHttpResponse = SdkHttpResponse.builder().putHeader("x-amz-bucket-region", regionList).build();
        AwsErrorDetails awsErrorDetails = AwsErrorDetails.builder().sdkHttpResponse(sdkHttpResponse).build();
        when(s3Exception.statusCode()).thenReturn(301);
        when(s3Exception.awsErrorDetails()).thenReturn(awsErrorDetails);
        when(localS3Client.headBucket(any(HeadBucketRequest.class))).thenThrow(s3Exception);
        Assert.assertEquals(Region.US_EAST_1, localS3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME));
        verify(localS3Client, times(1)).headBucket(any(HeadBucketRequest.class));

    }

    @Test
    public void call_bucket_region_cache_resolve_returns_redirect_with_null_region() throws InterruptedException {

        S3Client localS3Client = mock(S3Client.class);
        AwsServiceException s3Exception = mock(S3Exception.class);
        S3AccessGrantsCachedBucketRegionResolver localS3AccessGrantsCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().s3Client(localS3Client).build();
        List<String> regionList = new ArrayList<>();
        regionList.add(null);
        SdkHttpResponse sdkHttpResponse = SdkHttpResponse.builder().putHeader("x-amz-bucket-region", regionList).build();
        AwsErrorDetails awsErrorDetails = AwsErrorDetails.builder().sdkHttpResponse(sdkHttpResponse).build();
        when(s3Exception.statusCode()).thenReturn(301);
        when(s3Exception.awsErrorDetails()).thenReturn(awsErrorDetails);
        when(localS3Client.headBucket(any(HeadBucketRequest.class))).thenThrow(s3Exception);
        // resolving that exceptions are thrown when bucket region cannot be determined.
        Assertions.assertThatThrownBy(() -> localS3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME))
                .isInstanceOf(SdkServiceException.class);

    }

    @Test
    public void call_bucket_region_cache_resolve_returns_non_redirect_with_region() throws InterruptedException {

        S3Client localS3Client = mock(S3Client.class);
        AwsServiceException s3Exception = mock(S3Exception.class);
        S3AccessGrantsCachedBucketRegionResolver localS3AccessGrantsCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().s3Client(localS3Client).build();
        List<String> regionList = new ArrayList<>();
        regionList.add(Region.US_EAST_1.toString());
        SdkHttpResponse sdkHttpResponse = SdkHttpResponse.builder().putHeader("x-amz-bucket-region", regionList).build();
        AwsErrorDetails awsErrorDetails = AwsErrorDetails.builder().sdkHttpResponse(sdkHttpResponse).build();
        when(s3Exception.statusCode()).thenReturn(403);
        when(s3Exception.awsErrorDetails()).thenReturn(awsErrorDetails);
        when(localS3Client.headBucket(any(HeadBucketRequest.class))).thenThrow(s3Exception);
        Assert.assertEquals(Region.US_EAST_1, localS3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME));
        verify(localS3Client, times(1)).headBucket(any(HeadBucketRequest.class));


    }

}
