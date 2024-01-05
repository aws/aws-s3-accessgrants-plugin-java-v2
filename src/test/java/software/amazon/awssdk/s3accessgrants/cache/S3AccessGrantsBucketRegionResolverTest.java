package software.amazon.awssdk.s3accessgrants.cache;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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
import static org.mockito.Mockito.*;

public class S3AccessGrantsBucketRegionResolverTest {

    private S3Client s3Client;
    private S3AccessGrantsCachedBucketRegionResolver s3AccessGrantsCachedBucketRegionResolver;
    private String TEST_BUCKET_NAME;

    @Before
    public void setUp() {
        s3Client = mock(S3Client.class);
        TEST_BUCKET_NAME = "test-bucket";
        s3AccessGrantsCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver.builder().build();
        HeadBucketResponse headBucketResponse = HeadBucketResponse.builder().bucketRegion(Region.US_EAST_1.toString()).build();
        when(s3Client.headBucket(any(HeadBucketRequest.class))).thenReturn(headBucketResponse);
    }

    @Test
    public void call_resolve_with_invalid_s3Client() {
        S3Client invalidS3Client = null;
        Assertions.assertThatThrownBy(() -> s3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME, invalidS3Client))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_resolve_should_cache_the_bucket_region() {
        Assert.assertEquals(s3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME, s3Client), Region.US_EAST_1);
        // initial request should be made to the service
        verify(s3Client, times(1)).headBucket(any(HeadBucketRequest.class));
        Assert.assertEquals(s3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME, s3Client), Region.US_EAST_1);
        // No call should be made to the service as the region is already cached
        verify(s3Client, times(1)).headBucket(any(HeadBucketRequest.class));
    }

    @Test
    public void call_resolve_should_not_cache_the_bucket_region() {
        S3Client localS3Client = mock(S3Client.class);
        HeadBucketResponse headBucketResponse = HeadBucketResponse.builder().bucketRegion(null).build();
        when(localS3Client.headBucket(any(HeadBucketRequest.class))).thenReturn(headBucketResponse);
        Assertions.assertThatThrownBy(() -> s3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME, localS3Client)).isInstanceOf(SdkServiceException.class);
        verify(localS3Client, times(1)).headBucket(any(HeadBucketRequest.class));
        Assertions.assertThatThrownBy(() -> s3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME, localS3Client)).isInstanceOf(SdkServiceException.class);
        verify(localS3Client, times(2)).headBucket(any(HeadBucketRequest.class));
    }

    @Test
    public void verify_bucket_region_cache_expiration() throws InterruptedException {

        S3AccessGrantsCachedBucketRegionResolver localCachedBucketRegionResolver = S3AccessGrantsCachedBucketRegionResolver
                .builder()
                .expireCacheAfterWriteSeconds(1)
                .build();

        Assert.assertEquals(Region.US_EAST_1, localCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME, s3Client));
        verify(s3Client, times(1)).headBucket(any(HeadBucketRequest.class));
        Thread.sleep(1000);
        // should evict the entry after 1 sec and the subsequent request should call the service
        Assert.assertEquals(Region.US_EAST_1, localCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME, s3Client));
        verify(s3Client, times(2)).headBucket(any(HeadBucketRequest.class));

    }

    @Test
    public void call_bucket_region_cache_with_non_existent_bucket() throws InterruptedException {

        S3Client localS3Client = mock(S3Client.class);
        when(localS3Client.headBucket(any(HeadBucketRequest.class))).thenThrow(S3Exception.builder().message("Bucket does not exist").statusCode(404).build());
        Assertions.assertThatThrownBy(() ->  s3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME, localS3Client)).isInstanceOf(S3Exception.class);
        verify(localS3Client, times(1)).headBucket(any(HeadBucketRequest.class));

    }

    @Test
    public void call_bucket_region_cache_resolve_returns_redirect() throws InterruptedException {

        S3Client localS3Client = mock(S3Client.class);
        AwsServiceException s3Exception = mock(S3Exception.class);
        List<String> regionList = new ArrayList<>();
        regionList.add(Region.US_EAST_1.toString());
        SdkHttpResponse sdkHttpResponse = SdkHttpResponse.builder().putHeader("x-amz-bucket-region", regionList).build();
        AwsErrorDetails awsErrorDetails = AwsErrorDetails.builder().sdkHttpResponse(sdkHttpResponse).build();
        when(s3Exception.statusCode()).thenReturn(301);
        when(s3Exception.awsErrorDetails()).thenReturn(awsErrorDetails);
        when(localS3Client.headBucket(any(HeadBucketRequest.class))).thenThrow(s3Exception);
        Assert.assertEquals(Region.US_EAST_1, s3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME, localS3Client));
        verify(localS3Client, times(1)).headBucket(any(HeadBucketRequest.class));

    }

    @Test
    public void call_bucket_region_cache_resolve_returns_redirect_with_null_region() throws InterruptedException {

        S3Client localS3Client = mock(S3Client.class);
        AwsServiceException s3Exception = mock(S3Exception.class);
        List<String> regionList = new ArrayList<>();
        regionList.add(null);
        SdkHttpResponse sdkHttpResponse = SdkHttpResponse.builder().putHeader("x-amz-bucket-region", regionList).build();
        AwsErrorDetails awsErrorDetails = AwsErrorDetails.builder().sdkHttpResponse(sdkHttpResponse).build();
        when(s3Exception.statusCode()).thenReturn(301);
        when(s3Exception.awsErrorDetails()).thenReturn(awsErrorDetails);
        when(localS3Client.headBucket(any(HeadBucketRequest.class))).thenThrow(s3Exception);
        Assertions.assertThatThrownBy(() -> s3AccessGrantsCachedBucketRegionResolver.resolve(TEST_BUCKET_NAME, localS3Client))
                .isInstanceOf(SdkServiceException.class);

    }

}
