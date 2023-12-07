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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.services.s3control.model.Credentials;
import software.amazon.awssdk.services.s3control.model.GetDataAccessRequest;
import software.amazon.awssdk.services.s3control.model.GetDataAccessResponse;
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.services.s3control.model.S3ControlException;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.*;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.*;

public class S3AccessGrantsCacheTest {

    private S3AccessGrantsCache cache;
    private S3AccessGrantsCache cacheWithMockedAccountIdResolver;
    private S3AccessGrantsAccessDeniedCache accessDeniedCache;
    private S3ControlAsyncClient s3ControlAsyncClient;
    private S3AccessGrantsCachedAccountIdResolver mockResolver;

    @Before
    public void setup(){
        mockResolver = Mockito.mock(S3AccessGrantsCachedAccountIdResolver.class);
        s3ControlAsyncClient = Mockito.mock(S3ControlAsyncClient.class);
        cache = S3AccessGrantsCache.builder()
                                   .s3ControlAsyncClient(s3ControlAsyncClient)
                                   .cacheExpirationTimePercentage(60)
                                   .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).build();
        cacheWithMockedAccountIdResolver = S3AccessGrantsCache.builder()
                                                              .s3ControlAsyncClient(s3ControlAsyncClient)
                                                              .cacheExpirationTimePercentage(60)
                                                              .s3AccessGrantsCachedAccountIdResolver(mockResolver)
                                                              .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).buildWithAccountIdResolver();
        accessDeniedCache = S3AccessGrantsAccessDeniedCache.builder().maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).build();

    }

    @After
    public void clearCache(){
        cache.invalidateCache();
        cacheWithMockedAccountIdResolver.invalidateCache();
        accessDeniedCache.invalidateCache();
    }

    private CompletableFuture<GetDataAccessResponse> getDataAccessResponseSetUp(String s3Prefix) {
        Instant ttl  = Instant.now().plus(Duration.ofMinutes(1));
        Credentials creds = Credentials.builder()
                                       .accessKeyId(ACCESS_KEY_ID)
                                       .secretAccessKey(SECRET_ACCESS_KEY)
                                       .sessionToken(SESSION_TOKEN)
                                       .expiration(ttl).build();
        return CompletableFuture.supplyAsync(() -> GetDataAccessResponse.builder()
                                    .credentials(creds)
                                    .matchedGrantTarget(s3Prefix + "/*").build());
    }


    @Test
    public void accessGrantsCache_accessGrantsCacheHit() {
        // Given
        CacheKey key1 = CacheKey.builder()
                                .credentials(AWS_SESSION_CREDENTIALS)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar").build();
        cache.putValueInCache(key1, CompletableFuture.supplyAsync(() -> S3_ACCESS_GRANTS_CREDENTIALS), 2);
        AwsSessionCredentials sessionCredentials = AwsSessionCredentials.builder()
                                                                        .accessKeyId(ACCESS_KEY_ID)
                                                                        .secretAccessKey(SECRET_ACCESS_KEY)
                                                                        .sessionToken(SESSION_TOKEN).build();
        CacheKey key2 = CacheKey.builder()
                                .credentials(sessionCredentials)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar").build();
        // When
        AwsCredentialsIdentity cacheValue1 = cache.getCredentials(key1, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache).join();
        AwsCredentialsIdentity cacheValue2 = cache.getCredentials(key2, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache).join();
        // Then
        assertThat(cacheValue2).isEqualTo(cacheValue1);
    }

    @Test
    public void accessGrantsCache_grantPresentForHigherLevelPrefix() {
        // Given
        CacheKey key1 = CacheKey.builder()
                                .credentials(AWS_SESSION_CREDENTIALS)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar").build();
        cache.putValueInCache(key1, CompletableFuture.supplyAsync(() -> S3_ACCESS_GRANTS_CREDENTIALS), 2);

        AwsSessionCredentials sessionCredentials = AwsSessionCredentials.builder()
                                                                        .accessKeyId(ACCESS_KEY_ID)
                                                                        .secretAccessKey(SECRET_ACCESS_KEY)
                                                                        .sessionToken(SESSION_TOKEN).build();
        CacheKey key2 = CacheKey.builder()
                                .credentials(sessionCredentials)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar/logs").build();
        // When
        AwsCredentialsIdentity cacheValue1 = cache.getCredentials(key1, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache).join();
        AwsCredentialsIdentity cacheValue2 = cache.getCredentials(key2, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache).join();
        // Then
        assertThat(cacheValue2).isEqualTo(cacheValue1);
    }

    @Test
    public void accessGrantsCache_readRequestShouldCheckForExistingReadWriteGrant() {
        // Given
        CacheKey key1 = CacheKey.builder()
                                .credentials(AWS_BASIC_CREDENTIALS)
                                .permission(Permission.READWRITE)
                                .s3Prefix("s3://bucket2/foo/bar").build();
        cache.putValueInCache(key1, CompletableFuture.supplyAsync(() -> S3_ACCESS_GRANTS_CREDENTIALS), 2);

        AwsBasicCredentials credentials = AwsBasicCredentials.create(ACCESS_KEY_ID, SECRET_ACCESS_KEY);
        CacheKey key2 = CacheKey.builder()
                                .credentials(credentials)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar/logs").build();
        // When
        AwsCredentialsIdentity cacheValue1 = cache.getCredentials(key1, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache).join();
        AwsCredentialsIdentity cacheValue2 = cache.getCredentials(key2, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache).join();
        // Then
        assertThat(cacheValue2).isEqualTo(cacheValue1);
    }

    @Test
    public void accessGrantsCache_testCacheExpiry() throws Exception {
        // Given
        CacheKey key1 = CacheKey.builder()
                                .credentials(AWS_SESSION_CREDENTIALS)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar").build();
        cache.putValueInCache(key1, CompletableFuture.supplyAsync(() -> S3_ACCESS_GRANTS_CREDENTIALS),2);

        AwsSessionCredentials sessionCredentials = AwsSessionCredentials.builder()
                                                                        .accessKeyId(ACCESS_KEY_ID)
                                                                        .secretAccessKey(SECRET_ACCESS_KEY)
                                                                        .sessionToken(SESSION_TOKEN).build();
        CacheKey key2 = CacheKey.builder()
                                .credentials(sessionCredentials)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar").build();

        assertThat(S3_ACCESS_GRANTS_CREDENTIALS).isEqualTo(cache.getCredentials(key2, TEST_S3_ACCESSGRANTS_ACCOUNT,
                                                                                accessDeniedCache).join());
        // When
        Thread.sleep(3000);
        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlAsyncClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponseSetUp("s3://bucket2/foo/bar"));
        cacheWithMockedAccountIdResolver.getCredentials(key1, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache);
        // Then
        verify(s3ControlAsyncClient, atLeastOnce()).getDataAccess(any(GetDataAccessRequest.class));
    }


    @Test
    public void accessGrantsCache_accessGrantsCacheMiss() {
        // Given
        CacheKey key = CacheKey.builder()
                               .credentials(S3_ACCESS_GRANTS_CREDENTIALS)
                               .permission(Permission.READ)
                               .s3Prefix("s3://bucket/foo/bar").build();
        CompletableFuture<GetDataAccessResponse> getDataAccessResponse = getDataAccessResponseSetUp("s3://bucket2/foo/bar");

        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlAsyncClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        // When
        cacheWithMockedAccountIdResolver.getCredentials(key, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache);
        // Then
        verify(s3ControlAsyncClient, atLeastOnce()).getDataAccess(any(GetDataAccessRequest.class));
    }

    @Test
    public void accessGrantsCache_accessGrantsCacheMissForDifferentPermissions() {
        // Given
        CacheKey key1 = CacheKey.builder()
                                .credentials(AWS_BASIC_CREDENTIALS)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar").build();
        cache.putValueInCache(key1, CompletableFuture.supplyAsync(() -> S3_ACCESS_GRANTS_CREDENTIALS), 2);

        CacheKey key2 = CacheKey.builder()
                                .credentials(S3_ACCESS_GRANTS_CREDENTIALS)
                                .permission(Permission.WRITE)
                                .s3Prefix("s3://bucket2/foo/bar").build();

        CompletableFuture<GetDataAccessResponse> getDataAccessResponse = getDataAccessResponseSetUp("s3://bucket2/foo/bar");

        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlAsyncClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        // When
        cacheWithMockedAccountIdResolver.getCredentials(key2, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache);
        // Then
        verify(s3ControlAsyncClient, atLeastOnce()).getDataAccess(any(GetDataAccessRequest.class));

    }

    @Test
    public void accessGrantsCache_testNullS3ControlAsyncClientException() {
        assertThatThrownBy(() -> S3AccessGrantsCache.builder()
                                                    .s3ControlAsyncClient(null)
                                                    .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).build())
            .isInstanceOf(IllegalArgumentException.class);

    }

    @Test
    public void accessGrantsCache_throwsS3ControlException() {

        CacheKey key1 = CacheKey.builder()
                                .credentials(S3_ACCESS_GRANTS_CREDENTIALS)
                                .permission(Permission.WRITE)
                                .s3Prefix("s3://bucket2/foo/bar").build();

        S3ControlException s3ControlException = Mockito.mock(S3ControlException.class);

        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlAsyncClient.getDataAccess(any(GetDataAccessRequest.class))).thenThrow(s3ControlException);
        when(s3ControlException.statusCode()).thenReturn(403);
        // When
        try {
            cacheWithMockedAccountIdResolver.getCredentials(key1, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache);
        }catch (S3ControlException e){}
        // Then
        assertThat(accessDeniedCache.getValueFromCache(key1)).isInstanceOf(S3ControlException.class);
    }

    @Test
    public void accessGrantsCache_testTTL() {
        // When
        Instant expiration = Instant.now().plus(10, ChronoUnit.SECONDS);
        // Then
        assertThat(cacheWithMockedAccountIdResolver.getTTL(expiration)).isEqualTo(6);
    }

    @Test
    public void accessGrantsCache_testProcessingOfMatchedGrantsTarget() {
        // When
        String grant1 = "s3://bucket/foo/bar/*";
        String grant2 = "s3://bucket/foo/bar.txt";
        String grant3 = "s3://*";
        // Then
        assertThat(cache.processMatchedGrantTarget(grant1)).isEqualTo("s3://bucket/foo/bar");
        assertThat(cache.processMatchedGrantTarget(grant2)).isEqualTo("s3://bucket/foo/bar.txt");
        assertThat(cache.processMatchedGrantTarget(grant3)).isEqualTo("s3:/");
    }

    @Test
    public void accessGrantsCache_testGrantPresentForLocation() {
        // Given
        CompletableFuture<GetDataAccessResponse> getDataAccessResponse = getDataAccessResponseSetUp("s3://bucket/foo");
        CacheKey key1 = CacheKey.builder()
                                .credentials(AWS_BASIC_CREDENTIALS)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket/foo/bar/text.txt").build();

        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlAsyncClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        cacheWithMockedAccountIdResolver.getCredentials(key1, TEST_S3_ACCESSGRANTS_ACCOUNT,accessDeniedCache);
        // When
        CacheKey key2 = CacheKey.builder()
                                .credentials(AWS_BASIC_CREDENTIALS)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket/foo/log/text.txt").build();
        cacheWithMockedAccountIdResolver.getCredentials(key2, TEST_S3_ACCESSGRANTS_ACCOUNT,accessDeniedCache);
        // Then
        verify(s3ControlAsyncClient, times(1)).getDataAccess(any(GetDataAccessRequest.class));
    }
    @Test
    public void accessGrantsCache_testGrantWithPrefix() {
        // Given
        CacheKey key1 = CacheKey.builder()
                                .credentials(AWS_BASIC_CREDENTIALS)
                                .permission(Permission.READWRITE)
                                .s3Prefix("s3://bucket2/foo*").build();
        CacheKey key2 = CacheKey.builder()
                                .credentials(AWS_BASIC_CREDENTIALS)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/text.txt").build();
        cache.putValueInCache(key1, CompletableFuture.supplyAsync(() -> S3_ACCESS_GRANTS_CREDENTIALS), 2);
        // When
        AwsCredentialsIdentity cacheValue1 = cache.getCredentials(key2, TEST_S3_ACCESSGRANTS_ACCOUNT, accessDeniedCache).join();
        // Then
        assertThat(cacheValue1).isEqualTo(S3_ACCESS_GRANTS_CREDENTIALS);
        verify(s3ControlAsyncClient, times(0)).getDataAccess(any(GetDataAccessRequest.class));
    }

    @Test
    public void accessGrantsCache_testPutValueInCacheForObjectLevelGrant() {
        // When
        Instant ttl  = Instant.now().plus(Duration.ofMinutes(1));
        Credentials creds = Credentials.builder()
                                       .accessKeyId(ACCESS_KEY_ID)
                                       .secretAccessKey(SECRET_ACCESS_KEY)
                                       .sessionToken(SESSION_TOKEN)
                                       .expiration(ttl).build();
        CompletableFuture<GetDataAccessResponse> getDataAccessResponse =
            CompletableFuture.supplyAsync(() -> GetDataAccessResponse.builder()
                                                                     .credentials(creds)
                                                                     .matchedGrantTarget("s3://bucket/foo").build());
        CacheKey key = CacheKey.builder()
                                .credentials(AWS_BASIC_CREDENTIALS)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket/foo/bar/text.txt").build();
        // When
        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlAsyncClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        cacheWithMockedAccountIdResolver.getCredentials(key, TEST_S3_ACCESSGRANTS_ACCOUNT,accessDeniedCache);
        cacheWithMockedAccountIdResolver.getCredentials(key, TEST_S3_ACCESSGRANTS_ACCOUNT,accessDeniedCache);
        // Then
        verify(s3ControlAsyncClient, times(2)).getDataAccess(any(GetDataAccessRequest.class));

    }

}
