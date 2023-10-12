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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.Duration;
import java.time.Instant;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.services.s3control.S3ControlClient;
import software.amazon.awssdk.services.s3control.model.Credentials;
import software.amazon.awssdk.services.s3control.model.GetDataAccessRequest;
import software.amazon.awssdk.services.s3control.model.GetDataAccessResponse;
import software.amazon.awssdk.services.s3control.model.Permission;

import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.ACCESS_KEY_ID;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.AWS_BASIC_CREDENTIALS;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.AWS_SESSION_CREDENTIALS;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.S3_ACCESS_GRANTS_CREDENTIALS;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.SECRET_ACCESS_KEY;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.SESSION_TOKEN;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_ACCESSGRANTS_ACCOUNT;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE;

public class S3AccessGrantsCacheTest {

    static S3AccessGrantsCache cache;
    static S3AccessGrantsCache cacheWithMockedAccountIdResolver;
    static S3ControlClient s3ControlClient = Mockito.mock(S3ControlClient.class);
    static S3AccessGrantsCachedAccountIdResolver mockResolver = Mockito.mock(S3AccessGrantsCachedAccountIdResolver.class);

    @Before
    public void setup(){
        cache = S3AccessGrantsCache.builder()
                                   .s3ControlClient(s3ControlClient)
                                   .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).build();
        cacheWithMockedAccountIdResolver = S3AccessGrantsCache.builder()
                                                              .s3ControlClient(s3ControlClient)
                                                              .s3AccessGrantsCachedAccountIdResolver(mockResolver)
                                                              .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).buildWithAccountIdResolver();
    }

    @Before
    public void clearCache(){
        cache.invalidateCache();
        cacheWithMockedAccountIdResolver.invalidateCache();
    }

    public GetDataAccessResponse getDataAccessResponseSetUp(String s3Prefix) {
        Instant ttl  = Instant.now().plus(Duration.ofMinutes(1));
        Credentials creds = Credentials.builder()
                                       .accessKeyId(ACCESS_KEY_ID)
                                       .secretAccessKey(SECRET_ACCESS_KEY)
                                       .sessionToken(SESSION_TOKEN)
                                       .expiration(ttl).build();
        return GetDataAccessResponse.builder()
                                    .credentials(creds)
                                    .matchedGrantTarget(s3Prefix).build();
    }


    @Test
    public void accessGrantsCache_accessGrantsCacheHit() {
        // Given
        CacheKey key1 = CacheKey.builder()
                                .credentials(AWS_SESSION_CREDENTIALS)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar").build();
        cache.putValueInCache(key1, S3_ACCESS_GRANTS_CREDENTIALS, 2);
        AwsSessionCredentials sessionCredentials = AwsSessionCredentials.builder()
                                                                        .accessKeyId(ACCESS_KEY_ID)
                                                                        .secretAccessKey(SECRET_ACCESS_KEY)
                                                                        .sessionToken(SESSION_TOKEN).build();
        CacheKey key2 = CacheKey.builder()
                                .credentials(sessionCredentials)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar").build();
        // When
        AwsCredentialsIdentity cacheValue1 = cache.getValueFromCache(key1, TEST_S3_ACCESSGRANTS_ACCOUNT);
        AwsCredentialsIdentity cacheValue2 = cache.getValueFromCache(key2, TEST_S3_ACCESSGRANTS_ACCOUNT);
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
        cache.putValueInCache(key1, S3_ACCESS_GRANTS_CREDENTIALS, 2);

        AwsSessionCredentials sessionCredentials = AwsSessionCredentials.builder()
                                                                        .accessKeyId(ACCESS_KEY_ID)
                                                                        .secretAccessKey(SECRET_ACCESS_KEY)
                                                                        .sessionToken(SESSION_TOKEN).build();
        CacheKey key2 = CacheKey.builder()
                                .credentials(sessionCredentials)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar/logs").build();
        // When
        AwsCredentialsIdentity cacheValue1 = cache.getValueFromCache(key1, TEST_S3_ACCESSGRANTS_ACCOUNT);
        AwsCredentialsIdentity cacheValue2 = cache.getValueFromCache(key2, TEST_S3_ACCESSGRANTS_ACCOUNT);
        // Then
        assertThat(cacheValue2).isEqualTo(cacheValue1);
    }

    @Test
    public void accessGrantsCache_readRequestShouldCheckForExistingReadWriteGrant() {
        // Given
        CacheKey key1 = CacheKey.builder()
                                .credentials(AWS_BASIC_CREDENTIALS)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar").build();
        cache.putValueInCache(key1, S3_ACCESS_GRANTS_CREDENTIALS, 2);

        AwsBasicCredentials credentials = AwsBasicCredentials.create(ACCESS_KEY_ID, SECRET_ACCESS_KEY);
        CacheKey key2 = CacheKey.builder()
                                .credentials(credentials)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar/logs").build();
        // When
        AwsCredentialsIdentity cacheValue1 = cache.getValueFromCache(key1, TEST_S3_ACCESSGRANTS_ACCOUNT);
        AwsCredentialsIdentity cacheValue2 = cache.getValueFromCache(key2, TEST_S3_ACCESSGRANTS_ACCOUNT);
        // Then
        assertThat(cacheValue2).isEqualTo(cacheValue1);
    }

    @Test
    public void accessGrantsCache_testCacheExpiry() throws InterruptedException{
        // Given
        CacheKey key1 = CacheKey.builder()
                                .credentials(AWS_SESSION_CREDENTIALS)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar").build();
        cache.putValueInCache(key1, S3_ACCESS_GRANTS_CREDENTIALS,2);

        AwsSessionCredentials sessionCredentials = AwsSessionCredentials.builder()
                                                                        .accessKeyId(ACCESS_KEY_ID)
                                                                        .secretAccessKey(SECRET_ACCESS_KEY)
                                                                        .sessionToken(SESSION_TOKEN).build();
        CacheKey key2 = CacheKey.builder()
                                .credentials(sessionCredentials)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar").build();

        assertThat(S3_ACCESS_GRANTS_CREDENTIALS).isEqualTo(cache.getValueFromCache(key2, TEST_S3_ACCESSGRANTS_ACCOUNT));
        // When
        Thread.sleep(3000);
        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponseSetUp("s3://bucket2/foo/bar"));
        cacheWithMockedAccountIdResolver.getValueFromCache(key1, TEST_S3_ACCESSGRANTS_ACCOUNT);
        // Then
        verify(s3ControlClient, atLeastOnce()).getDataAccess(any(GetDataAccessRequest.class));
    }


    @Test
    public void accessGrantsCache_accessGrantsCacheMiss() {
        // Given
        CacheKey key = CacheKey.builder()
                               .credentials(S3_ACCESS_GRANTS_CREDENTIALS)
                               .permission(Permission.READ)
                               .s3Prefix("s3://bucket2/foo/bar").build();
        GetDataAccessResponse getDataAccessResponse = getDataAccessResponseSetUp("s3://bucket2/foo/bar");

        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        // When
        cacheWithMockedAccountIdResolver.getValueFromCache(key, TEST_S3_ACCESSGRANTS_ACCOUNT);
        // Then
        verify(s3ControlClient, atLeastOnce()).getDataAccess(any(GetDataAccessRequest.class));
    }

    @Test
    public void accessGrantsCache_accessGrantsCacheMissForDifferentPermissions() {
        // Given
        CacheKey key1 = CacheKey.builder()
                                .credentials(AWS_BASIC_CREDENTIALS)
                                .permission(Permission.READ)
                                .s3Prefix("s3://bucket2/foo/bar").build();
        cache.putValueInCache(key1, S3_ACCESS_GRANTS_CREDENTIALS, 2);

        CacheKey key2 = CacheKey.builder()
                                .credentials(S3_ACCESS_GRANTS_CREDENTIALS)
                                .permission(Permission.WRITE)
                                .s3Prefix("s3://bucket2/foo/bar").build();

        GetDataAccessResponse getDataAccessResponse = getDataAccessResponseSetUp("s3://bucket2/foo/bar");

        when(mockResolver.resolve(any(String.class), any(String.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(s3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        // When
        cacheWithMockedAccountIdResolver.getValueFromCache(key2, TEST_S3_ACCESSGRANTS_ACCOUNT);
        // Then
        verify(s3ControlClient, atLeastOnce()).getDataAccess(any(GetDataAccessRequest.class));

    }

    @Test
    public void accessGrantsCache_testNullS3ControlClientException() {
        assertThatThrownBy(() -> S3AccessGrantsCache.builder()
                                                    .s3ControlClient(null)
                                                    .maxCacheSize(DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE).build())
            .isInstanceOf(IllegalArgumentException.class);

    }

}
