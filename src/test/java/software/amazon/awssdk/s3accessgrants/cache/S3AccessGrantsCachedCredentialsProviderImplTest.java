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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.ACCESS_KEY_ID;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.AWS_SESSION_CREDENTIALS;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.SECRET_ACCESS_KEY;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.SESSION_TOKEN;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants. TEST_S3_ACCESSGRANTS_ACCOUNT;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.services.s3control.model.Credentials;
import software.amazon.awssdk.services.s3control.model.GetDataAccessRequest;
import software.amazon.awssdk.services.s3control.model.GetDataAccessResponse;
import software.amazon.awssdk.services.s3control.model.Permission;

public class S3AccessGrantsCachedCredentialsProviderImplTest {
    S3AccessGrantsCachedCredentialsProviderImpl cache;
    S3AccessGrantsCachedCredentialsProviderImpl cacheWithMockedAccountIdResolver;
    static S3ControlAsyncClient S3ControlAsyncClient = Mockito.mock(S3ControlAsyncClient.class);
    static S3AccessGrantsCachedAccountIdResolver mockResolver = Mockito.mock(S3AccessGrantsCachedAccountIdResolver.class);
    static Credentials credentials;

    @Before
    public void setup() {
        cache = S3AccessGrantsCachedCredentialsProviderImpl.builder().build();
        cacheWithMockedAccountIdResolver = S3AccessGrantsCachedCredentialsProviderImpl.builder()
                                                                                      .s3AccessGrantsCachedAccountIdResolver(mockResolver)
                                                                                      .buildWithAccountIdResolver();
    }

    @Before
    public void clearCache(){
        cache.invalidateCache();
    }

    public CompletableFuture<GetDataAccessResponse> getDataAccessResponseSetUp(String s3Prefix) {

        Instant ttl  = Instant.now().plus(Duration.ofMinutes(1));
        credentials = Credentials.builder()
                                 .accessKeyId(ACCESS_KEY_ID)
                                 .secretAccessKey(SECRET_ACCESS_KEY)
                                 .sessionToken(SESSION_TOKEN)
                                 .expiration(ttl).build();
        return CompletableFuture.supplyAsync(() -> GetDataAccessResponse.builder()
                                    .credentials(credentials)
                                    .matchedGrantTarget(s3Prefix).build());
    }

    @Test
    public void cacheImpl_cacheHit() {
        // Given
        CompletableFuture<GetDataAccessResponse> getDataAccessResponse = getDataAccessResponseSetUp("s3://bucket2/foo/*");
        when(mockResolver.resolve(any(String.class), any(String.class), any(S3ControlAsyncClient.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(S3ControlAsyncClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        cacheWithMockedAccountIdResolver.getDataAccess(AWS_SESSION_CREDENTIALS, Permission.READ, "s3://bucket2/foo/bar", TEST_S3_ACCESSGRANTS_ACCOUNT, S3ControlAsyncClient);
        AwsSessionCredentials sessionCredentials = AwsSessionCredentials.builder().accessKeyId(credentials.accessKeyId())
                                                                        .secretAccessKey(credentials.secretAccessKey())
                                                                        .sessionToken(credentials.sessionToken()).build();
        // When
        AwsCredentialsIdentity credentialsIdentity = cacheWithMockedAccountIdResolver.getDataAccess(AWS_SESSION_CREDENTIALS,
                                                                                      Permission.READ,
                                                                                     "s3://bucket2/foo/bar",
                                                                                                    TEST_S3_ACCESSGRANTS_ACCOUNT, S3ControlAsyncClient).join();
        // Then
        assertThat(credentialsIdentity).isEqualTo(sessionCredentials);
        verify(S3ControlAsyncClient, times(1)).getDataAccess(any(GetDataAccessRequest.class));

    }

    @Test
    public void cacheImpl_cacheMiss() {
        // Given
        CompletableFuture<GetDataAccessResponse> getDataAccessResponse = getDataAccessResponseSetUp("s3://bucket2/foo/bar");
        when(mockResolver.resolve(any(String.class), any(String.class), any(S3ControlAsyncClient.class))).thenReturn(TEST_S3_ACCESSGRANTS_ACCOUNT);
        when(S3ControlAsyncClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        // When
        cacheWithMockedAccountIdResolver.getDataAccess(AWS_SESSION_CREDENTIALS, Permission.READ, "s3://bucket2/foo/bar", TEST_S3_ACCESSGRANTS_ACCOUNT, S3ControlAsyncClient);
        cacheWithMockedAccountIdResolver.getDataAccess(AWS_SESSION_CREDENTIALS, Permission.READ, "s3://bucket2/foo/bar", TEST_S3_ACCESSGRANTS_ACCOUNT, S3ControlAsyncClient);
        // Then
        verify(S3ControlAsyncClient, times(2)).getDataAccess(any(GetDataAccessRequest.class));

    }
}
