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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_ACCESSGRANTS_ACCOUNT;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_ACCESSGRANTS_INSTANCE_ARN;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_PREFIX;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_PREFIX_2;

import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixRequest;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixResponse;
import software.amazon.awssdk.services.s3control.model.S3ControlException;

public class S3AccessGrantsCachedAccountIdResolverTest {

    private S3ControlAsyncClient S3ControlAsyncClient;
    private S3AccessGrantsAccountIdResolver resolver;

    @Before
    public void setup() {
        S3ControlAsyncClient = Mockito.mock(S3ControlAsyncClient.class);
        resolver = S3AccessGrantsCachedAccountIdResolver
            .builder()
            .build();
    }

    @Test
    public void resolver_Returns_ExpectedAccountId() throws S3ControlException {
        // Given
        ArgumentCaptor<GetAccessGrantsInstanceForPrefixRequest> requestArgumentCaptor =
            ArgumentCaptor.forClass(GetAccessGrantsInstanceForPrefixRequest.class);

        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse> response =
            CompletableFuture.supplyAsync(() -> GetAccessGrantsInstanceForPrefixResponse.builder()
                                                    .accessGrantsInstanceId(TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT)
                                                    .accessGrantsInstanceArn(TEST_S3_ACCESSGRANTS_INSTANCE_ARN).build());
        when(S3ControlAsyncClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class)))
            .thenReturn(response);
        // When
        String accountId = resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX, S3ControlAsyncClient);
        // Then
        assertThat(accountId).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        verify(S3ControlAsyncClient, times(1)).getAccessGrantsInstanceForPrefix(requestArgumentCaptor.capture());
        assertThat(requestArgumentCaptor.getValue().accountId()).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        assertThat(requestArgumentCaptor.getValue().s3Prefix()).isEqualTo(TEST_S3_PREFIX);
    }

    @Test
    public void resolver_Returns_CachedAccountId() throws S3ControlException {
        // Given
        ArgumentCaptor<GetAccessGrantsInstanceForPrefixRequest> requestArgumentCaptor =
            ArgumentCaptor.forClass(GetAccessGrantsInstanceForPrefixRequest.class);

        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse> response =
            CompletableFuture.supplyAsync(() -> GetAccessGrantsInstanceForPrefixResponse.builder()
                                                                                        .accessGrantsInstanceId(TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT)
                                                                                        .accessGrantsInstanceArn(TEST_S3_ACCESSGRANTS_INSTANCE_ARN).build());
        when(S3ControlAsyncClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(response);
        // When attempting to resolve same prefix back to back
        String accountId1 = resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX, S3ControlAsyncClient);
        String accountId2 = resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX, S3ControlAsyncClient);
        // Then
        assertThat(accountId1).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        assertThat(accountId2).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        // Verify that we only call service 1 time and expect the next call retrieve accountId from cache
        verify(S3ControlAsyncClient, times(1)).getAccessGrantsInstanceForPrefix(requestArgumentCaptor.capture());
    }

    @Test
    public void resolver_Returns_CachedAccountId_of_Same_Bucket() throws S3ControlException {
        // Given
        ArgumentCaptor<GetAccessGrantsInstanceForPrefixRequest> requestArgumentCaptor =
            ArgumentCaptor.forClass(GetAccessGrantsInstanceForPrefixRequest.class);

        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse> response =
            CompletableFuture.supplyAsync(() -> GetAccessGrantsInstanceForPrefixResponse.builder()
                                                    .accessGrantsInstanceId(TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT)
                                                    .accessGrantsInstanceArn(TEST_S3_ACCESSGRANTS_INSTANCE_ARN).build());
        when(S3ControlAsyncClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(response);
        // When attempting to resolve same prefix back to back
        String accountId1 = resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX, S3ControlAsyncClient);
        String accountId2 = resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX_2, S3ControlAsyncClient);
        // Then
        assertThat(accountId1).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        assertThat(accountId2).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        // Verify that we only call service 1 time and expect the next call retrieve accountId from cache
        verify(S3ControlAsyncClient, times(1)).getAccessGrantsInstanceForPrefix(requestArgumentCaptor.capture());
    }

    @Test
    public void resolver_Rethrow_S3ControlException_On_ServiceError() {
        // When
        when(S3ControlAsyncClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class)))
            .thenThrow(S3ControlException.builder().build());
        // Then
        assertThatThrownBy(() -> resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX, S3ControlAsyncClient)).isInstanceOf(S3ControlException.class);

    }

    @Test
    public void resolver_Throw_S3ControlException_On_Empty_ResponseArn() {
        // Given
        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse> response =
            CompletableFuture.supplyAsync(() -> GetAccessGrantsInstanceForPrefixResponse.builder()
                                                    .accessGrantsInstanceId(TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT)
                                                    .accessGrantsInstanceArn("").build());
        when(S3ControlAsyncClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(response);
        // Then
        assertThatThrownBy(() -> resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX, S3ControlAsyncClient)).isInstanceOf(S3ControlException.class);
    }

    @Test
    public void resolve_concurrentAccess_shouldMakeOnlyOneServiceCall() throws InterruptedException {
        // Given
        String bucketName = "test-bucket";
        String s3Prefix = "s3://" + bucketName + "/path/to/object";
        String expectedAccountId = "123456789012";
        
        GetAccessGrantsInstanceForPrefixResponse response = GetAccessGrantsInstanceForPrefixResponse.builder()
                .accessGrantsInstanceId("instance-id")
                .accessGrantsInstanceArn("arn:aws:s3:us-east-1:" + expectedAccountId + ":access-grants/instance-id")
                .build();
        
        when(S3ControlAsyncClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));
        
        // When - simulate 5 concurrent threads
        int threadCount = 5;
        CompletableFuture<String>[] futures = new CompletableFuture[threadCount];
        
        for (int i = 0; i < threadCount; i++) {
            futures[i] = CompletableFuture.supplyAsync(() -> 
                resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, s3Prefix, S3ControlAsyncClient)
            );
        }
        
        // Wait for all to complete
        CompletableFuture.allOf(futures).join();
        
        // Then - verify only one service call was made
        verify(S3ControlAsyncClient, times(1)).getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class));
        
        // Verify all threads got the same account ID
        for (int i = 0; i < threadCount; i++) {
            assertThat(futures[i].join()).isEqualTo(expectedAccountId);
        }
    }

    @Test
    public void resolver_CachesAccessDeniedResponse() {
        // Given
        String s3Prefix = "s3://test-bucket/path/to/object";
        S3ControlException accessDeniedException = (S3ControlException) S3ControlException.builder()
                .message("Access Denied")
                .statusCode(403)
                .build();
        
        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(accessDeniedException);
        
        when(S3ControlAsyncClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class)))
                .thenReturn(failedFuture);
        
        // When - first call should throw and cache the exception
        assertThatThrownBy(() -> resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, s3Prefix, S3ControlAsyncClient))
                .isInstanceOf(S3ControlException.class)
                .matches(e -> ((S3ControlException) e).statusCode() == 403);
        
        // When - second call should throw cached exception without calling service
        assertThatThrownBy(() -> resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, s3Prefix, S3ControlAsyncClient))
                .isInstanceOf(S3ControlException.class)
                .matches(e -> ((S3ControlException) e).statusCode() == 403);
        
        // Then - verify service was only called once
        verify(S3ControlAsyncClient, times(1)).getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class));
    }

    @Test
    public void resolver_AccessDeniedCache_DifferentPrefix_NotCached() {
        // Given
        String s3Prefix1 = "s3://test-bucket/path/to/object1";
        String s3Prefix2 = "s3://test-bucket/path/to/object2";
        S3ControlException accessDeniedException = (S3ControlException) S3ControlException.builder()
                .message("Access Denied")
                .statusCode(403)
                .build();
        
        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse> failedFuture1 = new CompletableFuture<>();
        failedFuture1.completeExceptionally(accessDeniedException);
        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse> failedFuture2 = new CompletableFuture<>();
        failedFuture2.completeExceptionally(accessDeniedException);
        
        doReturn(failedFuture1, failedFuture2)
                .when(S3ControlAsyncClient).getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class));
        
        // When - first call should throw and cache the exception
        assertThatThrownBy(() -> resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, s3Prefix1, S3ControlAsyncClient))
                .isInstanceOf(S3ControlException.class)
                .matches(e -> ((S3ControlException) e).statusCode() == 403);
        
        // When - second call with different prefix should call service not cache
        assertThatThrownBy(() -> resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, s3Prefix2, S3ControlAsyncClient))
                .isInstanceOf(S3ControlException.class)
                .matches(e -> ((S3ControlException) e).statusCode() == 403);
        
        // Then - verify service was called twice
        verify(S3ControlAsyncClient, times(2)).getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class));
    }

    @Test
    public void resolver_AccessDeniedCache_DifferentAccountId_NotCached() {
        // Given
        String s3Prefix = "s3://test-bucket/path/to/object";
        String accountId1 = "111111111111";
        String accountId2 = "222222222222";
        S3ControlException accessDeniedException = (S3ControlException) S3ControlException.builder()
                .message("Access Denied")
                .statusCode(403)
                .build();
        
        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse> failedFuture1 = new CompletableFuture<>();
        failedFuture1.completeExceptionally(accessDeniedException);
        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse> failedFuture2 = new CompletableFuture<>();
        failedFuture2.completeExceptionally(accessDeniedException);
        
        doReturn(failedFuture1, failedFuture2)
                .when(S3ControlAsyncClient).getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class));
        
        // When - first call with accountId1 should throw and cache the exception
        assertThatThrownBy(() -> resolver.resolve(accountId1, s3Prefix, S3ControlAsyncClient))
                .isInstanceOf(S3ControlException.class)
                .matches(e -> ((S3ControlException) e).statusCode() == 403);
        
        // When - second call with different accountId should still call service
        assertThatThrownBy(() -> resolver.resolve(accountId2, s3Prefix, S3ControlAsyncClient))
                .isInstanceOf(S3ControlException.class)
                .matches(e -> ((S3ControlException) e).statusCode() == 403);
        
        // Then - verify service was called twice (once per accountId)
        verify(S3ControlAsyncClient, times(2)).getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class));
    }
}
