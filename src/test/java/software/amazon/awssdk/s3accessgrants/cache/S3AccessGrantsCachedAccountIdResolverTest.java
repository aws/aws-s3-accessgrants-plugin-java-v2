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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_ACCESSGRANTS_ACCOUNT;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_ACCESSGRANTS_INSTANCE_ARN;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_PREFIX;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_PREFIX_2;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3control.S3ControlClient;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixRequest;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixResponse;
import software.amazon.awssdk.services.s3control.model.S3ControlException;

public class S3AccessGrantsCachedAccountIdResolverTest {

    private S3ControlClient s3ControlClient;
    private S3AccessGrantsAccountIdResolver resolver;

    @Before
    public void setup() {
        s3ControlClient = Mockito.mock(S3ControlClient.class);
        resolver = S3AccessGrantsCachedAccountIdResolver
            .builder()
            .accountId(TEST_S3_ACCESSGRANTS_ACCOUNT)
            .s3ControlClient(s3ControlClient)
            .build();
    }

    @Test
    public void resolver_Returns_ExpectedAccountId() throws S3ControlException {
        // Given
        ArgumentCaptor<GetAccessGrantsInstanceForPrefixRequest> requestArgumentCaptor =
            ArgumentCaptor.forClass(GetAccessGrantsInstanceForPrefixRequest.class);

        GetAccessGrantsInstanceForPrefixResponse response =
            GetAccessGrantsInstanceForPrefixResponse.builder()
                                                    .accessGrantsInstanceId(TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT)
                                                    .accessGrantsInstanceArn(TEST_S3_ACCESSGRANTS_INSTANCE_ARN).build();
        when(s3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(response);
        // When
        String accountId = resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX);
        // Then
        assertThat(accountId).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        verify(s3ControlClient, times(1)).getAccessGrantsInstanceForPrefix(requestArgumentCaptor.capture());
        assertThat(requestArgumentCaptor.getValue().accountId()).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        assertThat(requestArgumentCaptor.getValue().s3Prefix()).isEqualTo(TEST_S3_PREFIX);
    }

    @Test
    public void resolver_Returns_CachedAccountId() throws S3ControlException {
        // Given
        ArgumentCaptor<GetAccessGrantsInstanceForPrefixRequest> requestArgumentCaptor =
            ArgumentCaptor.forClass(GetAccessGrantsInstanceForPrefixRequest.class);

        GetAccessGrantsInstanceForPrefixResponse response =
            GetAccessGrantsInstanceForPrefixResponse.builder()
                                                    .accessGrantsInstanceId(TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT)
                                                    .accessGrantsInstanceArn(TEST_S3_ACCESSGRANTS_INSTANCE_ARN).build();
        when(s3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(response);
        // When attempting to resolve same prefix back to back
        String accountId1 = resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX);
        String accountId2 = resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX);
        // Then
        assertThat(accountId1).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        assertThat(accountId2).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        // Verify that we only call service 1 time and expect the next call retrieve accountId from cache
        verify(s3ControlClient, times(1)).getAccessGrantsInstanceForPrefix(requestArgumentCaptor.capture());
    }

    @Test
    public void resolver_Returns_CachedAccountId_of_Same_Bucket() throws S3ControlException {
        // Given
        ArgumentCaptor<GetAccessGrantsInstanceForPrefixRequest> requestArgumentCaptor =
            ArgumentCaptor.forClass(GetAccessGrantsInstanceForPrefixRequest.class);

        GetAccessGrantsInstanceForPrefixResponse response =
            GetAccessGrantsInstanceForPrefixResponse.builder()
                                                    .accessGrantsInstanceId(TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT)
                                                    .accessGrantsInstanceArn(TEST_S3_ACCESSGRANTS_INSTANCE_ARN).build();
        when(s3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(response);
        // When attempting to resolve same prefix back to back
        String accountId1 = resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX);
        String accountId2 = resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX_2);
        // Then
        assertThat(accountId1).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        assertThat(accountId2).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        // Verify that we only call service 1 time and expect the next call retrieve accountId from cache
        verify(s3ControlClient, times(1)).getAccessGrantsInstanceForPrefix(requestArgumentCaptor.capture());
    }

    @Test
    public void resolver_Rethrow_S3ControlException_On_ServiceError() {
        // Given
        ArgumentCaptor<GetAccessGrantsInstanceForPrefixRequest> requestArgumentCaptor =
            ArgumentCaptor.forClass(GetAccessGrantsInstanceForPrefixRequest.class);
        when(s3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class)))
            .thenThrow(S3ControlException.builder().build());
        // Then
        assertThatThrownBy(() -> resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX)).isInstanceOf(S3ControlException.class);

    }

    @Test
    public void resolver_Throw_S3ControlException_On_Empty_ResponseArn() {
        // Given
        GetAccessGrantsInstanceForPrefixResponse response =
            GetAccessGrantsInstanceForPrefixResponse.builder()
                                                    .accessGrantsInstanceId(TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT)
                                                    .accessGrantsInstanceArn("").build();
        when(s3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(response);
        // Then
        assertThatThrownBy(() -> resolver.resolve(TEST_S3_ACCESSGRANTS_ACCOUNT, TEST_S3_PREFIX)).isInstanceOf(S3ControlException.class);
    }

}
