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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_ACCESSGRANTS_ACCOUNT;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_ACCESSGRANTS_INSTANCE_ARN;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_PREFIX;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3control.S3ControlClient;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixRequest;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixResponse;
import software.amazon.awssdk.services.s3control.model.S3ControlException;
import software.amazon.awssdk.services.s3control.model.S3ControlResponse;

public class S3AccessGrantsCachedAccountIdResolverTest {

    private S3ControlClient s3ControlClient;
    private S3AccessGrantsAccountIdResolver resolver;

    @Before
    public void setup() {
        s3ControlClient = Mockito.mock(S3ControlClient.class);
        resolver = new S3AccessGrantsCachedAccountIdResolver(s3ControlClient);
    }

    @Test
    public void resolver_Returns_ExpectedAccountId() throws S3ControlException {
        // Given
        ArgumentCaptor<GetAccessGrantsInstanceForPrefixRequest> requestArgumentCaptor =
            ArgumentCaptor.forClass(GetAccessGrantsInstanceForPrefixRequest.class);

        S3ControlResponse response =
            GetAccessGrantsInstanceForPrefixResponse.builder()
                                                    .accessGrantsInstanceId(TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT)
                                                    .accessGrantsInstanceArn(TEST_S3_ACCESSGRANTS_INSTANCE_ARN).build();
        when(s3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn((GetAccessGrantsInstanceForPrefixResponse) response);
        // When
        String accountId = resolver.resolve(TEST_S3_PREFIX);
        // Then
        assertThat(accountId).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        verify(s3ControlClient, times(1)).getAccessGrantsInstanceForPrefix(requestArgumentCaptor.capture());
    }

    @Test
    public void resolver_Returns_CachedAccountId() throws S3ControlException {
        // Given
        ArgumentCaptor<GetAccessGrantsInstanceForPrefixRequest> requestArgumentCaptor =
            ArgumentCaptor.forClass(GetAccessGrantsInstanceForPrefixRequest.class);

        S3ControlResponse response =
            GetAccessGrantsInstanceForPrefixResponse.builder()
                                                    .accessGrantsInstanceId(TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT)
                                                    .accessGrantsInstanceArn(TEST_S3_ACCESSGRANTS_INSTANCE_ARN).build();
        when(s3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn((GetAccessGrantsInstanceForPrefixResponse) response);
        // When attempting to resolve same prefix back to back
        String accountId1 = resolver.resolve(TEST_S3_PREFIX);
        String accountId2 = resolver.resolve(TEST_S3_PREFIX);
        // Then
        assertThat(accountId1).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        assertThat(accountId2).isEqualTo(TEST_S3_ACCESSGRANTS_ACCOUNT);
        // Verify that we only call service 1 time and expect the next call retrieve accountId from cache
        verify(s3ControlClient, times(1)).getAccessGrantsInstanceForPrefix(requestArgumentCaptor.capture());
    }
}
