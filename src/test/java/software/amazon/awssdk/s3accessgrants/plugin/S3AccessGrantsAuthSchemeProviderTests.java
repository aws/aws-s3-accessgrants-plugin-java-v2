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

package software.amazon.awssdk.s3accessgrants.plugin;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.List;
import java.util.ArrayList;

import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeParams;
import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3control.model.Permission;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.any;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.*;


public class S3AccessGrantsAuthSchemeProviderTests {

    private final String BUCKET_NAME = "test-bucket";
    private final String KEY = "test-key";
    private final String OPERATION = "GetObject";

    private static List<AuthSchemeOption> authSchemeResolverResult;
    private static final String SIGNING_SCHEME = "aws.auth#sigv4";

    private static final S3Client s3client = mock(S3Client.class);

    private static final Boolean DefaultCrossRegionAccess = false;

    @BeforeClass
    public static void setUp() {
        authSchemeResolverResult = new ArrayList<>();
        authSchemeResolverResult.add(AuthSchemeOption.builder().schemeId(SIGNING_SCHEME).build());
        when(s3client.serviceClientConfiguration()).thenReturn(S3ServiceClientConfiguration.builder().region(Region.US_EAST_2).build());
        when(s3client.headBucket(any(HeadBucketRequest.class))).thenReturn(HeadBucketResponse.builder().bucketRegion(Region.US_EAST_1.toString()).build());
    }

    @Test
    public void create_authSchemeProvider_with_no_DefaultAuthProvider() {

       Assertions.assertThatThrownBy(() -> new S3AccessGrantsAuthSchemeProvider(null, s3client, DefaultCrossRegionAccess)).isInstanceOf(IllegalArgumentException.class);

    }

    @Test
    public void create_authSchemeProvider_with_no_s3client() {
        S3AuthSchemeProvider authSchemeProvider = S3AuthSchemeProvider.defaultProvider();
        Assertions.assertThatThrownBy(() -> new S3AccessGrantsAuthSchemeProvider(authSchemeProvider, null, DefaultCrossRegionAccess)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void create_authSchemeProvider_with_no_cross_region_access_setting() {
        S3AuthSchemeProvider authSchemeProvider = S3AuthSchemeProvider.defaultProvider();
        Assertions.assertThatNoException().isThrownBy(() -> new S3AccessGrantsAuthSchemeProvider(authSchemeProvider, s3client, null));
    }

    @Test
    public void create_authSchemeProvider_with_valid_DefaultAuthProvider() {
        S3AuthSchemeProvider authSchemeProvider = S3AuthSchemeProvider.defaultProvider();

        Assertions.assertThatNoException().isThrownBy(() -> new S3AccessGrantsAuthSchemeProvider(authSchemeProvider, s3client, DefaultCrossRegionAccess));
    }

    @Test
    public void call_authSchemeProvider_with_null_params() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider, s3client, DefaultCrossRegionAccess);
        S3AuthSchemeParams authSchemeParams = null;

        Assertions.assertThatThrownBy(()->accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams)).isInstanceOf(IllegalArgumentException.class);
        verify(authSchemeProvider,never()).resolveAuthScheme(authSchemeParams);
    }

    @Test
    public void call_authSchemeProvider_without_valid_region() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3Client mockS3Client = mock(S3Client.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider, mockS3Client, DefaultCrossRegionAccess);
        S3AuthSchemeParams authSchemeParams = mock(S3AuthSchemeParams.class);

        when(authSchemeParams.bucket()).thenReturn(BUCKET_NAME);
        when(authSchemeParams.operation()).thenReturn("GetObject");
        when(authSchemeProvider.resolveAuthScheme(authSchemeParams)).thenReturn(authSchemeResolverResult);
        when(mockS3Client.serviceClientConfiguration()).thenReturn(S3ServiceClientConfiguration.builder().build());
        Assertions.assertThatThrownBy(() -> accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams)).isInstanceOf(IllegalArgumentException.class);

    }

    @Test
    public void call_authSchemeProvider_with_valid_params_valid_bucket_cross_region_access_disabled() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider, s3client, DefaultCrossRegionAccess);
        S3AuthSchemeParams authSchemeParams = mock(S3AuthSchemeParams.class);

        when(authSchemeParams.bucket()).thenReturn(BUCKET_NAME);
        when(authSchemeParams.operation()).thenReturn("GetObject");
        when(authSchemeProvider.resolveAuthScheme(authSchemeParams)).thenReturn(authSchemeResolverResult);
        List<AuthSchemeOption> accessGrantsAuthSchemeResult = accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams);
        Assertions.assertThat(accessGrantsAuthSchemeResult.get(0).identityProperty(BUCKET_LOCATION_PROPERTY)).isEqualTo(Region.US_EAST_2);
    }

    @Test
    public void call_authSchemeProvider_with_valid_params_valid_bucket_cross_region_access_enabled() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider, s3client, !DefaultCrossRegionAccess);
        S3AuthSchemeParams authSchemeParams = mock(S3AuthSchemeParams.class);

        when(authSchemeParams.bucket()).thenReturn(BUCKET_NAME);
        when(authSchemeParams.operation()).thenReturn("GetObject");
        when(authSchemeProvider.resolveAuthScheme(authSchemeParams)).thenReturn(authSchemeResolverResult);
        List<AuthSchemeOption> accessGrantsAuthSchemeResult = accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams);
        Assertions.assertThat(accessGrantsAuthSchemeResult.get(0).identityProperty(BUCKET_LOCATION_PROPERTY)).isEqualTo(Region.US_EAST_1);
    }

    @Test
    public void call_authSchemeProvider_with_valid_params_invalid_bucket_operation_supported() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider, s3client, !DefaultCrossRegionAccess);
        S3AuthSchemeParams authSchemeParams = mock(S3AuthSchemeParams.class);

        when(authSchemeParams.bucket()).thenReturn(null);
        when(authSchemeParams.operation()).thenReturn("GetObject");
        Assertions.assertThatThrownBy(()->accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams)).isInstanceOf(IllegalArgumentException.class);
        verify(authSchemeProvider, times(1)).resolveAuthScheme(authSchemeParams);
    }

    @Test
    public void call_authSchemeProvider_with_valid_params_invalid_bucket_operation_not_supported() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider, s3client, !DefaultCrossRegionAccess);
        S3AuthSchemeParams authSchemeParams = mock(S3AuthSchemeParams.class);

        when(authSchemeParams.bucket()).thenReturn(null);
        when(authSchemeParams.operation()).thenReturn("ListBucket");
        when(authSchemeProvider.resolveAuthScheme(authSchemeParams)).thenReturn(authSchemeResolverResult);
        List<AuthSchemeOption> accessGrantsAuthSchemeResult = accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams);
        SdkServiceException e = (SdkServiceException) accessGrantsAuthSchemeResult.get(0).identityProperty(AUTH_EXCEPTIONS_PROPERTY);
        Assertions.assertThat(e.getCause()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void call_authSchemeProvider_with_valid_params_null_key() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider, s3client, DefaultCrossRegionAccess);
        S3AuthSchemeParams authSchemeParams = S3AuthSchemeParams.builder().bucket(BUCKET_NAME).key(null).operation(OPERATION).build();

        when(authSchemeProvider.resolveAuthScheme(authSchemeParams)).thenReturn(authSchemeResolverResult);

        List<AuthSchemeOption> accessGrantsAuthSchemeResult = accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams);

        Assertions.assertThat(accessGrantsAuthSchemeResult.get(0).identityProperty(PREFIX_PROPERTY)).isEqualTo("s3://test-bucket/*");
        Assertions.assertThat(accessGrantsAuthSchemeResult.get(0).identityProperty(AUTH_EXCEPTIONS_PROPERTY)).isNull();
        Assertions.assertThat(accessGrantsAuthSchemeResult.get(0).identityProperty(BUCKET_LOCATION_PROPERTY)).isEqualTo(Region.US_EAST_2);
    }

    @Test
    public void call_authSchemeProvider_with_valid_params_invokes_default_authSchemeProvider() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider, s3client, DefaultCrossRegionAccess);
        S3AuthSchemeParams authSchemeParams = mock(S3AuthSchemeParams.class);

        when(authSchemeParams.bucket()).thenReturn(BUCKET_NAME);
        when(authSchemeParams.operation()).thenReturn("GetObject");

        Assertions.assertThatNoException().isThrownBy(()->accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams));
        verify(authSchemeProvider, times(1)).resolveAuthScheme(authSchemeParams);
    }

    @Test
    public void call_authSchemeProvider_with_valid_params_invokes_default_authSchemeProvider_returning_valid_result() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider, s3client, DefaultCrossRegionAccess);
        S3AuthSchemeParams authSchemeParams = mock(S3AuthSchemeParams.class);

        when(authSchemeParams.bucket()).thenReturn(BUCKET_NAME);
        when(authSchemeParams.operation()).thenReturn("GetObject");
        when(authSchemeProvider.resolveAuthScheme(authSchemeParams)).thenReturn(authSchemeResolverResult);

        Assertions.assertThat(accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams).get(0).schemeId()).isEqualTo(SIGNING_SCHEME);
        verify(authSchemeProvider, times(1)).resolveAuthScheme(authSchemeParams);
    }

    @Test
    public void call_authSchemeProvider_with_valid_params_captures_all_params_on_auth_scheme() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider, s3client, DefaultCrossRegionAccess);
        S3AuthSchemeParams authSchemeParams = S3AuthSchemeParams.builder().bucket(BUCKET_NAME).key(KEY).operation(OPERATION).build();

        when(authSchemeProvider.resolveAuthScheme(authSchemeParams)).thenReturn(authSchemeResolverResult);

        List<AuthSchemeOption> accessGrantsAuthSchemeResult = accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams);

        Assertions.assertThat(accessGrantsAuthSchemeResult.get(0).identityProperty(PREFIX_PROPERTY)).isEqualTo("s3://test-bucket/test-key");
        Assertions.assertThat(accessGrantsAuthSchemeResult.get(0).identityProperty(BUCKET_LOCATION_PROPERTY)).isEqualTo(Region.US_EAST_2);
        Assertions.assertThat(accessGrantsAuthSchemeResult.get(0).identityProperty(PERMISSION_PROPERTY)).isEqualTo(Permission.READ);
    }

    @Test
    public void call_authSchemeProvider_with_valid_params_captures_all_params_with_prefix_as_object_key_on_auth_scheme() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider, s3client, DefaultCrossRegionAccess);
        S3AuthSchemeParams authSchemeParams = S3AuthSchemeParams.builder().bucket(BUCKET_NAME).prefix(KEY).operation(OPERATION).build();

        when(authSchemeProvider.resolveAuthScheme(authSchemeParams)).thenReturn(authSchemeResolverResult);

        List<AuthSchemeOption> accessGrantsAuthSchemeResult = accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams);

        Assertions.assertThat(accessGrantsAuthSchemeResult.get(0).identityProperty(PREFIX_PROPERTY)).isEqualTo("s3://test-bucket/test-key");
        Assertions.assertThat(accessGrantsAuthSchemeResult.get(0).identityProperty(BUCKET_LOCATION_PROPERTY)).isEqualTo(Region.US_EAST_2);
        Assertions.assertThat(accessGrantsAuthSchemeResult.get(0).identityProperty(PERMISSION_PROPERTY)).isEqualTo(Permission.READ);
    }

}
