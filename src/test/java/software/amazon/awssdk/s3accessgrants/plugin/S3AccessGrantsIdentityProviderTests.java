///*
// * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License").
// * You may not use this file except in compliance with the License.
// * A copy of the License is located at
// *
// *  http://aws.amazon.com/apache2.0
// *
// * or in the "license" file accompanying this file. This file is distributed
// * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// * express or implied. See the License for the specific language governing
// * permissions and limitations under the License.
// */
//
package software.amazon.awssdk.s3accessgrants.plugin;

import java.nio.file.AccessDeniedException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.assertj.core.api.Assertions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProvider;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProviderImpl;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClientBuilder;
import software.amazon.awssdk.services.s3control.model.Credentials;
import software.amazon.awssdk.services.s3control.model.GetDataAccessRequest;
import software.amazon.awssdk.services.s3control.model.GetDataAccessResponse;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixRequest;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixResponse;
import software.amazon.awssdk.services.s3control.model.InvalidRequestException;
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.services.s3control.model.Privilege;
import software.amazon.awssdk.services.s3control.model.S3ControlException;


import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;

import org.junit.BeforeClass;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PERMISSION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.BUCKET_LOCATION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.AUTH_EXCEPTIONS_PROPERTY;


public class S3AccessGrantsIdentityProviderTests {

    private final Region TEST_REGION = Region.US_EAST_2;

    private final String TEST_OPERATION = "GetObject";

    private static final String TEST_ACCESS_KEY = "ARAGXXXXXXX123";

    private static final String TEST_SECRET_KEY = "ARAGXXXXXXX123112e2e3aadadwefdscac";

    private static final String TEST_SESSION_TOKEN = "ARAGXXXXXXX123112e2e3aadadwefdscacadascaacacacasc";

    private final Privilege TEST_PRIVILEGE = Privilege.DEFAULT;

    private final Boolean TEST_CACHE_ENABLED = true;
    private final Boolean TEST_CACHE_DISABLED = false;

    private final Boolean TEST_FALLBACK_ENABLED = false;

    private static final String TEST_ACCOUNT = "12345028312";
    private static AwsCredentialsProvider credentialsProvider;

    private static S3ControlAsyncClientBuilder s3ControlAsyncClientBuilder;

    private static S3ControlAsyncClient s3ControlClient;

    private static S3AccessGrantsCachedCredentialsProvider cache;

    private static ResolveIdentityRequest resolveIdentityRequest;

    private static StsAsyncClient stsAsyncClient;

    private static MetricPublisher metricsPublisher;

    private static MetricCollector metricsCollector;

    private static ConcurrentHashMap<Region, S3ControlAsyncClient> clientsCache;

    private static ClientOverrideConfiguration overrideConfig;

    @Before
    public void setUp() throws Exception {
        s3ControlAsyncClientBuilder = mock(S3ControlAsyncClientBuilder.class);
        s3ControlClient = mock(S3ControlAsyncClient.class);
        cache = mock(S3AccessGrantsCachedCredentialsProvider.class);
        credentialsProvider = mock(AwsCredentialsProvider.class);
        stsAsyncClient = mock(StsAsyncClient.class);
        metricsPublisher = mock(MetricPublisher.class);
        metricsCollector = mock(MetricCollector.class);
        AwsSessionCredentials credentials = AwsSessionCredentials.builder()
                .accessKeyId(TEST_ACCESS_KEY)
                .secretAccessKey(TEST_SECRET_KEY)
                .sessionToken(TEST_SESSION_TOKEN)
                .build();
        CompletableFuture<GetDataAccessResponse> getDataAccessResponse = CompletableFuture.supplyAsync(() ->
                GetDataAccessResponse.builder().credentials(
                        Credentials.builder()
                                .accessKeyId(credentials.accessKeyId())
                                .secretAccessKey(credentials.secretAccessKey())
                                .build())
                        .build());
        CompletableFuture<AwsCredentialsIdentity> cacheResponse  = CompletableFuture.supplyAsync(() -> credentials);
        CompletableFuture<GetCallerIdentityResponse> callerIdentityResponse = CompletableFuture.supplyAsync(() ->
                GetCallerIdentityResponse.builder()
                        .account(TEST_ACCOUNT)
                        .build());
        resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        clientsCache = mock(ConcurrentHashMap.class);
        overrideConfig = mock(ClientOverrideConfiguration.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket/");
        when(resolveIdentityRequest.property(PERMISSION_PROPERTY)).thenReturn(Permission.READ);
        when(resolveIdentityRequest.property(BUCKET_LOCATION_PROPERTY)).thenReturn(Region.US_EAST_2);
        when(s3ControlAsyncClientBuilder.region(Region.US_EAST_2)).thenReturn(s3ControlAsyncClientBuilder);
        when(s3ControlAsyncClientBuilder.region(Region.US_EAST_2).build()).thenReturn(s3ControlClient);
        when(s3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        when(cache.getDataAccess(any(), any(), any(), any(), any())).thenReturn(cacheResponse);
        when(stsAsyncClient.getCallerIdentity()).thenReturn(callerIdentityResponse);
        when(cache.getAccessGrantsMetrics()).thenReturn(metricsCollector);
        when(metricsCollector.collect()).thenReturn(mock(MetricCollection.class));
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> credentials));
        when(clientsCache.get(any(Region.class))).thenReturn(s3ControlClient);
    }


    @AfterClass
    public static void runAfterAllTests() {
        verify(metricsPublisher, never()).publish(any());
    }

    @Test
    public void create_identity_provider_without_default_identity_provider() {
        Assertions.assertThatThrownBy(() -> new S3AccessGrantsIdentityProvider(null, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_identity_type_returns_valid_result() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig);
        Assertions.assertThat(accessGrantsIdentityProvider.identityType()).isEqualTo(AwsCredentialsIdentity.class);
    }

    @Test
    public void call_identity_provider_with_invalid_request() {
        StsAsyncClient localAsyncStsClient = mock(StsAsyncClient.class);
        when(localAsyncStsClient.getCallerIdentity()).thenReturn(CompletableFuture.supplyAsync(() ->
                GetCallerIdentityResponse.builder()
                        .account(TEST_ACCOUNT)
                        .build()));
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, localAsyncStsClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig);
        ResolveIdentityRequest resolveIdentityRequest = null;
        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_identity_provider_with_invalid_sts_client() {
          Assertions.assertThatThrownBy(() -> new S3AccessGrantsIdentityProvider(credentialsProvider, null, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig));
    }

    @Test
    public void call_identity_provider_with_invalid_privilege() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, null, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig);
        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_identity_provider_with_no_metrics_publisher() {
        new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, null, clientsCache, overrideConfig);
    }

    @Test
    public void call_identity_provider_with_no_metrics_client_caching() {
        Assertions.assertThatThrownBy(() -> new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, metricsPublisher, null, overrideConfig)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_identity_provider_get_account_id() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, null, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig);
        CompletableFuture<AwsSessionCredentials> localCredentials = CompletableFuture.supplyAsync(() -> AwsSessionCredentials.builder()
                .accessKeyId(TEST_ACCESS_KEY)
                .secretAccessKey(TEST_SECRET_KEY)
                .sessionToken(TEST_SESSION_TOKEN)
                .build());
        Assertions.assertThat(accessGrantsIdentityProvider.getCallerAccountID(localCredentials)).isEqualTo(TEST_ACCOUNT);
        verify(stsAsyncClient, times(1)).getCallerIdentity();
    }

    @Test
    public void call_identity_provider_get_account_id_cache_test_with_same_credentials() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, null, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig);
        CompletableFuture<AwsSessionCredentials> localCredentials = CompletableFuture.supplyAsync(() -> AwsSessionCredentials.builder()
                .accessKeyId(TEST_ACCESS_KEY)
                .secretAccessKey(TEST_SECRET_KEY)
                .sessionToken(TEST_SESSION_TOKEN)
                .build());
        Assertions.assertThat(accessGrantsIdentityProvider.getCallerAccountID(localCredentials)).isEqualTo(TEST_ACCOUNT);
        verify(stsAsyncClient, times(1)).getCallerIdentity();
        Assertions.assertThat(accessGrantsIdentityProvider.getCallerAccountID(localCredentials)).isEqualTo(TEST_ACCOUNT);
        verify(stsAsyncClient, times(1)).getCallerIdentity();
    }

    @Test
    public void call_identity_provider_get_account_id_cache_test_with_different_credentials() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, null, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig);
        CompletableFuture<AwsSessionCredentials> localCredentials = CompletableFuture.supplyAsync(() -> AwsSessionCredentials.builder()
                .accessKeyId(TEST_ACCESS_KEY)
                .secretAccessKey(TEST_SECRET_KEY)
                .sessionToken(TEST_SESSION_TOKEN)
                .build());
        CompletableFuture<AwsSessionCredentials> localCredentials2 = CompletableFuture.supplyAsync(() -> AwsSessionCredentials.builder()
                .accessKeyId(TEST_ACCESS_KEY)
                .secretAccessKey(TEST_SECRET_KEY)
                .sessionToken(TEST_SESSION_TOKEN+"xx")
                .build());
        Assertions.assertThat(accessGrantsIdentityProvider.getCallerAccountID(localCredentials)).isEqualTo(TEST_ACCOUNT);
        verify(stsAsyncClient, times(1)).getCallerIdentity();
        Assertions.assertThat(accessGrantsIdentityProvider.getCallerAccountID(localCredentials2)).isEqualTo(TEST_ACCOUNT);
        verify(stsAsyncClient, times(2)).getCallerIdentity();
        Assertions.assertThat(accessGrantsIdentityProvider.getCallerAccountID(localCredentials2)).isEqualTo(TEST_ACCOUNT);
        verify(stsAsyncClient, times(2)).getCallerIdentity();
    }

    @Test
    public void call_identity_provider_with_invalid_cache_enabled_setting() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, null, s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig);
        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_should_fallback_with_response_codes_fallback_turned_off() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig);
        Assertions.assertThat(accessGrantsIdentityProvider.shouldFallbackToDefaultCredentialsForThisCase(404, mock(UnsupportedOperationException.class))).isTrue();
        Assertions.assertThat(accessGrantsIdentityProvider.shouldFallbackToDefaultCredentialsForThisCase(404, mock(Exception.class))).isFalse();
        Assertions.assertThat(accessGrantsIdentityProvider.shouldFallbackToDefaultCredentialsForThisCase(403, mock(AccessDeniedException.class))).isFalse();
    }

    @Test
    public void call_should_fallback_with_response_codes_fallback_turned_on() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, !TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig);
        Assertions.assertThat(accessGrantsIdentityProvider.shouldFallbackToDefaultCredentialsForThisCase(404, mock(UnsupportedOperationException.class))).isTrue();
        Assertions.assertThat(accessGrantsIdentityProvider.shouldFallbackToDefaultCredentialsForThisCase(404, mock(Exception.class))).isTrue();
        Assertions.assertThat(accessGrantsIdentityProvider.shouldFallbackToDefaultCredentialsForThisCase(403, mock(AccessDeniedException.class))).isTrue();
    }

    @Test
    public void call_resolve_identity_tries_to_fetch_user_credentials() throws Exception {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClientBuilder localS3ControlClientBuilder = mock(S3ControlAsyncClientBuilder.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider testCache = mock(S3AccessGrantsCachedCredentialsProvider.class);
        MetricPublisher testMetricPublisher = mock(MetricPublisher.class);
        MetricCollector testMetricCollector = mock(MetricCollector.class);
        AwsSessionCredentials credentials = AwsSessionCredentials.builder()
                .accessKeyId(TEST_ACCESS_KEY)
                .secretAccessKey(TEST_SECRET_KEY)
                .sessionToken(TEST_SESSION_TOKEN)
                .build();
        CompletableFuture<AwsCredentialsIdentity> cacheResponse  = CompletableFuture.supplyAsync(() -> credentials);
        when(localS3ControlClientBuilder.region(Region.US_EAST_2)).thenReturn(localS3ControlClientBuilder);
        when(localS3ControlClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(localS3ControlClientBuilder);
        when(localS3ControlClientBuilder.region(Region.US_EAST_2).build()).thenReturn(localS3ControlClient);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, localS3ControlClientBuilder, testCache, TEST_FALLBACK_ENABLED, testMetricPublisher, clientsCache, overrideConfig);
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> credentials));
        when(testCache.getAccessGrantsMetrics()).thenReturn(testMetricCollector);
        when(testMetricCollector.collect()).thenReturn(mock(MetricCollection.class));
        when(testCache.getDataAccess(any(), any(), any(), any(), any())).thenReturn(cacheResponse);

        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest));
        verify(credentialsProvider, times(1)).resolveIdentity(resolveIdentityRequest);
        verify(testMetricPublisher, times(1)).publish(any());
    }

    @Test
    public void call_resolve_identity_with_invalid_request_params_s3Prefix() {

        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig);
        ResolveIdentityRequest localResolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(localResolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn(null);

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(localResolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);

        when(localResolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("S3://test");

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(localResolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_resolve_identity_with_invalid_request_params_operation() {

        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig);
        ResolveIdentityRequest localResolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(localResolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://");

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(localResolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_get_data_access_with_access_denied_response_with_cache() throws Exception {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClientBuilder localS3ControlClientBuilder = mock(S3ControlAsyncClientBuilder.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider testCache = mock(S3AccessGrantsCachedCredentialsProvider.class);
        MetricPublisher testMetricPublisher = mock(MetricPublisher.class);
        MetricCollector testMetricCollector = mock(MetricCollector.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, localS3ControlClientBuilder, testCache, !TEST_FALLBACK_ENABLED, testMetricPublisher, clientsCache, overrideConfig);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(BUCKET_LOCATION_PROPERTY)).thenReturn(Region.US_EAST_2);
        when(resolveIdentityRequest.property(PERMISSION_PROPERTY)).thenReturn(Permission.READ);
        when(localS3ControlClientBuilder.region(Region.US_EAST_2)).thenReturn(localS3ControlClientBuilder);
        when(localS3ControlClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(localS3ControlClientBuilder);
        when(localS3ControlClientBuilder.region(Region.US_EAST_2).build()).thenReturn(localS3ControlClient);
        when(testCache.getDataAccess(any(), any(), any(), any(), any())).thenThrow(S3ControlException.builder().statusCode(403).message("Access denied for the user").build());
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build()));
        when(testCache.getAccessGrantsMetrics()).thenReturn(testMetricCollector);
        when(testMetricCollector.collect()).thenReturn(mock(MetricCollection.class));

        AwsCredentialsIdentity credentialsIdentity = accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join();
        Assertions.assertThat(credentialsIdentity.accessKeyId()).isEqualTo(TEST_ACCESS_KEY);
        Assertions.assertThat(credentialsIdentity.secretAccessKey()).isEqualTo(TEST_SECRET_KEY);
        verify(testMetricPublisher, times(1)).publish(any());
    }

    @Test
    public void call_get_data_access_with_exceptions_from_cache() throws Exception {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClientBuilder localS3ControlClientBuilder = mock(S3ControlAsyncClientBuilder.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider testCache = mock(S3AccessGrantsCachedCredentialsProvider.class);
        MetricPublisher testMetricPublisher = mock(MetricPublisher.class);
        MetricCollector testMetricCollector = mock(MetricCollector.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, localS3ControlClientBuilder, testCache, !TEST_FALLBACK_ENABLED, testMetricPublisher, clientsCache, overrideConfig);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(BUCKET_LOCATION_PROPERTY)).thenReturn(Region.US_EAST_2);
        when(resolveIdentityRequest.property(PERMISSION_PROPERTY)).thenReturn(Permission.READ);
        when(localS3ControlClientBuilder.region(Region.US_EAST_2)).thenReturn(localS3ControlClientBuilder);
        when(localS3ControlClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(localS3ControlClientBuilder);
        when(localS3ControlClientBuilder.region(Region.US_EAST_2).build()).thenReturn(localS3ControlClient);
        when(testCache.getDataAccess(any(), any(), any(), any(), any())).thenThrow(NullPointerException.class);
        when(localS3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> GetDataAccessResponse.builder().build()).whenComplete((r,e) -> { throw S3ControlException.builder().statusCode(403).build(); }));
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build()));
        when(testCache.getAccessGrantsMetrics()).thenReturn(testMetricCollector);
        when(testMetricCollector.collect()).thenReturn(mock(MetricCollection.class));

        AwsCredentialsIdentity credentialsIdentity = accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join();

        Assertions.assertThat(credentialsIdentity.accessKeyId()).isEqualTo(TEST_ACCESS_KEY);
        Assertions.assertThat(credentialsIdentity.secretAccessKey()).isEqualTo(TEST_SECRET_KEY);
        verify(testMetricPublisher, times(1)).publish(any());
    }


    @Test
    public void call_access_grants_identity_provider_with_cache_enabled_request_success() throws Exception {

        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClientBuilder localS3ControlClientBuilder = mock(S3ControlAsyncClientBuilder.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider testCache = mock(S3AccessGrantsCachedCredentialsProvider.class);
        MetricPublisher testMetricPublisher = mock(MetricPublisher.class);
        MetricCollector testMetricCollector = mock(MetricCollector.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, localS3ControlClientBuilder, testCache, TEST_FALLBACK_ENABLED, testMetricPublisher, clientsCache, overrideConfig);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        AwsCredentialsIdentity credentials = AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build();
        CompletableFuture<AwsCredentialsIdentity> cacheResponse = CompletableFuture.supplyAsync(() -> credentials);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(BUCKET_LOCATION_PROPERTY)).thenReturn(Region.US_EAST_2);
        when(resolveIdentityRequest.property(PERMISSION_PROPERTY)).thenReturn(Permission.READ);
        when(localS3ControlClientBuilder.region(Region.US_EAST_2)).thenReturn(localS3ControlClientBuilder);
        when(localS3ControlClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(localS3ControlClientBuilder);
        when(localS3ControlClientBuilder.region(Region.US_EAST_2).build()).thenReturn(localS3ControlClient);
        when(testCache.getDataAccess(any(), any(), any(), any(), any())).thenReturn(cacheResponse);
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            return credentials;
        }));
        when(testCache.getAccessGrantsMetrics()).thenReturn(testMetricCollector);
        when(testMetricCollector.collect()).thenReturn(mock(MetricCollection.class));

        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join());
        verify(testCache, times(1)).getDataAccess(any(), any(), any(), any(), any());
        verify(testMetricPublisher, times(1)).publish(any());
    }

    @Test
    public void call_access_grants_identity_provider_with_cache_enabled_request_failure_returns_user_credentials() {

        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClientBuilder localS3ControlClientBuilder = mock(S3ControlAsyncClientBuilder.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider testCache = S3AccessGrantsCachedCredentialsProviderImpl
                .builder()
                .build();
        MetricPublisher testMetricPublisher = mock(MetricPublisher.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, localS3ControlClientBuilder, testCache, !TEST_FALLBACK_ENABLED, testMetricPublisher, clientsCache, overrideConfig);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        AwsCredentialsIdentity credentials = AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build();
        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse>  getAccessGrantsInstanceForPrefixResponse = CompletableFuture.supplyAsync(() -> GetAccessGrantsInstanceForPrefixResponse.builder()
                .accessGrantsInstanceArn("arn:aws:s3:us-east-2:123456678:access-grants/default")
                .accessGrantsInstanceId("default").build());

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(BUCKET_LOCATION_PROPERTY)).thenReturn(Region.US_EAST_2);
        when(resolveIdentityRequest.property(PERMISSION_PROPERTY)).thenReturn(Permission.READ);
        when(localS3ControlClientBuilder.region(Region.US_EAST_2)).thenReturn(localS3ControlClientBuilder);
        when(localS3ControlClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(localS3ControlClientBuilder);
        when(localS3ControlClientBuilder.region(Region.US_EAST_2).build()).thenReturn(localS3ControlClient);
        when(localS3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenThrow(InvalidRequestException.class);
        when(localS3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(getAccessGrantsInstanceForPrefixResponse);
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> credentials));

        AwsCredentialsIdentity credentialsIdentity = accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join();
        Assertions.assertThat(credentialsIdentity.accessKeyId()).isEqualTo(credentials.accessKeyId());
        Assertions.assertThat(credentialsIdentity.secretAccessKey()).isEqualTo(credentials.secretAccessKey());
        verify(testMetricPublisher, times(1)).publish(any());
    }

    @Test
    public void call_access_grants_identity_provider_with_cache_enabled_request_failure_does_not_returns_user_credentials() {

        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClientBuilder localS3ControlClientBuilder = mock(S3ControlAsyncClientBuilder.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider testCache = S3AccessGrantsCachedCredentialsProviderImpl
                .builder()
                .build();
        MetricPublisher testMetricPublisher = mock(MetricPublisher.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, localS3ControlClientBuilder, testCache, TEST_FALLBACK_ENABLED, testMetricPublisher, clientsCache, overrideConfig);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        AwsCredentialsIdentity credentials = AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build();
        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse>  getAccessGrantsInstanceForPrefixResponse = CompletableFuture.supplyAsync(() -> GetAccessGrantsInstanceForPrefixResponse.builder()
                .accessGrantsInstanceArn("arn:aws:s3:us-east-2:123456678:access-grants/default")
                .accessGrantsInstanceId("default").build());

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(BUCKET_LOCATION_PROPERTY)).thenReturn(Region.US_EAST_2);
        when(resolveIdentityRequest.property(PERMISSION_PROPERTY)).thenReturn(Permission.READ);
        when(localS3ControlClientBuilder.region(Region.US_EAST_2)).thenReturn(localS3ControlClientBuilder);
        when(localS3ControlClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(localS3ControlClientBuilder);
        when(localS3ControlClientBuilder.region(Region.US_EAST_2).build()).thenReturn(localS3ControlClient);
        when(localS3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> GetDataAccessResponse.builder().build()).whenComplete((r,e) -> { throw InvalidRequestException.builder().statusCode(400).build(); }));
        when(localS3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(getAccessGrantsInstanceForPrefixResponse);
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> credentials));

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join()).isInstanceOf(CompletionException.class).getCause().isInstanceOf(SdkServiceException.class).getCause().isInstanceOf(InvalidRequestException.class);
        verify(testMetricPublisher, times(1)).publish(any());
    }

    @Test
    public void call_access_grants_identity_provider_with_unsupported_operation() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, !TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        AwsCredentialsIdentity credentials = AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build();

        when(resolveIdentityRequest.property(AUTH_EXCEPTIONS_PROPERTY)).thenReturn(SdkServiceException.builder().statusCode(404).cause(new UnsupportedOperationException()).build());
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> credentials));

        AwsCredentialsIdentity awsCredentialsIdentity = accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join();
        Assertions.assertThat(awsCredentialsIdentity.secretAccessKey()).isEqualTo(TEST_SECRET_KEY);
        Assertions.assertThat(awsCredentialsIdentity.accessKeyId()).isEqualTo(TEST_ACCESS_KEY);
    }

    @Test
    public void call_access_grants_identity_provider_with_unsupported_operation_on_disabled_fallback() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        AwsCredentialsIdentity credentials = AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build();

        when(resolveIdentityRequest.property(AUTH_EXCEPTIONS_PROPERTY)).thenReturn(SdkServiceException.builder().statusCode(404).cause(new UnsupportedOperationException()).build());
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> credentials));

        AwsCredentialsIdentity awsCredentialsIdentity = accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join();
        Assertions.assertThat(awsCredentialsIdentity.secretAccessKey()).isEqualTo(TEST_SECRET_KEY);
        Assertions.assertThat(awsCredentialsIdentity.accessKeyId()).isEqualTo(TEST_ACCESS_KEY);
    }

    @Test
    public void call_access_grants_identity_provider_clients_cache_test() throws Exception {

        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClientBuilder localS3ControlClientBuilder = mock(S3ControlAsyncClientBuilder.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider testCache = mock(S3AccessGrantsCachedCredentialsProvider.class);
        MetricPublisher testMetricPublisher = mock(MetricPublisher.class);
        MetricCollector testMetricCollector = mock(MetricCollector.class);
        ConcurrentHashMap<Region, S3ControlAsyncClient> mockClientsCache = spy(new ConcurrentHashMap<>());
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, localS3ControlClientBuilder, testCache, TEST_FALLBACK_ENABLED, testMetricPublisher, mockClientsCache, overrideConfig);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        AwsCredentialsIdentity credentials = AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build();
        CompletableFuture<AwsCredentialsIdentity> cacheResponse = CompletableFuture.supplyAsync(() -> credentials);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(BUCKET_LOCATION_PROPERTY)).thenReturn(Region.US_EAST_2);
        when(resolveIdentityRequest.property(PERMISSION_PROPERTY)).thenReturn(Permission.READ);
        when(localS3ControlClientBuilder.region(Region.US_EAST_2)).thenReturn(localS3ControlClientBuilder);
        when(localS3ControlClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(localS3ControlClientBuilder);
        when(localS3ControlClientBuilder.region(Region.US_EAST_2).build()).thenReturn(localS3ControlClient);
        when(testCache.getDataAccess(any(), any(), any(), any(), any())).thenReturn(cacheResponse);
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            return credentials;
        }));
        when(testCache.getAccessGrantsMetrics()).thenReturn(testMetricCollector);
        when(testMetricCollector.collect()).thenReturn(mock(MetricCollection.class));

        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join());

        verify(mockClientsCache, times(1)).containsKey(any());
        verify(mockClientsCache, times(1)).get(any()); // Should not invoke get but containsKey invokes get internally
        verify(mockClientsCache, times(1)).put(any(), any()); // Should have called containsKey but no client in the hashMap for the region, so PUT should be invoked

        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join());
        verify(mockClientsCache, times(2)).containsKey(any());
        verify(mockClientsCache, times(3)).get(any()); // verifying if the
        verify(mockClientsCache, times(1)).put(any(), any()); // Client already in the cache, so no PUT calls are expected for the second request.
    }

    @Test
    public void getCallerAccountID_concurrentAccess_shouldMakeOnlyOneSTSCall() throws InterruptedException {
        // Given
        AwsCredentialsIdentity testCredentials = AwsBasicCredentials.create("accessKey", "secretKey");
        String expectedAccountId = "123456789012";
        
        StsAsyncClient localStsClient = mock(StsAsyncClient.class);
        when(localStsClient.getCallerIdentity()).thenReturn(
            CompletableFuture.completedFuture(GetCallerIdentityResponse.builder().account(expectedAccountId).build())
        );
        
        S3AccessGrantsIdentityProvider testProvider = new S3AccessGrantsIdentityProvider(
            credentialsProvider, localStsClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, 
            s3ControlAsyncClientBuilder, cache, TEST_FALLBACK_ENABLED, metricsPublisher, clientsCache, overrideConfig
        );
        
        // When - simulate 5 concurrent threads with same credentials
        int threadCount = 5;
        CompletableFuture<String>[] futures = new CompletableFuture[threadCount];
        
        for (int i = 0; i < threadCount; i++) {
            futures[i] = CompletableFuture.supplyAsync(() -> 
                testProvider.getCallerAccountID(CompletableFuture.completedFuture(testCredentials))
            );
        }
        
        // Wait for all to complete
        CompletableFuture.allOf(futures).join();
        
        // Then - verify only one STS call was made
        verify(localStsClient, times(1)).getCallerIdentity();
        
        // Verify all threads got the same account ID
        for (int i = 0; i < threadCount; i++) {
            assertThat(futures[i].join()).isEqualTo(expectedAccountId);
        }
    }
}