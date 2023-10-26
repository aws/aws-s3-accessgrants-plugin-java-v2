package software.amazon.awssdk.s3accessgrants.plugin;

import java.nio.file.AccessDeniedException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.Test;
import org.assertj.core.api.Assertions;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProvider;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProviderImpl;
import software.amazon.awssdk.services.s3control.model.Privilege;
import software.amazon.awssdk.services.s3control.model.GetDataAccessResponse;
import software.amazon.awssdk.services.s3control.model.GetDataAccessRequest;
import software.amazon.awssdk.services.s3control.model.Credentials;
import software.amazon.awssdk.services.s3control.model.S3ControlException;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixResponse;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixRequest;
import software.amazon.awssdk.services.s3control.model.InvalidRequestException;


import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;

import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.OPERATION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;

import org.junit.BeforeClass;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;

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
    private static DefaultCredentialsProvider credentialsProvider;

    private static S3ControlAsyncClient s3ControlClient;

    private static S3AccessGrantsCachedCredentialsProvider cache;

    private static ResolveIdentityRequest resolveIdentityRequest;

    private static StsAsyncClient stsAsyncClient;

    @BeforeClass
    public static void setUp() throws Exception {
        s3ControlClient = mock(S3ControlAsyncClient.class);
        cache = mock(S3AccessGrantsCachedCredentialsProvider.class);
        credentialsProvider = DefaultCredentialsProvider.create();
        stsAsyncClient = mock(StsAsyncClient.class);
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

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket/");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn("GetObject");
        when(s3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        when(cache.getDataAccess(any(), any(), any(), any())).thenReturn(cacheResponse);
        when(stsAsyncClient.getCallerIdentity()).thenReturn(callerIdentityResponse);
    }

    @Test
    public void create_identity_provider_without_default_identity_provider() {
        Assertions.assertThatThrownBy(() -> new S3AccessGrantsIdentityProvider(null, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlClient, cache, TEST_FALLBACK_ENABLED)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void create_identity_provider_without_valid_region() {
        Assertions.assertThatThrownBy(() -> new S3AccessGrantsIdentityProvider(credentialsProvider, null, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlClient, cache, TEST_FALLBACK_ENABLED)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_identity_type_returns_valid_result() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlClient, cache, TEST_FALLBACK_ENABLED);
        Assertions.assertThat(accessGrantsIdentityProvider.identityType()).isEqualTo(AwsCredentialsIdentity.class);
    }

    @Test
    public void call_identity_provider_with_invalid_request() {
        StsAsyncClient localAsyncStsClient = mock(StsAsyncClient.class);
        when(localAsyncStsClient.getCallerIdentity()).thenReturn(CompletableFuture.supplyAsync(() ->
                GetCallerIdentityResponse.builder()
                        .account(TEST_ACCOUNT)
                        .build()));
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, localAsyncStsClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlClient, cache, TEST_FALLBACK_ENABLED);
        ResolveIdentityRequest resolveIdentityRequest = null;
        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);
        verify(localAsyncStsClient, atLeast(1)).getCallerIdentity();
    }

    @Test
    public void call_identity_provider_with_invalid_sts_client() {
          Assertions.assertThatThrownBy(() -> new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, null, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlClient, cache, TEST_FALLBACK_ENABLED));
    }

    @Test
    public void call_identity_provider_with_invalid_privilege() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, null, TEST_CACHE_ENABLED, s3ControlClient, cache, TEST_FALLBACK_ENABLED);
        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_identity_provider_get_account_id() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, null, TEST_CACHE_ENABLED, s3ControlClient, cache, TEST_FALLBACK_ENABLED);
        Assertions.assertThat(accessGrantsIdentityProvider.getCallerAccountID().join().account()).isEqualTo(TEST_ACCOUNT);
    }

    @Test
    public void call_identity_provider_with_invalid_cache_enabled_setting() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, null, s3ControlClient, cache, TEST_FALLBACK_ENABLED);
        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_should_fallback_with_response_codes_fallback_turned_off() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlClient, cache, TEST_FALLBACK_ENABLED);
        Assertions.assertThat(accessGrantsIdentityProvider.shouldFallbackToDefaultCredentialsForThisCase(404, mock(UnsupportedOperationException.class))).isTrue();
        Assertions.assertThat(accessGrantsIdentityProvider.shouldFallbackToDefaultCredentialsForThisCase(404, mock(Exception.class))).isFalse();
        Assertions.assertThat(accessGrantsIdentityProvider.shouldFallbackToDefaultCredentialsForThisCase(403, mock(AccessDeniedException.class))).isFalse();
    }

    @Test
    public void call_should_fallback_with_response_codes_fallback_turned_on() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlClient, cache, !TEST_FALLBACK_ENABLED);
        Assertions.assertThat(accessGrantsIdentityProvider.shouldFallbackToDefaultCredentialsForThisCase(404, mock(UnsupportedOperationException.class))).isTrue();
        Assertions.assertThat(accessGrantsIdentityProvider.shouldFallbackToDefaultCredentialsForThisCase(404, mock(Exception.class))).isTrue();
        Assertions.assertThat(accessGrantsIdentityProvider.shouldFallbackToDefaultCredentialsForThisCase(403, mock(AccessDeniedException.class))).isTrue();
    }

    @Test
    public void call_resolve_identity_tries_to_fetch_user_credentials() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlClient, cache, TEST_FALLBACK_ENABLED);

        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> null));

        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest));
        verify(credentialsProvider, times(1)).resolveIdentity(resolveIdentityRequest);
    }

    @Test
    public void call_resolve_identity_with_invalid_request_params_s3Prefix() {

        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlClient, cache, TEST_FALLBACK_ENABLED);
        ResolveIdentityRequest localResolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(localResolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn(null);

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(localResolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);

        when(localResolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("S3://test");

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(localResolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_resolve_identity_with_invalid_request_params_operation() {

        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlClient, cache, TEST_FALLBACK_ENABLED);
        ResolveIdentityRequest localResolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(localResolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://");
        when(localResolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(null);

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(localResolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_get_data_access_with_invalid_response_without_cache() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_DISABLED, localS3ControlClient, cache, TEST_FALLBACK_ENABLED);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        CompletableFuture<GetDataAccessResponse> getDataAccessResponse = CompletableFuture.supplyAsync(() -> GetDataAccessResponse.builder().build());

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(TEST_OPERATION);
        when(localS3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build()));

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join())
                .isInstanceOf(CompletionException.class).getCause()
                .isInstanceOf(NullPointerException.class);

    }

    @Test
    public void call_get_data_access_with_access_denied_response_without_cache() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_DISABLED, localS3ControlClient, cache, TEST_FALLBACK_ENABLED);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(TEST_OPERATION);
        when(localS3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> GetDataAccessResponse.builder().build()).whenComplete((r,e) -> { throw S3ControlException.builder().statusCode(403).build(); }));
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build()));

        Throwable exc = Assertions.catchThrowableOfType(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join(), CompletionException.class);
        Assertions.assertThat(exc.getCause()).isInstanceOf(S3ControlException.class);
        Assertions.assertThat(((S3ControlException) exc.getCause()).statusCode()).isEqualTo(403);
    }

    @Test
    public void call_get_data_access_with_access_denied_response_with_cache() throws Exception {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider testCache = mock(S3AccessGrantsCachedCredentialsProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, localS3ControlClient, testCache, !TEST_FALLBACK_ENABLED);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(TEST_OPERATION);
        when(testCache.getDataAccess(any(), any(), any(), any())).thenThrow(S3ControlException.builder().statusCode(403).message("Access denied for the user").build());
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build()));

        AwsCredentialsIdentity credentialsIdentity = accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join();
        Assertions.assertThat(credentialsIdentity.accessKeyId()).isEqualTo(TEST_ACCESS_KEY);
        Assertions.assertThat(credentialsIdentity.secretAccessKey()).isEqualTo(TEST_SECRET_KEY);
    }

    @Test
    public void call_get_data_access_with_exceptions_from_cache() throws Exception {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider testCache = mock(S3AccessGrantsCachedCredentialsProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, localS3ControlClient, testCache, !TEST_FALLBACK_ENABLED);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(TEST_OPERATION);
        when(testCache.getDataAccess(any(), any(), any(), any())).thenThrow(NullPointerException.class);
        when(localS3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> GetDataAccessResponse.builder().build()).whenComplete((r,e) -> { throw S3ControlException.builder().statusCode(403).build(); }));
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build()));

        AwsCredentialsIdentity credentialsIdentity = accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join();

        Assertions.assertThat(credentialsIdentity.accessKeyId()).isEqualTo(TEST_ACCESS_KEY);
        Assertions.assertThat(credentialsIdentity.secretAccessKey()).isEqualTo(TEST_SECRET_KEY);
    }

    @Test
    public void call_get_data_access_with_success_response_without_cache() throws Exception {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider testCache = mock(S3AccessGrantsCachedCredentialsProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_DISABLED, localS3ControlClient, testCache, TEST_FALLBACK_ENABLED);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        CompletableFuture<GetDataAccessResponse> getDataAccessResponse = CompletableFuture.supplyAsync(() -> GetDataAccessResponse.builder()
                .credentials(Credentials.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).sessionToken(TEST_SESSION_TOKEN).build()).build());

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(TEST_OPERATION);
        when(localS3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build()));
        when(testCache.getDataAccess(any(), any(), any(), any())).thenReturn(null);

        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join());

        verify(testCache, never()).getDataAccess(any(), any(), any(), any());

    }

    @Test
    public void call_access_grants_identity_provider_with_cache_enabled_request_success() throws Exception {

        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider testCache = mock(S3AccessGrantsCachedCredentialsProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, localS3ControlClient, testCache, TEST_FALLBACK_ENABLED);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        AwsCredentialsIdentity credentials = AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build();
        CompletableFuture<AwsCredentialsIdentity> cacheResponse = CompletableFuture.supplyAsync(() -> credentials);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(TEST_OPERATION);
        when(testCache.getDataAccess(any(), any(), any(), any())).thenReturn(cacheResponse);
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            return credentials;
        }));

        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join());
        verify(testCache, times(1)).getDataAccess(any(), any(), any(), any());
    }

    @Test
    public void call_access_grants_identity_provider_with_cache_enabled_request_failure_returns_user_credentials() {

        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider testCache = S3AccessGrantsCachedCredentialsProviderImpl
                .builder()
                .S3ControlAsyncClient(localS3ControlClient)
                .build();
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, localS3ControlClient, testCache, !TEST_FALLBACK_ENABLED);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        AwsCredentialsIdentity credentials = AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build();
        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse>  getAccessGrantsInstanceForPrefixResponse = CompletableFuture.supplyAsync(() -> GetAccessGrantsInstanceForPrefixResponse.builder()
                .accessGrantsInstanceArn("arn:aws:s3:us-east-2:123456678:access-grants/default")
                .accessGrantsInstanceId("default").build());

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(TEST_OPERATION);
        when(localS3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenThrow(InvalidRequestException.class);
        when(localS3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(getAccessGrantsInstanceForPrefixResponse);
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> credentials));

        AwsCredentialsIdentity credentialsIdentity = accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join();
        // invalid request should still return the default user credentials
        Assertions.assertThat(credentialsIdentity.accessKeyId()).isEqualTo(credentials.accessKeyId());
        Assertions.assertThat(credentialsIdentity.secretAccessKey()).isEqualTo(credentials.secretAccessKey());
    }

    @Test
    public void call_access_grants_identity_provider_with_cache_enabled_request_failure_does_not_returns_user_credentials() {

        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider testCache = S3AccessGrantsCachedCredentialsProviderImpl
                .builder()
                .S3ControlAsyncClient(localS3ControlClient)
                .build();
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, localS3ControlClient, testCache, TEST_FALLBACK_ENABLED);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        AwsCredentialsIdentity credentials = AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build();
        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse>  getAccessGrantsInstanceForPrefixResponse = CompletableFuture.supplyAsync(() -> GetAccessGrantsInstanceForPrefixResponse.builder()
                .accessGrantsInstanceArn("arn:aws:s3:us-east-2:123456678:access-grants/default")
                .accessGrantsInstanceId("default").build());

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(TEST_OPERATION);
        when(localS3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> GetDataAccessResponse.builder().build()).whenComplete((r,e) -> { throw InvalidRequestException.builder().statusCode(400).build(); }));
        when(localS3ControlClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(getAccessGrantsInstanceForPrefixResponse);
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> credentials));

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join()).isInstanceOf(CompletionException.class).getCause().isInstanceOf(SdkServiceException.class).getCause().isInstanceOf(InvalidRequestException.class);

    }

    @Test
    public void call_access_grants_identity_provider_with_unsupported_operation() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlClient, cache, !TEST_FALLBACK_ENABLED);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        AwsCredentialsIdentity credentials = AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build();

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn("CreateBucket");
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> credentials));

        AwsCredentialsIdentity awsCredentialsIdentity = accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join();
        Assertions.assertThat(awsCredentialsIdentity.secretAccessKey()).isEqualTo(TEST_SECRET_KEY);
        Assertions.assertThat(awsCredentialsIdentity.accessKeyId()).isEqualTo(TEST_ACCESS_KEY);
    }

    @Test
    public void call_access_grants_identity_provider_with_unsupported_operation_on_disabled_fallback() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, stsAsyncClient, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlClient, cache, TEST_FALLBACK_ENABLED);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        AwsCredentialsIdentity credentials = AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build();

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn("CreateBucket");
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> credentials));

        AwsCredentialsIdentity awsCredentialsIdentity = accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join();
        Assertions.assertThat(awsCredentialsIdentity.secretAccessKey()).isEqualTo(TEST_SECRET_KEY);
        Assertions.assertThat(awsCredentialsIdentity.accessKeyId()).isEqualTo(TEST_ACCESS_KEY);
    }
}