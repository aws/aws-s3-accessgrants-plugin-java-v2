package software.amazon.awssdk.s3accessgrants.plugin;

import java.util.concurrent.CompletableFuture;
import java.util.Optional;
import java.util.concurrent.CompletionException;

import org.junit.Test;
import org.assertj.core.api.Assertions;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCache;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProvider;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProviderImpl;
import software.amazon.awssdk.services.s3control.model.*;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;

import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.OPERATION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;

import org.junit.BeforeClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class S3AccessGrantsIdentityProviderTests {

    private final Region TEST_REGION = Region.US_EAST_2;
    private final String TEST_ACCOUNT = "12345028312";

    private final String TEST_OPERATION = "GetObject";

    private static final String TEST_ACCESS_KEY = "ARAGXXXXXXX123";

    private static final String TEST_SECRET_KEY = "ARAGXXXXXXX123112e2e3aadadwefdscac";

    private static final String TEST_SESSION_TOKEN = "ARAGXXXXXXX123112e2e3aadadwefdscacadascaacacacasc";

    private final Optional<Privilege> TEST_PRIVILEGE = Optional.of(Privilege.DEFAULT);

    private final Optional<Boolean> TEST_CACHE_ENABLED = Optional.of(true);

    private final Optional<Boolean> TEST_CACHE_DISABLED = Optional.of(false);

    private static DefaultCredentialsProvider credentialsProvider;

    private static S3ControlAsyncClient s3ControlClient;

    private static S3AccessGrantsCachedCredentialsProvider cache;

    private static ResolveIdentityRequest resolveIdentityRequest;

    @BeforeClass
    public static void setUp() throws Exception{
        s3ControlClient = mock(S3ControlAsyncClient.class);
        cache = mock(S3AccessGrantsCachedCredentialsProvider.class);
        credentialsProvider = DefaultCredentialsProvider.create();
        AwsSessionCredentials credentials = AwsSessionCredentials.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).sessionToken(TEST_SESSION_TOKEN).build();
        CompletableFuture<GetDataAccessResponse> getDataAccessResponse = CompletableFuture.supplyAsync(() ->
                GetDataAccessResponse.builder().credentials(
                        Credentials.builder()
                                .accessKeyId(credentials.accessKeyId())
                                .secretAccessKey(credentials.secretAccessKey())
                                .build())
                        .build());
        resolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket/");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn("GetObject");
        when(s3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        when(cache.getDataAccess(any(), any(), any(), any())).thenReturn(credentials);
    }

    @Test
    public void create_identity_provider_without_default_identity_provider() {
        Assertions.assertThatThrownBy(() -> new S3AccessGrantsIdentityProvider(null, TEST_REGION, TEST_ACCOUNT, TEST_PRIVILEGE, TEST_CACHE_DISABLED, s3ControlClient, cache)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void create_identity_provider_without_valid_region() {
        Assertions.assertThatThrownBy(() -> new S3AccessGrantsIdentityProvider(credentialsProvider, null, TEST_ACCOUNT, TEST_PRIVILEGE, TEST_CACHE_DISABLED, s3ControlClient, cache)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_identity_type_returns_valid_result() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, TEST_PRIVILEGE, TEST_CACHE_DISABLED, s3ControlClient, cache);
        Assertions.assertThat(accessGrantsIdentityProvider.identityType()).isEqualTo(AwsCredentialsIdentity.class);
    }

    @Test
    public void call_identity_provider_with_invalid_request() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, TEST_PRIVILEGE, TEST_CACHE_DISABLED, s3ControlClient, cache);
        ResolveIdentityRequest resolveIdentityRequest = null;
        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_get_account() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, TEST_PRIVILEGE, TEST_CACHE_DISABLED, s3ControlClient, cache);
        Assertions.assertThat(accessGrantsIdentityProvider.getAccountId()).isEqualTo(TEST_ACCOUNT);
    }

    // TODO : test will fail until we decide how to determine the account id from the request
    @Test
    public void call_get_account_with_invalid_account() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, null, TEST_PRIVILEGE, TEST_CACHE_ENABLED, s3ControlClient, cache);
        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest));
    }

    @Test
    public void call_get_privilege() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, TEST_PRIVILEGE, TEST_CACHE_DISABLED, s3ControlClient, cache);
        Assertions.assertThat(accessGrantsIdentityProvider.getPrivilege(TEST_PRIVILEGE)).isEqualTo(TEST_PRIVILEGE.get());
    }

    @Test
    public void call_get_privilege_with_invalid_privilege() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, Optional.ofNullable(null), TEST_CACHE_DISABLED, s3ControlClient, cache);
        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest));
    }

    @Test
    public void call_get_is_cache_enabled() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, TEST_PRIVILEGE, TEST_CACHE_DISABLED, s3ControlClient, cache);
        Assertions.assertThat(accessGrantsIdentityProvider.getIsCacheEnabled(TEST_CACHE_DISABLED)).isEqualTo(TEST_CACHE_DISABLED.get());
    }

    @Test
    public void call_get_is_cache_enabled_with_invalid_cache_enabled_setting() {
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, TEST_PRIVILEGE, Optional.ofNullable(null), s3ControlClient, cache);
        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest));
    }

    @Test
    public void call_resolve_identity_fetches_default_credentials() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, TEST_PRIVILEGE, TEST_CACHE_DISABLED, s3ControlClient, cache);
        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest));
        verify(credentialsProvider, times(1)).resolveIdentity(resolveIdentityRequest);
    }

    @Test
    public void call_resolve_identity_with_invalid_request_params_s3Prefix() {

        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, TEST_PRIVILEGE, TEST_CACHE_DISABLED, s3ControlClient, cache);
        ResolveIdentityRequest localResolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(localResolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn(null);

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(localResolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);

        when(localResolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("S3://");

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(localResolveIdentityRequest)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void call_resolve_identity_with_invalid_request_params_operation() {

        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, TEST_PRIVILEGE, TEST_CACHE_DISABLED, s3ControlClient, cache);
        ResolveIdentityRequest localResolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(localResolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://");
        when(localResolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(null);

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(localResolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_get_data_access_with_invalid_response_without_cache() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, TEST_PRIVILEGE, TEST_CACHE_DISABLED, localS3ControlClient, cache);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        CompletableFuture<GetDataAccessResponse> getDataAccessResponse = CompletableFuture.supplyAsync(() -> GetDataAccessResponse.builder().build());

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(TEST_OPERATION);
        when(localS3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            return AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build();
        }));

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join())
                .isInstanceOf(java.util.concurrent.CompletionException.class).getCause()
                .isInstanceOf(S3ControlException.class).getCause()
                .isInstanceOf(NullPointerException.class);

    }

    @Test
    public void call_access_grants_identity_provider_with_cache_enabed_request_success() throws Exception {

        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider testCache = mock(S3AccessGrantsCachedCredentialsProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, TEST_PRIVILEGE, TEST_CACHE_ENABLED, localS3ControlClient, testCache);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        AwsCredentialsIdentity credentials = AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build();

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(TEST_OPERATION);
        when(testCache.getDataAccess(any(), any(), any(), any())).thenReturn(credentials);
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            return credentials;
        }));

        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join());
        verify(testCache, times(1)).getDataAccess(any(), any(), any(), any());
    }

    // TODO: The below test will fail until the async control client is supported in cache
    @Test
    public void call_access_grants_identity_provider_with_cache_enabed_request_failure() throws Exception {

        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider testCache = S3AccessGrantsCachedCredentialsProviderImpl
                .builder()
                .S3ControlAsyncClient(localS3ControlClient)
                .build();
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, TEST_PRIVILEGE, TEST_CACHE_ENABLED, localS3ControlClient, testCache);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        AwsCredentialsIdentity credentials = AwsCredentialsIdentity.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).build();

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(TEST_OPERATION);
        when(localS3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenThrow(InvalidRequestException.class);
        when(credentialsProvider.resolveIdentity(any(ResolveIdentityRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            return credentials;
        }));

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join())
                .isInstanceOf(CompletionException.class)
                        .getCause()
                                .isInstanceOf(S3ControlException.class);
    }

}