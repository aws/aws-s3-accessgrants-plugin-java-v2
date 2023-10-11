package software.amazon.awssdk.s3accessgrants.plugin;

import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.assertj.core.api.Assertions;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3control.model.Credentials;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.services.s3control.model.S3ControlException;
import software.amazon.awssdk.services.s3control.model.GetDataAccessRequest;
import software.amazon.awssdk.services.s3control.model.GetDataAccessResponse;
import software.amazon.awssdk.services.s3control.model.InvalidRequestException;


import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.OPERATION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;

import org.junit.BeforeClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;

public class S3AccessGrantsIdentityProviderTests {

    private final Region TEST_REGION = Region.US_EAST_2;
    private final String TEST_ACCOUNT = "12345028312";

    private final String TEST_OPERATION = "GetObject";

    private static final String TEST_ACCESS_KEY = "ARAGXXXXXXX123";

    private static final String TEST_SECRET_KEY = "ARAGXXXXXXX123112e2e3aadadwefdscac";

    private static final String TEST_SESSION_TOKEN = "ARAGXXXXXXX123112e2e3aadadwefdscacadascaacacacasc";

    private static S3ControlAsyncClient s3ControlClient;

    @BeforeClass
    public static void setUp() {
        s3ControlClient = mock(S3ControlAsyncClient.class);
        Credentials credentials = Credentials.builder().accessKeyId(TEST_ACCESS_KEY).secretAccessKey(TEST_SECRET_KEY).sessionToken(TEST_SESSION_TOKEN).build();
        CompletableFuture<GetDataAccessResponse> getDataAccessResponse = CompletableFuture.supplyAsync(() -> GetDataAccessResponse.builder().credentials(credentials).build());

        when(s3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
    }

    @Test
    public void create_identity_provider_without_default_identity_provider() {
        Assertions.assertThatThrownBy(() -> new S3AccessGrantsIdentityProvider(null, TEST_REGION, TEST_ACCOUNT, s3ControlClient)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void create_identity_provider_without_valid_region() {
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        Assertions.assertThatThrownBy(() -> new S3AccessGrantsIdentityProvider(credentialsProvider, null, TEST_ACCOUNT, s3ControlClient)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_identity_type_returns_valid_result() {
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, s3ControlClient);
        Assertions.assertThat(accessGrantsIdentityProvider.identityType()).isEqualTo(AwsCredentialsIdentity.class);
    }

    @Test
    public void call_identity_provider_with_invalid_request() {
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, s3ControlClient);
        ResolveIdentityRequest resolveIdentityRequest = null;
        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_get_account() {
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, s3ControlClient);
        Assertions.assertThat(accessGrantsIdentityProvider.getAccountId()).isEqualTo(TEST_ACCOUNT);
    }

    @Test
    public void call_get_account_with_invalid_account() {
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, null, s3ControlClient);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket/");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn("GetObject");

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class).hasMessage("Expecting account id to be configured on the S3 Client!");
    }

    @Test
    public void call_resolve_identity_fetches_default_credentials() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, s3ControlClient);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket/");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn("GetObject");

        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest));
        verify(credentialsProvider, times(1)).resolveIdentity(resolveIdentityRequest);
    }

    @Test
    public void call_resolve_identity_with_invalid_request_params_s3Prefix() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, s3ControlClient);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn(null);

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("S3://");

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void call_resolve_identity_with_invalid_request_params_operation() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, s3ControlClient);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(null);

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_get_data_access_with_invalid_response() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3ControlAsyncClient localS3ControlClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT, localS3ControlClient);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        CompletableFuture<GetDataAccessResponse> getDataAccessResponse = CompletableFuture.supplyAsync(() -> GetDataAccessResponse.builder().build());

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(TEST_OPERATION);
        when(localS3ControlClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest).join())
                .isInstanceOf(java.util.concurrent.CompletionException.class).getCause()
                .isInstanceOf(S3ControlException.class).getCause()
                .isInstanceOf(NullPointerException.class);

    }

}
