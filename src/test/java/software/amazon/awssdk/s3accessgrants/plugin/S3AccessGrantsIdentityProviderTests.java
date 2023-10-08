package software.amazon.awssdk.s3accessgrants.plugin;

import org.junit.Test;
import org.assertj.core.api.Assertions;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.regions.Region;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.OPERATION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;

public class S3AccessGrantsIdentityProviderTests {

    private final Region TEST_REGION = Region.US_EAST_2;
    private final String TEST_ACCOUNT = "12345028312";


    @Test
    public void create_identity_provider_without_default_identity_provider() {
        Assertions.assertThatThrownBy(() -> new S3AccessGrantsIdentityProvider(null, TEST_REGION, TEST_ACCOUNT)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void create_identity_provider_without_valid_region() {
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        Assertions.assertThatThrownBy(() -> new S3AccessGrantsIdentityProvider(credentialsProvider, null, TEST_ACCOUNT)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void call_identity_type_returns_valid_result() {
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT);
        Assertions.assertThat(accessGrantsIdentityProvider.identityType()).isEqualTo(AwsCredentialsIdentity.class);
    }

    @Test
    public void call_identity_provider_with_invalid_request() {
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT);
        ResolveIdentityRequest resolveIdentityRequest = null;
        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void call_get_account() {
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT);
        Assertions.assertThat(accessGrantsIdentityProvider.getAccountId()).isEqualTo(TEST_ACCOUNT);
    }

    @Test
    public void call_get_account_with_invalid_account() {
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, null);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket/");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn("GetObject");

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(NullPointerException.class).hasMessage("Expecting account id to be configured on the S3 Client!");
    }

    @Test
    public void call_resolve_identity_fetches_default_credentials() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket/");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn("GetObject");

        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest));
        verify(credentialsProvider, times(1)).resolveIdentity(resolveIdentityRequest);
    }

    @Test
    public void call_resolve_identity_with_invalid_request_params_s3Prefix() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn(null);

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(NullPointerException.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("S3://");

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void call_resolve_identity_with_invalid_request_params_operation() {
        IdentityProvider credentialsProvider = mock(IdentityProvider.class);
        S3AccessGrantsIdentityProvider accessGrantsIdentityProvider = new S3AccessGrantsIdentityProvider(credentialsProvider, TEST_REGION, TEST_ACCOUNT);
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn(null);

        Assertions.assertThatThrownBy(() -> accessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest)).isInstanceOf(NullPointerException.class);
    }

}
