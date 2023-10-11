package software.amazon.awssdk.s3accessgrants.plugin;

import org.junit.Test;
import org.assertj.core.api.Assertions;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeParams;
import java.util.List;
import java.util.ArrayList;
import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.OPERATION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;


public class S3AccessGrantsAuthSchemeProviderTests {

    private final String BUCKET_NAME = "test-bucket";
    private final String KEY = "test-key";
    private final String OPERATION = "GetObject";

    private final String SIGNING_SCHEME = "aws.auth#sigv4";

    @Test
    public void create_authSchemeProvider_with_no_DefaultAuthProvider() {

       Assertions.assertThatThrownBy(() -> new S3AccessGrantsAuthSchemeProvider(null)).isInstanceOf(IllegalArgumentException.class);

    }

    @Test
    public void create_authSchemeProvider_with_valid_DefaultAuthProvider() {
        S3AuthSchemeProvider authSchemeProvider = S3AuthSchemeProvider.defaultProvider();

        Assertions.assertThatNoException().isThrownBy(() -> new S3AccessGrantsAuthSchemeProvider(authSchemeProvider));
    }

    @Test
    public void call_authSchemeProvider_with_null_params() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider);
        S3AuthSchemeParams authSchemeParams = null;

        Assertions.assertThatThrownBy(()->accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams)).isInstanceOf(IllegalArgumentException.class);
        verify(authSchemeProvider,never()).resolveAuthScheme(authSchemeParams);
    }

    @Test
    public void call_authSchemeProvider_with_invalid_params_null_bucket() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider);
        S3AuthSchemeParams authSchemeParams = S3AuthSchemeParams.builder().bucket(null).key(KEY).operation(OPERATION).build();
        List<AuthSchemeOption> authSchemeResolverResult = new ArrayList<>();
        authSchemeResolverResult.add(AuthSchemeOption.builder().schemeId(SIGNING_SCHEME).build());

        when(authSchemeProvider.resolveAuthScheme(authSchemeParams)).thenReturn(authSchemeResolverResult);

        Assertions.assertThatThrownBy(()->accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("An internal exception has occurred. expecting bucket name to be specified for the request. Please contact the S3 Access Grants team!");
        verify(authSchemeProvider, never()).resolveAuthScheme(authSchemeParams);
    }

    @Test
    public void call_authSchemeProvider_with_valid_params_valid_bucket() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider);
        S3AuthSchemeParams authSchemeParams = mock(S3AuthSchemeParams.class);

        when(authSchemeParams.bucket()).thenReturn(BUCKET_NAME);
        Assertions.assertThatNoException().isThrownBy(()->accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams));
        verify(authSchemeProvider, times(1)).resolveAuthScheme(authSchemeParams);
    }

    @Test
    public void call_authSchemeProvider_with_valid_params_null_key() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider);
        S3AuthSchemeParams authSchemeParams = S3AuthSchemeParams.builder().bucket(BUCKET_NAME).key(null).operation(OPERATION).build();
        List<AuthSchemeOption> authSchemeResolverResult = new ArrayList<>();
        authSchemeResolverResult.add(AuthSchemeOption.builder().schemeId(SIGNING_SCHEME).build());

        when(authSchemeProvider.resolveAuthScheme(authSchemeParams)).thenReturn(authSchemeResolverResult);

        List<AuthSchemeOption> accessGrantsAuthSchemeResult = accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams);

        Assertions.assertThat(accessGrantsAuthSchemeResult.get(0).identityProperty(PREFIX_PROPERTY)).isEqualTo("s3://test-bucket/");
        Assertions.assertThat(accessGrantsAuthSchemeResult.get(0).identityProperty(OPERATION_PROPERTY)).isEqualTo(OPERATION);
    }

    @Test
    public void call_authSchemeProvider_with_valid_params_invokes_default_authSchemeProvider() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider);
        S3AuthSchemeParams authSchemeParams = mock(S3AuthSchemeParams.class);

        when(authSchemeParams.bucket()).thenReturn(BUCKET_NAME);

        Assertions.assertThatNoException().isThrownBy(()->accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams));
        verify(authSchemeProvider, times(1)).resolveAuthScheme(authSchemeParams);
    }

    @Test
    public void call_authSchemeProvider_with_valid_params_invokes_default_authSchemeProvider_returning_valid_result() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider);
        S3AuthSchemeParams authSchemeParams = mock(S3AuthSchemeParams.class);
        List<AuthSchemeOption> authSchemeResolverResult = new ArrayList<>();
        authSchemeResolverResult.add(AuthSchemeOption.builder().schemeId(SIGNING_SCHEME).build());

        when(authSchemeParams.bucket()).thenReturn(BUCKET_NAME);
        when(authSchemeProvider.resolveAuthScheme(authSchemeParams)).thenReturn(authSchemeResolverResult);

        Assertions.assertThat(accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams).get(0).schemeId()).isEqualTo(SIGNING_SCHEME);
        verify(authSchemeProvider, times(1)).resolveAuthScheme(authSchemeParams);
    }

    @Test
    public void call_authSchemeProvider_with_valid_params_captures_all_params_on_auth_scheme() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider);
        S3AuthSchemeParams authSchemeParams = S3AuthSchemeParams.builder().bucket(BUCKET_NAME).key(KEY).operation(OPERATION).build();
        List<AuthSchemeOption> authSchemeResolverResult = new ArrayList<>();
        authSchemeResolverResult.add(AuthSchemeOption.builder().schemeId(SIGNING_SCHEME).build());

        when(authSchemeProvider.resolveAuthScheme(authSchemeParams)).thenReturn(authSchemeResolverResult);

        List<AuthSchemeOption> accessGrantsAuthSchemeResult = accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams);

        Assertions.assertThat(accessGrantsAuthSchemeResult.get(0).identityProperty(PREFIX_PROPERTY)).isEqualTo("s3://test-bucket/test-key");
        Assertions.assertThat(accessGrantsAuthSchemeResult.get(0).identityProperty(OPERATION_PROPERTY)).isEqualTo(OPERATION);
    }

}
