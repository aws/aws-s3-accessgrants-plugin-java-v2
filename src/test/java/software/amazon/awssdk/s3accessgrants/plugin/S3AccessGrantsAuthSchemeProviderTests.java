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


public class S3AccessGrantsAuthSchemeProviderTests{

    @Test
    public void create_authSchemeProvider_with_invalid_DefaultAuthProvider() {

       Assertions.assertThatThrownBy(() -> new S3AccessGrantsAuthSchemeProvider(null)).isInstanceOf(NullPointerException.class);

    }

    @Test
    public void create_authSchemeProvider_with_valid_DefaultAuthProvider() {
        S3AuthSchemeProvider authSchemeProvider = S3AuthSchemeProvider.defaultProvider();

        Assertions.assertThatNoException().isThrownBy(() -> new S3AccessGrantsAuthSchemeProvider(authSchemeProvider));
    }

    @Test
    public void call_authSchemeProvider_with_invalid_params() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider);
        S3AuthSchemeParams authSchemeParams = null;

        Assertions.assertThatThrownBy(()->accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void call_authSchemeProvider_with_valid_params() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider);
        S3AuthSchemeParams authSchemeParams = mock(S3AuthSchemeParams.class);

        Assertions.assertThatNoException().isThrownBy(()->accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams));
    }

    @Test
    public void call_authSchemeProvider_with_valid_params_invokes_default_authSchemeProvider() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider);
        S3AuthSchemeParams authSchemeParams = mock(S3AuthSchemeParams.class);

        Assertions.assertThatNoException().isThrownBy(()->accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams));
        verify(authSchemeProvider, times(1)).resolveAuthScheme(authSchemeParams);
    }

    @Test
    public void call_authSchemeProvider_with_valid_params_invokes_default_authSchemeProvider_returning_valid_result() {
        S3AuthSchemeProvider authSchemeProvider = mock(S3AuthSchemeProvider.class);
        S3AccessGrantsAuthSchemeProvider accessGrantsAuthSchemeProvider = new S3AccessGrantsAuthSchemeProvider(authSchemeProvider);
        S3AuthSchemeParams authSchemeParams = mock(S3AuthSchemeParams.class);
        List<AuthSchemeOption> authSchemeResolverResult = new ArrayList<>();
        authSchemeResolverResult.add(AuthSchemeOption.builder().schemeId("aws.auth#sigv4").build());

        when(authSchemeProvider.resolveAuthScheme(authSchemeParams)).thenReturn(authSchemeResolverResult);

        Assertions.assertThat(accessGrantsAuthSchemeProvider.resolveAuthScheme(authSchemeParams).get(0).schemeId()).isEqualTo("aws.auth#sigv4");
        verify(authSchemeProvider, times(1)).resolveAuthScheme(authSchemeParams);
    }
}
