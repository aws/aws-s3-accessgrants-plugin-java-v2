package software.amazon.awssdk.s3accessgrants.plugin;

import org.junit.Test;
import org.assertj.core.api.Assertions;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeParams;

import static org.mockito.Mockito.*;


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

}
