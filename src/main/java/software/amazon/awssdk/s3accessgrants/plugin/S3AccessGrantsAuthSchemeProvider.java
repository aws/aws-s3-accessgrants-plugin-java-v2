package software.amazon.awssdk.s3accessgrants.plugin;

import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeParams;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.utils.Validate;

import java.util.List;

/**
 * This is an Auth Scheme Provider for S3 access grants.
 * It uses a default auth scheme configured on S3 Clients and appends parameters required by access grants to resolve a request
 * The auth scheme provider takes a set of parameters using
 *  * {@link S3AuthSchemeParams}, and resolves a list of {@link AuthSchemeOption} based on the given parameters.
 */
public class S3AccessGrantsAuthSchemeProvider implements S3AuthSchemeProvider {

    private S3AuthSchemeProvider authSchemeProvider;
    S3AccessGrantsAuthSchemeProvider(@NotNull S3AuthSchemeProvider authSchemeProvider) {
        Validate.notNull(authSchemeProvider,
                "Expecting an Auth Scheme Provider to be specified while configuring S3Clients!");
        this.authSchemeProvider = authSchemeProvider;
    }

    @Override
    public List<AuthSchemeOption> resolveAuthScheme(@NotNull S3AuthSchemeParams authSchemeParams) {
       Validate.notNull(authSchemeParams,
                "An internal exception has occurred. Valid auth scheme params were not passed to the Auth Scheme Provider. Please contact SDK team!");
        return null;
    }
}
