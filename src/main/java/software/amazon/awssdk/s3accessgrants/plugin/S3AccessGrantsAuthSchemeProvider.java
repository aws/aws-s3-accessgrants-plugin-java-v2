package software.amazon.awssdk.s3accessgrants.plugin;

import java.util.List;
import java.util.stream.Collectors;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils;
import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeParams;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;

import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.OPERATION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.logger;


/**
 * This is an Auth Scheme Provider for S3 access grants.
 * It uses a default auth scheme configured on S3 Clients and appends parameters specifically required for access grants to resolve a request.
 * The auth scheme provider takes a set of parameters using.
 * {@link S3AuthSchemeParams}, and resolves to a list of {@link AuthSchemeOption} based on the given parameters.
 */
public class S3AccessGrantsAuthSchemeProvider implements S3AuthSchemeProvider {

    private final S3AuthSchemeProvider authSchemeProvider;
    S3AccessGrantsAuthSchemeProvider(@NotNull S3AuthSchemeProvider authSchemeProvider) {
        S3AccessGrantsUtils.argumentNotNull(authSchemeProvider,
                "Expecting an Auth Scheme Provider to be specified while configuring S3Clients!");
        this.authSchemeProvider = authSchemeProvider;
    }

    /**
     * resolves to an auth scheme based on a set of input parameters.
     * @param authSchemeParams parameters necessary to determine auth scheme to use for resolving the request.
     * @return a list of auth schemes that can be used for resolving the request.
     */
    @Override
    public List<AuthSchemeOption> resolveAuthScheme(@NotNull S3AuthSchemeParams authSchemeParams) {
        S3AccessGrantsUtils.argumentNotNull(authSchemeParams,
                "An internal exception has occurred. Valid auth scheme params were not passed to the Auth Scheme Provider. Please contact the S3 Access Grants plugin team!");
        S3AccessGrantsUtils.argumentNotNull(authSchemeParams.bucket(), "An internal exception has occurred. expecting bucket name to be specified for the request. Please contact the S3 Access Grants plugin team!");
        List<AuthSchemeOption> availableAuthSchemes = authSchemeProvider.resolveAuthScheme(authSchemeParams);
        String S3Prefix = "s3://"+authSchemeParams.bucket()+"/"+getKeyIfExists(authSchemeParams);

        return availableAuthSchemes.stream()
                .map(authScheme -> authScheme.toBuilder().putIdentityProperty(OPERATION_PROPERTY,
                                authSchemeParams.operation())
                        .putIdentityProperty(PREFIX_PROPERTY,
                                S3Prefix)
                        .build()
                )
                .collect(Collectors.toList());
    }

    private String getKeyIfExists(S3AuthSchemeParams authSchemeParams) {

        Boolean keyDoesNotExists = (authSchemeParams.key() == null || authSchemeParams.key().isEmpty())
                && (authSchemeParams.prefix() == null || authSchemeParams.prefix().isEmpty());

        String validKey = !(authSchemeParams.key() == null || authSchemeParams.key().isEmpty()) ? authSchemeParams.key() :
                          !(authSchemeParams.prefix() == null || authSchemeParams.prefix().isEmpty()) ? authSchemeParams.prefix() : null;

        if(keyDoesNotExists) logger.debug(() -> "no object key was specified for the operation!");

        return keyDoesNotExists ? "*" : validKey;

    }
}
