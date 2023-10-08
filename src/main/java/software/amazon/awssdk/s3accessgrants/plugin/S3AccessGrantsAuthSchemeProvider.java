package software.amazon.awssdk.s3accessgrants.plugin;

import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeParams;

/**
 * This is an Auth Scheme Provider for S3 access grants.
 * The auth scheme provider takes a set of parameters using
 *  * {@link S3AuthSchemeParams}, and resolves a list of {@link AuthSchemeOption} based on the given parameters
 */
public class S3AccessGrantsAuthSchemeProvider {

}
