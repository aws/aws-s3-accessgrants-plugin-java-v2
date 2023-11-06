package software.amazon.awssdk.s3accessgrants.plugin;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.regex.Pattern;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.publishers.cloudwatch.CloudWatchMetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProvider;
import software.amazon.awssdk.services.s3control.model.*;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsStaticOperationToPermissionMapper;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;
import software.amazon.awssdk.utils.Validate;

import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.OPERATION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.logger;

/**
 * A {@link IdentityProvider} implementation for S3 access grants
 * The class provides functionality to get the credentials from S3 access grants
 * @author Shiva Kumar Mukkapati
 */
public class S3AccessGrantsIdentityProvider implements IdentityProvider<AwsCredentialsIdentity>{

    private final IdentityProvider<? extends AwsCredentialsIdentity> credentialsProvider;
    private final Region region;

    private final Privilege privilege;

    private final Boolean isCacheEnabled;

    private final S3ControlAsyncClient s3control;

    private final StsAsyncClient stsAsyncClient;

    private final S3AccessGrantsStaticOperationToPermissionMapper permissionMapper;

    private final S3AccessGrantsCachedCredentialsProvider cache;

    private final boolean enableFallback;

    private final MetricPublisher metricsPublisher;

    private String CONTACT_TEAM_MESSAGE_TEMPLATE = "An internal exception has occurred. Valid %s was not passed to the %s. Please contact S3 access grants plugin team!";

    public S3AccessGrantsIdentityProvider(@NotNull IdentityProvider<? extends AwsCredentialsIdentity> credentialsProvider,
                                          @NotNull Region region,
                                          @NotNull StsAsyncClient stsAsyncClient,
                                          @NotNull Privilege privilege,
                                          @NotNull Boolean isCacheEnabled,
                                          @NotNull S3ControlAsyncClient s3ControlAsyncClient,
                                          @NotNull S3AccessGrantsCachedCredentialsProvider cache,
                                          @NotNull boolean enableFallback,
                                          @NotNull MetricPublisher metricsPublisher) {
        S3AccessGrantsUtils.argumentNotNull(credentialsProvider, "Expecting an Identity Provider to be specified while configuring S3Clients!");
        S3AccessGrantsUtils.argumentNotNull(region, "Expecting a region to be configured on the S3Clients!");
        S3AccessGrantsUtils.argumentNotNull(stsAsyncClient, String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "sts client", "identity provider"));
        this.credentialsProvider = credentialsProvider;
        this.region = region;
        this.stsAsyncClient = stsAsyncClient;
        this.privilege = privilege;
        this.isCacheEnabled = isCacheEnabled;
        this.s3control = s3ControlAsyncClient;
        this.permissionMapper = new S3AccessGrantsStaticOperationToPermissionMapper();
        this.cache = cache;
        this.enableFallback = enableFallback;
        this.metricsPublisher = metricsPublisher;
    }

    /**
     * <p>This is a method that will return the credentials type that
     * the identity provider will return. The return type is used to determine
     * what identity provider to select for specific credentials request</p>
     * @return AwsCredentialsIdentity.class
     * */
    @Override
    public Class<AwsCredentialsIdentity> identityType() {
        return AwsCredentialsIdentity.class;
    }

    /**
     * <p> This is a method that will talk to access grants to process the request.
     * This method Will return the credentials for the role that is present in the grant allowing requesters access to the
     * specific resource.
     * This method uses cache to store credentials to reduce requests sent to S3 access grant APIs
     * This method Will throw an exception if the necessary grant is not available to the requester.
     * </p>
     * @param resolveIdentityRequest The request to resolve an Identity.
     * @return a completable future that will resolve to the credentials registered within a grant.
     * @throws {@link NullPointerException}
     * @throws {@link S3ControlException}
     * @throws {@link CompletionException}
     */
    @Override
    public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest resolveIdentityRequest) {

        CompletableFuture<? extends AwsCredentialsIdentity> userCredentials = null;

        try {
            String accountId = getCallerAccountID().join().account();

            validateRequestParameters(resolveIdentityRequest, accountId, privilege, isCacheEnabled);

            userCredentials = credentialsProvider.resolveIdentity(resolveIdentityRequest);

            String S3Prefix = resolveIdentityRequest.property(PREFIX_PROPERTY).toString();
            String operation = resolveIdentityRequest.property(OPERATION_PROPERTY).toString();

            logger.debug(() -> " Call access grants with the following request params! ");
            logger.debug(() -> " S3Prefix : " + S3Prefix);
            logger.debug(() -> " caller accountID : " + accountId);
            logger.debug(() -> " operation : " + operation);

            Permission permission = permissionMapper.getPermission(operation);

            logger.debug(() -> " permission : " + permission);

            return isCacheEnabled ? getCredentialsFromCache(userCredentials.join(), permission, S3Prefix, accountId) : getCredentialsFromAccessGrants(createDataAccessRequest(accountId, S3Prefix, permission, privilege));
        } catch(SdkServiceException e) {

            if(shouldFallbackToDefaultCredentialsForThisCase(e.statusCode(), e.getCause())) {
                return userCredentials;
            }
            throw e;
        }
    }

    /**
     * This method will create a request to talk to access grants.
     * @param accountId the accountId that contains the access grant instance with the desired bucket location registered.
     * @param S3Prefix the resource that the requester is accessing.
     * @param permission the permission level to access the resource. Permission is generated dynamically based on the
     *                   operation. See {@link S3AccessGrantsStaticOperationToPermissionMapper} for operation to permission mappings.
     * @param privilege specifies what privilege level does access grants need to use to determine if the request can be
     *                  authorized. The default value for this is {@link Privilege} DEFAULT.
     * @rturn the request created from the inputs.
     * */
    private GetDataAccessRequest createDataAccessRequest(String accountId,
                                                         String S3Prefix,
                                                         Permission permission,
                                                         Privilege privilege) {
        GetDataAccessRequest dataAccessRequest = GetDataAccessRequest.builder()
                .accountId(accountId)
                .target(S3Prefix)
                .permission(permission)
                .privilege(privilege)
                .build();

        return dataAccessRequest;
    }

    /**
     * Maintenance Purpose - In case we want to make cache as a optional opt-out in the future.
     * Sends a request to access grants to authorize if the requester has permissions to access the desired resource (S3Prefix).
     * @param getDataAccessRequest the request to talk to access grants
     * @return a completableFuture that resolves to credentials returned by access grants
     * @throws S3ControlException for any request failures
     * */
    CompletableFuture<? extends AwsCredentialsIdentity> getCredentialsFromAccessGrants(GetDataAccessRequest getDataAccessRequest) {

            S3AccessGrantsUtils.argumentNotNull(getDataAccessRequest, String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "request", "for calling access grants"));
            logger.debug(() -> " Calling S3 Access Grants to validate access permissions!");
            return s3control.getDataAccess(getDataAccessRequest).thenApply(getDataAccessResponse -> {
                Credentials credentials = getDataAccessResponse.credentials();
                return AwsSessionCredentials.builder().accessKeyId(credentials.accessKeyId())
                            .secretAccessKey(credentials.secretAccessKey())
                            .sessionToken(credentials.sessionToken()).build();
            });
    }

    /**
     * The class will communicate with the cache to fetch the credentials.
     * By default, requests are routed directly to the cache to handle the credentials fetching.
     */
    CompletableFuture<? extends AwsCredentialsIdentity> getCredentialsFromCache(AwsCredentialsIdentity credentials, Permission permission, String S3Prefix, String accountId) {

        try {
            return cache.getDataAccess(credentials, permission, S3Prefix, accountId).exceptionally(e -> {
                SdkServiceException throwableException = unwrapAndBuildException(e);
                if (shouldFallbackToDefaultCredentialsForThisCase(throwableException.statusCode(), throwableException)) return credentials;
                throw throwableException;
            });
        } catch (Exception e) {
            SdkServiceException throwableException = unwrapAndBuildException(e);
            if (shouldFallbackToDefaultCredentialsForThisCase(throwableException.statusCode(), throwableException)) return CompletableFuture.supplyAsync(() -> credentials);
            throw throwableException;
        } finally {
            if (metricsPublisher != null) publishMetrics();
        }

    }

    private void publishMetrics() {
        metricsPublisher.publish(cache.getAccessGrantsMetrics().collect());
        metricsPublisher.publish(cache.getAccessGrantsCacheMetrics().collect());
        metricsPublisher.close();
    }

    private void validateRequestParameters(ResolveIdentityRequest resolveIdentityRequest, String accountId, Privilege privilege, Boolean isCacheEnabled) {
        logger.debug(() -> "Validating the request parameters before sending a request to S3 Access grants!");
        S3AccessGrantsUtils.argumentNotNull(resolveIdentityRequest, String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "request", "identity provider"));
        S3AccessGrantsUtils.argumentNotNull(accountId, "Expecting account id to be configured on the plugin!");
        S3AccessGrantsUtils.argumentNotNull(privilege, String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "privilege", "identity provider"));
        S3AccessGrantsUtils.argumentNotNull(isCacheEnabled, String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "cache setting", "identity provider"));
        Pattern pattern = Pattern.compile("s3://[a-z0-9.-]*");
        S3AccessGrantsUtils.argumentNotNull(resolveIdentityRequest.property(PREFIX_PROPERTY), String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "S3Prefix", "identity provider"));
        Validate.isTrue(pattern.matcher(resolveIdentityRequest.property(PREFIX_PROPERTY).toString()).find(), String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "S3Prefix", "identity provider"));
        S3AccessGrantsUtils.argumentNotNull(resolveIdentityRequest.property(OPERATION_PROPERTY), String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "operation", "identity provider"));
        logger.debug(() -> "Validation Complete. The request parameters can be forwarded to S3 Access grants!");
    }

    /**
     * With asynchronous behavior, the exceptions are wrapped within CompletionException.
     * With Chaining CompletionExceptions, we can end up with a chain of wrapped exceptions.
     * The function helps to unwrap the main exception that caused a chain of CompletionExceptions.
     * */
    private SdkServiceException unwrapAndBuildException(Throwable e) {
        while(e.getCause() != null) {
            e = e.getCause();
        }
        if (e instanceof S3ControlException) {
            S3ControlException exc = (S3ControlException) e;
            return SdkServiceException.builder().statusCode(exc.statusCode())
                    .message(exc.getMessage())
                    .cause(e)
                    .build();
        }
        return SdkServiceException.builder()
                .message(e.getMessage())
                .cause(e)
                .build();
    }

    /**
     * Validates if the fallback should be enabled in case of any error during request processing
     * By default, unsupported operations invoke fallback regardless of user opt-in choice.
     * If user opts in to the fallback mechanism, we fall back for all the cases where access grants is not able to vend credentials.
     * @param statusCode status code returned by the access grants server
     * @param cause cause for why the request failed
     * @return
     */
    Boolean shouldFallbackToDefaultCredentialsForThisCase(int statusCode, Throwable cause) {

       if(enableFallback) {
           logger.debug(() -> " Fall back enabled on the plugin! falling back to evaluate permission through policies!");
           return true;
       }
       if(statusCode == 404 && cause instanceof UnsupportedOperationException) {
           logger.debug(() -> " Operation not supported by S3 access grants! fall back to evaluate permission through policies!");
           return true;
       }
       logger.error(() -> " Fall back not enabled! An attempt will not be made to evaluate permissions through policies! "+ cause.getMessage());
       return false;

    }

    /**
     * Fetches the caller accountID from the requester using STS.
     * @return a completableFuture containing response from STS.
     * */
    CompletableFuture<GetCallerIdentityResponse> getCallerAccountID() {
        logger.debug(() -> "requesting STS to fetch caller accountID!");
        return stsAsyncClient.getCallerIdentity();
    }
}
