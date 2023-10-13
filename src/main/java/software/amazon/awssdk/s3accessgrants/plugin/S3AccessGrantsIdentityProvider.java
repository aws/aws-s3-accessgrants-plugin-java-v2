package software.amazon.awssdk.s3accessgrants.plugin;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.regex.Pattern;
import java.util.Optional;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.s3accessgrants.cache.CacheKey;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCache;
import software.amazon.awssdk.services.s3control.model.Credentials;
import software.amazon.awssdk.services.s3control.model.GetDataAccessRequest;
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.services.s3control.model.Privilege;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsStaticOperationToPermissionMapper;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils;
import software.amazon.awssdk.services.s3control.model.S3ControlException;
import software.amazon.awssdk.utils.Validate;

import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.OPERATION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;

/**
 * A {@link IdentityProvider} implementation for S3 access grants
 * @author Shiva Kumar Mukkapati
 */
public class S3AccessGrantsIdentityProvider implements IdentityProvider<AwsCredentialsIdentity>{

    private final IdentityProvider<? extends AwsCredentialsIdentity> credentialsProvider;
    private final Region region;

    private final String accountId;

    private final Privilege privilege;

    private final Boolean isCacheEnabled;

    private final S3ControlAsyncClient s3control;

    private final S3AccessGrantsStaticOperationToPermissionMapper permissionMapper;

    private final S3AccessGrantsCache cache;

    public S3AccessGrantsIdentityProvider(@NotNull IdentityProvider<? extends AwsCredentialsIdentity> credentialsProvider,
                                          @NotNull Region region,
                                          @NotNull String accountId,
                                          @NotNull Optional<Privilege> privilege,
                                          @NotNull Optional<Boolean> isCacheEnabled,
                                          @NotNull S3ControlAsyncClient s3ControlAsyncClient,
                                          @NotNull S3AccessGrantsCache cache) {
        S3AccessGrantsUtils.argumentNotNull(credentialsProvider, "Expecting an Identity Provider to be specified while configuring S3Clients!");
        S3AccessGrantsUtils.argumentNotNull(region, "Expecting a region to be configured on the S3Clients!");
        this.credentialsProvider = credentialsProvider;
        this.region = region;
        this.accountId = accountId;
        this.privilege = getPrivilege(privilege);
        this.isCacheEnabled = getIsCacheEnabled(isCacheEnabled);
        this.s3control = s3ControlAsyncClient;
        this.permissionMapper = new S3AccessGrantsStaticOperationToPermissionMapper();
        this.cache = cache;
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
     * This method Will return the credentials for the role that is present in the grant allowing requester's access to the
     * specific resource.
     * This method uses cache to store credentials to reduce requests sent to S3 access grant APIs
     * This method Will throw an exception if the necessary grant is not available to the requester.
     * </p>
     * @param resolveIdentityRequest The request to resolve an Identity.
     * @return a completable future that will resolve to the credentials registered within a grant.
     * @throws {@link NullPointerException} in case that the credentials to talk to access grants are not available.
     * @throws {@link S3ControlException} in case that the requester is not authorized to access the resource.
     * @throws {@link CompletionException} in case that the credentials cannot be fetched from access grants.
     */
    @Override
    public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest resolveIdentityRequest) {

        String configuredAccountId = getAccountId();
        validateRequestParameters(resolveIdentityRequest, configuredAccountId, privilege);

        defaultCredentials = credentialsProvider.resolveIdentity(resolveIdentityRequest);

        String S3Prefix = resolveIdentityRequest.property(PREFIX_PROPERTY).toString();
        String operation = resolveIdentityRequest.property(OPERATION_PROPERTY).toString();
        Permission permission = permissionMapper.getPermission(operation);

        // TODO: Should we wait here or let the cache handle this?
        return isCacheEnabled ? getCredentialsFromCache(defaultCredentials.join(), permission, S3Prefix, accountId) : getCredentialsFromAccessGrants(createDataAccessRequest(configuredAccountId, S3Prefix, permission, privilege));

    }

    /**
     * This method will create a request to talk to access grants.
     * @param accountId the accountId that contains the access grant instance with the desired bucket location registered.
     * @param S3Prefix the S3Prefix location that the requester is accessing.
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
     * Sends a request to access grants to authorize if the requester has permissions to access the desired resource (S3Prefix).
     * @param getDataAccessRequest the request that contains all the inputs required to talk to access grants
     * @return a completableFuture that resolves to credentials returned by access grants
     * */
    private CompletableFuture<? extends AwsCredentialsIdentity> getCredentialsFromAccessGrants(GetDataAccessRequest getDataAccessRequest) {

            S3AccessGrantsUtils.argumentNotNull(getDataAccessRequest, "An internal exception has occurred. Valid request was not passed to call access grants. Please contact S3 access grants plugin team!");

            return s3control.getDataAccess(getDataAccessRequest).thenApply(getDataAccessResponse -> {
                Credentials credentials = getDataAccessResponse.credentials();
                return AwsSessionCredentials.builder().accessKeyId(credentials.accessKeyId())
                        .secretAccessKey(credentials.secretAccessKey())
                        .sessionToken(credentials.sessionToken()).build();
            }).exceptionally(e -> {
                throw S3ControlException.builder().message(e.getMessage()).cause(e.getCause()).build();
            });
    }

    /**
     * The class will try to communicate with the cache to fetch the credentials.
     */
    CompletableFuture<? extends AwsCredentialsIdentity> getCredentialsFromCache(AwsCredentialsIdentity credentials, Permission permission, String S3Prefix, String accountId) {

            // TODO: Remove the supplysync after cache starts supporting this.

            return CompletableFuture.supplyAsync(() ->  cache.getValueFromCache(CacheKey.builder().credentials(credentials).permission(permission).s3Prefix(S3Prefix).build(),accountId));
    }

    private void validateRequestParameters(ResolveIdentityRequest resolveIdentityRequest, String accountId, Privilege privilege) {
        S3AccessGrantsUtils.argumentNotNull(resolveIdentityRequest, "An internal exception has occurred. Valid request was not passed to the identity Provider. Please contact S3 access grants plugin team!");
        S3AccessGrantsUtils.argumentNotNull(accountId, "Expecting account id to be configured on the S3 Client!");
        S3AccessGrantsUtils.argumentNotNull(privilege, "An internal exception has occurred.  Valid privilege was not passed to the identity Provider. Please contact S3 access grants plugin team!");
        Pattern pattern = Pattern.compile("s3://[a-z0-9.-]*");
        S3AccessGrantsUtils.argumentNotNull(resolveIdentityRequest.property(PREFIX_PROPERTY),"An internal exception has occurred. Valid S3Prefix was not passed to the identity Provider. Please contact S3 access grants plugin team!");
        Validate.validState(pattern.matcher(resolveIdentityRequest.property(PREFIX_PROPERTY).toString()).find(), "An internal exception has occurred. Valid S3Prefix was not passed to identity providers. Please contact S3 access grants plugin team!");
        S3AccessGrantsUtils.argumentNotNull(resolveIdentityRequest.property(OPERATION_PROPERTY),"An internal exception has occurred. Valid operation was not passed to identity providers. Please contact S3 access grants plugin team!");
    }

    String getAccountId() {
        // TODO : Integrate with GetAccessGrantsInstanceForS3Prefix
        return accountId != null ? accountId : null;
    }

    Privilege getPrivilege(Optional<Privilege> privilege) {
        return privilege.isPresent() ? privilege.get() : Privilege.DEFAULT;
    }

    Boolean getIsCacheEnabled(Optional<Boolean> isCacheEnabled) {
        return isCacheEnabled.isPresent() ? isCacheEnabled.get() : true;
    }


}
