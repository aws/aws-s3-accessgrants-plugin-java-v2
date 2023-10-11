package software.amazon.awssdk.s3accessgrants.plugin;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.regex.Pattern;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3control.model.Credentials;
import software.amazon.awssdk.services.s3control.model.GetDataAccessRequest;
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.services.s3control.model.Privilege;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsStaticOperationToPermissionMapper;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils;
import software.amazon.awssdk.services.s3control.model.S3ControlException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.Validate;

import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.OPERATION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;

/**
 * A {@link IdentityProvider} implementation for S3 access grants
 * @author Shiva Kumar Mukkapati
 */
public class S3AccessGrantsIdentityProvider implements IdentityProvider<AwsCredentialsIdentity>{

    private IdentityProvider<? extends AwsCredentialsIdentity> credentialsProvider;
    private Region region;

    private String accountId;

    private S3ControlAsyncClient s3control;

    private S3AccessGrantsStaticOperationToPermissionMapper permissionMapper;

    S3AccessGrantsIdentityProvider(@NotNull IdentityProvider<? extends AwsCredentialsIdentity> credentialsProvider,
                                   @NotNull Region region,
                                   @NotNull String accountId,
                                   @NotNull S3ControlAsyncClient s3ControlAsyncClient) {
        S3AccessGrantsUtils.argumentNotNull(credentialsProvider, "Expecting an Identity Provider to be specified while configuring S3Clients!");
        S3AccessGrantsUtils.argumentNotNull(region, "Expecting a region to be configured on the S3Clients!");
        this.credentialsProvider = credentialsProvider;
        this.region = region;
        this.accountId = accountId;
        this.s3control = s3ControlAsyncClient;
        this.permissionMapper = new S3AccessGrantsStaticOperationToPermissionMapper();
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
     * This method uses cache
     * This method Will throw an exception if the necessary grant is not available to the requester.
     * </p>
     * @param resolveIdentityRequest The request to resolve an Identity.
     * @return a completable future that will resolve to the credentials registered within a grant.
     * @throws {@link NullPointerException} in case that the credentials to talk to access grants are not available.
     * @throws {@link S3ControlException} in case that the requestor is not authorized to access the resource.
     * @throws {@link CompletionException} in case that the credentials cannot be fetched from access grants.
     */
    @Override
    public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest resolveIdentityRequest) {
        validateRequestParameters(resolveIdentityRequest);

        accountId = getAccountId();

        String S3Prefix = resolveIdentityRequest.property(PREFIX_PROPERTY).toString();
        String operation = resolveIdentityRequest.property(OPERATION_PROPERTY).toString();
        Permission permission = permissionMapper.getPermission(operation);
        Privilege privilege = software.amazon.awssdk.services.s3control.model.Privilege.DEFAULT;

        CompletableFuture<? extends AwsCredentialsIdentity> defaultCredentials = credentialsProvider.resolveIdentity(resolveIdentityRequest);

        return getCredentialsFromAccessGrants(createDataAccessRequest(accountId, S3Prefix, permission, privilege));

    }

    /**
     * This method will create a request to talk to access grants.
     * @param accountId the accountId that contains the access grant instance with the desired bucket location registered.
     * @param S3Prefix the S3Prefix location that the requester is accessing.
     * @param permission the permission level to access the resource. Permission is generated dynamically based on the
     *                   operation. See {@link S3AccessGrantsStaticOperationToPermissionMapper} for operation to permission mappings.
     * @param privilege specifies what privilege level does access grants need to use to determine if the request can be
     *                  authorized. The default value for this is {@link Privilege} MOST_PRIVILEGE.
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
     * Sends a request to access grants to authorize if the requester has permissions to access the desired resource (S#Prefix).
     * @param getDataAccessRequest the request that contains all the inputs required to talk to access grants
     * @return a completableFuture that resolves to credentials returned by access grants
     * */
    private CompletableFuture<? extends AwsCredentialsIdentity> getCredentialsFromAccessGrants(GetDataAccessRequest getDataAccessRequest) {

            S3AccessGrantsUtils.argumentNotNull(getDataAccessRequest, "An internal exception has occurred. Valid request was not passed to the call access grants. Please contact SDK team!");

            return s3control.getDataAccess(getDataAccessRequest).thenApply(getDataAccessResponse -> {
                Credentials credentials = getDataAccessResponse.credentials();
                return AwsSessionCredentials.builder().accessKeyId(credentials.accessKeyId())
                        .secretAccessKey(credentials.secretAccessKey())
                        .sessionToken(credentials.sessionToken()).build();
            }).exceptionally(e -> {
                throw S3ControlException.builder().message(e.getMessage()).cause(e.getCause()).build();
            });
    }

    private void validateRequestParameters(ResolveIdentityRequest resolveIdentityRequest) {
        S3AccessGrantsUtils.argumentNotNull(resolveIdentityRequest, "An internal exception has occurred. Valid request was not passed to the identity Provider. Please contact SDK team!");
        S3AccessGrantsUtils.argumentNotNull(accountId, "Expecting account id to be configured on the S3 Client!");
        Pattern pattern = Pattern.compile("s3://[a-z0-9.-]*");
        S3AccessGrantsUtils.argumentNotNull(resolveIdentityRequest.property(PREFIX_PROPERTY),"An internal exception has occurred. Valid S3Prefix was not passed to the identity Provider. Please contact SDK team!");
        Validate.validState(pattern.matcher(resolveIdentityRequest.property(PREFIX_PROPERTY).toString()).find(), "An internal exception has occurred. Valid S3Prefix was not passed to identity providers. Please contact SDK team!");
        S3AccessGrantsUtils.argumentNotNull(resolveIdentityRequest.property(OPERATION_PROPERTY),"An internal exception has occurred. Valid operation was not passed to identity providers. Please contact SDK team!");
    }

    protected String getAccountId() {
        return accountId != null ? accountId : "";
    }
}
