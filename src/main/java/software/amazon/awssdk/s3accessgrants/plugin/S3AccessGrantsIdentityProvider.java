package software.amazon.awssdk.s3accessgrants.plugin;

import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.utils.Validate;

import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.OPERATION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;

public class S3AccessGrantsIdentityProvider implements IdentityProvider<AwsCredentialsIdentity>{

    private IdentityProvider<? extends AwsCredentialsIdentity> credentialsProvider;
    private Region region;

    private String accountId;

    S3AccessGrantsIdentityProvider(@NotNull IdentityProvider<? extends AwsCredentialsIdentity> credentialsProvider,
                                   @NotNull Region region,
                                   @NotNull String accountId) {
        Validate.notNull(credentialsProvider, "Expecting an Identity Provider to be specified while configuring S3Clients!");
        Validate.notNull(region, "Expecting a region to be configured on the S3Clients!");
        this.credentialsProvider = credentialsProvider;
        this.region = region;
        this.accountId = accountId;
    }


    @Override
    public Class<AwsCredentialsIdentity> identityType() {
        return AwsCredentialsIdentity.class;
    }

    @Override
    public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest resolveIdentityRequest) {
        validateRequestParameters(resolveIdentityRequest);

        CompletableFuture<? extends AwsCredentialsIdentity> defaultCredentials = credentialsProvider.resolveIdentity(resolveIdentityRequest);

        accountId = getAccountId();

        String S3Prefix = resolveIdentityRequest.property(PREFIX_PROPERTY).toString();
        String operation = resolveIdentityRequest.property(OPERATION_PROPERTY).toString();


        return null;
    }

    private void validateRequestParameters(ResolveIdentityRequest resolveIdentityRequest) {
        Validate.notNull(resolveIdentityRequest, "An internal exception has occurred. Valid request was not passed to the identity Provider. Please contact SDK team!");
        Validate.notNull(accountId, "Expecting account id to be configured on the S3 Client!");
        Pattern pattern = Pattern.compile("s3://[a-z0-9.-]*");
        Validate.notNull(resolveIdentityRequest.property(PREFIX_PROPERTY),"An internal exception has occurred. Valid S3Prefix was not passed to the identity Provider. Please contact SDK team!");
        Validate.validState(pattern.matcher(resolveIdentityRequest.property(PREFIX_PROPERTY).toString()).find(), "An internal exception has occurred. Valid S3Prefix was not passed to identity providers. Please contact SDK team!");
        Validate.notNull(resolveIdentityRequest.property(OPERATION_PROPERTY),"An internal exception has occurred. Valid operation was not passed to identity providers. Please contact SDK team!");
    }

    protected String getAccountId() {
        return accountId != null ? accountId : "";
    }
}
