package software.amazon.awssdk.s3accessgrants.plugin;

import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.utils.Validate;

public class S3AccessGrantsIdentityProvider {

    private IdentityProvider<? extends AwsCredentialsIdentity> credentialsProvider;
    private Region region;

    S3AccessGrantsIdentityProvider(@NotNull IdentityProvider<? extends AwsCredentialsIdentity> credentialsProvider,
                                   @NotNull Region region) {
        Validate.notNull(credentialsProvider, "Expecting an Identity Provider to be specified while configuring S3Clients!");
        Validate.notNull(region, "Expecting a region to be configured on the S3Clients!");
        this.credentialsProvider = credentialsProvider;
        this.region = region;
    }

}
