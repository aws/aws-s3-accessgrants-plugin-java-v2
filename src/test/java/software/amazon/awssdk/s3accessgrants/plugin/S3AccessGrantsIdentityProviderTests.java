package software.amazon.awssdk.s3accessgrants.plugin;

import org.junit.Test;
import org.assertj.core.api.Assertions;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

public class S3AccessGrantsIdentityProviderTests {

    @Test
    public void create_identity_provider_without_default_identity_provider() {
        Assertions.assertThatThrownBy(() -> new S3AccessGrantsIdentityProvider(null, Region.US_EAST_2)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void create_identity_provider_without_valid_region() {
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        Assertions.assertThatThrownBy(() -> new S3AccessGrantsIdentityProvider(credentialsProvider, null)).isInstanceOf(NullPointerException.class);
    }

}
