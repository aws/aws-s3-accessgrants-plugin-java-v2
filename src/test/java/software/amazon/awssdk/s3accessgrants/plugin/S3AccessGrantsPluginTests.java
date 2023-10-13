package software.amazon.awssdk.s3accessgrants.plugin;

import org.junit.Test;
import org.assertj.core.api.Assertions;
import software.amazon.awssdk.services.s3control.model.Privilege;
import software.amazon.awssdk.core.SdkServiceClientConfiguration;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

public class S3AccessGrantsPluginTests {

    private final String TEST_ACCOUNT = "123450013912";

    @Test
    public void create_access_grants_plugin() {
       Assertions.assertThatNoException().isThrownBy(() -> S3AccessGrantsPlugin.builder().accountId(TEST_ACCOUNT).build());
    }

    @Test
    public void create_access_grants_plugin_from_existing_plugin() {
        S3AccessGrantsPlugin accessGrantsPlugin = S3AccessGrantsPlugin.builder().accountId(TEST_ACCOUNT).build();
        Assertions.assertThatNoException().isThrownBy(() -> S3AccessGrantsPlugin.builder(accessGrantsPlugin));
    }

    @Test
    public void create_access_grants_rebuild_plugin_from_existing_plugin() {
        S3AccessGrantsPlugin accessGrantsPlugin = S3AccessGrantsPlugin.builder().accountId(TEST_ACCOUNT).build();
        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsPlugin.toBuilder().accountId(TEST_ACCOUNT).build());
    }

    @Test
    public void create_access_grants_plugin_without_params() {
        Assertions.assertThatNoException().isThrownBy(() -> S3AccessGrantsPlugin.builder().build());
    }

    @Test
    public void call_configure_client_with_no_config() {
        S3AccessGrantsPlugin accessGrantsPlugin = S3AccessGrantsPlugin.builder().build();
        Assertions.assertThatThrownBy(() -> accessGrantsPlugin.configureClient(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void call_configure_client_with_valid_config() {

        S3AccessGrantsPlugin accessGrantsPlugin = S3AccessGrantsPlugin.builder().build();
        SdkServiceClientConfiguration.Builder sdkServiceClientConfiguration = S3ServiceClientConfiguration.builder()
               .authSchemeProvider(S3AuthSchemeProvider.defaultProvider())
               .credentialsProvider(DefaultCredentialsProvider.create())
               .region(Region.US_EAST_2);


       Assertions.assertThatNoException().isThrownBy(() -> accessGrantsPlugin.configureClient(sdkServiceClientConfiguration));

    }

    @Test
    public void call_configure_client_with_invalid_auth_provider_in_config() {

        S3AccessGrantsPlugin accessGrantsPlugin = S3AccessGrantsPlugin.builder().build();
        SdkServiceClientConfiguration.Builder sdkServiceClientConfiguration = S3ServiceClientConfiguration.builder()
                .authSchemeProvider(null)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.US_EAST_2);


        Assertions.assertThatThrownBy(() -> accessGrantsPlugin.configureClient(sdkServiceClientConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expecting an Auth Scheme Provider to be specified while configuring S3Clients!");

    }

    @Test
    public void call_configure_client_with_invalid_identity_provider_in_config() {

        S3AccessGrantsPlugin accessGrantsPlugin = S3AccessGrantsPlugin.builder().build();
        SdkServiceClientConfiguration.Builder sdkServiceClientConfiguration = S3ServiceClientConfiguration.builder()
                .authSchemeProvider(S3AuthSchemeProvider.defaultProvider())
                .credentialsProvider(null)
                .region(Region.US_EAST_2);


        Assertions.assertThatThrownBy(() -> accessGrantsPlugin.configureClient(sdkServiceClientConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expecting an Identity Provider to be specified while configuring S3Clients!");

    }

    @Test
    public void call_configure_client_with_invalid_region_in_config() {

        S3AccessGrantsPlugin accessGrantsPlugin = S3AccessGrantsPlugin.builder().build();
        SdkServiceClientConfiguration.Builder sdkServiceClientConfiguration = S3ServiceClientConfiguration.builder()
                .authSchemeProvider(S3AuthSchemeProvider.defaultProvider())
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(null);


        Assertions.assertThatThrownBy(() -> accessGrantsPlugin.configureClient(sdkServiceClientConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expecting a region to be configured on the S3Clients!");

    }



}
