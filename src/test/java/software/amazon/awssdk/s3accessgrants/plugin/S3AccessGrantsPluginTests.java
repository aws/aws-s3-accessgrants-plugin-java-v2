/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.s3accessgrants.plugin;

import org.junit.Test;
import org.assertj.core.api.Assertions;
import software.amazon.awssdk.core.SdkServiceClientConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

public class S3AccessGrantsPluginTests {

    private final String TEST_ACCOUNT = "123450013912";

    @Test
    public void create_access_grants_plugin() {
       Assertions.assertThatNoException().isThrownBy(() -> S3AccessGrantsPlugin.builder().build());
    }

    @Test
    public void create_access_grants_plugin_from_existing_plugin() {
        S3AccessGrantsPlugin accessGrantsPlugin = S3AccessGrantsPlugin.builder().build();
        Assertions.assertThatNoException().isThrownBy(() -> S3AccessGrantsPlugin.builder(accessGrantsPlugin));
        Assertions.assertThat(accessGrantsPlugin.enableFallback()).isFalse();
    }

    @Test
    public void create_access_grants_plugin_with_fallback_specified() {
        S3AccessGrantsPlugin accessGrantsPlugin = S3AccessGrantsPlugin.builder().enableFallback(true).build();
        Assertions.assertThatNoException().isThrownBy(() -> S3AccessGrantsPlugin.builder(accessGrantsPlugin));
        Assertions.assertThat(accessGrantsPlugin.enableFallback()).isTrue();
    }

    @Test
    public void create_access_grants_rebuild_plugin_from_existing_plugin() {
        S3AccessGrantsPlugin accessGrantsPlugin = S3AccessGrantsPlugin.builder().build();
        Assertions.assertThatNoException().isThrownBy(() -> accessGrantsPlugin.toBuilder().build());
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
               .region(Region.US_EAST_1);


       Assertions.assertThatNoException().isThrownBy(() -> accessGrantsPlugin.configureClient(sdkServiceClientConfiguration));

    }

    @Test
    public void call_configure_client_with_invalid_auth_provider_in_config() {

        S3AccessGrantsPlugin accessGrantsPlugin = S3AccessGrantsPlugin.builder().build();
        SdkServiceClientConfiguration.Builder sdkServiceClientConfiguration = S3ServiceClientConfiguration.builder()
                .authSchemeProvider(null)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.US_EAST_1);


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
                .region(Region.US_EAST_1);


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
                .isInstanceOf(SdkClientException.class);
        // SDK supports default values for the plugin as well, which bypasses the custom validation.
        // SDK will throw SdkClientException when it attempts to look for the credentials in the environment config and does not find any.

    }

}
