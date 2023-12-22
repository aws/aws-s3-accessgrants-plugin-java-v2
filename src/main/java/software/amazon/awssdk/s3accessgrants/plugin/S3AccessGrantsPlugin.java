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

import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.core.SdkPlugin;
import software.amazon.awssdk.core.SdkServiceClientConfiguration;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProvider;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProviderImpl;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClientBuilder;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.utils.builder.ToCopyableBuilder;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.utils.Validate;

import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.DEFAULT_PRIVILEGE_FOR_PLUGIN;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.DEFAULT_CACHE_SETTING;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.DEFAULT_FALLBACK_SETTING;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.logger;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.DEFAULT_CROSS_REGION_ACCESS_SETTING;

/**
 * Access Grants Plugin that can be configured on S3 Clients
 * The class changes the configuration on the clients to use S3 Access Grants specific AuthScheme and IdentityProviders
 */
public class S3AccessGrantsPlugin  implements SdkPlugin, ToCopyableBuilder<Builder, S3AccessGrantsPlugin> {

    private boolean enableFallback;
    private boolean enableCrossRegionAccess;

    S3AccessGrantsPlugin(BuilderImpl builder) {
        this.enableFallback = builder.enableFallback;
        this.enableCrossRegionAccess = builder.enableCrossRegionAccess;
    }

    public static Builder builder() {
        return new BuilderImpl();
    }

    public static Builder builder(S3AccessGrantsPlugin plugin) {
        return new BuilderImpl(plugin);
    }

    boolean enableFallback() {
       return this.enableFallback;
    }

    boolean enableCrossRegionAccess() {
        return this.enableCrossRegionAccess;
    }

    /**
     * Change the configuration on the S3Clients to use S3 Access Grants specific AuthScheme and identityProviders.
     * @param config the existing configuration on the clients. Passed by the SDK on request path.
     * */
    @Override
    public void configureClient(SdkServiceClientConfiguration.Builder config) {
        logger.info(() -> "Configuring S3 Clients to use S3 Access Grants as a permission layer!");
        logger.info(() -> "Running the S3 Access grants plugin with fallback setting enabled : "+enableFallback());
        if(!enableFallback()) {
            logger.warn(() -> "Fallback not opted in! S3 Client will not fall back to evaluate policies if permissions are not provided through S3 Access Grants!");
        }

        S3ServiceClientConfiguration.Builder serviceClientConfiguration =
                Validate.isInstanceOf(S3ServiceClientConfiguration.Builder.class,
                        config,
                        "Expecting the plugin to be only "
                                + "configured on s3 clients");

        S3ControlAsyncClientBuilder s3ControlAsyncClientBuilder = S3ControlAsyncClient.builder()
                .credentialsProvider(serviceClientConfiguration.credentialsProvider());

        S3Client s3Client = S3Client
                .builder()
                .credentialsProvider(serviceClientConfiguration.credentialsProvider())
                .region(serviceClientConfiguration.region())
                .build();

        serviceClientConfiguration.authSchemeProvider(new S3AccessGrantsAuthSchemeProvider(serviceClientConfiguration.authSchemeProvider(), s3Client, enableCrossRegionAccess));

        S3AccessGrantsCachedCredentialsProvider cache = createAccessGrantsCache();

        StsAsyncClient stsClient = StsAsyncClient.builder()
                .credentialsProvider(serviceClientConfiguration.credentialsProvider())
                .region(serviceClientConfiguration.region())
                .build();

        MetricPublisher metricPublisher = config.overrideConfiguration() != null? (config.overrideConfiguration().metricPublishers() != null ? (config.overrideConfiguration().metricPublishers().size() > 0 ? config.overrideConfiguration().metricPublishers().get(0) : null) : null) : null;

        serviceClientConfiguration.credentialsProvider(new S3AccessGrantsIdentityProvider(serviceClientConfiguration.credentialsProvider(),
                stsClient,
                DEFAULT_PRIVILEGE_FOR_PLUGIN,
                DEFAULT_CACHE_SETTING,
                s3ControlAsyncClientBuilder,
                cache,
                enableFallback,
                metricPublisher
                ));

        logger.debug(() -> "Completed configuring S3 Clients to use S3 Access Grants as a permission layer!");

    }

    private S3AccessGrantsCachedCredentialsProvider createAccessGrantsCache() {

        return S3AccessGrantsCachedCredentialsProviderImpl.builder().build();

    }

    @Override
    public Builder toBuilder() {
        return new BuilderImpl(this);
    }

    public static final class BuilderImpl implements Builder{
        private boolean enableFallback;
        private boolean enableCrossRegionAccess;
        BuilderImpl() {
            this.enableFallback = DEFAULT_FALLBACK_SETTING;
            this.enableCrossRegionAccess = DEFAULT_CROSS_REGION_ACCESS_SETTING;
        }

        BuilderImpl(S3AccessGrantsPlugin plugin) {
            this.enableFallback = plugin.enableFallback;
            this.enableCrossRegionAccess = plugin.enableCrossRegionAccess;
        }

        @Override
        public S3AccessGrantsPlugin build() {
            return new S3AccessGrantsPlugin(this);
        }

        @Override
        public Builder enableFallback(@NotNull Boolean choice) {
           this.enableFallback = choice == null ? DEFAULT_FALLBACK_SETTING: choice;
           return this;
        }

        @Override
        public Builder enableCrossRegionAccess(@NotNull Boolean choice) {
            this.enableCrossRegionAccess = choice == null ? DEFAULT_CROSS_REGION_ACCESS_SETTING: choice;
            return this;
        }
    }
}




