package software.amazon.awssdk.s3accessgrants.plugin;

import java.util.Optional;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.core.SdkPlugin;
import software.amazon.awssdk.core.SdkServiceClientConfiguration;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCache;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProvider;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProviderImpl;
import software.amazon.awssdk.services.s3control.S3ControlClient;
import software.amazon.awssdk.services.s3control.model.Privilege;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.utils.builder.ToCopyableBuilder;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils;
import software.amazon.awssdk.utils.Validate;

import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.DEFAULT_CACHE_SETTING;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.DEFAULT_PRIVILEGE_FOR_PLUGIN;

/**
 * Access Grants Plugin that can be configured on S3 Clients
 * The class changes the configuration on the clients to use S3 Access Grants specific AuthScheme and IdentityProviders
 */
public class S3AccessGrantsPlugin  implements SdkPlugin, ToCopyableBuilder<Builder, S3AccessGrantsPlugin> {

    private String accountId;

    private Optional<Privilege> privilege;

    private Optional<Boolean> isCacheEnabled;

    S3AccessGrantsPlugin(BuilderImpl builder) {
        this.accountId = builder.accountId;
        this.privilege = builder.privilege;
        this.isCacheEnabled = builder.isCacheEnabled;
    }

    public static Builder builder() {
        return new BuilderImpl();
    }

    public static Builder builder(S3AccessGrantsPlugin accessGrantsPlugin) {
        return new BuilderImpl(accessGrantsPlugin);
    }

    /**
     * Change the configuration on the S3Clients to use S3 Access Grants specific AuthScheme and identityProviders.
     * @param config the existing configuration on the clients. Passed by the SDK on request path.
     * */
    @Override
    public void configureClient(SdkServiceClientConfiguration.Builder config) {

        S3ServiceClientConfiguration.Builder serviceClientConfiguration =
                Validate.isInstanceOf(S3ServiceClientConfiguration.Builder.class,
                        config,
                        "Expecting the plugin to be only "
                                + "configured on s3 clients");

        S3ControlAsyncClient s3ControlAsyncClient = S3ControlAsyncClient.builder()
                .credentialsProvider(serviceClientConfiguration.credentialsProvider())
                .region(serviceClientConfiguration.region()).build();

        serviceClientConfiguration.authSchemeProvider(new S3AccessGrantsAuthSchemeProvider(serviceClientConfiguration.authSchemeProvider()));

        S3AccessGrantsCachedCredentialsProvider cache = isCacheEnabled.isPresent() ? isCacheEnabled.get() ? createAccessGrantsCache(s3ControlAsyncClient) : null : createAccessGrantsCache(s3ControlAsyncClient);

        serviceClientConfiguration.credentialsProvider(new S3AccessGrantsIdentityProvider(serviceClientConfiguration.credentialsProvider(),
                serviceClientConfiguration.region(),
                this.accountId,
                this.privilege,
                this.isCacheEnabled,
                s3ControlAsyncClient,
                cache
                ));

    }

    private S3AccessGrantsCachedCredentialsProvider createAccessGrantsCache(S3ControlAsyncClient s3ControlAsyncClient) {
        // TODO: remove after the cache starts supporting async clients

        return S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .S3ControlAsyncClient(s3ControlAsyncClient)
                .build();

    }

    @Override
    public Builder toBuilder() {
        return new BuilderImpl(this);
    }

    public static final class BuilderImpl implements Builder{

        private String accountId;

        private Optional<Privilege> privilege;

        private Optional<Boolean> isCacheEnabled;

        /** Initializing access grants plugin instance. By default, customers do not need to specify any of the optional
         * customizations, default values will be used instead for these missing customizations.
         * Default values - {@link S3AccessGrantsUtils}}*/
        BuilderImpl() {
            this.accountId = null;
            this.privilege = Optional.of(DEFAULT_PRIVILEGE_FOR_PLUGIN);
            this.isCacheEnabled = Optional.of(DEFAULT_CACHE_SETTING);
        }

        BuilderImpl(S3AccessGrantsPlugin plugin) {
            this.accountId = plugin.accountId;
            this.privilege = plugin.privilege;
            this.isCacheEnabled = plugin.isCacheEnabled;
        }

        @Override
        public Builder accountId(@NotNull String accountId) {
            this.accountId = accountId;
            return this;
        }

        @Override
        public Builder privilege(@NotNull Privilege privilege) {
            this.privilege = Optional.ofNullable(privilege);
            return this;
        }

        @Override
        public Builder cacheEnabled(@NotNull Boolean isCacheEnabled) {
            this.isCacheEnabled = Optional.ofNullable(isCacheEnabled);
            return this;
        }

        @Override
        public S3AccessGrantsPlugin build() {
            return new S3AccessGrantsPlugin(this);
        }

    }
}




