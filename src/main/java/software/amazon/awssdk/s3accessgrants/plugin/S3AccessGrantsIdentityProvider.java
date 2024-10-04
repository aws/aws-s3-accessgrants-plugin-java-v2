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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProvider;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClientBuilder;
import software.amazon.awssdk.services.s3control.model.Privilege;
import software.amazon.awssdk.services.s3control.model.S3ControlException;
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.utils.Validate;

import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PERMISSION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.AUTH_EXCEPTIONS_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.BUCKET_LOCATION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.logger;

/**
 * A {@link IdentityProvider} implementation for S3 access grants
 * The class provides functionality to get the credentials from S3 access grants
 * @author Shiva Kumar Mukkapati
 */
public class S3AccessGrantsIdentityProvider implements IdentityProvider<AwsCredentialsIdentity>{

    private final IdentityProvider<? extends AwsCredentialsIdentity> credentialsProvider;

    private final Privilege privilege;

    private final Boolean isCacheEnabled;

    private final S3ControlAsyncClientBuilder s3ControlBuilder;

    private final StsAsyncClient stsAsyncClient;

    private final S3AccessGrantsCachedCredentialsProvider cache;

    private final boolean enableFallback;

    private final MetricPublisher metricsPublisher;

    private final ConcurrentHashMap<Region, S3ControlAsyncClient> clientsCache;

    private AwsCredentialsIdentity cachedCredentials;

    private String cachedAccountId;

    private String CONTACT_TEAM_MESSAGE_TEMPLATE = "An internal exception has occurred. Valid %s was not passed to the %s. Please contact S3 access grants plugin team!";

    ClientOverrideConfiguration.Builder overrideConfig = ClientOverrideConfiguration.builder()
                    .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, "aws-s3-accessgrants-java-sdk-v2-plugin");

    public S3AccessGrantsIdentityProvider(@NotNull IdentityProvider<? extends AwsCredentialsIdentity> credentialsProvider,
                                          @NotNull StsAsyncClient stsAsyncClient,
                                          @NotNull Privilege privilege,
                                          @NotNull Boolean isCacheEnabled,
                                          @NotNull S3ControlAsyncClientBuilder s3ControlAsyncClientBuilder,
                                          @NotNull S3AccessGrantsCachedCredentialsProvider cache,
                                          @NotNull boolean enableFallback,
                                          @NotNull MetricPublisher metricsPublisher,
                                          @NotNull ConcurrentHashMap<Region, S3ControlAsyncClient> clientsCache) {
        S3AccessGrantsUtils.argumentNotNull(credentialsProvider, "Expecting an Identity Provider to be specified while configuring S3Clients!");
        S3AccessGrantsUtils.argumentNotNull(stsAsyncClient, String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "sts client", "identity provider"));
        S3AccessGrantsUtils.argumentNotNull(clientsCache, String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "client cache", "identity provider"));
        this.credentialsProvider = credentialsProvider;
        this.stsAsyncClient = stsAsyncClient;
        this.privilege = privilege;
        this.isCacheEnabled = isCacheEnabled;
        this.s3ControlBuilder = s3ControlAsyncClientBuilder;
        this.cache = cache;
        this.enableFallback = enableFallback;
        this.metricsPublisher = metricsPublisher;
        this.clientsCache = clientsCache;
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
     * @param resolveIdentityRequest The request to resolve an Identity
     * @return a completable future that will resolve to the credentials registered within a grant
     * @throws NullPointerException if a null pointer is encountered in the execution path
     * @throws S3ControlException for any service failures
     */
    @Override
    public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest resolveIdentityRequest) {

        CompletableFuture<? extends AwsCredentialsIdentity> userCredentials;

        try {

            if(resolveIdentityRequest != null && resolveIdentityRequest.property(AUTH_EXCEPTIONS_PROPERTY) != null) {
                throw (SdkServiceException) resolveIdentityRequest.property(AUTH_EXCEPTIONS_PROPERTY);
            }

            userCredentials = credentialsProvider.resolveIdentity(resolveIdentityRequest);
            validateRequestParameters(resolveIdentityRequest, privilege, isCacheEnabled);
            String accountId = getCallerAccountID(userCredentials);
            String S3Prefix = resolveIdentityRequest.property(PREFIX_PROPERTY).toString();
            Permission permission = Permission.fromValue(resolveIdentityRequest.property(PERMISSION_PROPERTY).toString());
            Region destinationRegion = Region.of(resolveIdentityRequest.property(BUCKET_LOCATION_PROPERTY).toString());

            S3ControlAsyncClient s3ControlAsyncClient;
            CompletableFuture<? extends AwsCredentialsIdentity> getDataAccessResponse;

            logger.debug(() -> " Call access grants with the following request params! ");
            logger.debug(() -> " S3Prefix : " + S3Prefix);
            logger.debug(() -> " caller accountID : " + accountId);
            logger.debug(() -> " permission : " + permission);
            logger.debug(() -> " bucket region : " + destinationRegion);

            if(clientsCache.containsKey(destinationRegion)) {
                getDataAccessResponse = getCredentialsFromCache(userCredentials.join(), permission, S3Prefix, accountId,  clientsCache.get(destinationRegion));
            } else {
                s3ControlAsyncClient = s3ControlBuilder.region(destinationRegion).overrideConfiguration(this.overrideConfig.build()).build();
                clientsCache.put(destinationRegion, s3ControlAsyncClient);
                getDataAccessResponse = getCredentialsFromCache(userCredentials.join(), permission, S3Prefix, accountId,  s3ControlAsyncClient);
            }
            return getDataAccessResponse;

        } catch(SdkServiceException e) {

            unwrapAndBuildException(e);
            if(shouldFallbackToDefaultCredentialsForThisCase(e.statusCode(), e.getCause())) {
                return credentialsProvider.resolveIdentity(resolveIdentityRequest);
            }
            throw e;
        }
    }

    /**
     * The class will communicate with the cache to fetch the credentials.
     * By default, requests are routed directly to the cache to handle the credentials fetching.
     */
    CompletableFuture<? extends AwsCredentialsIdentity> getCredentialsFromCache(AwsCredentialsIdentity credentials, Permission permission, String S3Prefix, String accountId, S3ControlAsyncClient s3ControlAsyncClient) {

        try {
            return cache.getDataAccess(credentials, permission, S3Prefix, accountId, s3ControlAsyncClient).exceptionally(e -> {
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
        try {
            metricsPublisher.publish(cache.getAccessGrantsMetrics().collect());
            metricsPublisher.close();
        } catch (Exception e) {
            logger.warn(() -> "Something went wrong while publishing metrics using the metrics publisher. Please contact S3 access grants plugin team!");
            logger.warn(() -> "cause for metrics publisher error : " + e.getMessage());
        }
    }

    private void validateRequestParameters(ResolveIdentityRequest resolveIdentityRequest, Privilege privilege, Boolean isCacheEnabled) {
        logger.debug(() -> "Validating the request parameters before sending a request to S3 Access grants!");
        S3AccessGrantsUtils.argumentNotNull(resolveIdentityRequest, String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "request", "identity provider"));
        S3AccessGrantsUtils.argumentNotNull(privilege, String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "privilege", "identity provider"));
        S3AccessGrantsUtils.argumentNotNull(isCacheEnabled, String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "cache setting", "identity provider"));
        Pattern pattern = Pattern.compile("s3://[a-z0-9.-]*");
        S3AccessGrantsUtils.argumentNotNull(resolveIdentityRequest.property(PREFIX_PROPERTY), String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "S3Prefix", "identity provider"));
        Validate.isTrue(pattern.matcher(resolveIdentityRequest.property(PREFIX_PROPERTY).toString()).find(), String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "S3Prefix", "identity provider"));
        S3AccessGrantsUtils.argumentNotNull(resolveIdentityRequest.property(BUCKET_LOCATION_PROPERTY), String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "request region", "identity provider"));
        S3AccessGrantsUtils.argumentNotNull(resolveIdentityRequest.property(PERMISSION_PROPERTY), String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "permission", "identity provider"));
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
     * For every request, if the caller credentials have been used previously, the accountID resolved for that credentials will be returned.
     * If a new set of credentials are being used, then a request will be forwarded to STS to fetch the caller accountID and cache it.
     * Each Identity provider is only going to cache one set of credentials/accountID at any point of time.
     * This should be a safe considering service clients can refer to only one set of credentials for each request.
     * @return a completableFuture containing response from STS.
     * */
    String getCallerAccountID(CompletableFuture<? extends AwsCredentialsIdentity> userCredentials) {
        AwsCredentialsIdentity credentials = userCredentials.join();
        if(credentials.equals(cachedCredentials)) {
            logger.debug(() -> "caller account cached, avoiding sending requests to STS");
            return cachedAccountId;
        }
        logger.debug(() -> "caller account not cached, requesting STS to fetch caller accountID!");
        cachedAccountId = stsAsyncClient.getCallerIdentity().join().account();
        cachedCredentials = credentials;
        return cachedAccountId;
    }
}
