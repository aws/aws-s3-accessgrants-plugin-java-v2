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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedBucketRegionResolver;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsOperationToPermissionMapper;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsStaticOperationToPermissionMapper;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils;
import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeParams;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;
import software.amazon.awssdk.services.s3control.model.Permission;

import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PERMISSION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.BUCKET_LOCATION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.logger;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.CONTACT_TEAM_MESSAGE_TEMPLATE;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.DEFAULT_CROSS_REGION_ACCESS_SETTING;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.AUTH_EXCEPTIONS_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.getCommonPrefixFromMultiplePrefixes;


/**
 * This is an Auth Scheme Provider for S3 access grants.
 * It uses a default auth scheme configured on S3 Clients and appends parameters specifically required for access grants to resolve a request.
 * The auth scheme provider takes a set of parameters using.
 * {@link S3AuthSchemeParams}, and resolves to a list of {@link AuthSchemeOption} based on the given parameters.
 */
public class S3AccessGrantsAuthSchemeProvider implements S3AuthSchemeProvider {

    private final S3AuthSchemeProvider authSchemeProvider;
    private final S3Client s3Client;

    private final Boolean isCrossRegionAccessEnabled;

    private final S3AccessGrantsOperationToPermissionMapper permissionMapper;

    private final S3AccessGrantsCachedBucketRegionResolver bucketRegionCache;

    S3AccessGrantsAuthSchemeProvider(@NotNull S3AuthSchemeProvider authSchemeProvider, S3Client s3Client, Boolean isCrossRegionAccessEnabled) {
        S3AccessGrantsUtils.argumentNotNull(authSchemeProvider,
                "Expecting an Auth Scheme Provider to be specified while configuring S3Clients!");
        S3AccessGrantsUtils.argumentNotNull(s3Client, String.format(CONTACT_TEAM_MESSAGE_TEMPLATE, "S3 Client", "Plugin"));
        this.authSchemeProvider = authSchemeProvider;
        this.s3Client = s3Client;
        this.isCrossRegionAccessEnabled = isCrossRegionAccessEnabled == null ? DEFAULT_CROSS_REGION_ACCESS_SETTING : isCrossRegionAccessEnabled;
        this.permissionMapper = new S3AccessGrantsStaticOperationToPermissionMapper();
        this.bucketRegionCache = S3AccessGrantsCachedBucketRegionResolver.builder().s3Client(s3Client).build();
    }

    /**
     * resolves to an auth scheme based on a set of input parameters.
     * @param authSchemeParams parameters necessary to determine auth scheme to use for resolving the request.
     * @return a list of auth schemes that can be used for resolving the request.
     */
    @Override
    public List<AuthSchemeOption> resolveAuthScheme(@NotNull S3AuthSchemeParams authSchemeParams) {
        S3AccessGrantsUtils.argumentNotNull(authSchemeParams,
                "An internal exception has occurred. Valid auth scheme params were not passed to the Auth Scheme Provider. Please contact the S3 Access Grants plugin team!");

        List<AuthSchemeOption> availableAuthSchemes = authSchemeProvider.resolveAuthScheme(authSchemeParams);

        try {
            String operation = authSchemeParams.operation();
            logger.debug(() -> "operation : " + operation);
            final Permission permission = permissionMapper.getPermission(operation);

            S3AccessGrantsUtils.argumentNotNull(authSchemeParams.bucket(), "Please specify a valid bucket name for the operation!");

            final Region destinationRegion = getBucketLocation(authSchemeParams.bucket());

            logger.debug(() -> "Access Grants requests will be sent to the region "+destinationRegion);

            String S3Prefix;
            if (operation.equalsIgnoreCase("DELETEOBJECTS")) {
                S3Prefix = "s3://" + authSchemeParams.bucket() + getCommonPrefixFromMultiplePrefixes(authSchemeParams.deleteObjectKeys());
            }
            else if (operation.equalsIgnoreCase("COPYOBJECT")) {
                String[] copySourceArray = authSchemeParams.copySource().split("/", 2);
                if (!copySourceArray[0].equals(authSchemeParams.bucket())) {
                    throw SdkServiceException.builder().message("The requested operation cannot be completed!").statusCode(404).cause(new RuntimeException("Source bucket and destination bucket must be same")).build();
                }
                List<String> copyObjectKeys = new ArrayList<>();
                if (authSchemeParams.key() != null && !(authSchemeParams.key().isEmpty())){
                    copyObjectKeys.add(authSchemeParams.key());
                }
                copyObjectKeys.add(copySourceArray[1]);
                S3Prefix = "s3://" + authSchemeParams.bucket() + getCommonPrefixFromMultiplePrefixes(copyObjectKeys);
            }
            else {
                S3Prefix = "s3://" + authSchemeParams.bucket() + "/" + getKeyIfExists(authSchemeParams);
            }
            return availableAuthSchemes.stream()
                    .map(authScheme -> authScheme.toBuilder()
                            .putIdentityProperty(PREFIX_PROPERTY,
                                    S3Prefix)
                            .putIdentityProperty(BUCKET_LOCATION_PROPERTY, destinationRegion)
                            .putIdentityProperty(PERMISSION_PROPERTY, permission)
                            .build()
                    )
                    .collect(Collectors.toList());
        } catch (SdkServiceException e) {
            return availableAuthSchemes.stream()
                    .map(authScheme -> authScheme.toBuilder().putIdentityProperty(AUTH_EXCEPTIONS_PROPERTY, e).build())
                    .collect(Collectors.toList());
        }
    }

    private String getKeyIfExists(S3AuthSchemeParams authSchemeParams) {

        Boolean keyDoesNotExists = (authSchemeParams.key() == null || authSchemeParams.key().isEmpty())
                && (authSchemeParams.prefix() == null || authSchemeParams.prefix().isEmpty());

        String validKey = !(authSchemeParams.key() == null || authSchemeParams.key().isEmpty()) ? authSchemeParams.key() :
                          !(authSchemeParams.prefix() == null || authSchemeParams.prefix().isEmpty()) ? authSchemeParams.prefix() : null;

        if(keyDoesNotExists) logger.debug(() -> "no object key was specified for the operation!");

        return keyDoesNotExists ? "*" : validKey;

    }

    /**
     * Fetch the location where the bucket is created.
     * This is to ensure that the Access Grants requests are made to the correct region.
     * @param bucketName bucket name in the users request
     * @return Region where the S3 bucket exists
     */
    private Region getBucketLocation(String bucketName) {

        if(isCrossRegionAccessEnabled) {
            return bucketRegionCache.resolve(bucketName);
        }

        S3AccessGrantsUtils.argumentNotNull(s3Client.serviceClientConfiguration().region(), "Expecting a region to be configured on the S3Clients!");
        return s3Client.serviceClientConfiguration().region();

    }

}
