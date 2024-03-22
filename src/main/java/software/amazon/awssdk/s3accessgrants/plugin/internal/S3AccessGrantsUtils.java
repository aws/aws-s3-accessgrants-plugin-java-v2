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

package software.amazon.awssdk.s3accessgrants.plugin.internal;

import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.identity.spi.IdentityProperty;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3control.model.Privilege;
import software.amazon.awssdk.utils.Logger;
import software.amazon.awssdk.utils.Validate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The class is for defining all the utilities and constants to be used across the package
 * */
public class S3AccessGrantsUtils {

    public static Logger logger = Logger.loggerFor("software.amazon.awssdk.s3accessgrants");
    public static final IdentityProperty PREFIX_PROPERTY = IdentityProperty.create(String.class, "S3Prefix");
    public static final IdentityProperty OPERATION_PROPERTY = IdentityProperty.create(String.class,"Operation");
    public static final IdentityProperty BUCKET_LOCATION_PROPERTY = IdentityProperty.create(Region.class, "BucketLocation");

    public static final IdentityProperty AUTH_EXCEPTIONS_PROPERTY = IdentityProperty.create(SdkServiceException.class, "AuthExceptions");

    public static final IdentityProperty PERMISSION_PROPERTY = IdentityProperty.create(String.class, "PermissionProperty");

    public static String CONTACT_TEAM_MESSAGE_TEMPLATE = "An internal exception has occurred. Valid %s was not passed to the %s. Please contact S3 access grants plugin team!";

    public static final Boolean DEFAULT_CACHE_SETTING = true;

    public static final Privilege DEFAULT_PRIVILEGE_FOR_PLUGIN = Privilege.DEFAULT;

    public static final Boolean DEFAULT_FALLBACK_SETTING = false;

    public static final Boolean DEFAULT_CROSS_REGION_ACCESS_SETTING = false;

    public static void argumentNotNull(Object param, String message) {
        try{
            Validate.notNull(param, message);
        } catch (NullPointerException e) {
            logger.error(() -> message);
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Gets the Lowest Common Ancestor (LCA) for all S3 resources defined.
     * This allows us to combine credentials to access paths like "s3://a/b/c" and "s3://a/b/d"
     * using a singular set of credentials (which may be required for calls like CopyObject or
     * DeleteObjects). In this case, the LCA of the example would be "s3://a/b" and we would use
     * that as our final resource to query S3AG.
     * @param s3Resources List of S3Resources we would like to find the LCA for
     * @return A singular S3 path that is the LCA of all the inputted resources.
     */
    public static String getLowestCommonAncestorPath(Collection<String> s3Resources) {
        Set<String> allBuckets =
            s3Resources.stream().map(S3AccessGrantsUtils::getBucketName).collect(Collectors.toSet());
        // If not all resources have the same bucket, we cannot have a common ancestor
        if (allBuckets.size() > 1) {
            String e = "[Finding LCA] LCA not possible due to multiple buckets found: " + allBuckets;
            logger.debug(() -> e);
            throw SdkServiceException.builder().message(e).build();
        }
        List<String> allPathPrefixes =
            s3Resources.stream().map(S3AccessGrantsUtils::getPrefix).collect(Collectors.toList());
        LinkedList<String> pathSoFar = new LinkedList<>();
        // Find the directory split by "/" and iterate through all splits until
        // we find one that doesn't apply to all resources
        String[] firstPathSplit = allPathPrefixes.get(0).split("/");
        logger.debug(() -> "[Finding LCA] All resources: " + allPathPrefixes);
        for (String fragment : firstPathSplit) {
            pathSoFar.addLast(fragment);
            String currentAncestor = String.join("/", pathSoFar);
            logger.debug(() -> "[Finding LCA] Checking prefix " + currentAncestor);
            if (!allPathPrefixes.stream().allMatch(resource -> resource.startsWith(currentAncestor))) {
                pathSoFar.removeLast();
                break;
            }
        }
        // Add the S3 bucket to the path and join the result
        pathSoFar.addFirst("s3://" + allBuckets.stream().findFirst().get());
        return String.join("/", pathSoFar);
    }

    /**
     * Get the S3 Bucket Name from a S3 Path. For example, the S3 path
     * s3://a-bucket/b/c/d.txt would resolve to `a-bucket`.
     * @param s3Resource String of s3 path we would like to find the S3 bucket name for
     * @return A String containing the extracted bucket name
     */
    private static String getBucketName(String s3Resource) {
        return s3Resource.substring(5).split("/")[0];
    }

    /**
     * Get the S3 Prefix from a S3 Path. For example, the S3 path
     * s3://a-bucket/b/c/d.txt would resolve to `b/c/d.txt`.
     * @param s3Resource String of s3 path we would like to find the S3 prefix for
     * @return A String containing the extracted prefix
     */
    private static String getPrefix(String s3Resource) {
        return s3Resource.substring(5).split("/", 2)[1];
    }

}
