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
import java.util.List;

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

    public static String getCommonPrefixFromMultiplePrefixes(List<String> keys) {
        String commonAncestor = keys.get(0);
        String lastPrefix = "";
        for (String i : keys) {
            while(!commonAncestor.equals("")) {
                if (!i.startsWith(commonAncestor)){
                    int lastIndex = commonAncestor.lastIndexOf("/");
                    if (lastIndex == -1){
                        return "/";
                    }
                    lastPrefix = commonAncestor.substring(lastIndex+1);
                    commonAncestor = commonAncestor.substring(0, lastIndex);
                } else {
                    break;
                }
            }
        }

        String newCommonAncestor = commonAncestor + "/" + lastPrefix;
        for (String i : keys) {
            while(!lastPrefix.equals("")){
                if (!i.startsWith(newCommonAncestor)){
                    lastPrefix = lastPrefix.substring(0, lastPrefix.length()-1);
                    newCommonAncestor = commonAncestor + "/" + lastPrefix;
                }
                else{
                    break;
                }
            }
        }
        return "/" + newCommonAncestor;
    }

}
