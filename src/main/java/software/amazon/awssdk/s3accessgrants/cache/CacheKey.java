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

package software.amazon.awssdk.s3accessgrants.cache;

import java.util.Objects;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.services.s3control.model.Permission;

public class CacheKey {

    final AwsCredentialsIdentity credentials;
    final Permission permission;
    final String s3Prefix;

    public CacheKey(AwsCredentialsIdentity credentials, Permission permission, String s3Prefix) {

        this.credentials = credentials;
        this.permission = permission;
        this.s3Prefix = s3Prefix;
    }

    public CacheKey(CacheKey cacheKey, String s3Prefix) {

        this.credentials = cacheKey.getAWSCredentials();
        this.permission = cacheKey.getPermission();
        this.s3Prefix = s3Prefix;
    }

    public CacheKey(CacheKey cacheKey, Permission permission) {

        this.credentials = cacheKey.getAWSCredentials();
        this.permission = permission;
        this.s3Prefix = cacheKey.getS3Prefix();
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CacheKey cacheKey = (CacheKey) o;
        return Objects.equals(credentials, cacheKey.credentials) &&
               Objects.equals(s3Prefix, cacheKey.s3Prefix) &&
               Objects.equals(permission, cacheKey.permission);
    }

    @Override
    public int hashCode() {

        return Objects.hash(credentials, permission, s3Prefix);
    }

    public String getS3Prefix() {

        return s3Prefix;
    }

    public AwsCredentialsIdentity getAWSCredentials() {

        return credentials;
    }

    public Permission getPermission() {

        return permission;
    }
}
