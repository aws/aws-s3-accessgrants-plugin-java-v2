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

import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.services.s3control.model.Permission;

import java.util.Objects;

public class CacheKey {

    final AwsCredentialsIdentity credentials;
    final Permission permission;
    final String s3Prefix;

    private CacheKey(AwsCredentialsIdentity credentials, Permission permission, String s3Prefix) {

        this.credentials = credentials;
        this.permission = permission;
        this.s3Prefix = s3Prefix;
    }

    public Builder toBuilder() {
        return new BuilderImpl(this);
    }

    public static Builder builder() {
        return new BuilderImpl();
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

    public interface Builder {
        CacheKey build();

        Builder credentials(AwsCredentialsIdentity credentials);

        Builder permission(Permission permission);

        Builder s3Prefix(String s3Prefix);

    }

    static final class BuilderImpl implements Builder {
        private AwsCredentialsIdentity credentials;
        private Permission permission;
        private String s3Prefix;

        private BuilderImpl() {
        }

        public BuilderImpl(CacheKey CacheKey) {
            credentials(CacheKey.credentials);
            permission(CacheKey.permission);
            s3Prefix(CacheKey.s3Prefix);
        }

        @Override
        public CacheKey build() {
            return new CacheKey(credentials, permission, s3Prefix);
        }

        @Override
        public Builder credentials(AwsCredentialsIdentity credentials) {
            this.credentials = credentials;
            return this;
        }

        @Override
        public Builder permission(Permission permission) {
            this.permission = permission;
            return this;
        }

        @Override
        public Builder s3Prefix(String s3Prefix) {
            this.s3Prefix = s3Prefix;
            return this;
        }
    }


}
