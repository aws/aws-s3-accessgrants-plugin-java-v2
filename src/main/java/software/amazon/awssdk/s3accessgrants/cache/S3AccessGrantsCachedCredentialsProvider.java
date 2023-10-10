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

import java.time.Instant;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.services.s3control.model.S3ControlException;

public interface S3AccessGrantsCachedCredentialsProvider {

    /**
     * @param credentials Credentials used for calling Access Grants.
     * @param permission Permission requested by the user. Can be Read, Write, or ReadWrite.
     * @param s3Prefix S3Prefix requested by the user. e.g., s3://bucket-name/path/to/helloworld.txt
     * @return Credentials from Access Grants.
     * @throws S3ControlException in-case exception is cached.
     */
    AwsCredentialsIdentity getValueFromCache (AwsCredentialsIdentity credentials, Permission permission, String s3Prefix) throws S3ControlException;

    /**
     * @param credentials Credentials used for calling Access Grants.
     * @param permission Permission requested by the user. Can be Read, Write, or ReadWrite.
     * @param s3Prefix S3Prefix requested by the user. e.g., s3://bucket-name/path/to/helloworld.txt
     * @param exception Exception to be stored in cache.
     */
    void putValueInCache (AwsCredentialsIdentity credentials, Permission permission, String s3Prefix, S3ControlException exception);

    /**
     * @param credentials Credentials used for calling Access Grants.
     * @param permission Permission requested by the user. Can be Read, Write, or ReadWrite.
     * @param s3Prefix S3Prefix requested by the user. e.g., s3://bucket-name/path/to/helloworld.txt
     * @param sessionCredentials credentials returned by Access Grants.
     * @param expiration expiration time of the credentials returned by Access Grants.
     */
    void putValueInCache (AwsCredentialsIdentity credentials, Permission permission, String s3Prefix, AwsCredentialsIdentity sessionCredentials, Instant expiration);

    /**
     * Invalidates the cache.
     */
    void invalidateCache ();

}

