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

import java.util.concurrent.CompletableFuture;
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
    CompletableFuture<AwsCredentialsIdentity> getDataAccess (AwsCredentialsIdentity credentials, Permission permission, String s3Prefix,
                                                             String accountId) throws Exception;

}

