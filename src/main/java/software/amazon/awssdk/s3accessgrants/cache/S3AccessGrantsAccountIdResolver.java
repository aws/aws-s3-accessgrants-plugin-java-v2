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

import software.amazon.awssdk.services.s3control.model.S3ControlException;

public interface S3AccessGrantsAccountIdResolver {
    /**
     *
     * @param accountId AWS AccountId from the request context parameter
     * @param s3Prefix e.g., s3://bucket-name/path/to/helloworld.txt
     * @return AWS AccountId of the S3 Access Grants Instance that owns the location scope of the s3Prefix
     * @throws S3ControlException propagate S3ControlException from service call
     */
    String resolve(String accountId, String s3Prefix) throws S3ControlException;
}
