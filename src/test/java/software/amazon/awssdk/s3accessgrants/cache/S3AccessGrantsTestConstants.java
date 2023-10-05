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

public class S3AccessGrantsTestConstants {
    public static final String TEST_S3_PREFIX = "s3://bucket-name/path/to/helloworld.txt";
    public static final String TEST_S3_ACCESSGRANTS_ACCOUNT = "123456789012";
    public static final String TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT = "default";
    public static final String TEST_S3_ACCESSGRANTS_INSTANCE_ARN = "arn:aws:s3:us-east-2:"
                                                                   + TEST_S3_ACCESSGRANTS_ACCOUNT + ":access-grants/"
                                                                   + TEST_S3_ACCESSGRANTS_INSTANCE_DEFAULT;
}
