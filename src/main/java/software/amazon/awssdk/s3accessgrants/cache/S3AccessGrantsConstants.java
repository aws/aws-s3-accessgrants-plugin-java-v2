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

public class S3AccessGrantsConstants {
    public static final int DEFAULT_ACCOUNT_ID_MAX_CACHE_SIZE = 1_000;
    public static final int MAX_LIMIT_ACCOUNT_ID_MAX_CACHE_SIZE = 1_000_000;
    public static final int DEFAULT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS = 6_00; // 10 mins
    public static final int MAX_LIMIT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS = 2_592_000; // 30 days

    public static final int DEFAULT_ACCESS_GRANTS_MAX_CACHE_SIZE = 30_000;
    public static final int MAX_LIMIT_ACCESS_GRANTS_MAX_CACHE_SIZE = 1_000_000;
    public static final int CACHE_EXPIRATION_TIME_PERCENTAGE = 90;

    public static final int ACCESS_DENIED_CACHE_SIZE = 3_000;

    public static final int BUCKET_REGION_CACHE_SIZE = 1_000;

    public static final int MAX_BUCKET_REGION_CACHE_SIZE = 1_000_000;

    public static final int MAX_BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS = 6_00; // 10 mins

    public static final int BUCKET_REGION_EXPIRE_CACHE_AFTER_WRITE_SECONDS= 3_00; // 5 mins

}
