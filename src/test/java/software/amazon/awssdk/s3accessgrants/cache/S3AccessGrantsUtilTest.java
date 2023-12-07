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

import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_BUCKET;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsTestConstants.TEST_S3_PREFIX;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsUtil.getBucketName;

public class S3AccessGrantsUtilTest {

    @Test
    public void getBucketName_from_s3Prefix() {
        // When
        String bucketName = getBucketName(TEST_S3_PREFIX);
        // Then
        assertThat(bucketName).isEqualTo(TEST_S3_BUCKET);
    }

}