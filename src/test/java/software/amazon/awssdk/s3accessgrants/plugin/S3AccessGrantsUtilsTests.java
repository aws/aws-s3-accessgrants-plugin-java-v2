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

package software.amazon.awssdk.s3accessgrants.plugin;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils;

import java.util.ArrayList;

public class S3AccessGrantsUtilsTests {
    @Test
    public void test_lowest_common_ancestor_when_ancestor_exists() {
        ArrayList<String> paths = new ArrayList<String>();
        paths.add("s3://test-bucket/path1/path2/path3/some.txt");
        paths.add("s3://test-bucket/path1/path2/someother.txt");
        paths.add("s3://test-bucket/path1/path2/path99/more.txt");
        paths.add("s3://test-bucket/path1/path2/");

        Assert.assertEquals("s3://test-bucket/path1/path2", S3AccessGrantsUtils.getLowestCommonAncestorPath(paths));
    }

    @Test
    public void test_lowest_common_ancestor_when_ancestor_does_not_exist() {
        ArrayList<String> paths = new ArrayList<String>();
        paths.add("s3://test-bucket/path1/path2/path3/some.txt");
        paths.add("s3://test-bucket/path1/path2/someother.txt");
        paths.add("s3://random-bucket/path1/path2/path99/more.txt");
        paths.add("s3://test-bucket/path1/path2/");

        Assertions.assertThatThrownBy(()->S3AccessGrantsUtils.getLowestCommonAncestorPath(paths)).isInstanceOf(SdkServiceException.class);
    }

    @Test
    public void test_lowest_common_ancestor_when_ancestor_is_only_bucket() {
        ArrayList<String> paths = new ArrayList<String>();
        paths.add("s3://test-bucket/path1/path2/path3/some.txt");
        paths.add("s3://test-bucket/path1/path2/someother.txt");
        paths.add("s3://test-bucket/path98/path99/more.txt");
        paths.add("s3://test-bucket/path1/path2/");

        Assert.assertEquals("s3://test-bucket", S3AccessGrantsUtils.getLowestCommonAncestorPath(paths));
    }

    @Test
    public void test_lowest_common_ancestor_when_ancestor_is_full_path() {
        String path = "s3://test-bucket/path1/path2/path3/some.txt";
        ArrayList<String> paths = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            paths.add(path);
        }

        Assert.assertEquals(path, S3AccessGrantsUtils.getLowestCommonAncestorPath(paths));
    }

    @Test
    public void test_lowest_common_ancestor_when_singular_path() {
        String path = "s3://test-bucket/path1/path2/path3/some.txt";
        ArrayList<String> paths = new ArrayList<String>();
        paths.add(path);

        Assert.assertEquals(path, S3AccessGrantsUtils.getLowestCommonAncestorPath(paths));
    }
}
