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
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.services.s3control.model.S3ControlException;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsOperationToPermissionMapper;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsStaticOperationToPermissionMapper;

public class S3AccessGrantsStaticOperationToPermissionMapperTests {

    private final String GET_OPERATION_NAME = "GetObject";
    private final String PUT_OPERATION_NAME = "PutObject";
    private final String CREATE_OPERATION_NAME = "CreateBucket";

    private final String DELETE_OPERATION_NAME = "DeleteObject";
    private static S3AccessGrantsOperationToPermissionMapper mapper;

    @BeforeClass
    public static void setUp() {
        mapper = new S3AccessGrantsStaticOperationToPermissionMapper();
    }

    @Test
    public void call_get_permission_with_invalid_operation() {
        Assertions.assertThatThrownBy(()->mapper.getPermission(CREATE_OPERATION_NAME)).isInstanceOf(SdkServiceException.class);
    }

    @Test
    public void call_get_permission_with_null_operation() {
        Assertions.assertThatThrownBy(()->mapper.getPermission(null)).isInstanceOf(IllegalArgumentException.class);
    }


    @Test
    public void call_get_permission_with_valid_operation() {
        Assertions.assertThatNoException().isThrownBy(()->mapper.getPermission(GET_OPERATION_NAME));
    }

    @Test
    public void call_get_permission_with_valid_operation_maps_to_correct_permission() {
        Assertions.assertThat(mapper.getPermission(GET_OPERATION_NAME)).isEqualTo(Permission.READ);
        Assertions.assertThat(mapper.getPermission(PUT_OPERATION_NAME)).isEqualTo(Permission.WRITE);
        Assertions.assertThat(mapper.getPermission(DELETE_OPERATION_NAME)).isEqualTo(Permission.WRITE);
    }


}
