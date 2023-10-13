package software.amazon.awssdk.s3accessgrants.plugin;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;
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
        Assertions.assertThatThrownBy(()->mapper.getPermission(CREATE_OPERATION_NAME)).isInstanceOf(S3ControlException.class);
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
