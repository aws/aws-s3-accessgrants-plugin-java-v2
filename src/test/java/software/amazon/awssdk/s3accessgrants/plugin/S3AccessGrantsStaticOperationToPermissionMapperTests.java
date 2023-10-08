package software.amazon.awssdk.s3accessgrants.plugin;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsOperationToPermissionMapper;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsStaticOperationToPermissionMapper;

public class S3AccessGrantsStaticOperationToPermissionMapperTests {

    private final String GET_OPERATION_NAME = "GetObject";
    private final String PUT_OPERATION_NAME = "PutObject";
    private final String CREATE_OPERATION_NAME = "CreateBucket";

    private final String DELETE_OPERATION_NAME = "DeleteObject";

    @Test
    public void call_get_permission_with_invalid_operation() {
        S3AccessGrantsOperationToPermissionMapper mapper = new S3AccessGrantsStaticOperationToPermissionMapper();
        Assertions.assertThatThrownBy(()->mapper.getPermission(CREATE_OPERATION_NAME)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void call_get_permission_with_valid_operation() {
        S3AccessGrantsOperationToPermissionMapper mapper = new S3AccessGrantsStaticOperationToPermissionMapper();
        Assertions.assertThatNoException().isThrownBy(()->mapper.getPermission(GET_OPERATION_NAME));
    }

    @Test
    public void call_get_permission_with_valid_operation_maps_to_correct_permission() {
        S3AccessGrantsOperationToPermissionMapper mapper = new S3AccessGrantsStaticOperationToPermissionMapper();
        Assertions.assertThat(mapper.getPermission(GET_OPERATION_NAME)).isEqualTo(Permission.READ);
        Assertions.assertThat(mapper.getPermission(PUT_OPERATION_NAME)).isEqualTo(Permission.WRITE);
        Assertions.assertThat(mapper.getPermission(DELETE_OPERATION_NAME)).isEqualTo(Permission.WRITE);
    }


}
