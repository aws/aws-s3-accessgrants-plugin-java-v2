package software.amazon.awssdk.s3accessgrants.plugin.internal;

import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.services.s3control.model.S3ControlException;

public interface S3AccessGrantsOperationToPermissionMapper {

    public Permission getPermission(String operation) throws S3ControlException;

}
