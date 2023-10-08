package software.amazon.awssdk.s3accessgrants.plugin.internal;

import java.util.HashMap;
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.services.s3control.model.S3ControlException;

public class S3AccessGrantsStaticOperationToPermissionMapper implements S3AccessGrantsOperationToPermissionMapper{

    public static HashMap<String, Permission> supportedAccessGrantsOperations =  new HashMap<>();

    static {
        supportedAccessGrantsOperations.put("GETOBJECT", Permission.READ);
        supportedAccessGrantsOperations.put("GETOBJECTACL", Permission.READ);
        supportedAccessGrantsOperations.put("LISTMULTIPARTUPLOADS", Permission.READ);
        supportedAccessGrantsOperations.put("LISTOBJECTS", Permission.READ);

        supportedAccessGrantsOperations.put("PUTOBJECT", Permission.WRITE);
        supportedAccessGrantsOperations.put("PUTOBJECTACL", Permission.WRITE);
        supportedAccessGrantsOperations.put("DELETEOBJECT", Permission.WRITE);
        supportedAccessGrantsOperations.put("ABORTMULTIPARTUPLOADS", Permission.WRITE);

        supportedAccessGrantsOperations.put("DECRYPT", Permission.READ);
        supportedAccessGrantsOperations.put("GENERATEDATAKEY", Permission.WRITE);

    }
    @Override
    public Permission getPermission(String operation) throws S3ControlException {
        if (supportedAccessGrantsOperations.containsKey(operation.toUpperCase())) {
            return supportedAccessGrantsOperations.get(operation.toUpperCase());
        }

        throw new UnsupportedOperationException("Access Grants does not support the requested operation!");
    }
}
