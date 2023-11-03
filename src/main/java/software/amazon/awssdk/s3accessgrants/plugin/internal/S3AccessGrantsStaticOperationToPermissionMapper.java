package software.amazon.awssdk.s3accessgrants.plugin.internal;

import java.util.HashMap;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.services.s3control.model.S3ControlException;

public class S3AccessGrantsStaticOperationToPermissionMapper implements S3AccessGrantsOperationToPermissionMapper {

    private static final HashMap<String, Permission> supportedAccessGrantsOperations =  new HashMap<>();

    static {
        supportedAccessGrantsOperations.put("HEADOBJECT", Permission.READ);
        supportedAccessGrantsOperations.put("GETOBJECT", Permission.READ);
        supportedAccessGrantsOperations.put("GETOBJECTACL", Permission.READ);
        supportedAccessGrantsOperations.put("LISTMULTIPARTUPLOADS", Permission.READ);
        supportedAccessGrantsOperations.put("LISTOBJECTS", Permission.READ);
        supportedAccessGrantsOperations.put("LISTOBJECTSV2", Permission.READ);
        supportedAccessGrantsOperations.put("LISTOBJECTVERSIONS", Permission.READ);
        supportedAccessGrantsOperations.put("LISTPARTS", Permission.READ);

        supportedAccessGrantsOperations.put("PUTOBJECT", Permission.WRITE);
        supportedAccessGrantsOperations.put("PUTOBJECTACL", Permission.WRITE);
        supportedAccessGrantsOperations.put("DELETEOBJECT", Permission.WRITE);

        // below operation for DELETEOBJECTS is not supported yet because of usage of multiple prefixes. Leaving it here for future reference.
        // supportedAccessGrantsOperations.put("DELETEOBJECTS", Permission.WRITE);
        supportedAccessGrantsOperations.put("ABORTMULTIPARTUPLOAD", Permission.WRITE);
        supportedAccessGrantsOperations.put("CREATEMULTIPARTUPLOAD", Permission.WRITE);
        supportedAccessGrantsOperations.put("UPLOADPART", Permission.WRITE);
        supportedAccessGrantsOperations.put("COMPLETEMULTIPARTUPLOAD", Permission.WRITE);

        supportedAccessGrantsOperations.put("DECRYPT", Permission.READ);
        supportedAccessGrantsOperations.put("GENERATEDATAKEY", Permission.WRITE);

    }
    @Override
    public Permission getPermission(@NotNull String operation) throws SdkServiceException {

        S3AccessGrantsUtils.argumentNotNull(operation, "An internal exception has occurred. expecting operation to be specified for the request. Please contact S3 access grants plugin team!");
        if (supportedAccessGrantsOperations.containsKey(operation.toUpperCase())) {
            return supportedAccessGrantsOperations.get(operation.toUpperCase());
        }

        throw SdkServiceException.builder().message("The requested operation cannot be completed!").statusCode(404).cause(new UnsupportedOperationException("Access Grants does not support the requested operation!")).build();
    }
}
