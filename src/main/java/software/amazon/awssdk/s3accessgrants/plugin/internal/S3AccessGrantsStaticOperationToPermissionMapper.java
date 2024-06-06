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
        supportedAccessGrantsOperations.put("DELETEOBJECTS", Permission.WRITE);
        supportedAccessGrantsOperations.put("ABORTMULTIPARTUPLOAD", Permission.WRITE);
        supportedAccessGrantsOperations.put("CREATEMULTIPARTUPLOAD", Permission.WRITE);
        supportedAccessGrantsOperations.put("UPLOADPART", Permission.WRITE);
        supportedAccessGrantsOperations.put("COMPLETEMULTIPARTUPLOAD", Permission.WRITE);

        supportedAccessGrantsOperations.put("COPYOBJECT", Permission.READWRITE);

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
