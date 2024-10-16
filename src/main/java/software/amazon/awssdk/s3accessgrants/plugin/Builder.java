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

import software.amazon.awssdk.services.s3control.model.Privilege;
import software.amazon.awssdk.utils.builder.CopyableBuilder;

public interface Builder extends CopyableBuilder<Builder, S3AccessGrantsPlugin> {
    Builder enableFallback(Boolean choice);
    Builder userAgent (String userAgent);
    
}
