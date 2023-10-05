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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.time.Duration;
import software.amazon.awssdk.services.s3control.S3ControlClient;
import software.amazon.awssdk.services.s3control.endpoints.internal.Arn;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixRequest;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceForPrefixResponse;

/**
 * A loading cache accountId resolver
 */
public class S3AccessGrantsCachedAccountIdResolver implements S3AccessGrantsAccountIdResolver {

    private S3ControlClient s3ControlClient;
    private LoadingCache<String, String> cache = Caffeine.newBuilder()
                                                         .maximumSize(10_000)
                                                         .expireAfterWrite(Duration.ofDays(30))
                                                         .build(s3Prefix -> resolveFromService(s3Prefix));

    public S3AccessGrantsCachedAccountIdResolver(S3ControlClient s3ControlClient) {
        this.s3ControlClient = s3ControlClient;
    }

    /**
     *
     * @param s3Prefix e.g., s3://bucket-name/path/to/helloworld.txt
     * @return resolved accountId from loading cache
     */
    public String resolve(String s3Prefix) {
        return cache.get(s3Prefix);
    }

    /**
     *
     * @param s3Prefix e.g., s3://bucket-name/path/to/helloworld.txt
     * @return accountId from the service response
     */
    private String resolveFromService(String s3Prefix) {
        GetAccessGrantsInstanceForPrefixResponse accessGrantsInstanceForPrefix =
            s3ControlClient.getAccessGrantsInstanceForPrefix(GetAccessGrantsInstanceForPrefixRequest.builder().s3Prefix(s3Prefix).build());
        String accessGrantsInstanceArn = accessGrantsInstanceForPrefix.accessGrantsInstanceArn();
        return Arn.parse(accessGrantsInstanceArn).get().accountId();
    }
}
