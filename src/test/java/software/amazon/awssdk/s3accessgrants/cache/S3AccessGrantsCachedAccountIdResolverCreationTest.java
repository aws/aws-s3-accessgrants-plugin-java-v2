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

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.DEFAULT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS;
import static software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsConstants.DEFAULT_ACCOUNT_ID_MAX_CACHE_SIZE;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;

public class S3AccessGrantsCachedAccountIdResolverCreationTest {
    private S3ControlAsyncClient S3ControlAsyncClient;

    @Before
    public void setup() {
        S3ControlAsyncClient = Mockito.mock(S3ControlAsyncClient.class);
    }

    @Test
    public void create_DefaultResolver_via_Constructor() {
        // When
        S3AccessGrantsCachedAccountIdResolver resolver = new S3AccessGrantsCachedAccountIdResolver();
        // Then
        assertThat(resolver).isNotNull();
        assertThat(resolver.maxCacheSize()).isEqualTo(DEFAULT_ACCOUNT_ID_MAX_CACHE_SIZE);
        assertThat(resolver.expireCacheAfterWriteSeconds()).isEqualTo(DEFAULT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS);
    }

    @Test
    public void create_Resolver_via_Builder() {
        assertThatNoException().isThrownBy(() -> S3AccessGrantsCachedAccountIdResolver
            .builder()
            .build());
    }

    @Test
    public void create_Resolver_without_S3ControlAsyncClient_via_Builder() {
        // Given
        S3ControlAsyncClient = null;
        //Then
        assertThatNoException().isThrownBy(() -> S3AccessGrantsCachedAccountIdResolver
            .builder()
            .build());
    }

    @Test
    public void create_DefaultResolver_via_Builder() {
        // When
        S3AccessGrantsCachedAccountIdResolver resolver = S3AccessGrantsCachedAccountIdResolver
            .builder()
            .build();
        // Then
        assertThat(resolver).isNotNull();
        assertThat(resolver.maxCacheSize()).isEqualTo(DEFAULT_ACCOUNT_ID_MAX_CACHE_SIZE);
        assertThat(resolver.expireCacheAfterWriteSeconds()).isEqualTo(DEFAULT_ACCOUNT_ID_EXPIRE_CACHE_AFTER_WRITE_SECONDS);
    }

    @Test
    public void create_FullCustomizedResolver() {
        // Given
        int customMaxCacheSize = 2_000;
        int customExpireCacheAfterWriteSeconds = 3600;
        // When
        S3AccessGrantsCachedAccountIdResolver resolver = S3AccessGrantsCachedAccountIdResolver
            .builder()
            .maxCacheSize(customMaxCacheSize)
            .expireCacheAfterWriteSeconds(customExpireCacheAfterWriteSeconds)
            .build();
        // Then
        assertThat(resolver).isNotNull();
        assertThat(resolver.maxCacheSize()).isEqualTo(customMaxCacheSize);
        assertThat(resolver.expireCacheAfterWriteSeconds()).isEqualTo(customExpireCacheAfterWriteSeconds);
    }

    @Test
    public void create_CustomizedResolver_exceeds_MaxCacheSize() {
        // Given
        int customMaxCacheSize = 2_000_000;
        // Then
        assertThatIllegalArgumentException().isThrownBy(() -> S3AccessGrantsCachedAccountIdResolver
            .builder()
            .maxCacheSize(customMaxCacheSize)
            .build());
    }

    @Test
    public void create_CustomizedResolver_exceeds_ExpireCacheAfterWriteSeconds() {
        // Given
        int customExpireCacheAfterWriteSeconds = 3_000_000;
        // Then
        assertThatIllegalArgumentException().isThrownBy(() -> S3AccessGrantsCachedAccountIdResolver
            .builder()
            .expireCacheAfterWriteSeconds(customExpireCacheAfterWriteSeconds)
            .build());
    }

    @Test
    public void copy_Resolver() {
        // Given
        int customMaxCacheSize = 2_000;
        int customExpireCacheAfterWriteSeconds = 3600;
        // When
        S3AccessGrantsCachedAccountIdResolver resolver = S3AccessGrantsCachedAccountIdResolver
            .builder()
            .maxCacheSize(customMaxCacheSize)
            .expireCacheAfterWriteSeconds(customExpireCacheAfterWriteSeconds)
            .build();
        S3AccessGrantsCachedAccountIdResolver copy = resolver.toBuilder().build();
        // Then
        assertThat(copy).isNotNull();
        assertThat(copy.maxCacheSize()).isEqualTo(customMaxCacheSize);
        assertThat(copy.expireCacheAfterWriteSeconds()).isEqualTo(customExpireCacheAfterWriteSeconds);
    }
}
