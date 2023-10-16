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
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProvider;
import software.amazon.awssdk.s3accessgrants.cache.S3AccessGrantsCachedCredentialsProviderImpl;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeParams;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.services.s3control.model.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.OPERATION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;

/** Readme.md (will be moved to readme or contributing guide in the repo)
 * Before running the integration tests with your account be aware that the integration tests make requests to the AWS accounts
 * incurring costs to the account owner. Also, the resources created will be cleaned up after each test. That means additional
 * costs will be incurred for each time tests are run. A workaround we suggest would be to disable teardown method in {@link S3AccessGrantsIntegrationTestsUtils}
 * and
 * re-enable it when you are sure that you no longer need the resources.
 *
 * To run the integration tests, create a role with the name aws-s3-access-grants-sdk-plugin-integration-role in your account. Assume the credentials of this account you want to use. By default, we try to identify the
 * credentials from the 'aws-test-account' profile.
 *
 * Note - The role that is assumed to work with the tests needs to contain below actions at least for the tests to run properly.
 *                 "s3:PutObject",
 *                 "s3:GetObject",
 *                 "s3:DeleteObject",
 *                 "s3:ListBucket",
 *                 "s3:CreateBucket",
 *                 "s3:DeleteBucket",
 *                 "s3:GetDataAccess",
 *                 "s3:CreateAccessGrantsInstance",
 *                 "s3:CreateAccessGrants",
 *                 "s3:CreateAccessGrantsLocation",
 *                 "s3:ListAccessGrants",
 *                 "s3:GetAccessGrant",
 *                 "iam:CreateRole",
 *                 "iam:DeleteRole",
 *                 "iam:CreatePolicy"
 *
 */


public class S3AccessGrantsIntegrationTests {

    private final String TEST_ACCESS_KEY = "ARAGXXXXXXX123";

    private final String TEST_SECRET_KEY = "ARAGXXXXXXX123112e2e3aadadwefdscac";
    private final String TEST_SESSION_TOKEN = "AESAGAXXVAVACVCAKCCBCBCXXXXXXAKDHCXXXADKAKXXXXXXXXXABDASHJBXX";

    private static ProfileCredentialsProvider credentialsProvider;

    private static S3ControlAsyncClient s3ControlAsyncClient;

    private static S3AuthSchemeProvider authSchemeProvider;

    @BeforeClass
    public static void testsSetUp() throws IOException {

        S3AccessGrantsInstanceSetUpUtils.setUpAccessGrantsInstanceForTests();
        credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();
        s3ControlAsyncClient = S3AccessGrantsIntegrationTestsUtils.s3ControlAsyncClientBuilder(credentialsProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);
        authSchemeProvider = new S3AccessGrantsAuthSchemeProvider(S3AuthSchemeProvider.defaultProvider());

    }

    @AfterClass
    public static void tearDown() {
        if (!S3AccessGrantsIntegrationTestsUtils.DISABLE_TEAR_DOWN) {
            S3AccessGrantsInstanceSetUpUtils.tearDown();
        }
    }

    @Test
    public void call_s3_with_operation_not_supported_by_access_grants_request_failure() throws Exception {

        S3AccessGrantsCachedCredentialsProvider cache = spy(S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .S3ControlAsyncClient(s3ControlAsyncClient)
                .build());

        S3AccessGrantsIdentityProvider identityProvider =
                spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                        S3AccessGrantsIntegrationTestsUtils.TEST_REGION, S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                        S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                        S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                        s3ControlAsyncClient,
                        cache));

        String bucketName = "access-grants-sdk-create-test";

        S3Client s3client = S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, identityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        Throwable exc = Assertions.catchThrowableOfType(() -> S3AccessGrantsIntegrationTestsUtils.CreateBucket(s3client, bucketName), S3ControlException.class);

        Assertions.assertThat(((S3ControlException) exc).statusCode()).isEqualTo(404);

        verify(identityProvider, times(1)).resolveIdentity(any(ResolveIdentityRequest.class));

        verify(identityProvider, never()).getCredentialsFromCache(any(), any(), any(), any());

        verify(identityProvider, never()).getCredentialsFromAccessGrants(any());

        verify(cache, never()).getDataAccess(any(), any(), any(), any());

    }

    @Test
    public void call_s3_with_operation_supported_by_access_grants_request_success() throws Exception {

        S3AccessGrantsCachedCredentialsProvider cache = spy(S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .S3ControlAsyncClient(s3ControlAsyncClient)
                .build());

        S3AccessGrantsIdentityProvider identityProvider =
            spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                                                   s3ControlAsyncClient,
                                                   cache));

       S3Client s3client = S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, identityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

       ResponseInputStream<GetObjectResponse> responseInputStream = S3AccessGrantsIntegrationTestsUtils.GetObject(s3client,
                                                                                                                   S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                                                                                   S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT1);
       Assertions.assertThat(S3AccessGrantsIntegrationTestsUtils.getFileContentFromGetResponse(responseInputStream)).isEqualTo("access grants test content in file1!");

       Assertions.assertThat(S3AccessGrantsIntegrationTestsUtils.getStatusCodeFromGetResponse(responseInputStream)).isEqualTo(200);

       verify(identityProvider, times(1)).resolveIdentity(any(ResolveIdentityRequest.class));

       verify(identityProvider, times(1)).getCredentialsFromCache(any(), any(), any(), any());

       verify(cache, times(1)).getDataAccess(any(), any(), any(), any());

    }

    @Test
    public void call_s3_with_operation_supported_by_access_grants_request_success_cache_test() throws Exception {

        S3ControlAsyncClient s3ControlAsyncClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider cache = spy(S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .S3ControlAsyncClient(s3ControlAsyncClient)
                .build());

        S3AccessGrantsIdentityProvider identityProvider =
                spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                        S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                        S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                        S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                        S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                        s3ControlAsyncClient,
                        cache));
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        CompletableFuture<GetDataAccessResponse> getDataAccessResponse = CompletableFuture.supplyAsync(() -> GetDataAccessResponse.builder().credentials(Credentials.builder()
                    .accessKeyId(TEST_ACCESS_KEY)
                    .secretAccessKey(TEST_SECRET_KEY)
                    .sessionToken(TEST_SESSION_TOKEN)
                        .expiration(Instant.now().plusMillis(3000))
                    .build()).build());
        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse>  getAccessGrantsInstanceForPrefixResponse = CompletableFuture.supplyAsync(() -> GetAccessGrantsInstanceForPrefixResponse.builder()
                    .accessGrantsInstanceArn(S3AccessGrantsIntegrationTestsUtils.ACCESS_GRANTS_INSTANCE_ARN)
                    .accessGrantsInstanceId(S3AccessGrantsIntegrationTestsUtils.ACCESS_GRANTS_INSTANCE_ID).build());

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn("GetObject");
        when(s3ControlAsyncClient.getDataAccess(any(GetDataAccessRequest.class))).thenReturn(getDataAccessResponse);
        when(s3ControlAsyncClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(getAccessGrantsInstanceForPrefixResponse);

        AwsCredentialsIdentity credentialsIdentity = identityProvider.resolveIdentity(resolveIdentityRequest).join();

        Assertions.assertThat(credentialsIdentity.secretAccessKey()).isEqualTo(TEST_SECRET_KEY);
        Assertions.assertThat(credentialsIdentity.accessKeyId()).isEqualTo(TEST_ACCESS_KEY);
        verify(cache, times(1)).getDataAccess(any(), any(), any(), any());
        verify(s3ControlAsyncClient, times(1)).getDataAccess(any(GetDataAccessRequest.class));

        // resend the request and validate no interaction with data access request from the cache
        credentialsIdentity = identityProvider.resolveIdentity(resolveIdentityRequest).join();
        Assertions.assertThat(credentialsIdentity.secretAccessKey()).isEqualTo(TEST_SECRET_KEY);
        Assertions.assertThat(credentialsIdentity.accessKeyId()).isEqualTo(TEST_ACCESS_KEY);
        verify(cache, times(2)).getDataAccess(any(), any(), any(), any());
        verify(s3ControlAsyncClient, times(1)).getDataAccess(any(GetDataAccessRequest.class));

    }

    @Test
    public void call_s3_with_operation_supported_by_access_grants_request_failure_cache_test() throws Exception {

        S3ControlAsyncClient s3ControlAsyncClient = mock(S3ControlAsyncClient.class);
        S3AccessGrantsCachedCredentialsProvider cache = spy(S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .S3ControlAsyncClient(s3ControlAsyncClient)
                .build());

        S3AccessGrantsIdentityProvider identityProvider =
                spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                        S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                        S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                        S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                        S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                        s3ControlAsyncClient,
                        cache));
        ResolveIdentityRequest resolveIdentityRequest = mock(ResolveIdentityRequest.class);
        CompletableFuture<GetAccessGrantsInstanceForPrefixResponse>  getAccessGrantsInstanceForPrefixResponse = CompletableFuture.supplyAsync(() -> GetAccessGrantsInstanceForPrefixResponse.builder()
                .accessGrantsInstanceArn(S3AccessGrantsIntegrationTestsUtils.ACCESS_GRANTS_INSTANCE_ARN)
                .accessGrantsInstanceId(S3AccessGrantsIntegrationTestsUtils.ACCESS_GRANTS_INSTANCE_ID).build());

        when(resolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket");
        when(resolveIdentityRequest.property(OPERATION_PROPERTY)).thenReturn("GetObject");
        when(s3ControlAsyncClient.getDataAccess(any(GetDataAccessRequest.class))).thenThrow(S3ControlException.builder().statusCode(403).message("Access denied").build());
        when(s3ControlAsyncClient.getAccessGrantsInstanceForPrefix(any(GetAccessGrantsInstanceForPrefixRequest.class))).thenReturn(getAccessGrantsInstanceForPrefixResponse);

        try {
            identityProvider.resolveIdentity(resolveIdentityRequest).join();
            Assert.fail("Expecting an exception to be thrown as the request is denied by the server!");
        } catch(CompletionException e) {
            verify(cache, times(1)).getDataAccess(any(), any(), any(), any());
            verify(s3ControlAsyncClient, times(1)).getDataAccess(any(GetDataAccessRequest.class));
            Assertions.assertThat(e.getCause().getCause()).isInstanceOf(S3ControlException.class);
            Assertions.assertThat(((S3ControlException)e.getCause().getCause()).statusCode()).isEqualTo(403);
        }

        // resend the request and validate no interaction with the service.
        try {
            identityProvider.resolveIdentity(resolveIdentityRequest).join();
            Assert.fail("Expecting an exception to be thrown as the request is denied by the server and should be retrieved from cache!");
        } catch(SdkServiceException e) {
            verify(cache, times(2)).getDataAccess(any(), any(), any(), any());
            verify(s3ControlAsyncClient, times(1)).getDataAccess(any(GetDataAccessRequest.class));
            Assertions.assertThat(e.getCause()).isInstanceOf(S3ControlException.class);
            Assertions.assertThat(((S3ControlException)e.getCause()).statusCode()).isEqualTo(403);
        }

    }

    @Test
    public void call_s3_without_an_access_grant_request_failure() throws Exception {

        S3AccessGrantsCachedCredentialsProviderImpl cache = spy(S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .S3ControlAsyncClient(s3ControlAsyncClient)
                .build());

        S3AccessGrantsIdentityProvider identityProvider =
            spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                                                   s3ControlAsyncClient,
                                                   cache));

        S3Client s3Client = S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, identityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        try {
            S3AccessGrantsIntegrationTestsUtils.GetObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                          S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT2);

            Assert.fail("Expected an exception as no READ grant has been added for the desired prefix!");
        } catch (SdkServiceException e) {

            verify(identityProvider, times(1)).resolveIdentity(any(ResolveIdentityRequest.class));

            verify(identityProvider, never()).getCredentialsFromAccessGrants(any(GetDataAccessRequest.class));

            verify(cache, times(1)).getDataAccess(any(), any(), any(), any());

            Assertions.assertThat(e.getCause()).isInstanceOf(S3ControlException.class);
            Assertions.assertThat(e.statusCode()).isEqualTo(403);

        }
    }

    @Test
    public void call_s3_with_unregistered_access_grants_location_request_failure() throws Exception {

        S3AccessGrantsCachedCredentialsProviderImpl cache = spy(S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .S3ControlAsyncClient(s3ControlAsyncClient)
                .build());

        S3AccessGrantsIdentityProvider identityProvider =
            spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                                                   s3ControlAsyncClient,
                                                   cache));

        S3Client s3Client = S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, identityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        try {

            S3AccessGrantsIntegrationTestsUtils.PutObject(s3Client,
                                                          S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME_NOT_REGISTERED,
                                                          S3AccessGrantsIntegrationTestsUtils.TEST_LOCATION_1 + "/file3.txt",
                                                          "Non-registered bucket should not accept any new data!");

           Assert.fail("Expected an exception to occur as the bucket is not registered with access grants!");

        } catch (SdkServiceException e) {
            verify(identityProvider, times(1)).resolveIdentity(any(ResolveIdentityRequest.class));
            verify(identityProvider, never()).getCredentialsFromAccessGrants(any(GetDataAccessRequest.class));
            verify(cache, times(1)).getDataAccess(any(), any(), any(), any());
            Assertions.assertThat(e.getCause()).isInstanceOf(S3ControlException.class);
            Assertions.assertThat(e.statusCode()).isEqualTo(403);
        }
    }

    @Test
    public void call_s3_with_supported_operation_but_no_grant_request_failure() throws Exception {

        S3AccessGrantsCachedCredentialsProvider cache = spy(S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .S3ControlAsyncClient(s3ControlAsyncClient)
                .build());

        S3AccessGrantsIdentityProvider identityProvider =
            spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                                                   s3ControlAsyncClient,
                                                   cache));

        S3Client s3Client = S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, identityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        try {

            S3AccessGrantsIntegrationTestsUtils.PutObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME_NOT_REGISTERED,
                                                          "PrefixA/file3.txt", "Writing a file to  a non permissed location!");

            Assert.fail("Expected an exception to occur as no WRITE grant has been added to the prefix where we are adding a file!");

        } catch (SdkServiceException e) {

            verify(identityProvider, times(1)).resolveIdentity(any(ResolveIdentityRequest.class));

            verify(identityProvider, never()).getCredentialsFromAccessGrants(any(GetDataAccessRequest.class));

            verify(cache, times(1)).getDataAccess(any(), any(), any(), any());

            Assertions.assertThat(e.getCause()).isInstanceOf(S3ControlException.class);

            Assertions.assertThat(e.statusCode()).isEqualTo(403);

        }

    }

    @Test
    public void call_s3_with_non_existent_location_request_failure() throws Exception {

        S3AccessGrantsCachedCredentialsProvider cache = spy(S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .S3ControlAsyncClient(s3ControlAsyncClient)
                .build());

        S3AccessGrantsIdentityProvider identityProvider =
            spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                                                   s3ControlAsyncClient,
                                                   cache));

        S3Client s3Client = S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, identityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        try {

            S3AccessGrantsIntegrationTestsUtils.PutObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                          "PrefixC/file4.txt", "Writing a file to the non-existent location!");

            Assert.fail("Expected an exception to occur as the location where we are writing a file does not exist!");
        } catch (SdkServiceException e) {

            verify(identityProvider, times(1)).resolveIdentity(any(ResolveIdentityRequest.class));

            verify(identityProvider, never()).getCredentialsFromAccessGrants(any(GetDataAccessRequest.class));

            verify(cache, times(1)).getDataAccess(any(), any(), any(), any());

            Assertions.assertThat(e.getCause()).isInstanceOf(S3ControlException.class);

            Assertions.assertThat(e.statusCode()).isEqualTo(403);

        }
     }

    @Test
    public void faulty_auth_scheme_returning_unsupported_scheme_request_failure() {
        class InvalidAuthSchemeProvider implements S3AuthSchemeProvider {
            @Override
            public List<AuthSchemeOption> resolveAuthScheme(S3AuthSchemeParams authSchemeParams) {
                List<AuthSchemeOption> authSchemes = new ArrayList<>();

                authSchemes.add(AuthSchemeOption.builder().schemeId("smithy.api#noAuth").build());

                return authSchemes.stream()
                        .map(authScheme -> authScheme.toBuilder().putIdentityProperty(S3AccessGrantsUtils.OPERATION_PROPERTY,
                                        authSchemeParams.operation())
                                .putIdentityProperty(S3AccessGrantsUtils.PREFIX_PROPERTY,
                                        "s3://")
                                .build()
                        )
                        .collect(Collectors.toList());
            }
        }

        S3AccessGrantsCachedCredentialsProvider cache = spy(S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .S3ControlAsyncClient(s3ControlAsyncClient)
                .build());

        S3AccessGrantsIdentityProvider validIdentityProvider =
                spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                        S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                        S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                        S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                        S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                        s3ControlAsyncClient,
                        cache));

        InvalidAuthSchemeProvider invalidAuthSchemeProvider = spy(new InvalidAuthSchemeProvider());

        try {
            S3Client invalidS3Client =
                S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(invalidAuthSchemeProvider, validIdentityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

            S3AccessGrantsIntegrationTestsUtils.GetObject(invalidS3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                          S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT2);

            Assert.fail("Expected an exception as we are making a request using a scheme the endpoint does not support!");
        } catch (S3Exception e) {

            verify(validIdentityProvider, never()).resolveIdentity(any(ResolveIdentityRequest.class));

            verify(invalidAuthSchemeProvider, times(1)).resolveAuthScheme(any(S3AuthSchemeParams.class));

        }
    }

    @Test
    public void faulty_auth_scheme_provider_dropping_auth_params_request_failure() throws Exception {

        class InvalidAuthSchemeProvider implements S3AuthSchemeProvider {
            @Override
            public List<AuthSchemeOption> resolveAuthScheme(S3AuthSchemeParams authSchemeParams) {
                List<AuthSchemeOption> authSchemes = new ArrayList<>();

                authSchemes.add(AuthSchemeOption.builder().schemeId("aws.auth#sigv4").build());

                return authSchemes;
            }
        }

        S3AccessGrantsCachedCredentialsProvider cache = spy(S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .S3ControlAsyncClient(s3ControlAsyncClient)
                .build());


        S3AccessGrantsIdentityProvider validIdentityProvider =
            spy(new S3AccessGrantsIdentityProvider(software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build(),
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                                                   s3ControlAsyncClient,
                                                   cache));

        InvalidAuthSchemeProvider invalidAuthSchemeProvider = spy(new InvalidAuthSchemeProvider());

        try {

           S3Client invalidS3Client =
                S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(invalidAuthSchemeProvider, validIdentityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

            S3AccessGrantsIntegrationTestsUtils.GetObject(invalidS3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                          S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT2);

            Assert.fail("Expected an exception as we are making a request without the mandatory parameters that staircase "
                        + "requires!");
        } catch (IllegalArgumentException e) {

            verify(invalidAuthSchemeProvider, times(1)).resolveAuthScheme(any(S3AuthSchemeParams.class));

            verify(validIdentityProvider, times(1)).resolveIdentity(any(ResolveIdentityRequest.class));

            verify(validIdentityProvider, never()).getCredentialsFromAccessGrants(any(GetDataAccessRequest.class));

            verify(validIdentityProvider, never()).getCredentialsFromCache(any(), any(), any(), any());

            verify(cache, never()).getDataAccess(any(), any(), any(), any());

        }

    }

    @Test
    public void faulty_default_credentials_provider_configuration_request_failure() throws Exception {

        S3AccessGrantsCachedCredentialsProvider cache = spy(S3AccessGrantsCachedCredentialsProviderImpl.builder()
                .S3ControlAsyncClient(s3ControlAsyncClient)
                .build());

        S3AccessGrantsIdentityProvider invalidIdentityProvider =
            spy(new S3AccessGrantsIdentityProvider(software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider.builder().profileName("random-non-existent"
                                                                                                    + "-profile").build(),
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                                                   s3ControlAsyncClient,
                                                   cache));

        try {
           S3Client invalidS3Client =
                S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, invalidIdentityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

            S3AccessGrantsIntegrationTestsUtils.GetObject(invalidS3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME, S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT2);

            Assert.fail("Expected an exception as there is no valid credentials available to talk to access grants!");

        } catch (SdkClientException e) {

            verify(invalidIdentityProvider, times(1)).resolveIdentity(any(software.amazon.awssdk.identity.spi.ResolveIdentityRequest.class));

            verify(invalidIdentityProvider, never()).getCredentialsFromAccessGrants(any(GetDataAccessRequest.class));

            verify(invalidIdentityProvider, never()).getCredentialsFromCache(any(), any(), any(), any());

            verify(cache, never()).getDataAccess(any(), any(), any(), any());
        }

    }

    @Test
    public void call_s3_with_plugin_default_configuration_success_response() throws IOException {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3AccessGrantsPlugin accessGrantsPlugin = spy(S3AccessGrantsPlugin.builder().accountId(S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT).build());

        S3Client s3Client =
            S3Client.builder()
                    .addPlugin(accessGrantsPlugin)
                    .credentialsProvider(credentialsProvider)
                    .region(S3AccessGrantsIntegrationTestsUtils.TEST_REGION)
                    .build();

       ResponseInputStream<GetObjectResponse> responseInputStream = S3AccessGrantsIntegrationTestsUtils.GetObject(s3Client,
                                                                                                                   S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                                                                                   S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT1);

        verify(accessGrantsPlugin, times(1)).configureClient(any());

        Assertions.assertThat(S3AccessGrantsIntegrationTestsUtils.getFileContentFromGetResponse(responseInputStream)).isEqualTo("access grants test content in file1!");

        Assertions.assertThat(S3AccessGrantsIntegrationTestsUtils.getStatusCodeFromGetResponse(responseInputStream)).isEqualTo(200);

    }

    @Test
    public void call_s3_with_plugin_valid_configuration_success_response() throws IOException {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3AccessGrantsPlugin accessGrantsPlugin =
            spy(S3AccessGrantsPlugin.builder().accountId(S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT).build());

        S3Client s3Client =
            S3Client.builder()
                    .credentialsProvider(credentialsProvider)
                    .addPlugin(accessGrantsPlugin)
                    .region(S3AccessGrantsIntegrationTestsUtils.TEST_REGION)
                    .build();

        ResponseInputStream<GetObjectResponse> responseInputStream = S3AccessGrantsIntegrationTestsUtils.GetObject(s3Client,
                                                                                                                   S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                                                                                   S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT1);

        verify(accessGrantsPlugin, times(1)).configureClient(any());

        Assertions.assertThat(S3AccessGrantsIntegrationTestsUtils.getFileContentFromGetResponse(responseInputStream)).isEqualTo("access grants test content in file1!");

        Assertions.assertThat(S3AccessGrantsIntegrationTestsUtils.getStatusCodeFromGetResponse(responseInputStream)).isEqualTo(200);

    }

    @Test
    public void call_s3_with_plugin_valid_configuration_Access_denied_failure_response() throws IOException {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3AccessGrantsPlugin accessGrantsPlugin =
                spy(S3AccessGrantsPlugin.builder().accountId(S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT).build());

        S3Client s3Client =
                S3Client.builder()
                        .credentialsProvider(credentialsProvider)
                        .addPlugin(accessGrantsPlugin)
                        .region(S3AccessGrantsIntegrationTestsUtils.TEST_REGION)
                        .build();
        try {
            ResponseInputStream<GetObjectResponse> responseInputStream = S3AccessGrantsIntegrationTestsUtils.GetObject(s3Client,
                    S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                    S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT2);
        } catch (SdkServiceException e) {
            verify(accessGrantsPlugin, times(1)).configureClient(any());
            Assertions.assertThat(((S3ControlException)e.getCause()).statusCode()).isEqualTo(403);
        }


    }

    @Test
    public void call_s3_with_plugin_invalid_default_credentials_provider_request_failure() {
        S3AccessGrantsPlugin accessGrantsPlugin =
            spy(S3AccessGrantsPlugin.builder().accountId(S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT).build());
        try {
           S3Client s3Client =
               S3Client.builder()
                       .addPlugin(accessGrantsPlugin)
                       .region(S3AccessGrantsIntegrationTestsUtils.TEST_REGION)
                       .build();

           S3AccessGrantsIntegrationTestsUtils.GetObject(s3Client,
                                                         S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                         S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT1);

           Assert.fail("Expected an exception to occur as as a valid credentials is not provided to talk to access grants!");
       } catch (IllegalArgumentException e) {
           verify(accessGrantsPlugin, times(1)).configureClient(any());
       }
    }

    @Test
    public void call_s3_with_plugin_invalid_auth_scheme_provider_request_success() throws IOException {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3AccessGrantsPlugin accessGrantsPlugin =
            spy(S3AccessGrantsPlugin.builder().accountId(S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT).build());

        S3Client s3Client =
                S3Client.builder()
                        .authSchemeProvider(null)
                        .credentialsProvider(credentialsProvider)
                        .addPlugin(accessGrantsPlugin)
                        .region(S3AccessGrantsIntegrationTestsUtils.TEST_REGION)
                        .build();

        ResponseInputStream<GetObjectResponse> responseInputStream = S3AccessGrantsIntegrationTestsUtils.GetObject(s3Client,
                                                          S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                          S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT1);

        Assertions.assertThat(S3AccessGrantsIntegrationTestsUtils.getFileContentFromGetResponse(responseInputStream)).isEqualTo("access grants test content in file1!");

        Assertions.assertThat(S3AccessGrantsIntegrationTestsUtils.getStatusCodeFromGetResponse(responseInputStream)).isEqualTo(200);

    }

}
