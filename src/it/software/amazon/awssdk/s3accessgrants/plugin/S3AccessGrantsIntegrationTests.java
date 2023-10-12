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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeParams;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.services.s3control.model.S3ControlException;
import software.amazon.awssdk.services.s3control.model.GetDataAccessRequest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Readme.md (will be moved to readme or contributing guide in the repo)
 * Before running the integration tests with your account be aware that the integration tests make requests to the AWS accounts
 * incurring costs to the account owner. Also, the resources created will be cleaned up after each test. That means additional
 * costs will be incurred for each time tests are run. A workaround we suggest would be to disable teardown method in {@link S3AccessGrantsIntegrationTestsUtils}
 * and
 * re-enable it when you are sure that you no longer need the resources.
 *
 * To run the integration tests assume the credentials of the account you want to use. By default, we try to identify the
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

    @org.junit.BeforeClass
    public static void testsSetUp() {

        S3AccessGrantsInstanceSetUpUtils.setUpAccessGrantsInstanceForTests();


    }

    @org.junit.AfterClass
    public static void tearDown() {
        if (!S3AccessGrantsIntegrationTestsUtils.DISABLE_TEAR_DOWN) {
            S3AccessGrantsInstanceSetUpUtils.tearDown();
        }
    }

    @Test
    public void call_s3_with_operation_not_supported_by_access_grants_request_failure() {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3ControlAsyncClient s3ControlAsyncClient = S3AccessGrantsIntegrationTestsUtils.s3ControlAsyncClientBuilder(credentialsProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        S3AccessGrantsIdentityProvider identityProvider =
                spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                        S3AccessGrantsIntegrationTestsUtils.TEST_REGION, S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                        S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                        S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                        s3ControlAsyncClient));

        S3AuthSchemeProvider authSchemeProvider =
                spy(new S3AccessGrantsAuthSchemeProvider(S3AuthSchemeProvider.defaultProvider()));

        String bucketName = "access-grants-sdk-create-test";

        S3Client s3client = S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, identityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        try {
            CreateBucketResponse createBucketResponse = S3AccessGrantsIntegrationTestsUtils.CreateBucket(s3client, bucketName);

            Assertions.assertThat(createBucketResponse.sdkHttpResponse().statusCode()).isEqualTo(200);

            verify(identityProvider, times(1)).resolveIdentity(any(ResolveIdentityRequest.class));

            verify(identityProvider, never()).getCredentialsFromAccessGrants(any());

        } finally {
            S3AccessGrantsIntegrationTestsUtils.DeleteBucket(s3client, bucketName);
        }

    }

    @Test
    public void call_s3_with_supported_operation_request_success() throws IOException {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3ControlAsyncClient s3ControlAsyncClient = S3AccessGrantsIntegrationTestsUtils.s3ControlAsyncClientBuilder(credentialsProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        S3AccessGrantsIdentityProvider identityProvider =
            spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                                                   s3ControlAsyncClient));


       S3AuthSchemeProvider authSchemeProvider =
            spy(new S3AccessGrantsAuthSchemeProvider(S3AuthSchemeProvider.defaultProvider()));


       S3Client s3client = S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, identityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

       ResponseInputStream<GetObjectResponse> responseInputStream = S3AccessGrantsIntegrationTestsUtils.GetObject(s3client,
                                                                                                                   S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                                                                                   S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT1);
       Assertions.assertThat(S3AccessGrantsIntegrationTestsUtils.getFileContentFromGetResponse(responseInputStream)).isEqualTo("access grants test content in file1!");

       Assertions.assertThat(S3AccessGrantsIntegrationTestsUtils.getStatusCodeFromGetResponse(responseInputStream)).isEqualTo(200);

       verify(identityProvider, times(1)).resolveIdentity(any(ResolveIdentityRequest.class));

       verify(identityProvider, times(1)).getCredentialsFromAccessGrants(any());

       // TODO: cache testing goes here!

    }

    @Test
    public void call_s3_without_access_grants_request_failure() {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3ControlAsyncClient s3ControlAsyncClient = S3AccessGrantsIntegrationTestsUtils.s3ControlAsyncClientBuilder(credentialsProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        S3AccessGrantsIdentityProvider identityProvider =
            spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                                                   s3ControlAsyncClient));


       S3AuthSchemeProvider authSchemeProvider =
            spy(new S3AccessGrantsAuthSchemeProvider(S3AuthSchemeProvider.defaultProvider()));

        S3Client s3Client = S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, identityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        try {

            S3AccessGrantsIntegrationTestsUtils.GetObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                          S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT2);

            Assert.fail("Expected an exception as no READ grant has been added for the desired prefix!");

        } catch (S3ControlException e) {

            verify(identityProvider, times(1)).resolveIdentity(any(ResolveIdentityRequest.class));

            verify(identityProvider, times(1)).getCredentialsFromAccessGrants(any(GetDataAccessRequest.class));

            Assertions.assertThat(e.statusCode()).isEqualTo(403);

        }

        // TODO: cache testing here!

    }

    @Test
    public void call_s3_with_unregistered_access_grants_location_request_failure() {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3ControlAsyncClient s3ControlAsyncClient = S3AccessGrantsIntegrationTestsUtils.s3ControlAsyncClientBuilder(credentialsProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        S3AccessGrantsIdentityProvider identityProvider =
            spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                                                   s3ControlAsyncClient));


        S3AuthSchemeProvider authSchemeProvider =
            spy(new S3AccessGrantsAuthSchemeProvider(S3AuthSchemeProvider.defaultProvider()));

        S3Client s3Client = S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, identityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        try {

            S3AccessGrantsIntegrationTestsUtils.PutObject(s3Client,
                                                          S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME_NOT_REGISTERED,
                                                          S3AccessGrantsIntegrationTestsUtils.TEST_LOCATION_1 + "/file3.txt",
                                                          "Non-registered bucket should not accept any new data!");

           Assert.fail("Expected an exception to occur as the bucket is not registered with access grants!");

        } catch (S3ControlException e) {

            verify(identityProvider, times(1)).resolveIdentity(any(ResolveIdentityRequest.class));

            verify(identityProvider, times(1)).getCredentialsFromAccessGrants(any(GetDataAccessRequest.class));

            Assertions.assertThat(e.statusCode()).isEqualTo(403);
        }

        // TODO: cache related testing here !

    }

    @Test
    public void call_s3_with_supported_operation_but_no_grant_request_failure() {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3ControlAsyncClient s3ControlAsyncClient = S3AccessGrantsIntegrationTestsUtils.s3ControlAsyncClientBuilder(credentialsProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        S3AccessGrantsIdentityProvider identityProvider =
            spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                                                   s3ControlAsyncClient));


        S3AuthSchemeProvider authSchemeProvider =
            spy(new S3AccessGrantsAuthSchemeProvider(S3AuthSchemeProvider.defaultProvider()));

        S3Client s3Client = S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, identityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        try {

            S3AccessGrantsIntegrationTestsUtils.PutObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME_NOT_REGISTERED,
                                                          "PrefixA/file3.txt", "Writing a file to  a non permissed location!");

            Assert.fail("Expected an exception to occur as no grant has been added to the prefix where we are adding a file!");

        } catch (S3ControlException e) {

            verify(identityProvider, times(1)).resolveIdentity(any(ResolveIdentityRequest.class));

            verify(identityProvider, times(1)).getCredentialsFromAccessGrants(any(GetDataAccessRequest.class));

            Assertions.assertThat(e.statusCode()).isEqualTo(403);

        }

        // TODO: cache related testing here !

    }

    @Test
    public void call_s3_with_non_existent_location_request_failure() {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3ControlAsyncClient s3ControlAsyncClient = S3AccessGrantsIntegrationTestsUtils.s3ControlAsyncClientBuilder(credentialsProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        S3AccessGrantsIdentityProvider identityProvider =
            spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                                                   s3ControlAsyncClient));


        S3AuthSchemeProvider authSchemeProvider =
            spy(new S3AccessGrantsAuthSchemeProvider(S3AuthSchemeProvider.defaultProvider()));

        S3Client s3Client = S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, identityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        try {

            S3AccessGrantsIntegrationTestsUtils.PutObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                          "PrefixC/file4.txt", "Writing a file to the non-existent location!");

            Assert.fail("Expected an exception to occur as the location where we are adding a file does not exist!");
        } catch (S3ControlException e) {

            verify(identityProvider, times(1)).resolveIdentity(any(ResolveIdentityRequest.class));

            verify(identityProvider, times(1)).getCredentialsFromAccessGrants(any(GetDataAccessRequest.class));

            Assertions.assertThat(e.statusCode()).isEqualTo(403);

        }

        // TODO: cache related testing here !

    }

    @Test
    public void faulty_auth_scheme_returing_unsupported_scheme_request_failure() {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3ControlAsyncClient s3ControlAsyncClient = S3AccessGrantsIntegrationTestsUtils.s3ControlAsyncClientBuilder(credentialsProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        S3AccessGrantsIdentityProvider validIdentityProvider =
            spy(new S3AccessGrantsIdentityProvider(credentialsProvider,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                                                   s3ControlAsyncClient));

        try {
            class InvalidAccessGrantsProvider implements S3AuthSchemeProvider {
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

            S3Client invalidS3Client =
                S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(new InvalidAccessGrantsProvider(), validIdentityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

            S3AccessGrantsIntegrationTestsUtils.GetObject(invalidS3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                          S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT2);

            Assert.fail("Expected an exception as we are making a request using a scheme the endpoint does not support!");
        } catch (S3Exception e) {

            verify(validIdentityProvider, never()).resolveIdentity(any(ResolveIdentityRequest.class));

        }

    }

    //
    @Test
    public void faulty_auth_scheme_provider_dropping_auth_params_request_failure() {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3ControlAsyncClient s3ControlAsyncClient = S3AccessGrantsIntegrationTestsUtils.s3ControlAsyncClientBuilder(credentialsProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        S3AccessGrantsIdentityProvider validIdentityProvider =
            spy(new S3AccessGrantsIdentityProvider(software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build(),
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                                                   s3ControlAsyncClient));

        try {
            class InvalidAccessGrantsProvider implements S3AuthSchemeProvider {
                @Override
                public List<AuthSchemeOption> resolveAuthScheme(S3AuthSchemeParams authSchemeParams) {
                    List<AuthSchemeOption> authSchemes = new ArrayList<>();

                    authSchemes.add(software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption.builder().schemeId("aws.auth#sigv4").build());

                    return authSchemes;
                }
            }

           S3Client invalidS3Client =
                S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(new InvalidAccessGrantsProvider(), validIdentityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

            S3AccessGrantsIntegrationTestsUtils.GetObject(invalidS3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                          S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT2);

            Assert.fail("Expected an exception as we are making a request without the mandatory parameters that staircase "
                        + "requires!");
        } catch (IllegalArgumentException e) {

            verify(validIdentityProvider, times(1)).resolveIdentity(any(software.amazon.awssdk.identity.spi.ResolveIdentityRequest.class));

            verify(validIdentityProvider, never()).getCredentialsFromAccessGrants(any(GetDataAccessRequest.class));

        }

    }

    @Test
    public void faulty_default_credentials_provider_configuration_request_failure() {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3ControlAsyncClient s3ControlAsyncClient = S3AccessGrantsIntegrationTestsUtils.s3ControlAsyncClientBuilder(credentialsProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        S3AccessGrantsIdentityProvider invalidIdentityProvider =
            spy(new S3AccessGrantsIdentityProvider(software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider.builder().profileName("random-non-existent"
                                                                                                    + "-profile").build(),
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_REGION,
                                                   S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE,
                                                   S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED,
                                                   s3ControlAsyncClient));

        S3AuthSchemeProvider authSchemeProvider =
            spy(new S3AccessGrantsAuthSchemeProvider(S3AuthSchemeProvider.defaultProvider()));

        try {
           S3Client invalidS3Client =
                S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, invalidIdentityProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

            S3AccessGrantsIntegrationTestsUtils.GetObject(invalidS3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME, S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT2);

            Assert.fail("Expected an exception as there is no valid credentials available to talk to access grants!");

        } catch (SdkClientException e) {

            verify(invalidIdentityProvider, times(1)).resolveIdentity(any(software.amazon.awssdk.identity.spi.ResolveIdentityRequest.class));

            verify(invalidIdentityProvider, never()).getCredentialsFromAccessGrants(any(GetDataAccessRequest.class));

        }

    }

    @Test
    public void call_s3_with_plugin_default_configuration_success_response() throws IOException {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3AccessGrantsPlugin accessGrantsPlugin = spy(S3AccessGrantsPlugin.builder().build());

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
            spy(S3AccessGrantsPlugin.builder().accountId(S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT).privilege(software.amazon.awssdk.services.s3control.model.Privilege.DEFAULT).cacheEnabled(false).build());

        software.amazon.awssdk.services.s3.S3Client s3Client =
            software.amazon.awssdk.services.s3.S3Client.builder()
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
    public void call_s3_with_plugin_invalid_default_credentials_provider_request_failure() {
        S3AccessGrantsPlugin accessGrantsPlugin =
            spy(S3AccessGrantsPlugin.builder().accountId(S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT).privilege(S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE.get()).cacheEnabled(!S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED.get()).build());
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
    public void call_s3_with_plugin_invalid_auth_scheme_provider_request_failure() {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3AccessGrantsPlugin accessGrantsPlugin =
            spy(S3AccessGrantsPlugin.builder().accountId(S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT).privilege(S3AccessGrantsIntegrationTestsUtils.DEFAULT_PRIVILEGE.get()).cacheEnabled(!S3AccessGrantsIntegrationTestsUtils.DEFAULT_IS_CACHE_ENABLED.get()).build());

        S3Client s3Client =
                S3Client.builder()
                        .authSchemeProvider(null)
                        .credentialsProvider(credentialsProvider)
                        .addPlugin(accessGrantsPlugin)
                        .region(S3AccessGrantsIntegrationTestsUtils.TEST_REGION)
                        .build();

        S3AccessGrantsIntegrationTestsUtils.GetObject(s3Client,
                                                          S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                          S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT1);

    }

    @Test
    public void testAccessGrants_IdentifiesTheCorrectAccessGrantsAccount() {


    }


}