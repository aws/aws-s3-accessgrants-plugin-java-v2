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

import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.*;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClientBuilder;
import software.amazon.awssdk.utils.Logger;

import java.io.IOException;
import java.util.stream.Collectors;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketResponse;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;
import software.amazon.awssdk.services.s3control.S3ControlClient;
import software.amazon.awssdk.services.s3control.model.AccessGrantsLocationConfiguration;
import software.amazon.awssdk.services.s3control.model.CreateAccessGrantsInstanceRequest;
import software.amazon.awssdk.services.s3control.model.CreateAccessGrantsInstanceResponse;
import software.amazon.awssdk.services.s3control.model.CreateAccessGrantsLocationRequest;
import software.amazon.awssdk.services.s3control.model.CreateAccessGrantRequest;
import software.amazon.awssdk.services.s3control.model.DeleteAccessGrantRequest;
import software.amazon.awssdk.services.s3control.model.DeleteAccessGrantsLocationRequest;
import software.amazon.awssdk.services.s3control.model.DeleteAccessGrantsInstanceRequest;
import software.amazon.awssdk.services.s3control.model.ListAccessGrantsResponse;
import software.amazon.awssdk.services.s3control.model.ListAccessGrantEntry;
import software.amazon.awssdk.services.s3control.model.Grantee;
import software.amazon.awssdk.services.s3control.model.GranteeType;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantRequest;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantResponse;
import software.amazon.awssdk.services.s3control.model.ListAccessGrantsLocationsRequest;
import software.amazon.awssdk.services.s3control.model.ListAccessGrantsLocationsResponse;

import software.amazon.awssdk.services.s3control.model.Privilege;
import software.amazon.awssdk.services.s3control.model.Permission;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceRequest;
import software.amazon.awssdk.services.s3control.model.GetAccessGrantsInstanceResponse;
import software.amazon.awssdk.services.s3control.model.ListAccessGrantsRequest;
import software.amazon.awssdk.services.s3control.model.S3ControlException;


public class S3AccessGrantsIntegrationTestsUtils {

    public static software.amazon.awssdk.services.s3.S3Client s3Client = null;

    public static software.amazon.awssdk.services.s3control.S3ControlClient s3ControlClient = null;

    public static Region TEST_REGION;

    public static Region TEST_REGION_2;

    public static String TEST_CREDENTIALS_PROFILE_NAME;

    public static String TEST_ACCOUNT;

    public static final String TEST_BUCKET_NAME = "access-grants-sdk-test-bucket";

    public static final String TEST_BUCKET_NAME_NOT_REGISTERED = "access-grants-sdk-test-bucket-not-registered";

    public static final String TEST_BUCKET_READWRITE = "access-grants-sdk-test-bucket-readwrite";

    public static final String TEST_BUCKET_READWRITE_CROSS_REGION = "access-grants-sdk-test-bucket-readwrite-cross-region";

    public static final String TEST_LOCATION_1 = "PrefixA/prefixB/";

    public static final String TEST_LOCATION_2 = "PrefixA/";

    public static final String TEST_OBJECT1 = TEST_LOCATION_1+"file1.txt";

    public static final String TEST_OBJECT2 = TEST_LOCATION_2+"file2.txt";

    public static final String ACCESS_GRANTS_POLICY_NAME = "access-grants-policy-sdk-test";

    public static String ACCESS_GRANTS_IAM_ROLE_NAME;

    public static final String ALLOWED_BUCKET_PREFIX = TEST_LOCATION_1+"*";

    public static final String ALLOWED_BUCKET_PREFIX2 = TEST_LOCATION_2+"*";

    public static final Privilege DEFAULT_PRIVILEGE = Privilege.DEFAULT;

    public static final Boolean DEFAULT_IS_CACHE_ENABLED = true;

    public static String TEST_OBJECT_1_CONTENTS = "access grants test content in file1!";

    public static String TEST_OBJECT_2_CONTENTS = "access grants test content in file2!";

    public static final boolean DISABLE_TEAR_DOWN = false;

    public static final boolean DEFAULT_FALLBACK_ENABLED = false;

    public static String ACCESS_GRANTS_INSTANCE_ARN;

    public static String ACCESS_GRANTS_INSTANCE_ID;

    private static final Logger logger = Logger.loggerFor(S3AccessGrantsIntegrationTestsUtils.class);

    public static S3Client s3clientBuilder(S3AuthSchemeProvider authSchemeProvider,
                                           IdentityProvider<AwsCredentialsIdentity> identityProvider,
                                           Region region
    ) {

        return S3Client.builder()
                .region(region)
                .authSchemeProvider(authSchemeProvider)
                .credentialsProvider(identityProvider)
                .build();

    }

    public static S3ControlClient s3ControlClientBuilder(IdentityProvider<AwsCredentialsIdentity> identityProvider,
                                                         Region region) {
        return S3ControlClient.builder()
                .region(region)
                .credentialsProvider(identityProvider)
                .build();

    }

    public static S3ControlAsyncClientBuilder s3ControlAsyncClientBuilder(IdentityProvider<AwsCredentialsIdentity> identityProvider,
                                                                          Region region) {
        return S3ControlAsyncClient.builder()
                .region(region)
                .credentialsProvider(identityProvider);

    }

    public static S3Client s3ClientBuilder(IdentityProvider<AwsCredentialsIdentity> identityProvider,
                                           Region region) {

        return S3Client.builder()
                .region(region)
                .credentialsProvider(identityProvider)
                .build();

    }

    public static IamClient iamClientBuilder(IdentityProvider<AwsCredentialsIdentity> identityProvider, Region region) {

        return IamClient.builder()
                .region(region)
                .credentialsProvider(identityProvider)
                .build();

    }

    public static CreateBucketResponse CreateBucket(S3Client s3Client, String bucketName) {

        CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                .bucket(bucketName)
                .build();

        return s3Client.createBucket(createBucketRequest);

    }

    public static PutObjectResponse PutObject(S3Client s3Client, String bucketName, String key, String content) {

        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            return s3Client.putObject(putObjectRequest, RequestBody.fromString(content));
        } catch (Exception e) {
            logger.info(() -> e.getMessage());
            throw e;
        }

    }

    public static ResponseInputStream<GetObjectResponse> GetObject(S3Client s3Client, String bucketName, String key) {

        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .key(key)
                    .bucket(bucketName)
                    .build();

            return s3Client.getObject(getObjectRequest);
        } catch (Exception e) {
            throw e;
        }
    }

    public static String getFileContentFromGetResponse(ResponseInputStream<GetObjectResponse> responseInputStream) throws IOException {
        GetObjectResponse getObjectResponse =  responseInputStream.response();

        byte responseData[] = new byte[getObjectResponse.contentLength().intValue()];

        responseInputStream.read(responseData);

        return new String(responseData);
    }

    public static int getStatusCodeFromGetResponse(ResponseInputStream<GetObjectResponse> responseInputStream) {

        GetObjectResponse getObjectResponse =  responseInputStream.response();

        SdkHttpResponse httpResponse = getObjectResponse.sdkHttpResponse();

        return httpResponse.statusCode();

    }

    public static String createAccessGrantsInstance(S3ControlClient s3ControlClient, String accountId) {

        GetAccessGrantsInstanceRequest getAccessGrantsInstanceRequest = GetAccessGrantsInstanceRequest.builder().accountId(
                accountId).build();

        try {

            GetAccessGrantsInstanceResponse getAccessGrantsInstanceResponse =
                    s3ControlClient.getAccessGrantsInstance(getAccessGrantsInstanceRequest);

            return getAccessGrantsInstanceResponse.accessGrantsInstanceArn();

        } catch (S3ControlException e) {

            CreateAccessGrantsInstanceRequest createAccessGrantsInstanceRequest =
                    CreateAccessGrantsInstanceRequest.builder().accountId(accountId).build();

            CreateAccessGrantsInstanceResponse createAccessGrantsInstanceResponse =
                    s3ControlClient.createAccessGrantsInstance(createAccessGrantsInstanceRequest);

            return createAccessGrantsInstanceResponse.accessGrantsInstanceArn();

        }

    }

    public static String createS3AccessGrantsLocation(S3ControlClient s3ControlClient, String s3Prefix, String accountId,
                                                      String iamRoleArn) {

        try {
            CreateAccessGrantsLocationRequest locationRequest = CreateAccessGrantsLocationRequest.builder()
                    .accountId(accountId)
                    .iamRoleArn(iamRoleArn)
                    .locationScope(s3Prefix)
                    .build();

            return s3ControlClient.createAccessGrantsLocation(locationRequest).accessGrantsLocationId();

        } catch (S3ControlException e) {

            // returns the first location id where the prefix is already registered

            ListAccessGrantsLocationsRequest listAccessGrantsLocationsRequest = ListAccessGrantsLocationsRequest.builder()
                    .accountId(accountId)
                    .locationScope(s3Prefix)
                    .build();

            ListAccessGrantsLocationsResponse listAccessGrantsLocationsResponse =
                    s3ControlClient.listAccessGrantsLocations(listAccessGrantsLocationsRequest);

            return listAccessGrantsLocationsResponse.accessGrantsLocationsList().get(0).accessGrantsLocationId();

        }
    }

    public static String registerAccessGrant(S3ControlClient s3ControlClient, String s3prefix, Permission permission,
                                             String iamRoleArn,
                                             String accountId,
                                             String accessGrantsInstanceLocationId) {

        try {
            Grantee grantee = Grantee.builder().granteeType(GranteeType.IAM)
                    .granteeIdentifier(iamRoleArn)
                    .build();

            AccessGrantsLocationConfiguration accessGrantsLocationConfiguration = AccessGrantsLocationConfiguration.builder()
                    .s3SubPrefix(s3prefix)
                    .build();

            CreateAccessGrantRequest accessGrantRequest = CreateAccessGrantRequest.builder()
                    .accessGrantsLocationId(accessGrantsInstanceLocationId)
                    .accountId(accountId)
                    .grantee(grantee)
                    .permission(permission)
                    .accessGrantsLocationConfiguration(accessGrantsLocationConfiguration)
                    .build();

            return s3ControlClient.createAccessGrant(accessGrantRequest).accessGrantId();

        } catch (S3ControlException e) {
            if(e.statusCode() == 409) {
                logger.info( () -> "The grant has already been registered with the access grants location id!");

                // fetch the grant and return the id.
                ListAccessGrantsRequest listAccessGrantsRequest =
                        ListAccessGrantsRequest.builder()
                                .accountId(accountId)
                                .granteeIdentifier(iamRoleArn)
                                .granteeType(GranteeType.IAM)
                                .permission(permission)
                                .build();

                ListAccessGrantsResponse listAccessGrantsResponse = s3ControlClient.listAccessGrants(listAccessGrantsRequest);

                java.util.List<software.amazon.awssdk.services.s3control.model.ListAccessGrantEntry> listAccessGrantEntries =
                        listAccessGrantsResponse.accessGrantsList().parallelStream().filter(accessGrant -> validateIfAccessGrantMatches(s3ControlClient,
                                accessGrant,
                                accessGrantsInstanceLocationId,
                                permission,
                                s3prefix,
                                accountId)
                        ).collect(Collectors.toList());
                return listAccessGrantEntries.get(0).accessGrantId();

            }

            throw e;

        }
    }

    private static boolean validateIfAccessGrantMatches(S3ControlClient s3ControlClient, ListAccessGrantEntry accessGrant,
                                                        String accessGrantsInstanceLocationId,
                                                        Permission permission,
                                                        String s3prefix,
                                                        String accountId) {
        GetAccessGrantRequest getAccessGrantRequest =
                GetAccessGrantRequest.builder().accountId(accountId)
                        .accessGrantId(accessGrant.accessGrantId())
                        .build();

        GetAccessGrantResponse getAccessGrantResponse = s3ControlClient.getAccessGrant(getAccessGrantRequest);

        return getAccessGrantResponse.accessGrantsLocationId().equals(accessGrantsInstanceLocationId)
                && getAccessGrantResponse.permission().equals(permission)
                && getAccessGrantResponse.accessGrantsLocationConfiguration() != null
                && getAccessGrantResponse.accessGrantsLocationConfiguration().s3SubPrefix().equals(s3prefix);
    }

    public static String createS3AccessGrantsIAMPolicy(IamClient iamClient, String policyName, String policyStatement,
                                                       String accountId) {

        try {
            CreatePolicyRequest createPolicyRequest =
                    CreatePolicyRequest.builder().policyDocument(policyStatement).policyName(policyName).build();

            return iamClient.createPolicy(createPolicyRequest).policy().arn();
        } catch (EntityAlreadyExistsException e) {
            return "arn:aws:iam::" + accountId + ":policy/" + policyName;
        }

    }

    public static String createS3AccessGrantsIAMRole(IamClient iamClient, String roleName, String trustPolicy, String accountId) {

        try {

            CreateRoleRequest request =
                    CreateRoleRequest.builder().roleName(roleName).assumeRolePolicyDocument(trustPolicy).build();

            return iamClient.createRole(request).role().arn();

        } catch (EntityAlreadyExistsException e) {
            logger.info(() ->"IAM role is already existing " + "arn:aws:iam::" + accountId +
                    ":role/" + roleName);
            return "arn:aws:iam::" + accountId + ":role/" + roleName;

        }

    }

    public static void attachPolicyToRole(IamClient iamClient, String roleName, String policyArn) {

        AttachRolePolicyRequest rolePolicyRequest = AttachRolePolicyRequest.builder()
                .roleName(roleName)
                .policyArn(policyArn)
                .build();

        iamClient.attachRolePolicy(rolePolicyRequest);

    }

    public static void deleteAccessGrant(S3ControlClient s3ControlClient, String accessGrantId,
                                         String accountId) {
        try {

            logger.info(() -> "deleting access grants id "+ accessGrantId);
            DeleteAccessGrantRequest deleteAccessGrantRequest =
                    DeleteAccessGrantRequest.builder().accessGrantId(accessGrantId).accountId(accountId).build();
            s3ControlClient.deleteAccessGrant(deleteAccessGrantRequest);

            logger.info(() -> "successfully deleted the access grants during test teardown!");
        } catch (Exception e) {
            logger.info(() -> "Access Grants cannot be deleted during test setup teardown! "+ e.getMessage());
        }
    }

    public static void deleteAccessGrantLocation(S3ControlClient s3ControlClient, String accessGrantsInstanceLocationId) {
        try {
            DeleteAccessGrantsLocationRequest deleteAccessGrantsLocationRequest =
                    DeleteAccessGrantsLocationRequest.builder().accessGrantsLocationId(accessGrantsInstanceLocationId).accountId(S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT)
                            .build();

            s3ControlClient.deleteAccessGrantsLocation(deleteAccessGrantsLocationRequest);
            logger.info(() -> "successfully deleted the access grants location during test teardown!");
        } catch (Exception e) {
            logger.info(() -> "Access Grants Location cannot be deleted during test setup teardown! "+ e.getMessage());
        }
    }

    public static void deleteAccessGrantsInstance(S3ControlClient s3ControlClient,
                                                  String accountId) {
        try {
            DeleteAccessGrantsInstanceRequest deleteAccessGrantsInstanceRequest =
                    DeleteAccessGrantsInstanceRequest.builder().accountId(accountId).build();
            s3ControlClient.deleteAccessGrantsInstance(deleteAccessGrantsInstanceRequest);

            logger.info(() -> "successfully deleted the access grants instance during test teardown!");
        } catch (Exception e) {
           logger.info(() -> "Access Grants Instance cannot be deleted during test setup teardown! "+ e.getMessage());
        }
    }

    public static void deleteObject(S3Client s3Client, String bucketName, String bucketKey) {
        try {
            DeleteObjectRequest deleteObjectRequest =
                    DeleteObjectRequest
                            .builder()
                            .bucket(bucketName)
                            .key(bucketKey)
                            .build();

            s3Client.deleteObject(deleteObjectRequest);

            logger.info(() -> "successfully deleted the object " + bucketKey + " during test teardown!");

        } catch (Exception e) {
            logger.info(() -> "Object cannot be deleted during test teardown! "+ e.getMessage());
        }
    }

    public static DeleteBucketResponse deleteBucket(S3Client s3Client, String bucketName) {
        try {
            DeleteBucketRequest deleteBucketRequest =
                    DeleteBucketRequest.builder().bucket(bucketName).build();
            DeleteBucketResponse deleteBucketResponse = s3Client.deleteBucket(deleteBucketRequest);
            logger.info(() -> "successfully deleted the bucket during test teardown!");
            return deleteBucketResponse;
        } catch (Exception e) {
            logger.info(() -> "bucket cannot be deleted during test teardown! "+ e.getMessage());
            throw e;
        }
    }

    private static void deletePolicy(IamClient iamClient, String policyArn) {
        try {
            DeletePolicyRequest deletePolicyRequest = DeletePolicyRequest.builder().policyArn(policyArn).build();

            iamClient.deletePolicy(deletePolicyRequest);
            logger.info(() -> "successfully deleted the policy during test teardown!");
        } catch (Exception e) {
            logger.info(() -> "Policy cannot be deleted during test teardown! "+ e.getMessage());
        }
    }

    private static void deleteRole(IamClient iamClient, String roleName) {
        try {
            DeleteRoleRequest deleteRoleRequest =
                    DeleteRoleRequest.builder().roleName(roleName).build();
            iamClient.deleteRole(deleteRoleRequest);
            logger.info(() -> "successfully deleted the role during test teardown!");
        } catch (Exception e) {
            logger.info(() -> "role cannot be deleted during test teardown! "+ e.getMessage());
        }
    }

    private static void detachPolicy(IamClient iamClient, String policyArn, String roleName) {
        try {
            DetachRolePolicyRequest detachRolePolicyRequest =
                    DetachRolePolicyRequest.builder().roleName(roleName)
                            .policyArn(policyArn)
                            .build();

            iamClient.detachRolePolicy(detachRolePolicyRequest);
            logger.info(() -> "successfully deleted the role policy during test teardown!");
        } catch (Exception e) {
            logger.info(() -> "policy cannot be detached form the role during test teardown! "+e.getMessage());
        }
    }

}