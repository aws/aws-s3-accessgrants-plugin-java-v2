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

import java.util.List;
import java.util.ArrayList;

import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3control.S3ControlClient;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;


public class S3AccessGrantsInstanceSetUpUtils {

    private static S3Client s3Client = null;

    private static S3ControlClient s3ControlClient = null;

    private static IamClient iamClient = null;

    private static String accessGrantsInstanceLocationId = null;

    private static String accessGrantsArn = null;

    private static String policyArn = null;

    private static String iamRoleArn = null;

    private static List<String> registeredAccessGrants = new ArrayList<>();

    public static void setUpAccessGrantsInstanceForTests() {
        ProfileCredentialsProvider profileCredentialsProvider =
           ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3AuthSchemeProvider authSchemeProvider = S3AuthSchemeProvider.defaultProvider();

        s3Client = S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, profileCredentialsProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        s3ControlClient = S3AccessGrantsIntegrationTestsUtils.s3ControlClientBuilder(profileCredentialsProvider,
                                                                                     S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        accessGrantsArn =
            S3AccessGrantsIntegrationTestsUtils.createAccessGrantsInstance(s3ControlClient,
                                                                           S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT);

        iamClient =
            S3AccessGrantsIntegrationTestsUtils.iamClientBuilder(profileCredentialsProvider,
                             S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        policyArn = createS3AccessGrantsIAMPolicy();

        iamRoleArn = S3AccessGrantsIntegrationTestsUtils.createS3AccessGrantsIAMRole(iamClient,
                                                                                     S3AccessGrantsIntegrationTestsUtils.ACCESS_GRANTS_IAM_ROLE_NAME,
                                                                                     createS3AccessGrantsIAMTrustRelationship(),
                                                                                     S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT);

        // S3AccessGrantsIntegrationTestsUtils.attachPolicyToRole(iamClient,
        //                                                        S3AccessGrantsIntegrationTestsUtils.ACCESS_GRANTS_IAM_ROLE_NAME,
        //                                                        policyArn);

        CreateAccessGrantsBucket(S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME);

        CreateAccessGrantsBucket(S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME_NOT_REGISTERED);

        accessGrantsInstanceLocationId = S3AccessGrantsIntegrationTestsUtils.createS3AccessGrantsLocation(s3ControlClient,
                                                                      "s3://" + S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                                      S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                                      iamRoleArn);

        registeredAccessGrants.add(S3AccessGrantsIntegrationTestsUtils.registerAccessGrant(s3ControlClient,
                                                                                           S3AccessGrantsIntegrationTestsUtils.ALLOWED_BUCKET_PREFIX,
                                                        software.amazon.awssdk.services.s3control.model.Permission.READ, iamRoleArn,
                                                                                           S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT, accessGrantsInstanceLocationId));

        registeredAccessGrants.add(S3AccessGrantsIntegrationTestsUtils.
                                   registerAccessGrant(s3ControlClient, S3AccessGrantsIntegrationTestsUtils.ALLOWED_BUCKET_PREFIX,
                                                       software.amazon.awssdk.services.s3control.model.Permission.WRITE, iamRoleArn,
                                                       S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT, accessGrantsInstanceLocationId));

        registeredAccessGrants.add(S3AccessGrantsIntegrationTestsUtils.registerAccessGrant(s3ControlClient,
                                                                                           S3AccessGrantsIntegrationTestsUtils.ALLOWED_BUCKET_PREFIX2, software.amazon.awssdk.services.s3control.model.Permission.WRITE, iamRoleArn, S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT, accessGrantsInstanceLocationId));

        S3AccessGrantsIntegrationTestsUtils.PutObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                      S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT1,
                                                      S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT_1_CONTENTS);

        S3AccessGrantsIntegrationTestsUtils.PutObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                      S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT2,
                                                      S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT_2_CONTENTS);

    }

    public static String createS3AccessGrantsIAMPolicy() {

        String policyStatement = "{\n"
                                 + "   \"Version\":\"2012-10-17\",\n"
                                 + "   \"Statement\":[\n"
                                 + "      {\n"
                                 + "         \"Effect\":\"Allow\",\n"
                                 + "         \"Action\":[\n"
                                 + "            \"s3:PutObject\",\n"
                                 + "            \"s3:GetObject\",\n"
                                 + "            \"s3:DeleteObject\"\n"
                                 + "         ],\n"
                                 + "         \"Resource\":[\n"
                                 + "            \"arn:aws:s3:::"+S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME+"/*\"\n"
                                 + "         ]\n"
                                 + "      },\n"
                                 + "      {\n"
                                 + "         \"Effect\":\"Allow\",\n"
                                 + "         \"Action\":[\n"
                                 + "            \"s3:ListBucket\"\n"
                                 + "         ],\n"
                                 + "         \"Resource\":[\n"
                                 + "            \"arn:aws:s3:::"+S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME+"\""
                                 + "         ]\n"
                                 + "      },\n"
                                 + "       {\n"
                                 + "         \"Effect\":\"Allow\",\n"
                                 + "         \"Action\":[\n"
                                 + "            \"*\""
                                 + "         ],\n"
                                 + "         \"Resource\":[\n"
                                 + "            \"*\""
                                 + "         ]\n"
                                 + "      }"
                                 + "   ]\n"
                                 + "}";

    return S3AccessGrantsIntegrationTestsUtils.createS3AccessGrantsIAMPolicy(iamClient,
                                                                             S3AccessGrantsIntegrationTestsUtils.ACCESS_GRANTS_POLICY_NAME,
                                                                             policyStatement,
                                                                             S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT);

    }

    public static String createS3AccessGrantsIAMTrustRelationship() {
        return "{\n"
               + "  \"Version\": \"2012-10-17\",\n"
               + "  \"Statement\": [\n"
               + "    {\n"
               + "      \"Sid\": \"Stmt1685556427189\",\n"
               + "      \"Action\": [\"sts:AssumeRole\"],\n"
               + "      \"Effect\": \"Allow\",\n"
               + "      \"Principal\": {\"Service\": \"access-grants.s3.amazonaws.com\"}\n"
               + "    },\n"
               + "    {\n"
               + "      \"Action\": [\"sts:AssumeRole\"],\n"
               + "      \"Effect\": \"Allow\",\n"
               + "      \"Principal\": {\"AWS\": \""+S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT+"\"}\n"
               + "    }\n"
               + "  ]\n"
               + "}";
    }

    public static String createS3AccessGrantsConsumerIAMTrustRelationship() {
        return "{\n"
               + "  \"Version\": \"2012-10-17\",\n"
               + "  \"Statement\": [\n"
               + "    {\n"
               + "      \"Sid\": \"Stmt1685556427189\",\n"
               + "      \"Action\": [\"sts:AssumeRole\"],\n"
               + "      \"Effect\": \"Allow\",\n"
               + "      \"Principal\": {\"AWS\": \"" + S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT + "\"}\n"
               + "    }\n"
               + "  ]\n"
               + "}";
    }

    public static void CreateAccessGrantsBucket(String bucketName) {
        try {
            S3AccessGrantsIntegrationTestsUtils.CreateBucket(s3Client, bucketName);
        } catch (software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException e) {

        }
    }


    public static void tearDown() {

            registeredAccessGrants.forEach(accessGrantId -> {
                S3AccessGrantsIntegrationTestsUtils.deleteAccessGrant( s3ControlClient, accessGrantId,
                                                                    S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT);
            });

            S3AccessGrantsIntegrationTestsUtils.deleteAccessGrantLocation(s3ControlClient, accessGrantsInstanceLocationId);

           S3AccessGrantsIntegrationTestsUtils.deleteAccessGrantsInstance(s3ControlClient,
                                                                          S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT );


           S3AccessGrantsIntegrationTestsUtils.deleteObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                         S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT1);

           S3AccessGrantsIntegrationTestsUtils.deleteObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                     S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT2);

           S3AccessGrantsIntegrationTestsUtils.deleteBucket(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME);

            S3AccessGrantsIntegrationTestsUtils.deleteBucket(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME_NOT_REGISTERED);

    }
}
