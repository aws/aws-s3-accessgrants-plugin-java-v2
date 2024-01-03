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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3control.S3ControlClient;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;
import software.amazon.awssdk.services.s3control.model.Permission;


public class S3AccessGrantsInstanceSetUpUtils {

    private static S3Client s3Client = null;

    private static S3Client s3ClientForCrossRegion = null;

    private static S3ControlClient s3ControlClient = null;

    private static S3ControlClient s3ControlClientForCrossRegion = null;

    private static IamClient iamClient = null;

    private static String accessGrantsInstanceLocationId = null;

    private static String accessGrantsInstanceLocationIdForCrossRegion = null;

    private static String accessGrantsInstanceLocationIdReadWriteBucket = null;

    private static String accessGrantsArn = null;

    private static String accessGrantsArnCrossRegion = null;

    private static String iamRoleArn = null;

    private static List<String> registeredAccessGrants = new ArrayList<>();

    private static List<String> registeredAccessGrantsForCrossRegion = new ArrayList<>();

    public static void setUpAccessGrantsInstanceForTests() throws IOException {
        String defaultPropertiesFilePath = System.getProperty("user.dir")+"/default.properties";
        Properties testProps = new Properties();
        testProps.load(new FileInputStream(defaultPropertiesFilePath));
        S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME = testProps.getProperty("credentialsProfile").trim();
        S3AccessGrantsIntegrationTestsUtils.ACCESS_GRANTS_IAM_ROLE_NAME = testProps.getProperty("IamRoleName").trim();
        S3AccessGrantsIntegrationTestsUtils.TEST_REGION = Region.of(testProps.getProperty("region").trim());
        S3AccessGrantsIntegrationTestsUtils.TEST_REGION_2 = Region.of(testProps.getProperty("cross_region").trim());
        S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT = testProps.getProperty("accountId").trim();
        S3AccessGrantsIntegrationTestsUtils.ACCESS_GRANTS_INSTANCE_ARN = "arn:aws:s3:"+S3AccessGrantsIntegrationTestsUtils.TEST_REGION+":"+ S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT+":access-grants/default";
        S3AccessGrantsIntegrationTestsUtils.ACCESS_GRANTS_INSTANCE_ID = "default";

        ProfileCredentialsProvider profileCredentialsProvider =
           ProfileCredentialsProvider.builder().profileName(S3AccessGrantsIntegrationTestsUtils.TEST_CREDENTIALS_PROFILE_NAME).build();

        S3AuthSchemeProvider authSchemeProvider = S3AuthSchemeProvider.defaultProvider();

        s3Client = S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, profileCredentialsProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        s3ClientForCrossRegion = S3AccessGrantsIntegrationTestsUtils.s3clientBuilder(authSchemeProvider, profileCredentialsProvider, S3AccessGrantsIntegrationTestsUtils.TEST_REGION_2);

        s3ControlClient = S3AccessGrantsIntegrationTestsUtils.s3ControlClientBuilder(profileCredentialsProvider,
                                                                                     S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        s3ControlClientForCrossRegion = S3AccessGrantsIntegrationTestsUtils.s3ControlClientBuilder(profileCredentialsProvider,
                S3AccessGrantsIntegrationTestsUtils.TEST_REGION_2);

        accessGrantsArn =
            S3AccessGrantsIntegrationTestsUtils.createAccessGrantsInstance(s3ControlClient,
                                                                           S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT);

        accessGrantsArnCrossRegion  =  S3AccessGrantsIntegrationTestsUtils.createAccessGrantsInstance(s3ControlClientForCrossRegion,
                S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT);

        iamClient =
            S3AccessGrantsIntegrationTestsUtils.iamClientBuilder(profileCredentialsProvider,
                             S3AccessGrantsIntegrationTestsUtils.TEST_REGION);

        iamRoleArn = S3AccessGrantsIntegrationTestsUtils.createS3AccessGrantsIAMRole(iamClient,
                                                                                     S3AccessGrantsIntegrationTestsUtils.ACCESS_GRANTS_IAM_ROLE_NAME,
                                                                                     createS3AccessGrantsIAMTrustRelationship(),
                                                                                     S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT);

        CreateAccessGrantsBucket(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME);

        CreateAccessGrantsBucket(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME_NOT_REGISTERED);

        CreateAccessGrantsBucket(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_READWRITE);

        CreateAccessGrantsBucket(s3ClientForCrossRegion, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_READWRITE_CROSS_REGION);


        accessGrantsInstanceLocationId = S3AccessGrantsIntegrationTestsUtils.createS3AccessGrantsLocation(s3ControlClient,
                                                                      "s3://" + S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                                      S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                                                                      iamRoleArn);

        accessGrantsInstanceLocationIdReadWriteBucket = S3AccessGrantsIntegrationTestsUtils.createS3AccessGrantsLocation(s3ControlClient,
                "s3://" + S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_READWRITE,
                S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                iamRoleArn);

        accessGrantsInstanceLocationIdForCrossRegion = S3AccessGrantsIntegrationTestsUtils.createS3AccessGrantsLocation(s3ControlClientForCrossRegion,
                "s3://" + S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_READWRITE_CROSS_REGION,
                S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT,
                iamRoleArn);

        registeredAccessGrants.add(S3AccessGrantsIntegrationTestsUtils.registerAccessGrant(s3ControlClient,
                                                                                           S3AccessGrantsIntegrationTestsUtils.ALLOWED_BUCKET_PREFIX,
                                                                                           Permission.READ, iamRoleArn,
                                                                                           S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT, accessGrantsInstanceLocationId));

        registeredAccessGrants.add(S3AccessGrantsIntegrationTestsUtils.
                                   registerAccessGrant(s3ControlClient, S3AccessGrantsIntegrationTestsUtils.ALLOWED_BUCKET_PREFIX,
                                                       Permission.WRITE, iamRoleArn,
                                                       S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT, accessGrantsInstanceLocationId));

        registeredAccessGrants.add(S3AccessGrantsIntegrationTestsUtils.registerAccessGrant(s3ControlClient,
                                                                                           S3AccessGrantsIntegrationTestsUtils.ALLOWED_BUCKET_PREFIX2, Permission.WRITE, iamRoleArn, S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT, accessGrantsInstanceLocationId));

        registeredAccessGrants.add(S3AccessGrantsIntegrationTestsUtils.registerAccessGrant(s3ControlClient,
                "PrefixA/*", Permission.READWRITE, iamRoleArn, S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT, accessGrantsInstanceLocationIdReadWriteBucket));

        registeredAccessGrantsForCrossRegion.add(S3AccessGrantsIntegrationTestsUtils.registerAccessGrant(s3ControlClientForCrossRegion,
                S3AccessGrantsIntegrationTestsUtils.ALLOWED_BUCKET_PREFIX,
                Permission.READWRITE, iamRoleArn,
                S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT, accessGrantsInstanceLocationIdForCrossRegion));

        S3AccessGrantsIntegrationTestsUtils.PutObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                      S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT1,
                                                      S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT_1_CONTENTS);

        S3AccessGrantsIntegrationTestsUtils.PutObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                                                      S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT2,
                                                      S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT_2_CONTENTS);

        S3AccessGrantsIntegrationTestsUtils.PutObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_READWRITE,
                S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT2,
                S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT_2_CONTENTS);


        S3AccessGrantsIntegrationTestsUtils.PutObject(s3ClientForCrossRegion, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_READWRITE_CROSS_REGION,
                S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT1,
                S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT_1_CONTENTS);

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

    public static void CreateAccessGrantsBucket(S3Client s3Client, String bucketName) {
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

        registeredAccessGrantsForCrossRegion.forEach(accessGrantId -> {
            S3AccessGrantsIntegrationTestsUtils.deleteAccessGrant( s3ControlClientForCrossRegion, accessGrantId,
                    S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT);
        });

        S3AccessGrantsIntegrationTestsUtils.deleteAccessGrantLocation(s3ControlClient, accessGrantsInstanceLocationId);

        S3AccessGrantsIntegrationTestsUtils.deleteAccessGrantLocation(s3ControlClient, accessGrantsInstanceLocationIdReadWriteBucket);

        S3AccessGrantsIntegrationTestsUtils.deleteAccessGrantLocation(s3ControlClientForCrossRegion, accessGrantsInstanceLocationIdForCrossRegion);

        S3AccessGrantsIntegrationTestsUtils.deleteAccessGrantsInstance(s3ControlClient,
                                                                          S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT );

        S3AccessGrantsIntegrationTestsUtils.deleteAccessGrantsInstance(s3ControlClientForCrossRegion,
                S3AccessGrantsIntegrationTestsUtils.TEST_ACCOUNT );


        S3AccessGrantsIntegrationTestsUtils.deleteObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                         S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT1);

        S3AccessGrantsIntegrationTestsUtils.deleteObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME,
                     S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT2);

        S3AccessGrantsIntegrationTestsUtils.deleteObject(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_READWRITE,
                S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT2);

        S3AccessGrantsIntegrationTestsUtils.deleteObject(s3ClientForCrossRegion, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_READWRITE_CROSS_REGION,
                S3AccessGrantsIntegrationTestsUtils.TEST_OBJECT1);

        S3AccessGrantsIntegrationTestsUtils.deleteBucket(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME);

        S3AccessGrantsIntegrationTestsUtils.deleteBucket(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_NAME_NOT_REGISTERED);

        S3AccessGrantsIntegrationTestsUtils.deleteBucket(s3Client, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_READWRITE);

        S3AccessGrantsIntegrationTestsUtils.deleteBucket(s3ClientForCrossRegion, S3AccessGrantsIntegrationTestsUtils.TEST_BUCKET_READWRITE_CROSS_REGION);

    }
}
