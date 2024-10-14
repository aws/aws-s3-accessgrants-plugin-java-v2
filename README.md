## AMAZON S3 ACCESS GRANTS PLUGIN FOR AWS JAVA SDK 2.0

AMAZON S3 ACCESS GRANTS PLUGIN provides the functionality to enable S3 customers to configure S3 ACCESS GRANTS as a permission layer on top of the S3 Clients.

S3 ACCESS GRANTS is a feature from S3 that allows its customers to configure fine-grained access permissions for the data in their buckets.

### Things to Know

---

* AWS SDK Java 2.0 is built on Java 8
* Maven is used as the build and dependency management system

### Contributions

---
* Use [GitHub flow](https://docs.github.com/en/get-started/quickstart/github-flow) to commit/review/collaborate on changes
* After a PR is approved/merged, please delete the PR branch both remotely and locally

### Building From Source

---
Once you check out the code from GitHub, you can build it using the following commands.

Linux:

```./mvnw clean install```

Windows:

```./mvnw.cmd clean install```
### USING THE PLUGIN

---

The recommended way to use the S3 ACCESS GRANTS PLUGIN for Java in your project is to consume it from Maven Central


```
 <dependency>
    <groupId>software.amazon.s3.accessgrants</groupId>
    <artifactId>aws-s3-accessgrants-java-plugin</artifactId>
    <version>replace with latest version</version>
</dependency>
```

Create a S3AccessGrantsPlugin object and choose if you want to enable fallback.
1. If enableFallback option is set to false we will fallback only in case the operation/API is not supported by Access Grants.
2. If enableFallback is set to true then we will fall back every time we are not able to get the credentials from Access Grants, no matter the reason.

You can also choose to add custom user agent. A user agent is a request header that identifies the application, version number, operating system, vendor, and programming language of the user making the request. User agent names can consist only of lower case letters, upper case letters, numbers, dots (.), and hyphens (-).
1. If userAgent is set to null or not set, the user agent will be set to 'aws-s3-accessgrants-java-sdk-v2-plugin'.
2. If you pass a String object to user agent that string will be added as a suffix to 'aws-s3-accessgrants-java-sdk-v2-plugin-'.
```
S3AccessGrantsPlugin accessGrantsPlugin = S3AccessGrantsPlugin.builder().enableFallback(true).userAgent("userAgent").build();
```

While building S3 client you have to provide a credentialsProvider object which contains credentials that have access to get credentials from Access Grants.
We only support IAM credentials with this release.

````
S3Client s3Client = S3Client.builder()
                    .addPlugin(accessGrantsPlugin)
                    .credentialsProvider(credentialsProvider)
                    .region(REGION)
                    .build();
````

Using this S3Client to make API calls, you should be able to use Access Grants to get access to your resources.

### Turn on cross-region access

The plugin by default does not support cross-region access of S3 Buckets/data. 
In order to turn on the cross-region support, please configure the S3Client to support cross-region access. The plugin will default to the cross-region setting on the S3Client.

```
example - 
        S3AccessGrantsPlugin accessGrantsPlugin =
                S3AccessGrantsPlugin.builder().build();
                
        S3Client s3Client =
                S3Client.builder()
                        .crossRegionAccessEnabled(true)
                        .credentialsProvider(credentialsProvider)
                        .addPlugin(accessGrantsPlugin)
                        .region(S3AccessGrantsIntegrationTestsUtils.TEST_REGION)
                        .build();
```

### Cross-account support

The plugin makes S3 head bucket requests to determine bucket location. 
In case of cross-account access S3 expects s3:ListBucket permission for the requesting account on the requested bucket. Please add the necessary permission if the plugin will be used for cross-account access.

### Note
The plugin supports deleteObjects API and copyObject API which S3 Access Grants does not implicitly support. For these APIs we get the common prefix of all the object keys and find their common ancestor. If you have a grant present on the common ancestor, you will get Access Grants credentials based on that grant. For copyObject API the source and destination buckets should be same, since a grant cannot give access to multiple buckets.

### Turn on metrics

The plugin integrates with the Metrics publisher specified on the S3 Clients and does not require any separate metrics publisher to be defined during the plugin creation.


```

MetricPublisher metricPublisher = CloudWatchMetricPublisher.builder().namespace("S3AccessGrantsPlugin").cloudWatchClient(CloudWatchAsyncClient.builder().region(S3AccessGrantsIntegrationTestsUtils.TEST_REGION).credentialsProvider(credentialsProvider).build()).build();

S3Client s3Client = S3Client.builder()
                    .credentialsProvider(credentialsProvider)
                    .addPlugin(accessGrantsPlugin)
                    .region(S3AccessGrantsIntegrationTestsUtils.TEST_REGION)
                    .overrideConfiguration(config -> config.addMetricPublisher(metricPublisher))
                    .build();
            
```

### Change logging level

Turning on the AWS SDK level logging should turn on the logging for the S3 Access grants plugin. You can also control the logging for the plugin specifically by adding the below config to your log4j.properties file.

```
logger.s3accessgrants.name = software.amazon.awssdk.s3accessgrants
logger.s3accessgrants.level = debug
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.