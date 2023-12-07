## AWS S3 ACCESS GRANTS PLUGIN FOR AWS JAVA SDK 2.0

AWS S3 ACCESS GRANTS PLUGIN provides the functionality to enable S3 customers to configure S3 ACCESS GRANTS as a permission layer on top of the S3 Clients.

S3 ACCESS GRANTS is a feature from S3 that allows its customers to configure fine-grained access permissions for the data in their buckets.

### Things to Know

---

* AWS SDK Java 2.0 is built on Java 8
* [maven] is used as the build and dependency management system

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
1.  If enableFallback option is set to false we will fallback only in case the operation/API is not supported by Access Grants.
2.  If enableFallback is set to true then we will fall back every time we are not able to get the credentials from Access Grants, no matter the reason.


```
S3AccessGrantsPlugin accessGrantsPlugin = S3AccessGrantsPlugin.builder().enableFallback(true).build();
```

While building S3 client you have to provide a credentialsProvider object which contains credentials that have access to get credentials from Access Grants.

````
S3Client s3Client = S3Client.builder()
                    .addPlugin(accessGrantsPlugin)
                    .credentialsProvider(credentialsProvider)
                    .region(REGION)
                    .build();
````

Using this S3Client to make API calls, you should be able to use Access Grants to get access to your resources.

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