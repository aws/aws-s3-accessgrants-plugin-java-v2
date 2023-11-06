## AWS S3 ACCESS GRANTS PLUGIN FOR AWS JAVA SDK 2.0

AWS S3 ACCESS GRANTS PLUGIN provides the functionality to enable S3 customers to configure S3 ACCESS GRANTS as a permission layer on top of the S3 Clients.

S3 ACCESS GRANTS is a feature from S3 that allows its customers to configure fine-grained access permissions for the data in their buckets.

### USING THE PLUGIN

---

The recommended way to use the S3 ACCESS GRANTS PLUGIN or Java in your project is to consume it from Maven Central


```
 <dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>aws-s3-accessgrants-java-plugin</artifactId>
    <version>latest version</version>
  </dependency>
```

### Turn on metrics

The plugin integrates with the Metrics publisher specified on the S3 Clients and does not require any separate metrics publisher to be defined during the plugin creation.

example metrics publisher configuration on the S3 Client

```

MetricPublisher metricPublisher = CloudWatchMetricPublisher.builder().namespace("access-grants-plugin").cloudWatchClient(CloudWatchAsyncClient.builder().region(S3AccessGrantsIntegrationTestsUtils.TEST_REGION).credentialsProvider(credentialsProvider).build()).build();

S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .addPlugin(accessGrantsPlugin)
                .region(S3AccessGrantsIntegrationTestsUtils.TEST_REGION)
                .overrideConfiguration(config -> config.addMetricPublisher(metricPublisher))
                .build();
            
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

