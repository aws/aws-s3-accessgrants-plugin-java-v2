## Working on S3 AccessGrants Caching Java 2.0

### Things to Know
* AWS SDK Java 2.0 is built on Java 8
* [Maven][maven] is used as the build and dependency management system

### Development Environment Setup
#### Set up IntelliJ project
* Set up [connecting to github with ssh](https://docs.github.com/en/authentication/connecting-to-github-with-ssh)
* Check out team repos IntelliJ IDEA
  - File → New → Project from Version Control ...
    ```
    Version control: Git
    URL: git@github.com:aws/aws-s3-accessgrants-cache-java-v2.git
    Directory: {Your-Local-Workplace-Dir}/aws-s3-accessgrants-cache-java-v2
    ```
* Use [GitHub flow](https://docs.github.com/en/get-started/quickstart/github-flow) to commit/review/collaborate on changes
* After a [PR](https://github.com/aws/aws-s3-accessgrants-cache-java-v2/pull/1) is approved/merged, delete PR branch both remotely and locally

#### Set up Maven Repository backed by S3
In `~/.m2/settings.xml` or create a new file if it doesn't exist
```
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 
      http://maven.apache.org/xsd/settings-1.0.0.xsd">
    <servers>
        <server>
            <id>maven-s3-accessgrants-snapshot-repo</id>
            <username>[AWS Access Key ID]</username>
            <password>[AWS Secret Access Key]</password>
        </server>
    </servers>
</settings>
```

#### Build and internally SDK dependencies that's not GA yet
Skip this step if a teammate already published the dependent jar.
* git clone https://github.com/aws/aws-sdk-java-v2
* checkout the branch `feature/master/sra-identity-auth`
* go to [Trebuchet](https://trebuchet.corp.amazon.com/manage_feature.html?featureArn=arn:aws:trebuchet:::feature:v2:f3b3721f-33b8-487a-9989-eb3cd947a72a)
* click on `Provide Models`
* under Model Build Output, download the C2J Model Build Output
* unzip that and go to s3-control_build_output\output\s3control\2018-08-20
* copy the `service-2.json` in that folder and use it to replace the `service-2.json` file in the GitHub repo you cloned `aws-sdk-java-v2/services/s3control/src/main/resource/codegen-resources/service-2.json`
* under project `aws-sdk-java-v2` root directory, build `s3control` module by running `./mvnw clean install -pl :s3control -am -Pquick`
* built artifact can be found in your local maven repo, i.e., `~/.m2/repository`

### Building and Testing
`./mvnw clean install`

### Publishing
`./mvnw deploy`
Built artifact will be published at `s3://repository.s3accessgrantssdk.internal/snapshots/` in Isengard Account `s3-staircase-integration+sdkv2@amazon.com`

[maven]: https://maven.apache.org/
