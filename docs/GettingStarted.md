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

#### Build and internally publish S3 dependencies that's not GA yet
Skip this step if a teammate already published the dependent jar.
* git clone https://github.com/aws/aws-sdk-java-v2
* checkout the branch `feature/master/sra-identity-auth`
* go to [Trebuchet](https://trebuchet.corp.amazon.com/manage_feature.html?featureArn=arn:aws:trebuchet:::feature:v2:f3b3721f-33b8-487a-9989-eb3cd947a72a)
* click on `Provide Models`
* under Model Build Output, download the C2J Model Build Output
* unzip that and go to s3-control_build_output\output\s3control\2018-08-20
* copy the `service-2.json` in that folder and use it to replace the `service-2.json` file in the GitHub repo you cloned `aws-sdk-java-v2/services/s3control/src/main/resource/codegen-resources/service-2.json`
* under project `aws-sdk-java-v2` root directory, build `s3control` module by running `./mvnw clean install -pl :s3control -am -Pquick`
* locate build target `aws-sdk-java-s3control-{version}-SNAPSHOT.jar` under `aws-sdk-java-v2/services/s3control/target`
* upload to team's code share S3 bucket
```shell
kinit -f && mwinit
ada credentials update credentials update --account=527802564711 --provider=isengard --role=sdkv2-codeshare --profile=default --once
aws s3 cp services/s3control/target/aws-sdk-java-s3control-{version}-SNAPSHOT.jar s3://s3-staircase-integration-sdkv2-codeshare/libs/
```
#### Add internal S3 dependencies that's not GA yet
* under project `aws-s3-accessgrants-cache-java-v2` root directory, run
```shell
kinit -f && mwinit
ada credentials update --account=527802564711 --provider=isengard --role=sdkv2-codeshare --profile=default --once
aws s3 cp s3://s3-staircase-integration-sdkv2-codeshare/libs/aws-sdk-java-s3control-{version}-SNAPSHOT.jar ./lib/
```
* Open IntelliJ project module settings and add `./lib/aws-sdk-java-s3control-{version}-SNAPSHOT.jar` as a dependency

### Building
Since we have local dependency at the moment, we'll use IntelliJ to build/rebuild moduel `aws-s3-accessgrants-cache-java-v2`

### Testing
#### Unit Tests
Since we have local dependency at the moment, we'll use IntelliJ to Run "All Tests"

[maven]: https://maven.apache.org/
