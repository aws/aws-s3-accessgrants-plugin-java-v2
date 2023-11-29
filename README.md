## AWS S3 AccessGrants Caching for AWS SDK Java 2.0

The **AWS S3 AccessGrants Caching Java 2.0** is a library that provides client side caching capability on temporary data access credentials.

We implemented three caches to enhance S3 Access Grants Plugin performance. These caches act as loading caches which means if the entry is not available in cache, we get the value from the service and load it in the cache.
1. **Access Grants Cache** : To cache all the credentials returned by Access Grants except for object level credentials. The cache size is 30,000. The TTL of this cache is 90% of the duration returned by getDataAccess.
2. **Access Denied Cache** : To cache Access Denied responses from Access Grants. The cache size of Access Denied cache is 3,000 and the TTL is 5 mins.
3. **Account Id Resolver Cache** : To cache account Id of the bucket owner. The cache size of this cache is 1000 and TTL is one day.

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

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

