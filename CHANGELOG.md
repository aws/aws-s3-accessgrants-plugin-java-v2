## 2.4.1 2026-02-11
### Features
* Additional fixes for null pointer issues during concurrent operations

## 2.4.0 2026-01-27
### Features
* Added caching for AccessDenied responses when resolving account IDs to improve performance
* Fixed thread safety and null pointer issues during concurrent operations.

---
## 2.3.0 2024-10-23
### Features
* Customers can now set their own user agents on the internal clients for audit purposes.
* Added default user agent for internal clients.
* Increased accountIdResolverCache and BucketRegionResolverCache ttl to 1 hour.

---
## 2.2.0 2024-07-17
### Features
* Added support for HeadBucket API and GetObjectAttributes API.
* Added Change Log.
* Updated the ReadMe file.