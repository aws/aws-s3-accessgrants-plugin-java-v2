package software.amazon.awssdk.s3accessgrants.cache;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3control.S3ControlAsyncClient;
import software.amazon.awssdk.services.s3control.model.S3ControlException;

public interface S3AccessGrantsBucketRegionResolver {

    /**
     *
     * @param bucket name of the bucket being accessed
     * @param s3Client s3client that will be used for making the requests
     * @throws S3Exception propagate S3Exception from service call
     */

    Region resolve(String bucket, S3Client s3Client) throws S3Exception;

}
