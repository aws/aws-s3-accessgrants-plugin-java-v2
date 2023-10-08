package software.amazon.awssdk.s3accessgrants.plugin.internal;

import software.amazon.awssdk.identity.spi.IdentityProperty;

public class S3AccessGrantsUtils {

    public static final IdentityProperty PREFIX_PROPERTY = IdentityProperty.create(String.class, "S3Prefix");
    public static final IdentityProperty OPERATION_PROPERTY = IdentityProperty.create(String.class,"Operation");

}
