package software.amazon.awssdk.s3accessgrants.plugin.internal;

import software.amazon.awssdk.identity.spi.IdentityProperty;
import software.amazon.awssdk.utils.Validate;

/**
 * The class is for defining all the utilities and constants to be used across the package
 * */
public class S3AccessGrantsUtils {

    public static final IdentityProperty PREFIX_PROPERTY = IdentityProperty.create(String.class, "S3Prefix");
    public static final IdentityProperty OPERATION_PROPERTY = IdentityProperty.create(String.class,"Operation");

    public static void argumentNotNull(Object param, String message) {
        try{
            Validate.notNull(param, message);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException(message);
        }
    }

}
