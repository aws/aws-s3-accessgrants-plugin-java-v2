package software.amazon.awssdk.s3accessgrants.plugin.internal;

import software.amazon.awssdk.identity.spi.IdentityProperty;
import software.amazon.awssdk.services.s3control.model.Privilege;
import software.amazon.awssdk.utils.Logger;
import software.amazon.awssdk.utils.Validate;

/**
 * The class is for defining all the utilities and constants to be used across the package
 * */
public class S3AccessGrantsUtils {

    public static Logger logger = Logger.loggerFor("S3Access Grants Plugin");
    public static final IdentityProperty PREFIX_PROPERTY = IdentityProperty.create(String.class, "S3Prefix");
    public static final IdentityProperty OPERATION_PROPERTY = IdentityProperty.create(String.class,"Operation");

    public static final Boolean DEFAULT_CACHE_SETTING = true;

    public static final Privilege DEFAULT_PRIVILEGE_FOR_PLUGIN = Privilege.DEFAULT;

    public static final Boolean DEFAULT_FALLBACK_SETTING = false;

    public static void argumentNotNull(Object param, String message) {
        try{
            logger.error(() -> message);
            Validate.notNull(param, message);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException(message);
        }
    }

}
