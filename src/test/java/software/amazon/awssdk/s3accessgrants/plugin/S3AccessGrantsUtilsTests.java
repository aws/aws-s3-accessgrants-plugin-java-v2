package software.amazon.awssdk.s3accessgrants.plugin;

import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils;


import java.util.ArrayList;
import java.util.List;

public class S3AccessGrantsUtilsTests {

    @Test
    public void test_lowest_common_ancestor_when_ancestor_exists() {
        List<String> keys1 = new ArrayList<>();
        keys1.add("A/B/C/log.txt");
        keys1.add("B/A/C/log.txt");
        keys1.add("C/A/B/log.txt");
        Assert.assertEquals(S3AccessGrantsUtils.getCommonPrefixFromMultiplePrefixes(keys1), "/");
        List<String> keys2 = new ArrayList<>();
        keys2.add("ABC/A/B/C/log.txt");
        keys2.add("ABC/B/A/C/log.txt");
        keys2.add("ABC/C/A/B/log.txt");
        Assert.assertEquals(S3AccessGrantsUtils.getCommonPrefixFromMultiplePrefixes(keys2), "/ABC/");
        List<String> keys3 = new ArrayList<>();
        keys3.add("ABC/A/B/C/log.txt");
        keys3.add("ABC/B/A/C/log.txt");
        keys3.add("ABC/C/A/B/log.txt");
        keys3.add("XYZ/X/Y/Y/log.txt");
        keys3.add("XYZ/Y/X/Z/log.txt");
        keys3.add("XYZ/Z/X/Y/log.txt");
        Assert.assertEquals(S3AccessGrantsUtils.getCommonPrefixFromMultiplePrefixes(keys3), "/");
        List<String> keys4 = new ArrayList<>();
        keys4.add("folder/path123/A/logs");
        keys4.add("folder/path234/A/logs");
        keys4.add("folder/path234/A/artifacts");
        Assert.assertEquals(S3AccessGrantsUtils.getCommonPrefixFromMultiplePrefixes(keys4), "/folder/path");
    }
}
