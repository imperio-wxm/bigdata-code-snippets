package com.wxmimperio.spark;

import org.apache.commons.net.ntp.TimeStamp;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Integer.parseInt;

public class Test {

    @org.junit.Test
    public void test001() throws Exception {
        String time = "2019-01-24 17:00:00";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(new Timestamp(simpleDateFormat.parse(time).getTime()));

        System.out.println(simpleDateFormat.parse(time).getTime());

        System.out.println(System.currentTimeMillis());

        Integer a = Integer.valueOf(1);
        a.intValue();

        int i = 45;
        if (i >= IntegerCache.low && i <= IntegerCache.high) {
            System.out.println(IntegerCache.cache[i + (-IntegerCache.low)]);
        }

    }

    private static class IntegerCache {
        static final int low = -128;
        static final int high;
        static final Integer cache[];

        static {
            // high value may be configured by property
            int h = 127;
            String integerCacheHighPropValue =
                    sun.misc.VM.getSavedProperty("java.lang.Integer.IntegerCache.high");
            if (integerCacheHighPropValue != null) {
                try {
                    int i = parseInt(integerCacheHighPropValue);
                    i = Math.max(i, 127);
                    // Maximum array size is Integer.MAX_VALUE
                    h = Math.min(i, Integer.MAX_VALUE - (-low) - 1);
                } catch (NumberFormatException nfe) {
                    // If the property cannot be parsed into an int, ignore it.
                }
            }
            high = h;

            cache = new Integer[(high - low) + 1];
            int j = low;
            for (int k = 0; k < cache.length; k++)
                cache[k] = new Integer(j++);

            // range [-128, 127] must be interned (JLS7 5.1.7)
            assert IntegerCache.high >= 127;
        }

        private IntegerCache() {
        }
    }

    @org.junit.Test
    public void testLeecode() {
        int[] nums = new int[]{2, 7, 11, 15};
        System.out.println(Arrays.toString(twoSum(nums, 9)));

        System.out.println(Arrays.toString(twoSum2(nums, 9)));
    }

    public int[] twoSum(int[] nums, int target) {
        int[] result = new int[2];
        for (int i = 0; i < nums.length; i++) {
            for (int j = i + 1; j < nums.length; j++) {
                if (nums[i] + nums[j] == target) {
                    result = new int[]{i, j};
                }
            }
        }
        return result;
    }

    public int[] twoSum2(int[] nums, int target) {
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < nums.length; i++) {
            int sub = target - nums[i];
            if (map.containsKey(sub)) {
                return new int[]{map.get(sub), i};
            }
            map.put(nums[i], i);
        }
        return nums;
    }
}
