package org.openpredict.exchange.util;

import org.HdrHistogram.Histogram;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class LatencyTools {


    public static final double[] PERCENTILES = new double[]{50, 90, 95, 99, 99.9, 99.99};

    public static Map<String, String> createLatencyReportFast(Histogram histogram) {
        Map<String, String> fmt = new LinkedHashMap<>();
        Arrays.stream(PERCENTILES).forEach(p -> {
            String formattedValue = formatLatencyValueAsTime((int) histogram.getValueAtPercentile(p));
            fmt.put(p + "%", formattedValue);
        });
        fmt.put("W", formatLatencyValueAsTime((int) histogram.getMaxValue()));
        return fmt;
    }

    public static Map<String, String> createLatencyReportFast(IntLongHashMap latencies) {
        long size = latencies.values().sum();
        TreeMap<Integer, Long> grouped = new TreeMap<>();
        latencies.forEachKeyValue(grouped::put);
        int worstLatency = grouped.lastEntry().getKey();

        long[] percentileC = Arrays.stream(PERCENTILES).mapToLong(p -> (long) (size * p / 100.0)).toArray();

        Map<String, String> fmt = new LinkedHashMap<>();

        int stage = 0;
        long accum = 0;
        for (Map.Entry<Integer, Long> entry : grouped.entrySet()) {
            Long v = entry.getValue();
            accum += v;
            if (accum > percentileC[stage]) {
                String formattedValue = formatLatencyValueAsTime(entry.getKey());
                fmt.put((PERCENTILES[stage]) + "%", formattedValue);
                if (++stage >= percentileC.length) {
                    break;
                }
            }
        }
        fmt.put("W", formatLatencyValueAsTime(worstLatency));
        return fmt;
    }

    public static String formatLatencyValueAsTime(int v) {
        float value = v / 1000f;
        String timeUnit = "µs";
        if (value > 1000) {
            value /= 1000;
            timeUnit = "ms";
        }

        if (value < 3) {
            return Math.round(value * 100) / 100f + timeUnit;
        } else if (value < 30) {
            return Math.round(value * 10) / 10f + timeUnit;
        } else {
            return Math.round(value) + timeUnit;
        }
    }
}
