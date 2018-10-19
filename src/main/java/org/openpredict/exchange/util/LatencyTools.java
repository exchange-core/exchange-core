package org.openpredict.exchange.util;

import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.math.Quantiles.scale;

public class LatencyTools {

    public static final int LATENCY_RESOLUTION = 5; // Latency resolution: 32ns
    public static final float LATENCY_RESOLUTION_MULTIPLIER_US = (float) Math.pow(2, LATENCY_RESOLUTION) / 1000f;

    public static final double[] PERCENTILES = new double[]{50, 90, 95, 99, 99.9, 99.99};

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

    private static String formatLatencyValueAsTime(int v) {
        float value = v * LATENCY_RESOLUTION_MULTIPLIER_US;
        String timeUnit = "µs";
        if (value > 1000) {
            value /= 1000;
            timeUnit = "ms";
        }

        if (value < 3) {
            value = Math.round(value * 100) / 100f;
        } else if (value < 30) {
            value = Math.round(value * 10) / 10f;
        } else {
            value = Math.round(value);
        }

        return value + timeUnit;
    }


    private Map<String, Float> createLatencyReportGuava(IntArrayList latency, double warmupPercent) {
        final int warmupOrders = (int) (latency.size() * warmupPercent / 100.0);
        int[] dataset = latency.toArray();
        int newsize = latency.size() - warmupOrders;
        int[] dataset2 = new int[newsize];
        System.arraycopy(dataset, warmupOrders, dataset2, 0, newsize);
        Map<Integer, Double> latencyPercentiles = scale(10000).indexes(50_00, 90_00, 99_90, 99_99).compute(dataset2);
        Map<String, Float> fmt = new LinkedHashMap<>();
        latencyPercentiles.keySet().stream()
                .sorted()
                .forEach(k -> fmt.put(((float) k / 100) + "%", (float) (latencyPercentiles.get(k) / 2.0)));
        return fmt;
    }


}
