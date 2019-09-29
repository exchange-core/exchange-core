/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.tests.util;

import org.HdrHistogram.Histogram;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public final class LatencyTools {

    private static final double[] PERCENTILES = new double[]{50, 90, 95, 99, 99.9, 99.99};

    public static Map<String, String> createLatencyReportFast(Histogram histogram) {
        final Map<String, String> fmt = new LinkedHashMap<>();
        Arrays.stream(PERCENTILES).forEach(p -> fmt.put(p + "%", formatLatencyValueAsTime((int) histogram.getValueAtPercentile(p))));
        fmt.put("W", formatLatencyValueAsTime((int) histogram.getMaxValue()));
        return fmt;
    }

    private static String formatLatencyValueAsTime(int v) {
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
