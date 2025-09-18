/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import static java.lang.Double.parseDouble;
import static java.util.stream.Collectors.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class CalculateAverage_1 {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    // Runtime: 2 min, 1 sec
    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();

        Map<String, DoubleSummaryStatistics> allStats = new BufferedReader(new FileReader("measurements.txt"))
                .lines()
                .parallel()
                .collect(
                        groupingBy(line -> line.substring(0, line.indexOf(';')),
                                summarizingDouble(line ->
                                        parseDouble(line.substring(line.indexOf(';') + 1)))));
        Map<String, String> result = allStats.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> {
                    var stats = e.getValue();
                    return String.format("%.1f/%.1f/%.1f",
                            stats.getMin(), stats.getAverage(), stats.getMax());
                },
                (l, r) -> r,
                TreeMap::new));

        System.out.println(result);

        long end = System.currentTimeMillis();

        String time = String.format("%d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes(end - start),
                TimeUnit.MILLISECONDS.toSeconds(end - start) -
                        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(end - start))
        );
        System.out.println("Solution completed in " + time);
    }
}
