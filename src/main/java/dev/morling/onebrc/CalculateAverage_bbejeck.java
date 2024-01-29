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

import static java.util.stream.Collectors.*;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class CalculateAverage_bbejeck {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    public static void main(String[] args) throws IOException {
        // Map<String, Double> measurements1 = Files.lines(Paths.get(FILE))
        // .map(l -> l.split(";"))
        // .collect(groupingBy(m -> m[0], averagingDouble(m -> Double.parseDouble(m[1]))));
        //
        // measurements1 = new TreeMap<>(measurements1.entrySet()
        // .stream()
        // .collect(toMap(e -> e.getKey(), e -> Math.round(e.getValue() * 10.0) / 10.0)));
        // System.out.println(measurements1);

        Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
                MeasurementAggregator::new,
                (a, m) -> {
                    a.min = Math.min(a.min, m.value);
                    a.max = Math.max(a.max, m.value);
                    a.sum += m.value;
                    a.count++;
                },
                (agg1, agg2) -> {
                    var res = new MeasurementAggregator();
                    res.min = Math.min(agg1.min, agg2.min);
                    res.max = Math.max(agg1.max, agg2.max);
                    res.sum = agg1.sum + agg2.sum;
                    res.count = agg1.count + agg2.count;

                    return res;
                },
                agg -> {
                    return new ResultRow(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max);
                });

        Map<String, ResultRow> measurements = new TreeMap<>(Stream.generate(new MappedFileLineSupplier(FILE)).limit(1_000_000_000)
                .map(l -> new Measurement(l.split(";")))
                .collect(groupingBy(m -> m.station(), collector)));

        System.out.println(measurements);
    }

    static class MappedFileLineSupplier implements Supplier<String> {
        private final String file;
        private static final int LINE_COUNT = 1_000_000_000;
        private int completedLineCount = 0;
        LinkedBlockingQueue<String> queue;

        public MappedFileLineSupplier(final String file) {
            this.file = file;
            queue = new LinkedBlockingQueue<>();
            start();
        }

        private void start() {
            Thread.ofPlatform().start(() -> {
                try (BufferedReader reader = new BufferedReader(new FileReader((this.file)), 8 * 1024)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        completedLineCount += 1;
                        if (completedLineCount % 100_000_000 == 0) {
                            System.out.println("Processed 100_000_000 lines");
                        }
                        queue.put(line);
                    }
                    System.out.println("Quitting processing");
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public String get() {
            String line = "";
            try {
                line = queue.poll(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return line;
        }
    }
}
