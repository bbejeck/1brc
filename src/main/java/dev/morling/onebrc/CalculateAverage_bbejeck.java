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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    record MeasurementResult(long lineCount, Map<String, MeasurementAggregator> result) {
    }

    public static void main(String[] args) throws IOException {

        Function<Map.Entry<String, MeasurementAggregator>, String> toKey = Map.Entry::getKey;
        Function<Map.Entry<String, MeasurementAggregator>, MeasurementAggregator> aggregator = Map.Entry::getValue;
        Map<String, MeasurementAggregator> mergedMaps = getListOfMaps().stream()
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(toKey, aggregator, (agg1, agg2) -> {
                    var res = new MeasurementAggregator();
                    res.min = Math.min(agg1.min, agg2.min);
                    res.max = Math.max(agg1.max, agg2.max);
                    res.sum = agg1.sum + agg2.sum;
                    res.count = agg1.count + agg2.count;
                    return res;
                }));

        Map<String, ResultRow> measurements = new TreeMap<>(mergedMaps
                .entrySet()
                .stream()
                .collect(
                        Collectors.toMap(Map.Entry::getKey, (entry) -> {
                            MeasurementAggregator agg = entry.getValue();

                            return new ResultRow(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max);
                        })));

        System.out.println(measurements);
    }

    static List<Map<String, MeasurementAggregator>> getListOfMaps() {
        List<Map<String, MeasurementAggregator>> mapList;
        try (RandomAccessFile file = new RandomAccessFile(FILE, "r");
                FileChannel fileChannel = file.getChannel()) {
            long total = fileChannel.size();
            long segments = total / Integer.MAX_VALUE;
            List<MappedByteBuffer> buffers = new ArrayList<>();
            long end = Integer.MAX_VALUE;
            if (segments == 0) {
                buffers.add(fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, total));
            }
            else {
                long start = 0;
                for (int i = 0; i < segments; i++) {
                    MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, start, end);
                    int untilNewLineEndCounter = 0;
                    byte c = buffer.get((int) end - 1);
                    while (c != '\n') {
                        c = buffer.get((int) end - (++untilNewLineEndCounter));
                    }
                    buffer.limit((int) end - untilNewLineEndCounter);
                    buffers.add(buffer);
                    start = (start + Integer.MAX_VALUE + 1L) - untilNewLineEndCounter;
                }
                start = start + 1L;
                long leftOver = total - start;
                buffers.add(fileChannel.map(FileChannel.MapMode.READ_ONLY, start, leftOver));
            }
            List<Future<MeasurementResult>> lineProcessingFutures = new ArrayList<>();
            try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
                buffers.forEach(buffer -> {
                    lineProcessingFutures.add(executorService.submit(new MappedSegmentLineProcessor(buffer)));
                });
                mapList = new ArrayList<>(lineProcessingFutures.size());
                for (Future<MeasurementResult> mapFuture : lineProcessingFutures) {
                    try {
                        MeasurementResult measurementResult = mapFuture.get();
                        Map<String, MeasurementAggregator> map = measurementResult.result();
                        mapList.add(map);
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                return mapList;
            }

        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class MappedSegmentLineProcessor implements Callable<MeasurementResult> {
        private final MappedByteBuffer mappedByteBuffer;
        private final Map<String, MeasurementAggregator> map = new HashMap<>();

        public MappedSegmentLineProcessor(MappedByteBuffer mappedByteBuffer) {
            this.mappedByteBuffer = mappedByteBuffer;
        }

        private void process(byte[] collector, int stationEnd, int readingEnd) {

            String stationKey = new String(collector, 0, stationEnd);
            double reading = getDouble(collector, stationEnd, readingEnd);

            map.compute(stationKey, (key, value) -> {
                if (value == null) {
                    value = new MeasurementAggregator();
                    value.count = 1;
                    value.min = reading;
                    value.max = reading;
                    value.sum = reading;
                }
                else {
                    value.count = value.count + 1;
                    value.min = Math.min(reading, value.min);
                    value.max = Math.max(reading, value.max);
                    value.sum = value.sum + reading;
                }
                return value;
            });
        }

        private double getDouble(byte[] bytes, int start, int end) {
            double doubleValue = 0.0;
            int multiplier = 1;
            double dividend = 10.0;
            int signer = 1;
            boolean foundDot = false;
            for (int i = start; i < end; i++) {
                char c = (char) bytes[i];
                if (c == '-') {
                    signer *= -1;
                    continue;
                }
                if (c == '.') {
                    foundDot = true;
                    continue;
                }
                if (foundDot) {
                    doubleValue += (c - '0') / dividend;
                    dividend *= 10;
                }
                else {
                    doubleValue *= multiplier;
                    doubleValue += c - '0';
                    multiplier = (multiplier << 3) + (multiplier << 1);
                }
            }
            doubleValue *= signer;
            return doubleValue;
        }

        @Override
        public MeasurementResult call() throws Exception {
            byte[] collector = new byte[500];
            long lineCount = 0L;
            int currentIndex = 0;
            int stationEnd = 0;
            int readingEnd = 0;
            while (mappedByteBuffer.hasRemaining()) {
                byte c = mappedByteBuffer.get();
                if (c == ';') {
                    stationEnd = currentIndex;
                }
                else if (c == '\n') {
                    readingEnd = currentIndex;
                    currentIndex = 0;
                    lineCount += 1;
                    process(collector, stationEnd, readingEnd);
                }
                else {
                    collector[currentIndex++] = c;
                }
            }
            return new MeasurementResult(lineCount, map);
        }
    }
}
