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
import java.nio.charset.StandardCharsets;
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

    record MeasurementResult(long lineCount, Map<byte[], MeasurementAggregator> result) {
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

        Function<Map.Entry<byte[], MeasurementAggregator>, String> bytesToString = entry -> new String(entry.getKey(), 1, entry.getKey()[0], StandardCharsets.UTF_8);
        Function<Map.Entry<byte[], MeasurementAggregator>, MeasurementAggregator> aggregator = Map.Entry::getValue;
        long start = System.currentTimeMillis();
        Map<String, MeasurementAggregator> mergedMaps = getListOfMaps().stream()
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(bytesToString, aggregator, (agg1, agg2) -> {
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
        // .map(l -> new Measurement(l.split(";")))
        // .collect(groupingBy(m -> m.station(), collector)));
        long end = System.currentTimeMillis() - start;
        System.out.println(measurements);
        // System.out.printf("Took %d seconds", end / 1000);
    }

    static List<Map<byte[], MeasurementAggregator>> getListOfMaps() {
        List<Map<byte[], MeasurementAggregator>> mapList;
        try (RandomAccessFile file = new RandomAccessFile(FILE, "r");
                FileChannel fileChannel = file.getChannel()) {
            long total = fileChannel.size();
            long segments = total / Integer.MAX_VALUE;
            long remainder = total % Integer.MAX_VALUE;
            List<MappedByteBuffer> buffers = new ArrayList<>();
            long end = Integer.MAX_VALUE;
            long start = 0;
            for (int i = 0; i < segments; i++) {
                buffers.add(fileChannel.map(FileChannel.MapMode.READ_ONLY, start, end));
                start = start + Integer.MAX_VALUE + 1L;
            }
            start = start + 1L;
            long leftOver = total - start;
            buffers.add(fileChannel.map(FileChannel.MapMode.READ_ONLY, start, leftOver));
            // System.out.printf("File size is %d segments are %d remainder is %d %n", total, segments, remainder);
            // buffers.forEach(buffer -> System.out.printf("Buffer remaining %d %n", buffer.remaining()));
            List<Future<MeasurementResult>> lineProcessingFutures = new ArrayList<>();
            long allStart = System.currentTimeMillis();
            try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
                buffers.forEach(buffer -> {
                    lineProcessingFutures.add(executorService.submit(new MappedSegmentLineProcessor(buffer)));
                });
                // System.out.println("All tasks started, will start checking for completion");
                long totalLines = 0L;
                long totalRecords = 0L;
                mapList = new ArrayList<>(lineProcessingFutures.size());
                for (Future<MeasurementResult> mapFuture : lineProcessingFutures) {
                    try {
                        MeasurementResult measurementResult = mapFuture.get();
                        Map<byte[], MeasurementAggregator> map = measurementResult.result();
                        mapList.add(map);
                        totalRecords += map.size();
                        totalLines += measurementResult.lineCount();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }

                long allEnd = System.currentTimeMillis();
                // System.out.printf("Done processing 13G file, %d lines and %d records in %d seconds %n", totalLines, totalRecords, (allEnd - allStart) / 1000);
                return mapList;
            }

        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class MappedSegmentLineProcessor implements Callable<MeasurementResult> {
        private final MappedByteBuffer mappedByteBuffer;
        private final int listCapacity = 100;
        private final List<Object[]> records = new ArrayList<>(listCapacity);
        private final Map<byte[], MeasurementAggregator> map = new HashMap<>();
        private ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES * 4);

        public MappedSegmentLineProcessor(MappedByteBuffer mappedByteBuffer) {
            this.mappedByteBuffer = mappedByteBuffer;
            for (int i = 0; i < listCapacity; i++) {
                Object[] reading = new Object[2];
                byte[] st = new byte[100];
                byte[] val = new byte[25];
                reading[0] = st;
                reading[1] = val;
                records.add(reading);
            }
        }

        private void process(List<Object[]> records, int count) {
            for (int i = 0; i < count; i++) {
                byte[] stationBytes = (byte[]) records.get(i)[0];
                byte[] stationReading = (byte[]) records.get(i)[1];
                double reading = Double.parseDouble(new String(stationReading, 1, stationReading[0]));

                map.compute(stationBytes, (key, value) -> {
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
        }

        @Override
        public MeasurementResult call() throws Exception {
            byte[] station = null;
            byte[] reading = null;
            byte[] collector = new byte[500];
            int listCapacity = 100;
            long lineCount = 0L;
            long start = System.currentTimeMillis();
            int currentIndex = 0;
            int recordCount = 0;
            int totalRecordCount = 0;
            int remaining = mappedByteBuffer.remaining();
            while (mappedByteBuffer.hasRemaining()) {
                byte c = mappedByteBuffer.get();
                if (c == ';') {
                    station = (byte[]) records.get(recordCount)[0];
                    station[0] = (byte) (currentIndex);
                    System.arraycopy(collector, 0, station, 1, currentIndex + 1);
                    currentIndex = 0;
                }
                else if (c == '\n') {
                    reading = (byte[]) records.get(recordCount)[1];
                    reading[0] = (byte) (currentIndex);
                    System.arraycopy(collector, 0, reading, 1, currentIndex + 1);
                    currentIndex = 0;
                    lineCount += 1;
                    recordCount += 1;
                    if (recordCount == listCapacity) {
                        totalRecordCount += recordCount;
                        recordCount = 0;
                        process(records, recordCount);
                        buffer.clear();
                    }
                }
                else {
                    collector[currentIndex++] = c;
                }
            }
            process(records, recordCount);

            long end = System.currentTimeMillis() - start;
            // System.out.printf("Done processing %s lines in %d seconds %n", lineCount, end / 1000);
            // System.out.printf("Done processing %s records in %d seconds %n", totalRecordCount, end / 1000);
            return new MeasurementResult(lineCount, map);
        }
    }

}
