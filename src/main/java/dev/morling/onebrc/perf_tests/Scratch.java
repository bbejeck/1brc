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

package dev.morling.onebrc.perf_tests;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;

/**
 * User: Bill Bejeck
 * Date: 1/29/24
 * Time: 9:19 AM
 */
public class Scratch {
    private static final String COMPLETED = "COMPLETED";

    public static void main(String[] args) {
        String FILE = "./measurements.txt";

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
            System.out.printf("File size is %d  segments are %d remainder is %d %n", total, segments, remainder);
            buffers.forEach(buffer -> System.out.printf("Buffer remaining %d %n", buffer.remaining()));
            List<Future<Long>> lineProducerFutures = new ArrayList<>();
            List<Future<Map<String, MeasurementAggregator>>> measurementFutures = new ArrayList<>();
            long allStart = System.currentTimeMillis();
            try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
                buffers.forEach(buffer -> {
                    BlockingQueue<String> queue = new ArrayBlockingQueue<>(15_000);
                    lineProducerFutures.add(executorService.submit(new MappedSegmentLineProducer(buffer, queue)));
                    measurementFutures.add(executorService.submit(new MappedSegmentLineConsumer(queue)));
                });
                System.out.println("All tasks started, will start checking for completion");
                long totalLines = 0L;
                long totalRecords = 0L;
                for (Future<Long> future : lineProducerFutures) {
                    try {
                        long lines = future.get();
                        totalLines += lines;
                        System.out.printf("Producing Future %s  Done with %d lines %n", future, lines);
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }

                for (Future<Map<String, MeasurementAggregator>> mapFuture : measurementFutures) {
                    try {
                        Map<String, MeasurementAggregator> map = mapFuture.get();
                        totalRecords += map.size();
                        System.out.printf("Consuming Future %s Done with %d records %n", map, map.size());
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }

                long allEnd = System.currentTimeMillis();
                System.out.printf("Done processing 13G file, %d lines and %d records in %d seconds %n", totalLines, totalRecords, (allEnd - allStart) / 1000);
            }

        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class MappedSegmentLineProducer implements Callable<Long> {
        private final MappedByteBuffer mappedByteBuffer;
        private final BlockingQueue<String> queue;
        private final Map<String, MeasurementAggregator> map = new TreeMap<>();

        public MappedSegmentLineProducer(MappedByteBuffer mappedByteBuffer, BlockingQueue<String> queue) {
            this.mappedByteBuffer = mappedByteBuffer;
            this.queue = queue;
        }

        @Override
        public Long call() throws Exception {
            StringBuilder builder = new StringBuilder();
            long lineCount = 0L;
            long start = System.currentTimeMillis();
            System.out.printf("Starting processing buffer %s%n", this.mappedByteBuffer);
            while (mappedByteBuffer.hasRemaining()) {
                char c = (char) mappedByteBuffer.get();
                if (c == '\n') {
                    String line = builder.toString();
                    queue.put(line);
                    builder.setLength(0);
                    lineCount += 1;
                }
                else {
                    builder.append(c);
                }
            }
            long end = System.currentTimeMillis() - start;
            System.out.printf("Done processing %s lines in %d seconds %n", lineCount, end / 1000);
            return lineCount;
        }
    }

    static class MappedSegmentLineConsumer implements Callable<Map<String, MeasurementAggregator>> {

        private final BlockingQueue<String> queue;
        private final Map<String, MeasurementAggregator> map = new HashMap<>();

        public MappedSegmentLineConsumer(BlockingQueue<String> queue) {
            this.queue = queue;
        }

        @Override
        public Map<String, MeasurementAggregator> call() throws Exception {
            String line;
            long start = System.currentTimeMillis();
            int consumedCount = 0;
            System.out.println("Started consuming lines from queue");
            while ((line = queue.poll(5, TimeUnit.SECONDS)) != null) {
                String[] parts = line.split(";");
                if (parts.length < 2)
                    continue;
                double reading = Double.parseDouble(parts[1]);
                map.compute(parts[0], (key, value) -> {
                    if (value == null) {
                        return new MeasurementAggregator(reading);
                    }
                    else {
                        value.count = value.count + 1;
                        value.min = Math.min(reading, value.min);
                        value.max = Math.max(reading, value.max);
                        value.sum = value.sum + reading;
                        return value;
                    }
                });
            }
            long end = System.currentTimeMillis();
            System.out.printf("Done consuming %d records in %d seconds", map.size(), (end - start) / 1000);
            return map;
        }
    }

    static class MeasurementAggregator {

        public MeasurementAggregator(double initial) {
            min = initial;
            max = initial;
            sum = initial;
            count = 1;
        }

        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }
}
