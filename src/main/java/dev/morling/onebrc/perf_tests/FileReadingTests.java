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

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class FileReadingTests {

    @Benchmark
    public void measureBufferedReader(Blackhole blackhole) {
        String filePath = "./src/main/java/dev/morling/onebrc/perf_tests/measurements.txt";
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)))) {
            reader.lines().forEach(blackhole::consume);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void measureBufferedReaderCustomBuffer(Blackhole blackhole) {
        String filePath = "./src/main/java/dev/morling/onebrc/perf_tests/measurements.txt";
        try (BufferedReader reader = java.nio.file.Files.newBufferedReader(Path.of(filePath))) {
            reader.lines().forEach(blackhole::consume);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void measureReadLines(Blackhole blackhole) {
        ;
        try {
            Files.lines(Path.of("./src/main/java/dev/morling/onebrc/perf_tests/measurements.txt"))
                    .forEach(blackhole::consume);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public void measureMemoryMappedReader(Blackhole blackhole) {
        try (RandomAccessFile file = new RandomAccessFile("./src/main/java/dev/morling/onebrc/perf_tests/measurements.txt", "r");
             FileChannel fileChannel = file.getChannel()) {

            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
            StringBuilder builder = new StringBuilder();
            while (buffer.hasRemaining()) {
                char c = buffer.getChar();
                if (c == '\n') {
                    blackhole.consume(builder.toString());
                    builder.setLength(0);
                } else {
                    builder.append(c);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
