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

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.lang.Double.parseDouble;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class CalculateAverage_4 {

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

    // Runtime: 1 min, 15 sec
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        final File file = new File("measurements.txt");
        final long length = file.length();
        final int chunkCount = Runtime.getRuntime().availableProcessors();
        final StationStats[][] results = new StationStats[chunkCount][];
        final long[] chunkStartOffsets = new long[chunkCount];

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            for (int i = 1; i < chunkStartOffsets.length; i++) {
                var start = length * i / chunkStartOffsets.length;
                raf.seek(start);
                while (raf.read() != (byte) '\n') {}
                start = raf.getFilePointer();
                chunkStartOffsets[i] = start;
            }

            final var mappedFile = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, length, Arena.global());
            Thread[] threads = new Thread[chunkCount];
            for (int i = 0; i < chunkCount; i++) {
                final long chunkStart = chunkStartOffsets[i];
                final long chunkEnd = (i + 1 < chunkCount) ? chunkStartOffsets[i + 1] : length;
                threads[i] = new Thread(new ChunkProcessor(
                        mappedFile.asSlice(chunkStart, chunkEnd - chunkStart), results, i
                ));
            }

            for (Thread thread : threads) {
                thread.start();
            }

            for (Thread thread : threads) {
                thread.join();
            }
        }

        TreeMap<String, StationStats> totalsMap = new TreeMap<String, StationStats>();
        for (var statsArray : results) {
            for (var stats : statsArray) {
                totalsMap.merge(stats.name, stats, (old, curr) -> {
                    old.count += curr.count;
                    old.sum += curr.sum;
                    old.min = Math.min(old.min, curr.min);
                    old.max = Math.max(old.max, curr.max);
                    return old;
                });
            }
        }

        System.out.println(totalsMap);

        long endTime = System.currentTimeMillis();

        String time = String.format("%d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes(endTime - startTime),
                TimeUnit.MILLISECONDS.toSeconds(endTime - startTime) -
                        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(endTime - startTime))
        );
        System.out.println("Solution completed in " + time);
    }

    private static class ChunkProcessor implements Runnable {
        private static final int HASHTABLE_SIZE = 2048;
        private final MemorySegment chunk;
        private final StationStats[][] results;
        private  final int myIndex;
        private final StatsAcc[] hashtable = new StatsAcc[HASHTABLE_SIZE];

        ChunkProcessor(MemorySegment chunk, StationStats[][] results, int myIndex) {
            this.chunk = chunk;
            this.results = results;
            this.myIndex = myIndex;
        }

        @Override
        public void run() {
            for (var cursor = 0L; cursor < chunk.byteSize();) {
                long semicolonPos = findByte(cursor, ';');
                long newLinePos = findByte(cursor, '\n');
                String name = stringAt(cursor, semicolonPos);

                var intTemp = parseTemperature(semicolonPos);

                StatsAcc acc = findAcc(cursor, semicolonPos);
                acc.max += intTemp;
                acc.count++;
                acc.min = Math.min(acc.min, intTemp);
                acc.max = Math.max(acc.max, intTemp);
                cursor = newLinePos + 1;
            }

            results[myIndex] = Arrays.stream(hashtable)
                    .filter(Objects::nonNull)
                    .map(acc -> new StationStats(acc, chunk))
                    .toArray(StationStats[]::new);
        }

        private StatsAcc findAcc(long cursor, long semicolonPos) {
            int hash = hash(cursor, semicolonPos);
            int initialPos = hash & (HASHTABLE_SIZE - 1);
            int slotPos = initialPos;

            while (true) {
                var acc = hashtable[slotPos];

                if (acc == null) {
                    acc = new StatsAcc(hash, cursor, semicolonPos - cursor);
                    hashtable[slotPos] = acc;
                    return acc;
                }

                if (acc.hash == hash && acc.nameEquals(chunk, cursor, semicolonPos)) {
                    return acc;
                }

                slotPos = (slotPos + 1) & (HASHTABLE_SIZE - 1);
                if (slotPos == initialPos) {
                    throw new RuntimeException(String.format("hash %x, acc.hash %x", hash, acc.hash));
                }
            }
        }

        private int parseTemperature(long semicolonPos) {
            long off = semicolonPos + 1;
            int sign = 1;

            byte b = chunk.get(JAVA_BYTE, off++);
            if (b == '-') {
                sign = -1;
                b = chunk.get(JAVA_BYTE, off++);
            }

            int temp = b - '0';

            b = chunk.get(JAVA_BYTE, off++);
            if (b != '.') {
                temp = 10 * temp + b - '0';
                off++;
            }

            b = chunk.get(JAVA_BYTE, off);
            temp = 10 * temp + b - '0';
            return sign * temp;
        }

        private int hash(long startOffset, long limitOffset) {
            int h = 17;
            for (long off = startOffset; off < limitOffset; off++) {
                h = 31 * h + ((int) chunk.get(JAVA_BYTE, off) & 0xFF);
            }
            return h;
        }

        private long findByte(long cursor, int b) {
            for (var i = cursor; i < chunk.byteSize(); i++) {
                if (chunk.get(JAVA_BYTE, i) == b) {
                    return i;
                }
            }

            throw new RuntimeException(((char) b) + " not found");
        }

        private String stringAt(long start, long limit) {
            return new String(
                    chunk.asSlice(start, limit - start).toArray(JAVA_BYTE),
                    StandardCharsets.UTF_8
            );
        }
    }

    private static class StatsAcc {
        long nameOffset;
        long nameLen;
        int hash;
        long sum;
        int count;
        int min;
        int max;

        StatsAcc(int hash, long nameOffset, long nameLen) {
            this.hash = hash;
            this.nameOffset = nameOffset;
            this.nameLen = nameLen;
        }

        public boolean nameEquals(MemorySegment chunk, long otherNameOffset, long otherNameLimit) {
            long otherNameLen = otherNameLimit - otherNameOffset;

            return nameLen == otherNameLen && chunk.asSlice(nameOffset, nameLen).mismatch(chunk.asSlice(otherNameOffset, nameLen)) == -1;
        }
    }

    private static class StationStats implements Comparable<StationStats> {
        String name;
        long sum;
        int count;
        int min;
        int max;

        StationStats(StatsAcc acc, MemorySegment chunk) {
            name = new String(chunk.asSlice(acc.nameOffset, acc.nameLen).toArray(JAVA_BYTE), StandardCharsets.UTF_8);
            sum = acc.sum;
            count = acc.count;
            min = acc.min;
            max = acc.max;
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min / 10.0, Math.round((double) sum / count) / 10.0, max / 10.0);
        }

        @Override
        public boolean equals(Object that) {
            return that.getClass() == StationStats.class && ((StationStats) that).name.equals(this.name);
        }

        @Override
        public int compareTo(StationStats that) {
            return name.compareTo(that.name);
        }
    }
}
