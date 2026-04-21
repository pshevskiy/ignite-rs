package io.ignite.rs.parity;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Phase 6 — Java thin-client comparison bench.
 *
 * Not a proper JMH fork — just a nanoTime-based harness sufficient for
 * coarse-grained mean/p50/p95/p99 numbers against ignite-rs's criterion
 * baseline.
 *
 * Usage:
 *   java -cp target/ignite-rs-parity-driver.jar \
 *     io.ignite.rs.parity.JmhBench 127.0.0.1:10800 [reps]
 */
public class JmhBench {
    private static final int DEFAULT_REPS = 100_000;
    private static final int WARMUP = 1_000;

    public static void main(String[] args) throws Exception {
        String addr = args.length > 0 ? args[0] : "127.0.0.1:10800";
        int reps = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_REPS;

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(addr))) {
            benchPutSingle(client, reps);
            benchGetSingle(client, reps);
            benchPutAll(client, reps, 10);
            benchPutAll(client, reps, 100);
            benchPutAll(client, reps, 1000);
        }
    }

    private static void benchPutSingle(IgniteClient client, int reps) {
        ClientCache<Integer, byte[]> cache = client.getOrCreateCache("JMH_PUT_SINGLE");
        byte[] payload = new byte[16];

        // warmup
        for (int i = 0; i < WARMUP; i++) cache.put(i, payload);

        long[] samples = new long[reps];
        for (int i = 0; i < reps; i++) {
            long t0 = System.nanoTime();
            cache.put(i, payload);
            samples[i] = System.nanoTime() - t0;
        }
        report("put_single/1_entry_16b", samples);
    }

    private static void benchGetSingle(IgniteClient client, int reps) {
        ClientCache<Integer, byte[]> cache = client.getOrCreateCache("JMH_GET_SINGLE");
        byte[] payload = new byte[16];

        // seed 1024 keys to match Rust bench
        for (int i = 0; i < 1024; i++) cache.put(i, payload);

        // warmup
        for (int i = 0; i < WARMUP; i++) cache.get(i & 0x3FF);

        long[] samples = new long[reps];
        for (int i = 0; i < reps; i++) {
            int key = i & 0x3FF;
            long t0 = System.nanoTime();
            cache.get(key);
            samples[i] = System.nanoTime() - t0;
        }
        report("get_single/1_entry_16b", samples);
    }

    private static void benchPutAll(IgniteClient client, int reps, int batchSize) {
        ClientCache<Integer, byte[]> cache =
                client.getOrCreateCache("JMH_PUT_ALL_" + batchSize);
        byte[] payload = new byte[16];

        // fewer reps for bigger batches: we want ~same wall-clock total
        int effectiveReps = Math.max(1_000, reps / Math.max(1, batchSize / 10));

        // warmup
        Map<Integer, byte[]> warmMap = new HashMap<>(batchSize);
        for (int j = 0; j < batchSize; j++) warmMap.put(j, payload);
        for (int i = 0; i < Math.min(WARMUP, 100); i++) cache.putAll(warmMap);

        long[] samples = new long[effectiveReps];
        Map<Integer, byte[]> batch = new HashMap<>(batchSize);
        for (int i = 0; i < effectiveReps; i++) {
            int offset = i * batchSize;
            batch.clear();
            for (int j = 0; j < batchSize; j++) batch.put(offset + j, payload);
            long t0 = System.nanoTime();
            cache.putAll(batch);
            samples[i] = System.nanoTime() - t0;
        }
        report("put_all/" + batchSize, samples);
    }

    private static void report(String label, long[] ns) {
        long[] sorted = ns.clone();
        Arrays.sort(sorted);
        int n = sorted.length;
        double mean = 0.0;
        for (long v : ns) mean += v;
        mean /= n;
        long p50 = sorted[(int) (n * 0.50)];
        long p95 = sorted[Math.min(n - 1, (int) (n * 0.95))];
        long p99 = sorted[Math.min(n - 1, (int) (n * 0.99))];
        System.out.printf(
                "%-30s n=%d mean=%.0fns p50=%dns p95=%dns p99=%dns%n",
                label, n, mean, p50, p95, p99);
    }
}
