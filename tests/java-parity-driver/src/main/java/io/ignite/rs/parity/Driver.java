package io.ignite.rs.parity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientAtomicConfiguration;
import org.apache.ignite.client.ClientAtomicLong;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.ClientCollectionConfiguration;
import org.apache.ignite.client.ClientIgniteSet;
import org.apache.ignite.client.ClientServiceDescriptor;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import javax.cache.Cache;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.UUID;

/**
 * Parity driver — reads JSON commands from stdin, emits JSON responses on
 * stdout. Extended incrementally as Phase 5 parity cases require new ops.
 */
public class Driver {
    public static void main(String[] args) throws Exception {
        ObjectMapper m = new ObjectMapper();
        Map<String, ClientTransaction> openTxs = new HashMap<>();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String line;
            IgniteClient client = null;
            while ((line = r.readLine()) != null) {
                JsonNode req = m.readTree(line);
                String op = req.path("op").asText();
                ObjectNode resp = m.createObjectNode();
                resp.put("id", req.path("id").asText());
                try {
                    switch (op) {
                        case "connect": {
                            client = Ignition.startClient(new ClientConfiguration().setAddresses(req.path("addr").asText()));
                            resp.put("ok", true);
                            break;
                        }
                        case "put": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            c.put(req.path("key").asText(), req.path("value").asText());
                            resp.put("ok", true);
                            break;
                        }
                        case "get": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            Object v = c.get(req.path("key").asText());
                            resp.put("ok", true);
                            if (v == null) {
                                resp.putNull("value");
                            } else {
                                resp.put("value", v.toString());
                            }
                            break;
                        }
                        case "remove": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            boolean removed = c.remove(req.path("key").asText());
                            resp.put("ok", true);
                            resp.put("removed", removed);
                            break;
                        }
                        case "remove_if_equals": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            boolean removed = c.remove(req.path("key").asText(), req.path("value").asText());
                            resp.put("ok", true);
                            resp.put("removed", removed);
                            break;
                        }
                        case "size": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            long n = c.size();
                            resp.put("ok", true);
                            resp.put("size", n);
                            break;
                        }
                        case "clear": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            c.clear();
                            resp.put("ok", true);
                            break;
                        }
                        case "contains_key": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            boolean contains = c.containsKey(req.path("key").asText());
                            resp.put("ok", true);
                            resp.put("contains", contains);
                            break;
                        }
                        case "put_if_absent": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            boolean ok = c.putIfAbsent(req.path("key").asText(), req.path("value").asText());
                            resp.put("ok", true);
                            resp.put("inserted", ok);
                            break;
                        }
                        case "replace": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            boolean ok = c.replace(req.path("key").asText(), req.path("value").asText());
                            resp.put("ok", true);
                            resp.put("replaced", ok);
                            break;
                        }
                        case "replace_if_equals": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            boolean ok = c.replace(
                                    req.path("key").asText(),
                                    req.path("old").asText(),
                                    req.path("new").asText());
                            resp.put("ok", true);
                            resp.put("replaced", ok);
                            break;
                        }
                        case "get_and_put": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            Object prev = c.getAndPut(req.path("key").asText(), req.path("value").asText());
                            resp.put("ok", true);
                            if (prev == null) {
                                resp.putNull("previous");
                            } else {
                                resp.put("previous", prev.toString());
                            }
                            break;
                        }
                        case "get_and_remove": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            Object prev = c.getAndRemove(req.path("key").asText());
                            resp.put("ok", true);
                            if (prev == null) {
                                resp.putNull("previous");
                            } else {
                                resp.put("previous", prev.toString());
                            }
                            break;
                        }
                        case "put_all": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            Map<Object, Object> map = new HashMap<>();
                            JsonNode entries = req.path("entries");
                            for (JsonNode entry : entries) {
                                map.put(entry.path("key").asText(), entry.path("value").asText());
                            }
                            c.putAll(map);
                            resp.put("ok", true);
                            break;
                        }
                        case "get_all": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            JsonNode keysArr = req.path("keys");
                            java.util.Set<Object> keys = new java.util.HashSet<>();
                            for (JsonNode k : keysArr) {
                                keys.add(k.asText());
                            }
                            Map<Object, Object> got = c.getAll(keys);
                            resp.put("ok", true);
                            ObjectNode entries = resp.putObject("entries");
                            for (Map.Entry<Object, Object> e : got.entrySet()) {
                                entries.put(e.getKey().toString(), e.getValue().toString());
                            }
                            break;
                        }
                        case "remove_keys": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            JsonNode keysArr = req.path("keys");
                            java.util.Set<Object> keys = new java.util.HashSet<>();
                            for (JsonNode k : keysArr) {
                                keys.add(k.asText());
                            }
                            c.removeAll(keys);
                            resp.put("ok", true);
                            break;
                        }
                        case "get_or_create_cache": {
                            client.getOrCreateCache(req.path("cache").asText());
                            resp.put("ok", true);
                            break;
                        }
                        case "destroy_cache": {
                            client.destroyCache(req.path("cache").asText());
                            resp.put("ok", true);
                            break;
                        }
                        case "get_or_create_tx_cache": {
                            // Create cache with TRANSACTIONAL atomicity so tx ops can run.
                            org.apache.ignite.client.ClientCacheConfiguration cfg =
                                    new org.apache.ignite.client.ClientCacheConfiguration()
                                            .setName(req.path("cache").asText())
                                            .setAtomicityMode(
                                                    org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL);
                            client.getOrCreateCache(cfg);
                            resp.put("ok", true);
                            break;
                        }
                        case "cache_names": {
                            Collection<String> names = client.cacheNames();
                            resp.put("ok", true);
                            ArrayNode arr = resp.putArray("names");
                            for (String n : names) arr.add(n);
                            break;
                        }
                        case "handshake_features": {
                            // Probe: version + whether client is alive post-handshake.
                            resp.put("ok", client != null);
                            resp.put("alive", client != null);
                            break;
                        }
                        case "tx_put_commit": {
                            // Start transaction, put, commit. Isolation + concurrency optional.
                            TransactionConcurrency conc = TransactionConcurrency.valueOf(
                                    req.path("concurrency").asText("PESSIMISTIC"));
                            TransactionIsolation iso = TransactionIsolation.valueOf(
                                    req.path("isolation").asText("REPEATABLE_READ"));
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            try (ClientTransaction tx = client.transactions().txStart(conc, iso)) {
                                c.put(req.path("key").asText(), req.path("value").asText());
                                tx.commit();
                            }
                            resp.put("ok", true);
                            break;
                        }
                        case "tx_put_rollback": {
                            TransactionConcurrency conc = TransactionConcurrency.valueOf(
                                    req.path("concurrency").asText("PESSIMISTIC"));
                            TransactionIsolation iso = TransactionIsolation.valueOf(
                                    req.path("isolation").asText("REPEATABLE_READ"));
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            try (ClientTransaction tx = client.transactions().txStart(conc, iso)) {
                                c.put(req.path("key").asText(), req.path("value").asText());
                                tx.rollback();
                            }
                            resp.put("ok", true);
                            break;
                        }
                        case "tx_start_persist": {
                            // Open a tx and keep it open (stored by id) until tx_end_persist.
                            TransactionConcurrency conc = TransactionConcurrency.valueOf(
                                    req.path("concurrency").asText("PESSIMISTIC"));
                            TransactionIsolation iso = TransactionIsolation.valueOf(
                                    req.path("isolation").asText("REPEATABLE_READ"));
                            long timeoutMs = req.path("timeout_ms").asLong(0L);
                            ClientTransaction tx;
                            if (timeoutMs > 0) {
                                tx = client.transactions().txStart(conc, iso, timeoutMs);
                            } else {
                                tx = client.transactions().txStart(conc, iso);
                            }
                            String handle = UUID.randomUUID().toString();
                            openTxs.put(handle, tx);
                            resp.put("ok", true);
                            resp.put("handle", handle);
                            break;
                        }
                        case "tx_end_persist": {
                            String handle = req.path("handle").asText();
                            boolean commit = req.path("commit").asBoolean(true);
                            ClientTransaction tx = openTxs.remove(handle);
                            if (tx == null) {
                                resp.put("ok", false);
                                resp.put("error", "unknown handle: " + handle);
                            } else {
                                try {
                                    if (commit) tx.commit();
                                    else tx.rollback();
                                    resp.put("ok", true);
                                } finally {
                                    tx.close();
                                }
                            }
                            break;
                        }
                        case "tx_put_in_persist": {
                            String handle = req.path("handle").asText();
                            ClientTransaction tx = openTxs.get(handle);
                            if (tx == null) {
                                resp.put("ok", false);
                                resp.put("error", "unknown handle: " + handle);
                                break;
                            }
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            c.put(req.path("key").asText(), req.path("value").asText());
                            resp.put("ok", true);
                            break;
                        }
                        case "sql_scan_count": {
                            String sql = req.path("sql").asText();
                            String schema = req.has("schema") ? req.path("schema").asText() : "PUBLIC";
                            org.apache.ignite.cache.query.SqlFieldsQuery q =
                                    new org.apache.ignite.cache.query.SqlFieldsQuery(sql).setSchema(schema);
                            // SqlFieldsQuery against the first cache found; a no-cache variant
                            // uses ClientCacheQuery API but we keep it minimal.
                            ClientCache<Object, Object> any = client.cache(req.path("cache").asText());
                            long n = 0;
                            try (org.apache.ignite.cache.query.FieldsQueryCursor<List<?>> cur = any.query(q)) {
                                for (List<?> row : cur) {
                                    n++;
                                    if (n > 10_000) break; // sanity cap
                                }
                            }
                            resp.put("ok", true);
                            resp.put("rows", n);
                            break;
                        }
                        case "sql_exec": {
                            // Run a DDL/DML statement (returns rows-affected count for DML).
                            String sql = req.path("sql").asText();
                            String schema = req.has("schema") ? req.path("schema").asText() : "PUBLIC";
                            org.apache.ignite.cache.query.SqlFieldsQuery q =
                                    new org.apache.ignite.cache.query.SqlFieldsQuery(sql).setSchema(schema);
                            ClientCache<Object, Object> any = client.cache(req.path("cache").asText());
                            long rows = 0;
                            try (org.apache.ignite.cache.query.FieldsQueryCursor<List<?>> cur = any.query(q)) {
                                for (List<?> row : cur) rows++;
                            }
                            resp.put("ok", true);
                            resp.put("rows", rows);
                            break;
                        }
                        case "scan_query": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            org.apache.ignite.cache.query.ScanQuery<Object, Object> q =
                                    new org.apache.ignite.cache.query.ScanQuery<>();
                            if (req.has("page_size")) q.setPageSize(req.path("page_size").asInt());
                            if (req.has("partition")) q.setPartition(req.path("partition").asInt());
                            long n = 0;
                            try (org.apache.ignite.cache.query.QueryCursor<Cache.Entry<Object, Object>> cur = c.query(q)) {
                                for (Cache.Entry<Object, Object> row : cur) {
                                    n++;
                                    if (n > 100_000) break;
                                }
                            }
                            resp.put("ok", true);
                            resp.put("rows", n);
                            break;
                        }
                        case "compute_run": {
                            // Run a server-visible task by name.
                            String task = req.path("task").asText();
                            Object arg = req.has("arg") ? req.path("arg").asText() : null;
                            Object v = client.compute().execute(task, arg);
                            resp.put("ok", true);
                            if (v == null) {
                                resp.putNull("result");
                            } else {
                                resp.put("result", v.toString());
                            }
                            break;
                        }
                        case "service_invoke": {
                            // Call a deployed service method.
                            String svc = req.path("service").asText();
                            String method = req.path("method").asText();
                            Class<?> iface = Class.forName(req.path("interface").asText());
                            Object proxy = client.services().serviceProxy(svc, iface);
                            java.lang.reflect.Method mth = iface.getMethod(method);
                            Object v = mth.invoke(proxy);
                            resp.put("ok", true);
                            if (v == null) {
                                resp.putNull("result");
                            } else {
                                resp.put("result", v.toString());
                            }
                            break;
                        }
                        case "service_descriptors": {
                            // List deployed services — must return an empty list on a fresh node.
                            Collection<ClientServiceDescriptor> descs =
                                    client.services().serviceDescriptors();
                            resp.put("ok", true);
                            resp.put("count", descs.size());
                            break;
                        }
                        case "force_error": {
                            // Trigger a server-side error path to verify error-mapping parity.
                            // Example: get from a non-existent cache (server returns CACHE_DOES_NOT_EXIST).
                            try {
                                client.cache(req.path("cache").asText()).get("x");
                                resp.put("ok", true);
                                resp.put("triggered", false);
                            } catch (Exception e) {
                                resp.put("ok", true);
                                resp.put("triggered", true);
                                resp.put("error_class", e.getClass().getName());
                                resp.put("error_message", e.getMessage());
                            }
                            break;
                        }
                        case "force_error_op": {
                            // Parameterised error-probe: choose the op that triggers the error.
                            String kind = req.path("kind").asText();
                            try {
                                switch (kind) {
                                    case "cache_not_found":
                                        client.cache(req.path("cache").asText()).get("x");
                                        break;
                                    case "destroy_missing":
                                        client.destroyCache(req.path("cache").asText());
                                        break;
                                    case "sql_bad":
                                        client.cache("ignite-sys-cache").query(
                                                new org.apache.ignite.cache.query.SqlFieldsQuery(
                                                        req.path("sql").asText()).setSchema("PUBLIC"))
                                                .getAll();
                                        break;
                                    case "atomic_long_missing": {
                                        ClientAtomicLong al = client.atomicLong(
                                                req.path("name").asText(), 0L, false);
                                        if (al == null) {
                                            // Some versions return null instead of throwing.
                                            throw new IllegalStateException("AtomicLong does not exist");
                                        }
                                        al.get();
                                        break;
                                    }
                                    default:
                                        throw new IllegalStateException("unknown force_error_op kind: " + kind);
                                }
                                resp.put("ok", true);
                                resp.put("triggered", false);
                            } catch (Exception e) {
                                resp.put("ok", true);
                                resp.put("triggered", true);
                                resp.put("error_class", e.getClass().getName());
                                resp.put("error_message", e.getMessage() == null ? "" : e.getMessage());
                            }
                            break;
                        }
                        case "atomic_long_create": {
                            long initial = req.path("initial").asLong(0L);
                            ClientAtomicConfiguration cfg = new ClientAtomicConfiguration();
                            ClientAtomicLong al = client.atomicLong(req.path("name").asText(), cfg, initial, true);
                            resp.put("ok", true);
                            resp.put("value", al.get());
                            break;
                        }
                        case "atomic_long_get": {
                            ClientAtomicLong al = client.atomicLong(req.path("name").asText(), 0L, false);
                            if (al == null) {
                                resp.put("ok", false);
                                resp.put("error", "AtomicLong not found");
                            } else {
                                resp.put("ok", true);
                                resp.put("value", al.get());
                            }
                            break;
                        }
                        case "atomic_long_inc": {
                            ClientAtomicLong al = client.atomicLong(req.path("name").asText(), 0L, false);
                            if (al == null) {
                                resp.put("ok", false);
                                resp.put("error", "AtomicLong not found");
                            } else {
                                resp.put("ok", true);
                                resp.put("value", al.incrementAndGet());
                            }
                            break;
                        }
                        case "atomic_long_cas": {
                            ClientAtomicLong al = client.atomicLong(req.path("name").asText(), 0L, false);
                            if (al == null) {
                                resp.put("ok", false);
                                resp.put("error", "AtomicLong not found");
                            } else {
                                long expected = req.path("expected").asLong();
                                long value = req.path("value").asLong();
                                resp.put("ok", true);
                                resp.put("swapped", al.compareAndSet(expected, value));
                            }
                            break;
                        }
                        case "atomic_long_close": {
                            ClientAtomicLong al = client.atomicLong(req.path("name").asText(), 0L, false);
                            if (al != null) al.close();
                            resp.put("ok", true);
                            break;
                        }
                        case "set_create": {
                            ClientCollectionConfiguration cfg = new ClientCollectionConfiguration();
                            if (req.has("backups")) cfg.setBackups(req.path("backups").asInt());
                            ClientIgniteSet<Object> set = client.set(req.path("name").asText(), cfg);
                            resp.put("ok", true);
                            resp.put("size", set.size());
                            break;
                        }
                        case "set_add": {
                            ClientIgniteSet<Object> set = client.set(req.path("name").asText(), null);
                            if (set == null) {
                                resp.put("ok", false);
                                resp.put("error", "set not found");
                            } else {
                                boolean added = set.add(req.path("value").asText());
                                resp.put("ok", true);
                                resp.put("added", added);
                            }
                            break;
                        }
                        case "set_contains": {
                            ClientIgniteSet<Object> set = client.set(req.path("name").asText(), null);
                            if (set == null) {
                                resp.put("ok", false);
                                resp.put("error", "set not found");
                            } else {
                                resp.put("ok", true);
                                resp.put("contains", set.contains(req.path("value").asText()));
                            }
                            break;
                        }
                        case "set_size": {
                            ClientIgniteSet<Object> set = client.set(req.path("name").asText(), null);
                            if (set == null) {
                                resp.put("ok", false);
                                resp.put("error", "set not found");
                            } else {
                                resp.put("ok", true);
                                resp.put("size", set.size());
                            }
                            break;
                        }
                        case "set_close": {
                            ClientIgniteSet<Object> set = client.set(req.path("name").asText(), null);
                            if (set != null) set.close();
                            resp.put("ok", true);
                            break;
                        }
                        case "cluster_nodes_count": {
                            ClientClusterGroup grp = client.cluster();
                            Collection<org.apache.ignite.cluster.ClusterNode> nodes = grp.nodes();
                            resp.put("ok", true);
                            resp.put("count", nodes.size());
                            break;
                        }
                        case "cache_partition_count": {
                            // Probe partition count via a put/get round-trip on a well-known cache.
                            // The thin client doesn't expose the partition count directly.
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            // Do a ping operation — partition map is built on demand.
                            c.size();
                            resp.put("ok", true);
                            resp.put("exists", true);
                            break;
                        }
                        default: {
                            resp.put("ok", false);
                            resp.put("error", "unknown op: " + op);
                        }
                    }
                } catch (Exception e) {
                    resp.put("ok", false);
                    resp.put("error", e.getClass().getName() + ": " + e.getMessage());
                }
                System.out.println(m.writeValueAsString(resp));
                System.out.flush();
            }
            if (client != null) client.close();
        }
    }
}
