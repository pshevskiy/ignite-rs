package io.ignite.rs.parity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import javax.cache.Cache;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Parity driver — reads JSON commands from stdin, emits JSON responses on
 * stdout. Extended incrementally as Phase 5 parity cases require new ops.
 */
public class Driver {
    public static void main(String[] args) throws Exception {
        ObjectMapper m = new ObjectMapper();
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
                        case "size": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            long n = c.size();
                            resp.put("ok", true);
                            resp.put("size", n);
                            break;
                        }
                        case "put_if_absent": {
                            ClientCache<Object, Object> c = client.cache(req.path("cache").asText());
                            boolean ok = c.putIfAbsent(req.path("key").asText(), req.path("value").asText());
                            resp.put("ok", true);
                            resp.put("inserted", ok);
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
