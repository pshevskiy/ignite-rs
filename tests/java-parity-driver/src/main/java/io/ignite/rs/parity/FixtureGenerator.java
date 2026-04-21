package io.ignite.rs.parity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tier-3 byte-fixture generator.
 *
 * Emits byte sequences that match Ignite's thin-client binary wire format
 * for primitive + small-composite type codes, plus a companion .meta.json
 * describing the expected logical value. Ignite-rs reads these fixtures and
 * asserts `decode(bin) == meta.value` and `encode(meta.value) == bin`.
 *
 * Hand-built byte sequences (not via Ignite internal API) so that the fixture
 * layer stays independent of Ignite's private classes and Java-module
 * restrictions. Only the type codes that ignite-rs honors today are covered.
 *
 * Run: java -cp ignite-rs-parity-driver.jar io.ignite.rs.parity.FixtureGenerator &lt;outDir&gt;
 */
public class FixtureGenerator {
    // Ignite TypeCode values — must match /work/rsc_new/ignite_all/ignite-rs/ignite-rs/src/protocol/mod.rs TypeCode
    static final byte TYPE_BYTE     = 1;
    static final byte TYPE_SHORT    = 2;
    static final byte TYPE_INT      = 3;
    static final byte TYPE_LONG     = 4;
    static final byte TYPE_FLOAT    = 5;
    static final byte TYPE_DOUBLE   = 6;
    static final byte TYPE_CHAR     = 7;
    static final byte TYPE_BOOL     = 8;
    static final byte TYPE_STRING   = 9;
    static final byte TYPE_ARR_BYTE = 12;
    static final byte TYPE_ARR_INT  = 14;
    static final byte TYPE_ARR_LONG = 15;
    static final byte TYPE_ARR_STR  = 20;
    static final byte TYPE_COLLECTION = 24;
    static final byte TYPE_MAP      = 25;
    static final byte TYPE_DECIMAL  = 30;
    static final byte TYPE_TIMESTAMP = 33;
    static final byte TYPE_ENUM     = 28;
    static final byte TYPE_NULL     = 101;
    static final byte TYPE_OPT_MARSH = (byte) 0xFE;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: FixtureGenerator <outDir>");
            System.exit(2);
        }
        Path outDir = Path.of(args[0]);
        Files.createDirectories(outDir);
        ObjectMapper m = new ObjectMapper();

        Map<String, Fixture> corpus = new LinkedHashMap<>();

        // ----------------------- Primitives -----------------------
        corpus.put("byte_42",        primitive(TYPE_BYTE,  new byte[]{42},                    "byte",   "42"));
        corpus.put("i16_neg_12345",  primitive(TYPE_SHORT, le16((short) -12345),              "i16",    "-12345"));
        corpus.put("i32_max",        primitive(TYPE_INT,   le32(Integer.MAX_VALUE),           "i32",    String.valueOf(Integer.MAX_VALUE)));
        corpus.put("i32_zero",       primitive(TYPE_INT,   le32(0),                           "i32",    "0"));
        corpus.put("i64_min",        primitive(TYPE_LONG,  le64(Long.MIN_VALUE),              "i64",    String.valueOf(Long.MIN_VALUE)));
        corpus.put("f32_pi",         primitive(TYPE_FLOAT, le32bits(Float.floatToIntBits(3.14159f)), "f32", "3.14159"));
        corpus.put("f64_e",          primitive(TYPE_DOUBLE,le64bits(Double.doubleToLongBits(Math.E)),"f64","2.718281828459045"));
        corpus.put("bool_true",      primitive(TYPE_BOOL,  new byte[]{1},                     "bool",   "true"));
        corpus.put("bool_false",     primitive(TYPE_BOOL,  new byte[]{0},                     "bool",   "false"));
        corpus.put("char_A",         primitive(TYPE_CHAR,  le16((short) 'A'),                 "char",   "65"));

        // ----------------------- Strings -----------------------
        corpus.put("str_empty",      stringFixture(""));
        corpus.put("str_ascii",      stringFixture("hello"));
        corpus.put("str_utf8_rus",   stringFixture("Привет"));
        corpus.put("str_utf8_emoji", stringFixture("🚀"));

        // ----------------------- Arrays of primitives -----------------------
        corpus.put("arr_byte_small", arrByteFixture(new byte[]{1, 2, 3}));
        corpus.put("arr_i32_mixed",  arrInt32Fixture(new int[]{-1, 0, 1, Integer.MAX_VALUE}));
        corpus.put("arr_i64_small",  arrInt64Fixture(new long[]{-1L, 0L, 42L}));

        // ----------------------- Collections / Maps -----------------------
        corpus.put("list_of_str",    listOfStringFixture(new String[]{"a", "b", "c"}));
        corpus.put("map_str_i32",    mapStrI32Fixture(new String[]{"one", "two"}, new int[]{1, 2}));

        // ----------------------- Null / Decimal / Timestamp / Enum -----------------------
        corpus.put("null_value",     nullFixture());
        corpus.put("decimal_123_45", decimalFixture(2, new byte[]{0x30, 0x39})); // 12345 scale 2 → 123.45
        corpus.put("decimal_neg",    decimalFixture(0, new byte[]{(byte)0xFF, (byte)0xCE})); // -50 (two's comp)
        corpus.put("timestamp_ms",   timestampFixture(1_700_000_000_000L, 123_456_789));
        corpus.put("enum_ord_2",     enumFixture(12345, 2));

        // ----------------------- OptimizedMarshaller opaque blob -----------------------
        // Ignite-rs preserves the opaque blob verbatim; we synthesize a short one.
        corpus.put("opaque_blob",    opaqueFixture(new byte[]{(byte)0xAC, (byte)0xED, 0x00, 0x05}));

        // Emit.
        for (Map.Entry<String, Fixture> e : corpus.entrySet()) {
            Path bin = outDir.resolve(e.getKey() + ".bin");
            Path meta = outDir.resolve(e.getKey() + ".meta.json");
            Files.write(bin, e.getValue().bytes);

            ObjectNode node = m.createObjectNode();
            node.put("name", e.getKey());
            node.put("kind", e.getValue().kind);
            node.put("value", e.getValue().valueAsString);
            node.put("bytes_len", e.getValue().bytes.length);
            if (e.getValue().extra != null) {
                node.set("extra", e.getValue().extra);
            }
            Files.writeString(meta, m.writerWithDefaultPrettyPrinter().writeValueAsString(node));
        }

        System.out.println("Wrote " + corpus.size() + " fixtures to " + outDir.toAbsolutePath());
    }

    // ----------------------- Fixture encoding helpers -----------------------

    static Fixture primitive(byte typeCode, byte[] payload, String kind, String valueAsString) {
        byte[] buf = new byte[1 + payload.length];
        buf[0] = typeCode;
        System.arraycopy(payload, 0, buf, 1, payload.length);
        return new Fixture(buf, kind, valueAsString, null);
    }

    static Fixture stringFixture(String s) {
        byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_STRING);
        writeLeInt(bos, utf8.length);
        bos.write(utf8, 0, utf8.length);
        return new Fixture(bos.toByteArray(), "str", s, null);
    }

    static Fixture arrByteFixture(byte[] data) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ARR_BYTE);
        writeLeInt(bos, data.length);
        bos.write(data, 0, data.length);
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < data.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(data[i] & 0xFF);
        }
        sb.append("]");
        return new Fixture(bos.toByteArray(), "arr_byte", sb.toString(), null);
    }

    static Fixture arrInt32Fixture(int[] data) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ARR_INT);
        writeLeInt(bos, data.length);
        for (int v : data) {
            writeLeInt(bos, v);
        }
        return new Fixture(bos.toByteArray(), "arr_i32", java.util.Arrays.toString(data).replace(" ", ""), null);
    }

    static Fixture arrInt64Fixture(long[] data) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ARR_LONG);
        writeLeInt(bos, data.length);
        for (long v : data) {
            writeLeLong(bos, v);
        }
        return new Fixture(bos.toByteArray(), "arr_i64", java.util.Arrays.toString(data).replace(" ", ""), null);
    }

    static Fixture listOfStringFixture(String[] items) {
        // TYPE_COLLECTION: type | i32 count | u8 col_subtype | items...
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_COLLECTION);
        writeLeInt(bos, items.length);
        bos.write(1); // ArrayList
        for (String s : items) {
            byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
            bos.write(TYPE_STRING);
            writeLeInt(bos, utf8.length);
            bos.write(utf8, 0, utf8.length);
        }
        return new Fixture(bos.toByteArray(), "list_str", java.util.Arrays.toString(items).replace(" ", ""), null);
    }

    static Fixture mapStrI32Fixture(String[] keys, int[] values) {
        // TYPE_MAP: type | i32 count | u8 map_subtype | (k,v)+ where each is type-prefixed
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_MAP);
        writeLeInt(bos, keys.length);
        bos.write(1); // HashMap
        for (int i = 0; i < keys.length; i++) {
            byte[] utf8 = keys[i].getBytes(StandardCharsets.UTF_8);
            bos.write(TYPE_STRING);
            writeLeInt(bos, utf8.length);
            bos.write(utf8, 0, utf8.length);
            bos.write(TYPE_INT);
            writeLeInt(bos, values[i]);
        }
        // Build a small "k=v;..." description for the meta.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) sb.append(";");
            sb.append(keys[i]).append("=").append(values[i]);
        }
        return new Fixture(bos.toByteArray(), "map_str_i32", sb.toString(), null);
    }

    static Fixture nullFixture() {
        return new Fixture(new byte[]{TYPE_NULL}, "null", "null", null);
    }

    static Fixture decimalFixture(int scale, byte[] magnitude) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_DECIMAL);
        writeLeInt(bos, scale);
        writeLeInt(bos, magnitude.length);
        bos.write(magnitude, 0, magnitude.length);
        StringBuilder mag = new StringBuilder("[");
        for (int i = 0; i < magnitude.length; i++) {
            if (i > 0) mag.append(",");
            mag.append(magnitude[i] & 0xFF);
        }
        mag.append("]");
        return new Fixture(bos.toByteArray(), "decimal", "scale=" + scale + ",mag=" + mag, null);
    }

    static Fixture timestampFixture(long millis, int nanos) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_TIMESTAMP);
        writeLeLong(bos, millis);
        writeLeInt(bos, nanos);
        return new Fixture(bos.toByteArray(), "timestamp", millis + "," + nanos, null);
    }

    static Fixture enumFixture(int typeId, int ordinal) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ENUM);
        writeLeInt(bos, typeId);
        writeLeInt(bos, ordinal);
        return new Fixture(bos.toByteArray(), "enum", "type=" + typeId + ",ord=" + ordinal, null);
    }

    static Fixture opaqueFixture(byte[] blob) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_OPT_MARSH);
        writeLeInt(bos, blob.length);
        bos.write(blob, 0, blob.length);
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < blob.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(blob[i] & 0xFF);
        }
        sb.append("]");
        return new Fixture(bos.toByteArray(), "opaque", sb.toString(), null);
    }

    // ----------------------- Little-endian helpers -----------------------

    static byte[] le16(short v) {
        return new byte[]{(byte) (v & 0xFF), (byte) ((v >>> 8) & 0xFF)};
    }

    static byte[] le32(int v) {
        return new byte[]{
            (byte) (v & 0xFF),
            (byte) ((v >>> 8) & 0xFF),
            (byte) ((v >>> 16) & 0xFF),
            (byte) ((v >>> 24) & 0xFF),
        };
    }

    static byte[] le64(long v) {
        byte[] out = new byte[8];
        for (int i = 0; i < 8; i++) out[i] = (byte) ((v >>> (i * 8)) & 0xFF);
        return out;
    }

    static byte[] le32bits(int bits) { return le32(bits); }
    static byte[] le64bits(long bits) { return le64(bits); }

    static void writeLeInt(ByteArrayOutputStream bos, int v) {
        byte[] b = le32(v);
        bos.write(b, 0, b.length);
    }

    static void writeLeLong(ByteArrayOutputStream bos, long v) {
        byte[] b = le64(v);
        bos.write(b, 0, b.length);
    }

    // Unused (retained for future DataOutputStream-based fixtures).
    @SuppressWarnings("unused")
    static void writeBE(DataOutputStream dos, int v) throws Exception {
        dos.writeInt(v);
    }

    // ----------------------- Fixture record -----------------------

    static class Fixture {
        final byte[] bytes;
        final String kind;
        final String valueAsString;
        final ArrayNode extra;
        Fixture(byte[] bytes, String kind, String valueAsString, ArrayNode extra) {
            this.bytes = bytes;
            this.kind = kind;
            this.valueAsString = valueAsString;
            this.extra = extra;
        }
    }
}
