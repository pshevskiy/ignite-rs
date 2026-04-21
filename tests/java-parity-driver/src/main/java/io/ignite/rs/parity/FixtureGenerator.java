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
    static final byte TYPE_UUID     = 10;
    static final byte TYPE_DATE     = 11;
    static final byte TYPE_ARR_BYTE = 12;
    static final byte TYPE_ARR_SHORT= 13;
    static final byte TYPE_ARR_INT  = 14;
    static final byte TYPE_ARR_LONG = 15;
    static final byte TYPE_ARR_FLOAT= 16;
    static final byte TYPE_ARR_DOUBLE=17;
    static final byte TYPE_ARR_CHAR = 18;
    static final byte TYPE_ARR_BOOL = 19;
    static final byte TYPE_ARR_STR  = 20;
    static final byte TYPE_ARR_UUID = 21;
    static final byte TYPE_ARR_DATE = 22;
    static final byte TYPE_COLLECTION = 24;
    static final byte TYPE_MAP      = 25;
    static final byte TYPE_DECIMAL  = 30;
    static final byte TYPE_ARR_DECIMAL = 31;
    static final byte TYPE_TIMESTAMP = 33;
    static final byte TYPE_ARR_TIMESTAMP = 34;
    static final byte TYPE_TIME     = 36;
    static final byte TYPE_ARR_TIME = 37;
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

        // ----------------------- Primitives — Byte -----------------------
        corpus.put("byte_42",        primitive(TYPE_BYTE,  new byte[]{42},                    "byte",   "42"));
        corpus.put("byte_zero",      primitive(TYPE_BYTE,  new byte[]{0},                     "byte",   "0"));
        corpus.put("byte_max",       primitive(TYPE_BYTE,  new byte[]{(byte)0xFF},            "byte",   "255"));

        // ----------------------- Primitives — Short -----------------------
        corpus.put("i16_neg_12345",  primitive(TYPE_SHORT, le16((short) -12345),              "i16",    "-12345"));
        corpus.put("i16_zero",       primitive(TYPE_SHORT, le16((short) 0),                   "i16",    "0"));
        corpus.put("i16_max",        primitive(TYPE_SHORT, le16(Short.MAX_VALUE),             "i16",    String.valueOf(Short.MAX_VALUE)));
        corpus.put("i16_min",        primitive(TYPE_SHORT, le16(Short.MIN_VALUE),             "i16",    String.valueOf(Short.MIN_VALUE)));

        // ----------------------- Primitives — Int -----------------------
        corpus.put("i32_max",        primitive(TYPE_INT,   le32(Integer.MAX_VALUE),           "i32",    String.valueOf(Integer.MAX_VALUE)));
        corpus.put("i32_zero",       primitive(TYPE_INT,   le32(0),                           "i32",    "0"));
        corpus.put("i32_min",        primitive(TYPE_INT,   le32(Integer.MIN_VALUE),           "i32",    String.valueOf(Integer.MIN_VALUE)));
        corpus.put("i32_neg_one",    primitive(TYPE_INT,   le32(-1),                          "i32",    "-1"));
        corpus.put("i32_billion",    primitive(TYPE_INT,   le32(1_000_000_000),               "i32",    "1000000000"));

        // ----------------------- Primitives — Long -----------------------
        corpus.put("i64_min",        primitive(TYPE_LONG,  le64(Long.MIN_VALUE),              "i64",    String.valueOf(Long.MIN_VALUE)));
        corpus.put("i64_max",        primitive(TYPE_LONG,  le64(Long.MAX_VALUE),              "i64",    String.valueOf(Long.MAX_VALUE)));
        corpus.put("i64_zero",       primitive(TYPE_LONG,  le64(0L),                          "i64",    "0"));
        corpus.put("i64_neg_one",    primitive(TYPE_LONG,  le64(-1L),                         "i64",    "-1"));

        // ----------------------- Primitives — Float / Double -----------------------
        // NaN fixtures are omitted — the read-test tolerance uses `(v - expected).abs() < 1e-5`,
        // and NaN arithmetic returns NaN (never < 1e-5), so NaN assertions cannot pass as written.
        corpus.put("f32_pi",         primitive(TYPE_FLOAT, le32bits(Float.floatToIntBits(3.14159f)), "f32", "3.14159"));
        corpus.put("f32_zero",       primitive(TYPE_FLOAT, le32bits(Float.floatToIntBits(0.0f)),    "f32", "0"));
        corpus.put("f32_neg",        primitive(TYPE_FLOAT, le32bits(Float.floatToIntBits(-1.5f)),   "f32", "-1.5"));
        corpus.put("f32_inf",        primitive(TYPE_FLOAT, le32bits(Float.floatToIntBits(Float.POSITIVE_INFINITY)), "f32", "inf"));
        corpus.put("f64_e",          primitive(TYPE_DOUBLE,le64bits(Double.doubleToLongBits(Math.E)),"f64","2.718281828459045"));
        corpus.put("f64_zero",       primitive(TYPE_DOUBLE,le64bits(Double.doubleToLongBits(0.0d)), "f64","0"));
        corpus.put("f64_neg",        primitive(TYPE_DOUBLE,le64bits(Double.doubleToLongBits(-1.5d)),"f64","-1.5"));
        corpus.put("f64_inf",        primitive(TYPE_DOUBLE,le64bits(Double.doubleToLongBits(Double.POSITIVE_INFINITY)),"f64","inf"));

        // ----------------------- Primitives — Bool / Char -----------------------
        corpus.put("bool_true",      primitive(TYPE_BOOL,  new byte[]{1},                     "bool",   "true"));
        corpus.put("bool_false",     primitive(TYPE_BOOL,  new byte[]{0},                     "bool",   "false"));
        corpus.put("char_A",         primitive(TYPE_CHAR,  le16((short) 'A'),                 "char",   "65"));
        corpus.put("char_zero",      primitive(TYPE_CHAR,  le16((short) 0),                   "char",   "0"));
        corpus.put("char_cyrillic",  primitive(TYPE_CHAR,  le16((short) 'Я'),                 "char",   String.valueOf((int) 'Я')));
        corpus.put("char_high",      primitive(TYPE_CHAR,  le16((short) 0xD83D),              "char",   "55357")); // high surrogate

        // ----------------------- Strings -----------------------
        corpus.put("str_empty",      stringFixture(""));
        corpus.put("str_ascii",      stringFixture("hello"));
        corpus.put("str_single",     stringFixture("x"));
        corpus.put("str_long_ascii", stringFixture("The quick brown fox jumps over the lazy dog 01234567890"));
        corpus.put("str_utf8_rus",   stringFixture("Привет"));
        corpus.put("str_utf8_chinese", stringFixture("你好世界"));
        corpus.put("str_utf8_emoji", stringFixture("🚀"));
        corpus.put("str_utf8_mixed", stringFixture("a𝕏b日c"));
        corpus.put("str_whitespace", stringFixture(" \t\n\r "));

        // ----------------------- UUID / Date / Time -----------------------
        corpus.put("uuid_zero",      uuidFixture(0L, 0L));
        corpus.put("uuid_max",       uuidFixture(-1L, -1L));
        corpus.put("uuid_classic",   uuidFixture(0x0102030405060708L, 0x090A0B0C0D0E0F10L));
        corpus.put("date_zero",      dateFixture(0L));
        corpus.put("date_recent",    dateFixture(1_700_000_000_000L));
        corpus.put("date_negative",  dateFixture(-86_400_000L)); // one day before epoch
        corpus.put("time_zero",      timeFixture(0L));
        corpus.put("time_end_of_day",timeFixture(86_399_999L));
        corpus.put("time_negative",  timeFixture(-1L));

        // ----------------------- Arrays of primitives (ArrByte round-trips; others decode-only) -----------------------
        corpus.put("arr_byte_small", arrByteFixture(new byte[]{1, 2, 3}));
        corpus.put("arr_byte_empty", arrByteFixture(new byte[0]));
        corpus.put("arr_byte_bytes", arrByteFixture(new byte[]{0, 1, (byte)127, (byte)128, (byte)255}));

        // Arrays of non-byte primitives: ignite-rs `ComplexObject::read_unwrapped` doesn't cover
        // these top-level type codes (Arr{Short,Int,Long,Float,Double,Char,Bool}). They're
        // soft-skipped by the read/write tests — kept as regression guards that the generator
        // emits a valid wire layout per Java §2.1.
        corpus.put("arr_i16_small",  arrInt16Fixture(new short[]{(short)-1, 0, 1, Short.MAX_VALUE}));
        corpus.put("arr_i16_empty",  arrInt16Fixture(new short[0]));
        corpus.put("arr_i32_mixed",  arrInt32Fixture(new int[]{-1, 0, 1, Integer.MAX_VALUE}));
        corpus.put("arr_i32_empty",  arrInt32Fixture(new int[0]));
        corpus.put("arr_i64_small",  arrInt64Fixture(new long[]{-1L, 0L, 42L}));
        corpus.put("arr_i64_empty",  arrInt64Fixture(new long[0]));
        corpus.put("arr_f32_simple", arrFloatFixture(new float[]{0.0f, 1.0f, -1.5f}));
        corpus.put("arr_f64_simple", arrDoubleFixture(new double[]{0.0, 1.0, -2.5, Math.PI}));
        corpus.put("arr_char_simple",arrCharFixture(new char[]{'A', 'B', 'C'}));
        corpus.put("arr_bool_mixed", arrBoolFixture(new boolean[]{true, false, true, true}));

        // ----------------------- Collections (round-trip; subtype variants) -----------------------
        corpus.put("list_of_str",            listOfStringFixture(new String[]{"a", "b", "c"}));
        corpus.put("coll_arraylist_empty",   collectionFixture((byte)1, new String[0], "arraylist_empty"));
        corpus.put("coll_arraylist_ints",    collectionOfIntsFixture((byte)1, new int[]{1, 2, 3, 4, 5}, "arraylist"));
        corpus.put("coll_linkedlist_strs",   collectionFixture((byte)2, new String[]{"x", "y"}, "linkedlist"));
        corpus.put("coll_hashset_ints",      collectionOfIntsFixture((byte)3, new int[]{10, 20, 30}, "hashset"));
        corpus.put("coll_linkedhashset_strs",collectionFixture((byte)4, new String[]{"one", "two", "three"}, "linkedhashset"));

        // ----------------------- Maps (round-trip; subtype variants) -----------------------
        corpus.put("map_str_i32",            mapStrI32Fixture(new String[]{"one", "two"}, new int[]{1, 2}));
        corpus.put("map_empty",              mapEmptyFixture());
        corpus.put("map_linkedhashmap_strs", mapLinkedHashMapStrsFixture(new String[]{"k1", "k2"}, new String[]{"v1", "v2"}));
        corpus.put("map_single_entry",       mapStrI32Fixture(new String[]{"only"}, new int[]{42}));

        // ----------------------- Null / Decimal / Timestamp / Enum -----------------------
        corpus.put("null_value",     nullFixture());
        corpus.put("decimal_123_45", decimalFixture(2, new byte[]{0x30, 0x39})); // 12345 scale 2 → 123.45
        corpus.put("decimal_neg",    decimalFixture(0, new byte[]{(byte)0xFF, (byte)0xCE})); // -50 (two's comp)
        corpus.put("decimal_zero",   decimalFixture(0, new byte[]{0x00}));
        corpus.put("decimal_large",  decimalFixture(4, new byte[]{0x01, 0x23, 0x45, 0x67, (byte)0x89})); // large positive
        corpus.put("decimal_high_scale", decimalFixture(10, new byte[]{0x00, (byte)0xFF})); // 255 * 10^-10
        corpus.put("timestamp_ms",   timestampFixture(1_700_000_000_000L, 123_456_789));
        corpus.put("timestamp_zero", timestampFixture(0L, 0));
        corpus.put("timestamp_neg",  timestampFixture(-1L, 999_999));
        corpus.put("enum_ord_2",     enumFixture(12345, 2));
        corpus.put("enum_ord_zero",  enumFixture(12345, 0));
        corpus.put("enum_negative",  enumFixture(-42, -1));
        corpus.put("enum_large",     enumFixture(Integer.MAX_VALUE, 1000));

        // ----------------------- OptimizedMarshaller opaque blobs -----------------------
        // Ignite-rs preserves the opaque blob verbatim via OpaqueMarshal; both read and write
        // round-trip. REG-1 regression guard — must re-emit with TypeCode 0xFE (not ArrByte/0x0C).
        corpus.put("opaque_blob",    opaqueFixture(new byte[]{(byte)0xAC, (byte)0xED, 0x00, 0x05}));
        corpus.put("opaque_empty",   opaqueFixture(new byte[0]));
        corpus.put("opaque_large",   opaqueFixture(makeBlob(64)));

        // ----------------------- FND-014: typed arrays with per-element code -----------------------
        // Each element is `<inner-code> <body>` or `NULL`. Decoder preserves
        // the outer TypeCode via `IgniteValue::ArrTyped` so round-trip is
        // byte-identical.
        corpus.put("arr_string_roundtrip", arrStringRoundTripFixture());
        corpus.put("arr_string_empty",     arrTypedEmptyFixture(TYPE_ARR_STR, "arr_string"));
        corpus.put("arr_string_ascii",     arrStringPlainFixture(new String[]{"a", "b", "c", "d"}));
        corpus.put("arr_uuid_roundtrip",   arrUuidRoundTripFixture());
        corpus.put("arr_uuid_empty",       arrTypedEmptyFixture(TYPE_ARR_UUID, "arr_uuid"));
        corpus.put("arr_date_roundtrip",   arrDateRoundTripFixture());
        corpus.put("arr_date_empty",       arrTypedEmptyFixture(TYPE_ARR_DATE, "arr_date"));
        corpus.put("arr_decimal_roundtrip", arrDecimalRoundTripFixture());
        corpus.put("arr_decimal_empty",    arrTypedEmptyFixture(TYPE_ARR_DECIMAL, "arr_decimal"));
        corpus.put("arr_timestamp_roundtrip", arrTimestampRoundTripFixture());
        corpus.put("arr_timestamp_empty",  arrTypedEmptyFixture(TYPE_ARR_TIMESTAMP, "arr_timestamp"));
        corpus.put("arr_time_roundtrip",   arrTimeRoundTripFixture());
        corpus.put("arr_time_empty",       arrTypedEmptyFixture(TYPE_ARR_TIME, "arr_time"));

        // ----------------------- Nested composites -----------------------
        corpus.put("list_of_lists",  listOfListsFixture());
        corpus.put("list_of_maps",   listOfMapsFixture());
        corpus.put("map_of_lists",   mapOfListsFixture());
        corpus.put("list_with_nulls",listWithNullsFixture());
        corpus.put("map_str_mixed",  mapStrMixedFixture());

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

    static Fixture uuidFixture(long most, long least) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_UUID);
        writeLeLong(bos, most);
        writeLeLong(bos, least);
        return new Fixture(bos.toByteArray(), "uuid", most + "," + least, null);
    }

    static Fixture dateFixture(long millis) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_DATE);
        writeLeLong(bos, millis);
        return new Fixture(bos.toByteArray(), "date", String.valueOf(millis), null);
    }

    static Fixture timeFixture(long millis) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_TIME);
        writeLeLong(bos, millis);
        return new Fixture(bos.toByteArray(), "time", String.valueOf(millis), null);
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

    static Fixture arrInt16Fixture(short[] data) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ARR_SHORT);
        writeLeInt(bos, data.length);
        for (short v : data) {
            byte[] b = le16(v);
            bos.write(b, 0, b.length);
        }
        return new Fixture(bos.toByteArray(), "arr_i16", java.util.Arrays.toString(data).replace(" ", ""), null);
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

    static Fixture arrFloatFixture(float[] data) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ARR_FLOAT);
        writeLeInt(bos, data.length);
        for (float v : data) {
            byte[] b = le32bits(Float.floatToIntBits(v));
            bos.write(b, 0, b.length);
        }
        return new Fixture(bos.toByteArray(), "arr_f32", java.util.Arrays.toString(data).replace(" ", ""), null);
    }

    static Fixture arrDoubleFixture(double[] data) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ARR_DOUBLE);
        writeLeInt(bos, data.length);
        for (double v : data) {
            byte[] b = le64bits(Double.doubleToLongBits(v));
            bos.write(b, 0, b.length);
        }
        return new Fixture(bos.toByteArray(), "arr_f64", java.util.Arrays.toString(data).replace(" ", ""), null);
    }

    static Fixture arrCharFixture(char[] data) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ARR_CHAR);
        writeLeInt(bos, data.length);
        for (char v : data) {
            byte[] b = le16((short) v);
            bos.write(b, 0, b.length);
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < data.length; i++) {
            if (i > 0) sb.append(",");
            sb.append((int) data[i]);
        }
        sb.append("]");
        return new Fixture(bos.toByteArray(), "arr_char", sb.toString(), null);
    }

    static Fixture arrBoolFixture(boolean[] data) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ARR_BOOL);
        writeLeInt(bos, data.length);
        for (boolean v : data) {
            bos.write(v ? 1 : 0);
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < data.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(data[i]);
        }
        sb.append("]");
        return new Fixture(bos.toByteArray(), "arr_bool", sb.toString(), null);
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

    static Fixture collectionFixture(byte subtype, String[] items, String subtypeName) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_COLLECTION);
        writeLeInt(bos, items.length);
        bos.write(subtype);
        for (String s : items) {
            byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
            bos.write(TYPE_STRING);
            writeLeInt(bos, utf8.length);
            bos.write(utf8, 0, utf8.length);
        }
        return new Fixture(bos.toByteArray(), "coll_str",
                "subtype=" + subtypeName + ";items=" + java.util.Arrays.toString(items).replace(" ", ""), null);
    }

    static Fixture collectionOfIntsFixture(byte subtype, int[] items, String subtypeName) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_COLLECTION);
        writeLeInt(bos, items.length);
        bos.write(subtype);
        for (int v : items) {
            bos.write(TYPE_INT);
            writeLeInt(bos, v);
        }
        return new Fixture(bos.toByteArray(), "coll_i32",
                "subtype=" + subtypeName + ";items=" + java.util.Arrays.toString(items).replace(" ", ""), null);
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

    static Fixture mapEmptyFixture() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_MAP);
        writeLeInt(bos, 0);
        bos.write(1); // HashMap
        return new Fixture(bos.toByteArray(), "map_empty", "subtype=1", null);
    }

    static Fixture mapLinkedHashMapStrsFixture(String[] keys, String[] values) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_MAP);
        writeLeInt(bos, keys.length);
        bos.write(2); // LinkedHashMap
        for (int i = 0; i < keys.length; i++) {
            byte[] k = keys[i].getBytes(StandardCharsets.UTF_8);
            bos.write(TYPE_STRING);
            writeLeInt(bos, k.length);
            bos.write(k, 0, k.length);
            byte[] v = values[i].getBytes(StandardCharsets.UTF_8);
            bos.write(TYPE_STRING);
            writeLeInt(bos, v.length);
            bos.write(v, 0, v.length);
        }
        StringBuilder sb = new StringBuilder("subtype=2;");
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) sb.append(";");
            sb.append(keys[i]).append("=").append(values[i]);
        }
        return new Fixture(bos.toByteArray(), "map_str_str", sb.toString(), null);
    }

    static Fixture mapStrMixedFixture() {
        // Map<String, dynamic> — values are i32, String, bool. Subtype=1 (HashMap).
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_MAP);
        writeLeInt(bos, 3);
        bos.write(1);
        // k1 → i32(7)
        writeString(bos, "k1");
        bos.write(TYPE_INT);
        writeLeInt(bos, 7);
        // k2 → String("v2")
        writeString(bos, "k2");
        writeString(bos, "v2");
        // k3 → bool(true)
        writeString(bos, "k3");
        bos.write(TYPE_BOOL);
        bos.write(1);
        return new Fixture(bos.toByteArray(), "map_str_mixed",
                "subtype=1;k1=i32(7);k2=str(v2);k3=bool(true)", null);
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

    static byte[] makeBlob(int n) {
        byte[] out = new byte[n];
        for (int i = 0; i < n; i++) out[i] = (byte) (i * 31);
        return out;
    }

    // ----------------------- FND-014 typed-array fixtures -----------------------

    static Fixture arrStringRoundTripFixture() {
        // STRING_ARR = 0x14. Elements: "a", NULL, "мир".
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ARR_STR);
        writeLeInt(bos, 3);
        // "a"
        bos.write(TYPE_STRING);
        byte[] a = "a".getBytes(StandardCharsets.UTF_8);
        writeLeInt(bos, a.length);
        bos.write(a, 0, a.length);
        // NULL
        bos.write(TYPE_NULL);
        // "мир"
        bos.write(TYPE_STRING);
        byte[] mir = "мир".getBytes(StandardCharsets.UTF_8);
        writeLeInt(bos, mir.length);
        bos.write(mir, 0, mir.length);
        return new Fixture(bos.toByteArray(), "arr_string", "[a,null,мир]", null);
    }

    static Fixture arrStringPlainFixture(String[] items) {
        // STRING_ARR = 0x14. All elements non-null.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ARR_STR);
        writeLeInt(bos, items.length);
        for (String s : items) {
            bos.write(TYPE_STRING);
            byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
            writeLeInt(bos, utf8.length);
            bos.write(utf8, 0, utf8.length);
        }
        return new Fixture(bos.toByteArray(), "arr_string",
                java.util.Arrays.toString(items).replace(" ", ""), null);
    }

    static Fixture arrTypedEmptyFixture(byte outerCode, String kind) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(outerCode);
        writeLeInt(bos, 0);
        return new Fixture(bos.toByteArray(), kind, "[]", null);
    }

    static Fixture arrUuidRoundTripFixture() {
        // UUID_ARR = 0x15. Elements: two UUIDs, one NULL.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ARR_UUID);
        writeLeInt(bos, 3);
        bos.write(TYPE_UUID);
        writeLeLong(bos, 0x0102030405060708L);
        writeLeLong(bos, 0x090A0B0C0D0E0F10L);
        bos.write(TYPE_UUID);
        writeLeLong(bos, -1L);
        writeLeLong(bos, -2L);
        bos.write(TYPE_NULL);
        return new Fixture(bos.toByteArray(), "arr_uuid", "[uuid0,uuid1,null]", null);
    }

    static Fixture arrDateRoundTripFixture() {
        // DATE_ARR = 0x16. Elements: Date(0), Date(-1), NULL.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ARR_DATE);
        writeLeInt(bos, 3);
        bos.write(TYPE_DATE);
        writeLeLong(bos, 0L);
        bos.write(TYPE_DATE);
        writeLeLong(bos, -1L);
        bos.write(TYPE_NULL);
        return new Fixture(bos.toByteArray(), "arr_date", "[0,-1,null]", null);
    }

    static Fixture arrDecimalRoundTripFixture() {
        // DECIMAL_ARR = 0x1F. Elements:
        //   Decimal(scale=2, mag=[0x30,0x39]=12345 -> 123.45),
        //   Decimal(scale=0, mag=[0xFF,0xCE] = -50 via two's-complement),
        //   NULL.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ARR_DECIMAL);
        writeLeInt(bos, 3);
        bos.write(TYPE_DECIMAL);
        writeLeInt(bos, 2);
        writeLeInt(bos, 2);
        bos.write(new byte[]{0x30, 0x39}, 0, 2);
        bos.write(TYPE_DECIMAL);
        writeLeInt(bos, 0);
        writeLeInt(bos, 2);
        bos.write(new byte[]{(byte)0xFF, (byte)0xCE}, 0, 2);
        bos.write(TYPE_NULL);
        return new Fixture(bos.toByteArray(), "arr_decimal",
                "[scale=2|mag=[48,57],scale=0|mag=[255,206],null]", null);
    }

    static Fixture arrTimestampRoundTripFixture() {
        // TIMESTAMP_ARR = 0x22. Elements: Timestamp(1, 2),
        // Timestamp(-1, 999999), NULL.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ARR_TIMESTAMP);
        writeLeInt(bos, 3);
        bos.write(TYPE_TIMESTAMP);
        writeLeLong(bos, 1L);
        writeLeInt(bos, 2);
        bos.write(TYPE_TIMESTAMP);
        writeLeLong(bos, -1L);
        writeLeInt(bos, 999_999);
        bos.write(TYPE_NULL);
        return new Fixture(bos.toByteArray(), "arr_timestamp",
                "[1|2,-1|999999,null]", null);
    }

    static Fixture arrTimeRoundTripFixture() {
        // TIME_ARR = 0x25. Elements: Time(0), Time(86399999), NULL.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_ARR_TIME);
        writeLeInt(bos, 3);
        bos.write(TYPE_TIME);
        writeLeLong(bos, 0L);
        bos.write(TYPE_TIME);
        writeLeLong(bos, 86_399_999L);
        bos.write(TYPE_NULL);
        return new Fixture(bos.toByteArray(), "arr_time", "[0,86399999,null]", null);
    }

    // ----------------------- Nested composites -----------------------

    static Fixture listOfListsFixture() {
        // Collection<Collection<Int>> — outer ArrayList (subtype=1), inner ArrayLists.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_COLLECTION);
        writeLeInt(bos, 2);
        bos.write(1);
        // inner 1: [1, 2]
        bos.write(TYPE_COLLECTION);
        writeLeInt(bos, 2);
        bos.write(1);
        bos.write(TYPE_INT); writeLeInt(bos, 1);
        bos.write(TYPE_INT); writeLeInt(bos, 2);
        // inner 2: [3]
        bos.write(TYPE_COLLECTION);
        writeLeInt(bos, 1);
        bos.write(1);
        bos.write(TYPE_INT); writeLeInt(bos, 3);
        return new Fixture(bos.toByteArray(), "list_of_lists",
                "[[1,2],[3]]", null);
    }

    static Fixture listOfMapsFixture() {
        // List<Map<String,Int>> with two maps.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_COLLECTION);
        writeLeInt(bos, 2);
        bos.write(1);
        // map 1: {a=1}
        bos.write(TYPE_MAP);
        writeLeInt(bos, 1);
        bos.write(1);
        writeString(bos, "a");
        bos.write(TYPE_INT); writeLeInt(bos, 1);
        // map 2: {b=2, c=3}
        bos.write(TYPE_MAP);
        writeLeInt(bos, 2);
        bos.write(1);
        writeString(bos, "b");
        bos.write(TYPE_INT); writeLeInt(bos, 2);
        writeString(bos, "c");
        bos.write(TYPE_INT); writeLeInt(bos, 3);
        return new Fixture(bos.toByteArray(), "list_of_maps",
                "[{a=1},{b=2,c=3}]", null);
    }

    static Fixture mapOfListsFixture() {
        // Map<String, List<Int>> with two entries.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_MAP);
        writeLeInt(bos, 2);
        bos.write(1);
        // k="xs" → [10, 20]
        writeString(bos, "xs");
        bos.write(TYPE_COLLECTION);
        writeLeInt(bos, 2);
        bos.write(1);
        bos.write(TYPE_INT); writeLeInt(bos, 10);
        bos.write(TYPE_INT); writeLeInt(bos, 20);
        // k="ys" → []
        writeString(bos, "ys");
        bos.write(TYPE_COLLECTION);
        writeLeInt(bos, 0);
        bos.write(1);
        return new Fixture(bos.toByteArray(), "map_of_lists",
                "{xs=[10,20],ys=[]}", null);
    }

    static Fixture listWithNullsFixture() {
        // List<String | null> — three items, middle is NULL.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(TYPE_COLLECTION);
        writeLeInt(bos, 3);
        bos.write(1);
        writeString(bos, "first");
        bos.write(TYPE_NULL);
        writeString(bos, "third");
        return new Fixture(bos.toByteArray(), "list_with_nulls",
                "[first,null,third]", null);
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

    /** Emit TYPE_STRING + length + UTF-8 bytes. */
    static void writeString(ByteArrayOutputStream bos, String s) {
        byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
        bos.write(TYPE_STRING);
        writeLeInt(bos, utf8.length);
        bos.write(utf8, 0, utf8.length);
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
