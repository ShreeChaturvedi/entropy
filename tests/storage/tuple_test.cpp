/**
 * @file tuple_test.cpp
 * @brief Unit tests for Tuple and TupleValue classes
 */

#include <gtest/gtest.h>

#include <cmath>
#include <limits>

#include "catalog/schema.hpp"
#include "storage/tuple.hpp"

namespace entropy {
namespace {

// ─────────────────────────────────────────────────────────────────────────────
// TupleValue Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST(TupleValueTest, NullValue) {
    TupleValue val;
    EXPECT_TRUE(val.is_null());
    EXPECT_FALSE(val.is_bool());
    EXPECT_FALSE(val.is_integer());
    EXPECT_FALSE(val.is_string());
}

TEST(TupleValueTest, NullFactory) {
    TupleValue val = TupleValue::null();
    EXPECT_TRUE(val.is_null());
}

TEST(TupleValueTest, BooleanValue) {
    TupleValue val_true(true);
    TupleValue val_false(false);

    EXPECT_FALSE(val_true.is_null());
    EXPECT_TRUE(val_true.is_bool());
    EXPECT_TRUE(val_true.as_bool());

    EXPECT_FALSE(val_false.is_null());
    EXPECT_TRUE(val_false.is_bool());
    EXPECT_FALSE(val_false.as_bool());
}

TEST(TupleValueTest, TinyIntValue) {
    TupleValue val(static_cast<int8_t>(-42));

    EXPECT_FALSE(val.is_null());
    EXPECT_TRUE(val.is_tinyint());
    EXPECT_EQ(val.as_tinyint(), -42);
}

TEST(TupleValueTest, SmallIntValue) {
    TupleValue val(static_cast<int16_t>(1234));

    EXPECT_FALSE(val.is_null());
    EXPECT_TRUE(val.is_smallint());
    EXPECT_EQ(val.as_smallint(), 1234);
}

TEST(TupleValueTest, IntegerValue) {
    TupleValue val(42);

    EXPECT_FALSE(val.is_null());
    EXPECT_TRUE(val.is_integer());
    EXPECT_EQ(val.as_integer(), 42);
}

TEST(TupleValueTest, IntegerEdgeCases) {
    TupleValue max_val(std::numeric_limits<int32_t>::max());
    TupleValue min_val(std::numeric_limits<int32_t>::min());
    TupleValue zero_val(0);

    EXPECT_EQ(max_val.as_integer(), std::numeric_limits<int32_t>::max());
    EXPECT_EQ(min_val.as_integer(), std::numeric_limits<int32_t>::min());
    EXPECT_EQ(zero_val.as_integer(), 0);
}

TEST(TupleValueTest, BigIntValue) {
    TupleValue val(static_cast<int64_t>(9223372036854775807LL));

    EXPECT_FALSE(val.is_null());
    EXPECT_TRUE(val.is_bigint());
    EXPECT_EQ(val.as_bigint(), 9223372036854775807LL);
}

TEST(TupleValueTest, FloatValue) {
    TupleValue val(3.14f);

    EXPECT_FALSE(val.is_null());
    EXPECT_TRUE(val.is_float());
    EXPECT_FLOAT_EQ(val.as_float(), 3.14f);
}

TEST(TupleValueTest, DoubleValue) {
    TupleValue val(3.14159265358979);

    EXPECT_FALSE(val.is_null());
    EXPECT_TRUE(val.is_double());
    EXPECT_DOUBLE_EQ(val.as_double(), 3.14159265358979);
}

TEST(TupleValueTest, StringValue) {
    TupleValue val(std::string("Hello, World!"));

    EXPECT_FALSE(val.is_null());
    EXPECT_TRUE(val.is_string());
    EXPECT_EQ(val.as_string(), "Hello, World!");
}

TEST(TupleValueTest, StringFromCharPtr) {
    TupleValue val("Hello");

    EXPECT_FALSE(val.is_null());
    EXPECT_TRUE(val.is_string());
    EXPECT_EQ(val.as_string(), "Hello");
}

TEST(TupleValueTest, EmptyString) {
    TupleValue val(std::string(""));

    EXPECT_FALSE(val.is_null());
    EXPECT_TRUE(val.is_string());
    EXPECT_EQ(val.as_string(), "");
    EXPECT_TRUE(val.as_string().empty());
}

TEST(TupleValueTest, Equality) {
    TupleValue val1(42);
    TupleValue val2(42);
    TupleValue val3(43);
    TupleValue null1;
    TupleValue null2;

    EXPECT_EQ(val1, val2);
    EXPECT_NE(val1, val3);
    EXPECT_EQ(null1, null2);
    EXPECT_NE(val1, null1);
}

// ─────────────────────────────────────────────────────────────────────────────
// TupleValue Serialization Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST(TupleValueSerializeTest, BooleanRoundTrip) {
    TupleValue original_true(true);
    TupleValue original_false(false);

    auto bytes_true = original_true.to_bytes(TypeId::BOOLEAN);
    auto bytes_false = original_false.to_bytes(TypeId::BOOLEAN);

    EXPECT_EQ(bytes_true.size(), 1);
    EXPECT_EQ(bytes_false.size(), 1);

    TupleValue restored_true = TupleValue::from_bytes(TypeId::BOOLEAN, bytes_true.data(), bytes_true.size());
    TupleValue restored_false = TupleValue::from_bytes(TypeId::BOOLEAN, bytes_false.data(), bytes_false.size());

    EXPECT_TRUE(restored_true.as_bool());
    EXPECT_FALSE(restored_false.as_bool());
}

TEST(TupleValueSerializeTest, TinyIntRoundTrip) {
    TupleValue original(static_cast<int8_t>(-127));
    auto bytes = original.to_bytes(TypeId::TINYINT);

    EXPECT_EQ(bytes.size(), 1);

    TupleValue restored = TupleValue::from_bytes(TypeId::TINYINT, bytes.data(), bytes.size());
    EXPECT_EQ(restored.as_tinyint(), -127);
}

TEST(TupleValueSerializeTest, SmallIntRoundTrip) {
    TupleValue original(static_cast<int16_t>(-32000));
    auto bytes = original.to_bytes(TypeId::SMALLINT);

    EXPECT_EQ(bytes.size(), 2);

    TupleValue restored = TupleValue::from_bytes(TypeId::SMALLINT, bytes.data(), bytes.size());
    EXPECT_EQ(restored.as_smallint(), -32000);
}

TEST(TupleValueSerializeTest, IntegerRoundTrip) {
    TupleValue original(123456789);
    auto bytes = original.to_bytes(TypeId::INTEGER);

    EXPECT_EQ(bytes.size(), 4);

    TupleValue restored = TupleValue::from_bytes(TypeId::INTEGER, bytes.data(), bytes.size());
    EXPECT_EQ(restored.as_integer(), 123456789);
}

TEST(TupleValueSerializeTest, BigIntRoundTrip) {
    TupleValue original(static_cast<int64_t>(123456789012345LL));
    auto bytes = original.to_bytes(TypeId::BIGINT);

    EXPECT_EQ(bytes.size(), 8);

    TupleValue restored = TupleValue::from_bytes(TypeId::BIGINT, bytes.data(), bytes.size());
    EXPECT_EQ(restored.as_bigint(), 123456789012345LL);
}

TEST(TupleValueSerializeTest, FloatRoundTrip) {
    TupleValue original(123.456f);
    auto bytes = original.to_bytes(TypeId::FLOAT);

    EXPECT_EQ(bytes.size(), 4);

    TupleValue restored = TupleValue::from_bytes(TypeId::FLOAT, bytes.data(), bytes.size());
    EXPECT_FLOAT_EQ(restored.as_float(), 123.456f);
}

TEST(TupleValueSerializeTest, DoubleRoundTrip) {
    TupleValue original(123.456789012345);
    auto bytes = original.to_bytes(TypeId::DOUBLE);

    EXPECT_EQ(bytes.size(), 8);

    TupleValue restored = TupleValue::from_bytes(TypeId::DOUBLE, bytes.data(), bytes.size());
    EXPECT_DOUBLE_EQ(restored.as_double(), 123.456789012345);
}

TEST(TupleValueSerializeTest, VarcharRoundTrip) {
    TupleValue original(std::string("Hello, Database!"));
    auto bytes = original.to_bytes(TypeId::VARCHAR);

    // 2 bytes length prefix + string content
    EXPECT_EQ(bytes.size(), 2 + 16);

    // from_bytes takes the data WITHOUT the length prefix
    TupleValue restored = TupleValue::from_bytes(TypeId::VARCHAR, bytes.data() + 2, 16);
    EXPECT_EQ(restored.as_string(), "Hello, Database!");
}

TEST(TupleValueSerializeTest, NullBytesEmpty) {
    TupleValue null_val;
    auto bytes = null_val.to_bytes(TypeId::INTEGER);

    EXPECT_TRUE(bytes.empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// Tuple Tests with Schema
// ─────────────────────────────────────────────────────────────────────────────

class TupleTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a simple schema: id (INTEGER), name (VARCHAR), active (BOOLEAN)
        std::vector<Column> columns = {
            Column("id", TypeId::INTEGER),
            Column("name", TypeId::VARCHAR, 100),
            Column("active", TypeId::BOOLEAN)
        };
        simple_schema_ = Schema(std::move(columns));

        // Create a more complex schema with various types
        std::vector<Column> complex_columns = {
            Column("col_bool", TypeId::BOOLEAN),
            Column("col_tiny", TypeId::TINYINT),
            Column("col_small", TypeId::SMALLINT),
            Column("col_int", TypeId::INTEGER),
            Column("col_big", TypeId::BIGINT),
            Column("col_float", TypeId::FLOAT),
            Column("col_double", TypeId::DOUBLE),
            Column("col_varchar1", TypeId::VARCHAR, 50),
            Column("col_varchar2", TypeId::VARCHAR, 100)
        };
        complex_schema_ = Schema(std::move(complex_columns));
    }

    Schema simple_schema_;
    Schema complex_schema_;
};

TEST_F(TupleTest, CreateFromValues) {
    std::vector<TupleValue> values = {
        TupleValue(42),
        TupleValue(std::string("Alice")),
        TupleValue(true)
    };

    Tuple tuple(values, simple_schema_);

    EXPECT_TRUE(tuple.is_valid());
    EXPECT_FALSE(tuple.is_empty());
    EXPECT_GT(tuple.size(), 0);
}

TEST_F(TupleTest, GetValueInteger) {
    std::vector<TupleValue> values = {
        TupleValue(42),
        TupleValue(std::string("Alice")),
        TupleValue(true)
    };

    Tuple tuple(values, simple_schema_);

    TupleValue retrieved = tuple.get_value(simple_schema_, 0);
    EXPECT_FALSE(retrieved.is_null());
    EXPECT_TRUE(retrieved.is_integer());
    EXPECT_EQ(retrieved.as_integer(), 42);
}

TEST_F(TupleTest, GetValueVarchar) {
    std::vector<TupleValue> values = {
        TupleValue(42),
        TupleValue(std::string("Alice")),
        TupleValue(true)
    };

    Tuple tuple(values, simple_schema_);

    TupleValue retrieved = tuple.get_value(simple_schema_, 1);
    EXPECT_FALSE(retrieved.is_null());
    EXPECT_TRUE(retrieved.is_string());
    EXPECT_EQ(retrieved.as_string(), "Alice");
}

TEST_F(TupleTest, GetValueBoolean) {
    std::vector<TupleValue> values = {
        TupleValue(42),
        TupleValue(std::string("Alice")),
        TupleValue(true)
    };

    Tuple tuple(values, simple_schema_);

    TupleValue retrieved = tuple.get_value(simple_schema_, 2);
    EXPECT_FALSE(retrieved.is_null());
    EXPECT_TRUE(retrieved.is_bool());
    EXPECT_TRUE(retrieved.as_bool());
}

TEST_F(TupleTest, GetValueOutOfRange) {
    std::vector<TupleValue> values = {
        TupleValue(42),
        TupleValue(std::string("Alice")),
        TupleValue(true)
    };

    Tuple tuple(values, simple_schema_);

    EXPECT_THROW(tuple.get_value(simple_schema_, 10), std::out_of_range);
}

TEST_F(TupleTest, NullValue) {
    std::vector<TupleValue> values = {
        TupleValue(42),
        TupleValue::null(),  // null name
        TupleValue(true)
    };

    Tuple tuple(values, simple_schema_);

    EXPECT_FALSE(tuple.is_null(0));
    EXPECT_TRUE(tuple.is_null(1));
    EXPECT_FALSE(tuple.is_null(2));

    TupleValue retrieved = tuple.get_value(simple_schema_, 1);
    EXPECT_TRUE(retrieved.is_null());
}

TEST_F(TupleTest, AllNullValues) {
    std::vector<TupleValue> values = {
        TupleValue::null(),
        TupleValue::null(),
        TupleValue::null()
    };

    Tuple tuple(values, simple_schema_);

    EXPECT_TRUE(tuple.is_null(0));
    EXPECT_TRUE(tuple.is_null(1));
    EXPECT_TRUE(tuple.is_null(2));
}

TEST_F(TupleTest, ComplexSchema) {
    std::vector<TupleValue> values = {
        TupleValue(true),                       // col_bool
        TupleValue(static_cast<int8_t>(42)),    // col_tiny
        TupleValue(static_cast<int16_t>(1234)), // col_small
        TupleValue(123456),                     // col_int
        TupleValue(static_cast<int64_t>(9876543210LL)), // col_big
        TupleValue(3.14f),                      // col_float
        TupleValue(2.71828),                    // col_double
        TupleValue(std::string("Hello")),       // col_varchar1
        TupleValue(std::string("World"))        // col_varchar2
    };

    Tuple tuple(values, complex_schema_);

    EXPECT_TRUE(tuple.get_value(complex_schema_, 0).as_bool());
    EXPECT_EQ(tuple.get_value(complex_schema_, 1).as_tinyint(), 42);
    EXPECT_EQ(tuple.get_value(complex_schema_, 2).as_smallint(), 1234);
    EXPECT_EQ(tuple.get_value(complex_schema_, 3).as_integer(), 123456);
    EXPECT_EQ(tuple.get_value(complex_schema_, 4).as_bigint(), 9876543210LL);
    EXPECT_FLOAT_EQ(tuple.get_value(complex_schema_, 5).as_float(), 3.14f);
    EXPECT_DOUBLE_EQ(tuple.get_value(complex_schema_, 6).as_double(), 2.71828);
    EXPECT_EQ(tuple.get_value(complex_schema_, 7).as_string(), "Hello");
    EXPECT_EQ(tuple.get_value(complex_schema_, 8).as_string(), "World");
}

TEST_F(TupleTest, MixedNullAndValues) {
    std::vector<TupleValue> values = {
        TupleValue(true),                       // col_bool
        TupleValue::null(),                     // col_tiny (null)
        TupleValue(static_cast<int16_t>(1234)), // col_small
        TupleValue::null(),                     // col_int (null)
        TupleValue(static_cast<int64_t>(9876543210LL)), // col_big
        TupleValue::null(),                     // col_float (null)
        TupleValue(2.71828),                    // col_double
        TupleValue::null(),                     // col_varchar1 (null)
        TupleValue(std::string("World"))        // col_varchar2
    };

    Tuple tuple(values, complex_schema_);

    EXPECT_FALSE(tuple.is_null(0));
    EXPECT_TRUE(tuple.is_null(1));
    EXPECT_FALSE(tuple.is_null(2));
    EXPECT_TRUE(tuple.is_null(3));
    EXPECT_FALSE(tuple.is_null(4));
    EXPECT_TRUE(tuple.is_null(5));
    EXPECT_FALSE(tuple.is_null(6));
    EXPECT_TRUE(tuple.is_null(7));
    EXPECT_FALSE(tuple.is_null(8));

    // Verify non-null values are correct
    EXPECT_TRUE(tuple.get_value(complex_schema_, 0).as_bool());
    EXPECT_EQ(tuple.get_value(complex_schema_, 2).as_smallint(), 1234);
    EXPECT_EQ(tuple.get_value(complex_schema_, 4).as_bigint(), 9876543210LL);
    EXPECT_DOUBLE_EQ(tuple.get_value(complex_schema_, 6).as_double(), 2.71828);
    EXPECT_EQ(tuple.get_value(complex_schema_, 8).as_string(), "World");
}

TEST_F(TupleTest, RIDAssignment) {
    std::vector<TupleValue> values = {
        TupleValue(42),
        TupleValue(std::string("Alice")),
        TupleValue(true)
    };

    Tuple tuple(values, simple_schema_);

    // Initially RID is invalid
    EXPECT_FALSE(tuple.rid().is_valid());

    // Set RID
    RID rid(5, 10);
    tuple.set_rid(rid);

    EXPECT_TRUE(tuple.rid().is_valid());
    EXPECT_EQ(tuple.rid().page_id, 5);
    EXPECT_EQ(tuple.rid().slot_id, 10);
}

TEST_F(TupleTest, SerializedSize) {
    std::vector<TupleValue> values = {
        TupleValue(42),                   // 4 bytes
        TupleValue(std::string("Alice")), // 2 + 5 = 7 bytes
        TupleValue(true)                  // 1 byte
    };

    // Null bitmap: ceil(3/8) = 1 byte
    // Fixed: 4 (int) + 1 (bool) = 5 bytes
    // Variable: 2 (len) + 5 (data) = 7 bytes
    // Total: 1 + 5 + 7 = 13 bytes
    uint32_t expected_size = 1 + 5 + 7;

    uint32_t actual_size = Tuple::serialized_size(values, simple_schema_);
    EXPECT_EQ(actual_size, expected_size);
}

TEST_F(TupleTest, NullBitmapSize) {
    EXPECT_EQ(Tuple::null_bitmap_size(0), 0);
    EXPECT_EQ(Tuple::null_bitmap_size(1), 1);
    EXPECT_EQ(Tuple::null_bitmap_size(7), 1);
    EXPECT_EQ(Tuple::null_bitmap_size(8), 1);
    EXPECT_EQ(Tuple::null_bitmap_size(9), 2);
    EXPECT_EQ(Tuple::null_bitmap_size(16), 2);
    EXPECT_EQ(Tuple::null_bitmap_size(17), 3);
}

TEST_F(TupleTest, ConstructFromRawData) {
    // Create a tuple normally
    std::vector<TupleValue> values = {
        TupleValue(42),
        TupleValue(std::string("Alice")),
        TupleValue(true)
    };
    Tuple original(values, simple_schema_);

    // Create from raw data
    std::vector<char> raw_data(original.data(), original.data() + original.size());
    RID rid(1, 2);
    Tuple reconstructed(raw_data, rid);

    // Verify RID
    EXPECT_EQ(reconstructed.rid().page_id, 1);
    EXPECT_EQ(reconstructed.rid().slot_id, 2);

    // Verify values
    EXPECT_EQ(reconstructed.get_value(simple_schema_, 0).as_integer(), 42);
    EXPECT_EQ(reconstructed.get_value(simple_schema_, 1).as_string(), "Alice");
    EXPECT_TRUE(reconstructed.get_value(simple_schema_, 2).as_bool());
}

TEST_F(TupleTest, EmptyTuple) {
    Tuple empty_tuple;

    EXPECT_FALSE(empty_tuple.is_valid());
    EXPECT_TRUE(empty_tuple.is_empty());
    EXPECT_EQ(empty_tuple.size(), 0);
}

TEST_F(TupleTest, DataSpan) {
    std::vector<TupleValue> values = {
        TupleValue(42),
        TupleValue(std::string("Alice")),
        TupleValue(true)
    };
    Tuple tuple(values, simple_schema_);

    auto span = tuple.as_span();
    EXPECT_EQ(span.size(), tuple.size());
    EXPECT_EQ(span.data(), tuple.data());
}

TEST_F(TupleTest, LongVarchar) {
    // Test with a longer string
    std::string long_str(200, 'X');

    std::vector<TupleValue> values = {
        TupleValue(42),
        TupleValue(long_str),
        TupleValue(true)
    };

    Tuple tuple(values, simple_schema_);

    TupleValue retrieved = tuple.get_value(simple_schema_, 1);
    EXPECT_EQ(retrieved.as_string(), long_str);
    EXPECT_EQ(retrieved.as_string().size(), 200);
}

TEST_F(TupleTest, EmptyVarchar) {
    std::vector<TupleValue> values = {
        TupleValue(42),
        TupleValue(std::string("")),
        TupleValue(true)
    };

    Tuple tuple(values, simple_schema_);

    TupleValue retrieved = tuple.get_value(simple_schema_, 1);
    EXPECT_FALSE(retrieved.is_null());
    EXPECT_EQ(retrieved.as_string(), "");
}

TEST_F(TupleTest, MultipleVarcharColumns) {
    std::vector<TupleValue> values = {
        TupleValue(true),
        TupleValue(static_cast<int8_t>(42)),
        TupleValue(static_cast<int16_t>(1234)),
        TupleValue(123456),
        TupleValue(static_cast<int64_t>(9876543210LL)),
        TupleValue(3.14f),
        TupleValue(2.71828),
        TupleValue(std::string("First String")),
        TupleValue(std::string("Second String"))
    };

    Tuple tuple(values, complex_schema_);

    // Both varchar columns should be retrievable
    EXPECT_EQ(tuple.get_value(complex_schema_, 7).as_string(), "First String");
    EXPECT_EQ(tuple.get_value(complex_schema_, 8).as_string(), "Second String");
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge Case Tests
// ─────────────────────────────────────────────────────────────────────────────

TEST(TupleEdgeCaseTest, SingleColumnInteger) {
    std::vector<Column> columns = {Column("id", TypeId::INTEGER)};
    Schema schema(columns);

    std::vector<TupleValue> values = {TupleValue(12345)};
    Tuple tuple(values, schema);

    EXPECT_EQ(tuple.get_value(schema, 0).as_integer(), 12345);
}

TEST(TupleEdgeCaseTest, SingleColumnVarchar) {
    std::vector<Column> columns = {Column("name", TypeId::VARCHAR, 100)};
    Schema schema(columns);

    std::vector<TupleValue> values = {TupleValue(std::string("Test"))};
    Tuple tuple(values, schema);

    EXPECT_EQ(tuple.get_value(schema, 0).as_string(), "Test");
}

TEST(TupleEdgeCaseTest, ManyColumns) {
    std::vector<Column> columns;
    std::vector<TupleValue> values;

    // Create 32 integer columns (tests null bitmap with multiple bytes)
    for (int i = 0; i < 32; ++i) {
        columns.emplace_back("col_" + std::to_string(i), TypeId::INTEGER);
        values.emplace_back(i * 100);
    }

    Schema schema(columns);
    Tuple tuple(values, schema);

    // Verify all values
    for (int i = 0; i < 32; ++i) {
        EXPECT_EQ(tuple.get_value(schema, i).as_integer(), i * 100);
    }
}

TEST(TupleEdgeCaseTest, ManyNullColumns) {
    std::vector<Column> columns;
    std::vector<TupleValue> values;

    // Create 16 columns with alternating null values
    for (int i = 0; i < 16; ++i) {
        columns.emplace_back("col_" + std::to_string(i), TypeId::INTEGER);
        if (i % 2 == 0) {
            values.emplace_back(i);
        } else {
            values.push_back(TupleValue::null());
        }
    }

    Schema schema(columns);
    Tuple tuple(values, schema);

    // Verify null pattern
    for (int i = 0; i < 16; ++i) {
        if (i % 2 == 0) {
            EXPECT_FALSE(tuple.is_null(i)) << "Column " << i << " should not be null";
            EXPECT_EQ(tuple.get_value(schema, i).as_integer(), i);
        } else {
            EXPECT_TRUE(tuple.is_null(i)) << "Column " << i << " should be null";
        }
    }
}

TEST(TupleEdgeCaseTest, SpecialFloatValues) {
    std::vector<Column> columns = {
        Column("inf", TypeId::DOUBLE),
        Column("neg_inf", TypeId::DOUBLE),
        Column("zero", TypeId::DOUBLE)
    };
    Schema schema(columns);

    std::vector<TupleValue> values = {
        TupleValue(std::numeric_limits<double>::infinity()),
        TupleValue(-std::numeric_limits<double>::infinity()),
        TupleValue(0.0)
    };

    Tuple tuple(values, schema);

    EXPECT_TRUE(std::isinf(tuple.get_value(schema, 0).as_double()));
    EXPECT_TRUE(std::isinf(tuple.get_value(schema, 1).as_double()));
    EXPECT_TRUE(tuple.get_value(schema, 1).as_double() < 0);
    EXPECT_DOUBLE_EQ(tuple.get_value(schema, 2).as_double(), 0.0);
}

}  // namespace
}  // namespace entropy
