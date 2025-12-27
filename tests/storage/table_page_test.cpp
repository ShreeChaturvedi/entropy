/**
 * @file table_page_test.cpp
 * @brief Unit tests for TablePage (slotted page format)
 */

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

#include "storage/table_page.hpp"

namespace entropy {
namespace {

class TablePageTest : public ::testing::Test {
protected:
    void SetUp() override {
        page_ = std::make_unique<Page>();
        table_page_ = std::make_unique<TablePage>(page_.get());
        table_page_->init();
    }

    std::unique_ptr<Page> page_;
    std::unique_ptr<TablePage> table_page_;
};

TEST_F(TablePageTest, Init) {
    EXPECT_EQ(page_->page_type(), PageType::TABLE_PAGE);
    EXPECT_EQ(table_page_->get_slot_count(), 0);
    EXPECT_EQ(table_page_->get_record_count(), 0);
    EXPECT_GT(table_page_->get_free_space(), 0);
    EXPECT_EQ(table_page_->get_next_page_id(), INVALID_PAGE_ID);
    EXPECT_EQ(table_page_->get_prev_page_id(), INVALID_PAGE_ID);
}

TEST_F(TablePageTest, InsertRecord) {
    const char data[] = "Hello, World!";
    uint16_t size = sizeof(data);

    auto slot_id = table_page_->insert_record(data, size);
    ASSERT_TRUE(slot_id.has_value());
    EXPECT_EQ(slot_id.value(), 0);
    EXPECT_EQ(table_page_->get_slot_count(), 1);
    EXPECT_EQ(table_page_->get_record_count(), 1);
}

TEST_F(TablePageTest, InsertMultipleRecords) {
    std::vector<std::string> records = {
        "First record",
        "Second record with more data",
        "Third",
        "Fourth record is the longest one in this test"
    };

    for (size_t i = 0; i < records.size(); ++i) {
        auto slot_id = table_page_->insert_record(
            records[i].data(),
            static_cast<uint16_t>(records[i].size() + 1)  // Include null terminator
        );
        ASSERT_TRUE(slot_id.has_value());
        EXPECT_EQ(slot_id.value(), static_cast<slot_id_t>(i));
    }

    EXPECT_EQ(table_page_->get_slot_count(), 4);
    EXPECT_EQ(table_page_->get_record_count(), 4);
}

TEST_F(TablePageTest, GetRecord) {
    const std::string data = "Test record data";
    auto slot_id = table_page_->insert_record(
        data.data(),
        static_cast<uint16_t>(data.size() + 1)
    );
    ASSERT_TRUE(slot_id.has_value());

    auto record = table_page_->get_record(slot_id.value());
    ASSERT_FALSE(record.empty());
    EXPECT_EQ(record.size(), data.size() + 1);
    EXPECT_STREQ(record.data(), data.c_str());
}

TEST_F(TablePageTest, GetRecordInvalidSlot) {
    auto record = table_page_->get_record(999);
    EXPECT_TRUE(record.empty());
}

TEST_F(TablePageTest, DeleteRecord) {
    const char data[] = "To be deleted";
    auto slot_id = table_page_->insert_record(data, sizeof(data));
    ASSERT_TRUE(slot_id.has_value());

    EXPECT_TRUE(table_page_->delete_record(slot_id.value()));
    EXPECT_EQ(table_page_->get_record_count(), 0);
    EXPECT_EQ(table_page_->get_slot_count(), 1);  // Slot still exists

    // Getting deleted record should return empty
    auto record = table_page_->get_record(slot_id.value());
    EXPECT_TRUE(record.empty());
}

TEST_F(TablePageTest, DeleteInvalidSlot) {
    EXPECT_FALSE(table_page_->delete_record(0));  // No records
    EXPECT_FALSE(table_page_->delete_record(999));
}

TEST_F(TablePageTest, DeleteAlreadyDeleted) {
    const char data[] = "Test";
    auto slot_id = table_page_->insert_record(data, sizeof(data));
    ASSERT_TRUE(slot_id.has_value());

    EXPECT_TRUE(table_page_->delete_record(slot_id.value()));
    EXPECT_FALSE(table_page_->delete_record(slot_id.value()));  // Already deleted
}

TEST_F(TablePageTest, SlotReuse) {
    const char data1[] = "First";
    const char data2[] = "Second";
    const char data3[] = "Third";

    auto slot0 = table_page_->insert_record(data1, sizeof(data1));
    auto slot1 = table_page_->insert_record(data2, sizeof(data2));
    auto slot2 = table_page_->insert_record(data3, sizeof(data3));

    ASSERT_TRUE(slot0.has_value() && slot1.has_value() && slot2.has_value());
    EXPECT_EQ(slot0.value(), 0);
    EXPECT_EQ(slot1.value(), 1);
    EXPECT_EQ(slot2.value(), 2);

    // Delete middle slot
    EXPECT_TRUE(table_page_->delete_record(1));

    // Insert new record - should reuse slot 1
    const char data4[] = "Fourth";
    auto slot_new = table_page_->insert_record(data4, sizeof(data4));
    ASSERT_TRUE(slot_new.has_value());
    EXPECT_EQ(slot_new.value(), 1);  // Reused deleted slot
}

TEST_F(TablePageTest, UpdateRecordSameSize) {
    const char original[] = "Original data!";
    auto slot_id = table_page_->insert_record(original, sizeof(original));
    ASSERT_TRUE(slot_id.has_value());

    const char updated[] = "Updated data!!";  // Same size
    EXPECT_TRUE(table_page_->update_record(slot_id.value(), updated, sizeof(updated)));

    auto record = table_page_->get_record(slot_id.value());
    EXPECT_STREQ(record.data(), updated);
}

TEST_F(TablePageTest, UpdateRecordSmaller) {
    const char original[] = "Original longer data";
    auto slot_id = table_page_->insert_record(original, sizeof(original));
    ASSERT_TRUE(slot_id.has_value());

    const char updated[] = "Shorter";
    EXPECT_TRUE(table_page_->update_record(slot_id.value(), updated, sizeof(updated)));

    auto record = table_page_->get_record(slot_id.value());
    EXPECT_STREQ(record.data(), updated);
}

TEST_F(TablePageTest, UpdateRecordLarger) {
    const char original[] = "Short";
    auto slot_id = table_page_->insert_record(original, sizeof(original));
    ASSERT_TRUE(slot_id.has_value());

    const char updated[] = "Much longer updated data here";
    EXPECT_TRUE(table_page_->update_record(slot_id.value(), updated, sizeof(updated)));

    auto record = table_page_->get_record(slot_id.value());
    EXPECT_STREQ(record.data(), updated);
}

TEST_F(TablePageTest, FreeSpaceDecreases) {
    uint16_t initial_free = table_page_->get_free_space();

    const char data[] = "Some test data for the record";
    auto slot_id = table_page_->insert_record(data, sizeof(data));
    ASSERT_TRUE(slot_id.has_value());

    uint16_t after_insert = table_page_->get_free_space();
    EXPECT_LT(after_insert, initial_free);

    // Free space should decrease by at least record size + slot size
    uint16_t expected_decrease = sizeof(data) + TablePage::kSlotSize;
    EXPECT_GE(initial_free - after_insert, expected_decrease);
}

TEST_F(TablePageTest, CanFit) {
    EXPECT_TRUE(table_page_->can_fit(100));
    EXPECT_TRUE(table_page_->can_fit(1000));

    // Should not fit a record larger than page size
    EXPECT_FALSE(table_page_->can_fit(config::kDefaultPageSize));
}

TEST_F(TablePageTest, PageLinking) {
    table_page_->set_next_page_id(42);
    table_page_->set_prev_page_id(41);

    EXPECT_EQ(table_page_->get_next_page_id(), 42);
    EXPECT_EQ(table_page_->get_prev_page_id(), 41);
}

TEST_F(TablePageTest, Compact) {
    // Insert several records
    const char data1[] = "Record 1";
    const char data2[] = "Record 2 longer";
    const char data3[] = "Record 3";

    auto slot0 = table_page_->insert_record(data1, sizeof(data1));
    auto slot1 = table_page_->insert_record(data2, sizeof(data2));
    auto slot2 = table_page_->insert_record(data3, sizeof(data3));

    uint16_t free_before_delete = table_page_->get_free_space();

    // Delete middle record (creates fragmentation)
    table_page_->delete_record(slot1.value());

    // Free space shouldn't increase immediately (fragmentation)
    uint16_t free_after_delete = table_page_->get_free_space();
    EXPECT_EQ(free_after_delete, free_before_delete);

    // Compact the page
    table_page_->compact();

    // Free space should now include the deleted record's space
    uint16_t free_after_compact = table_page_->get_free_space();
    EXPECT_GT(free_after_compact, free_before_delete);

    // Verify remaining records are intact
    auto rec0 = table_page_->get_record(slot0.value());
    auto rec2 = table_page_->get_record(slot2.value());
    EXPECT_STREQ(rec0.data(), data1);
    EXPECT_STREQ(rec2.data(), data3);
}

TEST_F(TablePageTest, FillPage) {
    // Fill the page with records until it's full
    std::vector<slot_id_t> slots;
    const std::string record(100, 'X');

    while (table_page_->can_fit(static_cast<uint16_t>(record.size()))) {
        auto slot = table_page_->insert_record(
            record.data(),
            static_cast<uint16_t>(record.size())
        );
        if (!slot.has_value()) break;
        slots.push_back(slot.value());
    }

    EXPECT_GT(slots.size(), 0u);
    EXPECT_EQ(table_page_->get_record_count(), slots.size());

    // Should not be able to fit another record
    EXPECT_FALSE(table_page_->can_fit(static_cast<uint16_t>(record.size())));
}

TEST_F(TablePageTest, LargeRecord) {
    // Test with a large record
    std::string large_record(2000, 'L');

    auto slot = table_page_->insert_record(
        large_record.data(),
        static_cast<uint16_t>(large_record.size())
    );
    ASSERT_TRUE(slot.has_value());

    auto record = table_page_->get_record(slot.value());
    EXPECT_EQ(record.size(), large_record.size());
    EXPECT_EQ(std::string(record.data(), record.size()), large_record);
}

TEST_F(TablePageTest, GetRecordMut) {
    const char data[] = "Mutable test";
    auto slot = table_page_->insert_record(data, sizeof(data));
    ASSERT_TRUE(slot.has_value());

    auto record = table_page_->get_record_mut(slot.value());
    ASSERT_FALSE(record.empty());

    // Modify in place
    record[0] = 'm';
    record[1] = 'U';

    // Verify modification
    auto record2 = table_page_->get_record(slot.value());
    EXPECT_EQ(record2[0], 'm');
    EXPECT_EQ(record2[1], 'U');
}

}  // namespace
}  // namespace entropy
