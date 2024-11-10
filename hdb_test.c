// hdb tests
// BSD 3-Clause License
//
// Copyright (c) 2024, Alex Gaetano Padula
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer.
//
//  2. Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//
//  3. Neither the name of the copyright holder nor the names of its
//     contributors may be used to endorse or promote products derived from
//     this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include "hdb.h"

// Test helper functions
void test_hash_function() {
    uint8_t data[] = "hello";
    uint32_t hash = hash_function(data, strlen((char*)data));
    assert(hash != 0); // Ensure that the hash is not zero
    printf("hash_function test passed\n");
}

void test_db_open_close() {
    struct hdb *db = db_open("test_hash.db", "test_data.db", "test_deleted.db");
    assert(db != NULL);
    db_close(db);
    printf("db_open and db_close test passed\n");
}

void test_db_put_and_db_get() {
    struct hdb *db = db_open("test_hash.db", "test_data.db", "test_deleted.db");

    uint8_t key[] = "testkey";
    uint8_t value[] = "testvalue";
    size_t value_length = strlen((char*)value);

    // Insert data
    assert(db_put(db, key, strlen((char*)key), value, value_length) == 0);

    // Retrieve data
    uint8_t retrieved_value[1024];
    size_t retrieved_value_length;
    assert(db_get(db, key, strlen((char*)key), retrieved_value, &retrieved_value_length) == 0);
    assert(retrieved_value_length == value_length);
    assert(memcmp(retrieved_value, value, value_length) == 0);

    db_close(db);
    printf("db_put and db_get test passed\n");
}

void test_db_delete() {
    struct hdb *db = db_open("test_hash.db", "test_data.db", "test_deleted.db");

    uint8_t key[] = "deletekey";
    uint8_t value[] = "deletevalue";
    size_t value_length = strlen((char*)value);

    // Insert data
    assert(db_put(db, key, strlen((char*)key), value, value_length) == 0);

    // Delete data
    assert(db_delete(db, key, strlen((char*)key)) == 0);

    // Try to retrieve the deleted data
    uint8_t retrieved_value[1024];
    size_t retrieved_value_length;
    assert(db_get(db, key, strlen((char*)key), retrieved_value, &retrieved_value_length) == -1);

    db_close(db);
    printf("db_delete test passed\n");
}

void test_deleted_blocks_array() {
    struct hdb *db = db_open("test_hash.db", "test_data.db", "test_deleted.db");

    uint8_t key1[] = "key1";
    uint8_t value1[] = "value1";
    uint8_t key2[] = "key2";
    uint8_t value2[] = "value2";

    // Insert data
    assert(db_put(db, key1, strlen((char*)key1), value1, strlen((char*)value1)) == 0);
    assert(db_put(db, key2, strlen((char*)key2), value2, strlen((char*)value2)) == 0);

    // Delete data
    assert(db_delete(db, key1, strlen((char*)key1)) == 0);
    assert(db_delete(db, key2, strlen((char*)key2)) == 0);

    // Check that the deleted blocks array is populated
    assert(db->deleted_blocks_array_size == 2);

    db_close(db);
    printf("deleted blocks array test passed\n");
}

void test_concurrent_fsync_thread() {
    struct hdb *db = db_open("test_hash.db", "test_data.db", "test_deleted.db");

    // We will test that the fsync background thread is running properly by letting it run for a few seconds
    sleep(5); // Let the background fsync thread run

    // Check the state of files (simulate interaction, e.g., by adding or modifying data)
    uint8_t key[] = "fsynctestkey";
    uint8_t value[] = "fsynctestvalue";
    assert(db_put(db, key, strlen((char*)key), value, strlen((char*)value)) == 0);

    // Wait a bit more to ensure the fsync thread runs
    sleep(5);

    db_close(db);
    printf("concurrent fsync background thread test passed\n");
}

int main() {
    // Run tests
    test_hash_function();
    test_db_open_close();
    test_db_put_and_db_get();
    test_db_delete();
    test_deleted_blocks_array();
    test_concurrent_fsync_thread();

    printf("All tests passed\n");
    return 0;
}
