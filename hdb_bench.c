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
#include <time.h>
#include <stdlib.h>
#include <assert.h>
#include "hdb.h"

// Benchmark the put operation
void benchmark_put(struct hdb *db, int num_operations) {
    uint8_t key[32];
    uint8_t value[256];
    
    clock_t start = clock();
    for (int i = 0; i < num_operations; ++i) {
        // Generate a key and value
        snprintf((char*)key, sizeof(key), "key%d", i);
        snprintf((char*)value, sizeof(value), "value%d", i);

        // Perform put operation
        db_put(db, key, strlen((char*)key), value, strlen((char*)value));
    }
    clock_t end = clock();

    double duration = (double)(end - start) / CLOCKS_PER_SEC;
    printf("Put operation benchmark completed: %d operations in %.6f seconds (%.6f ops/sec)\n",
           num_operations, duration, num_operations / duration);
}

// Benchmark the get operation
void benchmark_get(struct hdb *db, int num_operations) {
    uint8_t key[32];
    uint8_t value[256];
    size_t value_length;

    // Insert some data first
    for (int i = 0; i < num_operations; ++i) {
        snprintf((char*)key, sizeof(key), "key%d", i);
        snprintf((char*)value, sizeof(value), "value%d", i);
        db_put(db, key, strlen((char*)key), value, strlen((char*)value));
    }

    clock_t start = clock();
    for (int i = 0; i < num_operations; ++i) {
        snprintf((char*)key, sizeof(key), "key%d", i);
        uint8_t retrieved_value[1024];

        // Perform get operation
        db_get(db, key, strlen((char*)key), retrieved_value, &value_length);
    }
    clock_t end = clock();

    double duration = (double)(end - start) / CLOCKS_PER_SEC;
    printf("Get operation benchmark completed: %d operations in %.6f seconds (%.6f ops/sec)\n",
           num_operations, duration, num_operations / duration);
}

// Benchmark the delete operation
void benchmark_delete(struct hdb *db, int num_operations) {
    uint8_t key[32];
    uint8_t value[256];

    // Insert some data first
    for (int i = 0; i < num_operations; ++i) {
        snprintf((char*)key, sizeof(key), "key%d", i);
        snprintf((char*)value, sizeof(value), "value%d", i);
        db_put(db, key, strlen((char*)key), value, strlen((char*)value));
    }

    clock_t start = clock();
    for (int i = 0; i < num_operations; ++i) {
        snprintf((char*)key, sizeof(key), "key%d", i);

        // Perform delete operation
        db_delete(db, key, strlen((char*)key));
    }
    clock_t end = clock();

    double duration = (double)(end - start) / CLOCKS_PER_SEC;
    printf("Delete operation benchmark completed: %d operations in %.6f seconds (%.6f ops/sec)\n",
           num_operations, duration, num_operations / duration);
}

int main() {
    int num_operations = 1000; // Number of operations to benchmark

    // Open the database
    struct hdb *db = db_open("benchmark_hash.db", "benchmark_data.db", "benchmark_deleted.db");
    assert(db != NULL);

    // Run the benchmarks
    benchmark_put(db, num_operations);
    benchmark_get(db, num_operations);
    benchmark_delete(db, num_operations);

    // Close the database
    db_close(db);

    return 0;
}
