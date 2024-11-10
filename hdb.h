// hdb - A simple hash database library
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

#ifndef HDB_H
#define HDB_H

#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <stdbool.h>
#include <pthread.h> 

#define BLOCK_SIZE 1024 // number of blocks to read at a time

struct hdb {
    FILE *hash_file;
    FILE *data_file;
    FILE *deleted_blocks;
    int *deleted_blocks_array;
    int deleted_blocks_array_size;
    pthread_t fsync_thread;
    bool stop_fsync_thread;
    pthread_mutex_t fsync_mutex;
};

uint32_t hash_function(const uint8_t *data, size_t length);

void db_close(struct hdb *db);
void decode_deleted_blocks_array(struct hdb *db);
void encode_deleted_blocks_array(struct hdb *db);
int db_delete(struct hdb *db, const uint8_t *key, size_t key_length);

void* fsync_background(void* arg) {
    struct hdb *db = (struct hdb*)arg;
    while (!db->stop_fsync_thread) {
        sleep(2); // Periodically fsync every 2 seconds
        pthread_mutex_lock(&db->fsync_mutex);
        fsync(fileno(db->hash_file));
        fsync(fileno(db->data_file));
        fsync(fileno(db->deleted_blocks));
        pthread_mutex_unlock(&db->fsync_mutex);
    }
    return NULL;
}

struct hdb* db_open(const char *hash_filename, const char *data_filename, const char *deleted_blocks_filename) {
    struct hdb *db = malloc(sizeof(struct hdb));
    if (!db) return NULL;

    db->hash_file = fopen(hash_filename, "rb+");
    if (!db->hash_file) {
        db->hash_file = fopen(hash_filename, "wb+");
    }
    db->data_file = fopen(data_filename, "rb+");
    if (!db->data_file) {
        db->data_file = fopen(data_filename, "wb+");
    }
    db->deleted_blocks = fopen(deleted_blocks_filename, "rb+");
    if (!db->deleted_blocks) {
        db->deleted_blocks = fopen(deleted_blocks_filename, "wb+");
    }
    db->deleted_blocks_array = NULL;
    db->deleted_blocks_array_size = 0;
    db->stop_fsync_thread = false;
    pthread_mutex_init(&db->fsync_mutex, NULL);

    if (!db->hash_file || !db->data_file || !db->deleted_blocks) {
        db_close(db);
        return NULL;
    }

    decode_deleted_blocks_array(db);

    pthread_create(&db->fsync_thread, NULL, fsync_background, db);

    return db;
}

void db_close(struct hdb *db) {
    if (db) {
        db->stop_fsync_thread = true;
        pthread_join(db->fsync_thread, NULL);

        pthread_mutex_lock(&db->fsync_mutex);
        if (db->deleted_blocks_array) {
            encode_deleted_blocks_array(db);
        }
        if (db->hash_file) {
            fsync(fileno(db->hash_file));
            fclose(db->hash_file);
        }
        if (db->data_file) {
            fsync(fileno(db->data_file));
            fclose(db->data_file);
        }
        if (db->deleted_blocks) {
            fsync(fileno(db->deleted_blocks));
            fclose(db->deleted_blocks);
        }
        if (db->deleted_blocks_array) free(db->deleted_blocks_array);
        pthread_mutex_unlock(&db->fsync_mutex);

        pthread_mutex_destroy(&db->fsync_mutex);
        free(db);
    }
}

uint32_t hash_function(const uint8_t *data, size_t length) {
    uint32_t hash = 0;
    uint32_t prime = 31;
    uint32_t prime2 = 37;
    for (size_t i = 0; i < length; ++i) {
        hash = (hash * prime) ^ (data[i] * prime2);
        prime = (prime * prime2) % 65521;
    }
    return hash;
}

int db_put(struct hdb *db, const uint8_t *key, size_t key_length, const uint8_t *value, size_t value_length) {
    uint32_t hash = hash_function(key, key_length);
    fseek(db->hash_file, hash % 128 * sizeof(uint32_t), SEEK_SET);
    uint32_t stored_hash;
    fread(&stored_hash, sizeof(uint32_t), 1, db->hash_file);

    if (stored_hash == hash) {
        // Key exists, delete the old value
        db_delete(db, key, key_length);
    }

    fseek(db->hash_file, hash % 128 * sizeof(uint32_t), SEEK_SET);
    fwrite(&hash, sizeof(uint32_t), 1, db->hash_file);

    uint64_t position;
    if (db->deleted_blocks_array_size > 0) {
        position = db->deleted_blocks_array[--db->deleted_blocks_array_size];
    } else {
        fseek(db->data_file, 0, SEEK_END);
        position = ftell(db->data_file);
    }
    fwrite(value, sizeof(uint8_t), value_length, db->data_file);

    fseek(db->hash_file, hash % 128 * sizeof(uint64_t) * 2, SEEK_SET);
    fwrite(&position, sizeof(uint64_t), 1, db->hash_file);
    fwrite(&value_length, sizeof(uint64_t), 1, db->hash_file);

    return 0;
}

int db_get(struct hdb *db, const uint8_t *key, size_t key_length, uint8_t *value, size_t *value_length) {
    uint32_t hash = hash_function(key, key_length);
    fseek(db->hash_file, hash % 128 * sizeof(uint32_t), SEEK_SET);
    uint32_t stored_hash;
    fread(&stored_hash, sizeof(uint32_t), 1, db->hash_file);

    if (stored_hash != hash) return -1;

    fseek(db->hash_file, hash % 128 * sizeof(uint64_t) * 2, SEEK_SET);
    uint64_t position, length;
    fread(&position, sizeof(uint64_t), 1, db->hash_file);
    fread(&length, sizeof(uint64_t), 1, db->hash_file);

    fseek(db->data_file, position, SEEK_SET);
    size_t total_read = 0;
    while (total_read < length) {
        size_t to_read = (length - total_read > BLOCK_SIZE) ? BLOCK_SIZE : length - total_read;
        fread(value + total_read, sizeof(uint8_t), to_read, db->data_file);
        total_read += to_read;
    }
    *value_length = length;

    return 0;
}

void encode_deleted_blocks_array(struct hdb *db) {
    fseek(db->deleted_blocks, 0, SEEK_SET);
    fwrite(db->deleted_blocks_array, sizeof(int), db->deleted_blocks_array_size, db->deleted_blocks);
    fsync(fileno(db->deleted_blocks));
}

void decode_deleted_blocks_array(struct hdb *db) {
    fseek(db->deleted_blocks, 0, SEEK_END);
    db->deleted_blocks_array_size = ftell(db->deleted_blocks) / sizeof(int);
    fseek(db->deleted_blocks, 0, SEEK_SET);
    db->deleted_blocks_array = malloc(db->deleted_blocks_array_size * sizeof(int));
    fread(db->deleted_blocks_array, sizeof(int), db->deleted_blocks_array_size, db->deleted_blocks);
}

int db_delete(struct hdb *db, const uint8_t *key, size_t key_length) {
    uint32_t hash = hash_function(key, key_length);
    fseek(db->hash_file, hash % 128 * sizeof(uint32_t), SEEK_SET);
    uint32_t stored_hash;
    fread(&stored_hash, sizeof(uint32_t), 1, db->hash_file);

    if (stored_hash != hash) return -1; // Hash doesn't match, key not found.

    // Mark the hash entry as deleted by setting it to 0
    fseek(db->hash_file, hash % 128 * sizeof(uint32_t), SEEK_SET);
    uint32_t zero = 0;
    fwrite(&zero, sizeof(uint32_t), 1, db->hash_file); // Clear the hash entry

    fseek(db->hash_file, hash % 128 * sizeof(uint64_t) * 2, SEEK_SET);
    uint64_t position, length;
    fread(&position, sizeof(uint64_t), 1, db->hash_file);
    fread(&length, sizeof(uint64_t), 1, db->hash_file);

    // Mark the position of the deleted block
    db->deleted_blocks_array = realloc(db->deleted_blocks_array, (db->deleted_blocks_array_size + 1) * sizeof(int));
    db->deleted_blocks_array[db->deleted_blocks_array_size++] = position;

    // Shift the remaining data after the deleted entry
    fseek(db->data_file, position + length, SEEK_SET);
    size_t remaining_size = ftell(db->data_file) - (position + length);
    uint8_t *buffer = malloc(remaining_size);
    fread(buffer, sizeof(uint8_t), remaining_size, db->data_file);
    fseek(db->data_file, position, SEEK_SET);
    fwrite(buffer, sizeof(uint8_t), remaining_size, db->data_file);
    free(buffer);
    ftruncate(fileno(db->data_file), position + remaining_size); // Resize data file

    // Update the hash file with the new data location
    fseek(db->hash_file, 0, SEEK_SET);
    for (int i = 0; i < 128; ++i) {
        uint32_t temp_hash;
        fread(&temp_hash, sizeof(uint32_t), 1, db->hash_file);
        if (temp_hash == 0) continue; // Skip empty slots

        uint64_t temp_position, temp_length;
        fread(&temp_position, sizeof(uint64_t), 1, db->hash_file);
        fread(&temp_length, sizeof(uint64_t), 1, db->hash_file);

        // If position is greater than the deleted one, adjust it
        if (temp_position > position) {
            temp_position -= length;
            fseek(db->hash_file, -2 * sizeof(uint64_t), SEEK_CUR);
            fwrite(&temp_position, sizeof(uint64_t), 1, db->hash_file);
            fseek(db->hash_file, sizeof(uint64_t), SEEK_CUR);
        }
    }

    return 0;
}


#endif // HDB_H