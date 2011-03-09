/*
 * Copyright (c) 2011, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdint.h>
#include <sys/types.h>

#define BTREE_CREAT 1

#define BTREE_PREALLOC_SIZE (1024*512)
#define BTREE_FREELIST_BLOCK_ITEMS 252
#define BTREE_MIN_KEYS 4
//#define BTREE_MAX_KEYS 255
#define BTREE_MAX_KEYS 7
#define BTREE_HASHED_KEY_LEN 16

/* We have free lists for the following sizes:
 * 16 32 64 128 256 512 1024 2048 4096 8192 16k 32k 64k 128k 256k 512k 1M 2M 4M 8M 16M 32M 64M 128M 256M 512M 1G 2G */
#define BTREE_FREELIST_COUNT 28

/* A free list block is composed of 2 pointers (prev, next), one count
 * (numitems), and a pointer for every free list item inside. */
#define BTREE_FREELIST_BLOCK_SIZE ((8*3)+(8*BTREE_FREELIST_BLOCK_ITEMS))
#define BTREE_FREELIST_SIZE_EXP 11   /* 2^11 = 2048 */

/* A node is composed of:
 * one count (startmark),
 * one count (numkeys),
 * one count (isleaf),
 * BTREE_MAX_KEYS keys (16 bytes for each key, as our keys are fixed size),
 * BTREE_MAX_KEYS pointers to values,
 * BTREE_MAX_KEYS+1 child pointers,
 * and a final count(endmark) */
#define BTREE_NODE_SIZE (4*4+BTREE_MAX_KEYS*BTREE_HASHED_KEY_LEN+((BTREE_MAX_KEYS*2)+1)*8+4)

/* Offsets inside the file of the 'free' and 'freeoff' fields */
#define BTREE_HDR_FREE_POS 16
#define BTREE_HDR_FREEOFF_POS 24
#define BTREE_HDR_ROOTPTR_POS (32+(BTREE_FREELIST_BLOCK_SIZE*BTREE_FREELIST_COUNT))

/* ------------------------------ VFS Layer --------------------------------- */

struct btree_vfs {
    void *(*open) (char *path, int flags);
    void (*close) (void *vfs_handle);
    ssize_t (*pread) (void *vfs_handle, void *buf, uint32_t nbytes,
                      uint64_t offset);
    ssize_t (*pwrite) (void *vfs_handle, const void *buf, uint32_t nbytes,
                       uint64_t offset);
    int (*resize) (void *vfs_handle, uint64_t length);
    int (*getsize) (void *vfs_handle, uint64_t *size);
    void (*sync) (void *vfs_handle);
};

extern struct btree_vfs bvfs_unistd;

/* ------------------------------ ALLOCATOR --------------------------------- */

struct btree_freelist {
    uint32_t numblocks;     /* number of freelist blocks */
    uint64_t *blocks;       /* blocks offsets. last is block[numblocks-1] */
    uint32_t last_items;    /* number of items in the last block */
    uint64_t last_block[BTREE_FREELIST_BLOCK_ITEMS];  /* last block cached */
};

/* -------------------------------- BTREE ----------------------------------- */

#define BTREE_FLAG_NOFLAG 0
#define BTREE_FLAG_USE_WRITE_BARRIER 1

/* This is our btree object, returned to the client when the btree is
 * opened, and used as first argument for all the btree API. */
struct btree {
    struct btree_vfs *vfs;  /* Our VFS API */
    void *vfs_handle;       /* The open VFS resource */
    /* Our free lists, from 4 bytes to 4 gigabytes, so freelist[0] is for
     * size 4, and freelist[BTREE_FREELIST_COUNT-1] is for 4GB. */
    struct btree_freelist freelist[BTREE_FREELIST_COUNT];
    /* We pre-allocate free space at the end of the file, as a room for
     * the allocator. Amount and location of free space is handled
     * by the following fields: */
    uint64_t free;          /* Amount of free space starting at freeoff */
    uint64_t freeoff;       /* Offset where free space starts */
    uint64_t rootptr;       /* Root node pointer */
    uint32_t mark;          /* This incremental number is used for
                               nodes start/end mark to detect corruptions. */
    int flags;              /* BTREE_FLAG_* */
};

/* In memory representation of a btree node. We manipulate this in memory
 * representation in order to avoid to deal with too much disk operations
 * and complexities. Once a node was modified it can be written back to disk
 * using btree_write_node. */
struct btree_node {
    uint32_t numkeys;
    uint32_t isleaf;
    char keys[BTREE_HASHED_KEY_LEN*BTREE_MAX_KEYS];
    uint64_t values[BTREE_MAX_KEYS];
    uint64_t children[BTREE_MAX_KEYS+1];
};

/* ---------------------------- EXPORTED API  ------------------------------- */

/* Btree */
struct btree *btree_open(struct btree_vfs *vfs, char *path, int flags);
void btree_close(struct btree *bt);
void btree_set_flags(struct btree *bt, int flags);
void btree_clear_flags(struct btree *bt, int flags);
int btree_add(struct btree *bt, unsigned char *key, unsigned char *val, size_t vlen, int replace);
int btree_find(struct btree *bt, unsigned char *key, uint64_t *voff);
void btree_walk(struct btree *bt, uint64_t nodeptr);

/* On disk allocator */
uint64_t btree_alloc(struct btree *bt, uint32_t size);
int btree_free(struct btree *bt, uint64_t ptr);
int btree_alloc_size(struct btree *bt, uint32_t *size, uint64_t ptr);
ssize_t btree_pread(struct btree *bt, void *buf, uint32_t nbytes,
                    uint64_t offset);
