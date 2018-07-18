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

#include "btree.h"

#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sys/time.h>

int btree_create(struct btree *bt);
int btree_read_metadata(struct btree *bt);
struct btree_node *btree_create_node(void);
void btree_free_node(struct btree_node *n);
int btree_write_node(struct btree *bt, struct btree_node *n, uint64_t offset);
int btree_freelist_index_by_exp(int exponent);
int btree_split_child(struct btree *bt, uint64_t pointedby, uint64_t parentoff,
                      int i, uint64_t childoff, uint64_t *newparent);

/* ------------------------ UNIX standard VFS Layer ------------------------- */
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

void *bvfs_unistd_open(char* path, int flags) {
    int fd;
    void *handle;

    fd = open(path,((flags & BTREE_CREAT) ? O_CREAT : 0)|O_RDWR,0644);
    if (fd == -1) return NULL;
    handle = malloc(sizeof(fd));
    *(int*)handle = fd;
    return handle;
}

void bvfs_unistd_close(void *handle) {
    int *fd = handle;

    close(*fd);
    free(handle);
}

ssize_t bvfs_unistd_pread(void *handle, void *buf, uint32_t nbytes,
                          uint64_t offset)
{
    int *fd = handle;

    return pread(*fd,buf,nbytes,offset);
}

ssize_t bvfs_unistd_pwrite(void *handle, const void *buf, uint32_t nbytes,
                           uint64_t offset)
{
    int *fd = handle;

    return pwrite(*fd,buf,nbytes,offset);
}

int bvfs_unistd_resize(void *handle, uint64_t length) {
    int *fd = handle;

    return ftruncate(*fd,length);
}

int bvfs_unistd_getsize(void *handle, uint64_t *size) {
    int *fd = handle;
    struct stat sb;

    if (fstat(*fd,&sb) == -1) return -1;
    *size = (uint64_t) sb.st_size;
    return 0;
}

void bvfs_unistd_sync(void *handle) {
    int *fd = handle;

    fsync(*fd);
}

struct btree_vfs bvfs_unistd = {
    bvfs_unistd_open,
    bvfs_unistd_close,
    bvfs_unistd_pread,
    bvfs_unistd_pwrite,
    bvfs_unistd_resize,
    bvfs_unistd_getsize,
    bvfs_unistd_sync
};

/* ------------------------- From/To Big endian ----------------------------- */

void btree_u32_to_big(unsigned char *buf, uint32_t val) {
    buf[0] = (val >> 24) & 0xff;
    buf[1] = (val >> 16) & 0xff;
    buf[2] = (val >> 8) & 0xff;
    buf[3] = val & 0xff;
}

void btree_u64_to_big(unsigned char *buf, uint64_t val) {
    buf[0] = (val >> 56) & 0xff;
    buf[1] = (val >> 48) & 0xff;
    buf[2] = (val >> 40) & 0xff;
    buf[3] = (val >> 32) & 0xff;
    buf[4] = (val >> 24) & 0xff;
    buf[5] = (val >> 16) & 0xff;
    buf[6] = (val >> 8) & 0xff;
    buf[7] = val & 0xff;
}

uint32_t btree_u32_from_big(unsigned char *buf) {
    uint32_t val = 0;

    val |= buf[0] << 24;
    val |= buf[1] << 16;
    val |= buf[2] << 8;
    val |= buf[3];
    return val;
}

uint64_t btree_u64_from_big(unsigned char *buf) {
    uint64_t val = 0;

    val |= (uint64_t) buf[0] << 56;
    val |= (uint64_t) buf[1] << 48;
    val |= (uint64_t) buf[2] << 40;
    val |= (uint64_t) buf[3] << 32;
    val |= (uint64_t) buf[4] << 24;
    val |= (uint64_t) buf[5] << 16;
    val |= (uint64_t) buf[6] << 8;
    val |= (uint64_t) buf[7];
    return val;
}

/* -------------------------- Utility functions ----------------------------- */

/* We read and write too often to write bt->vfs->...(bt->vfs_handle...) all the
 * times, so we use this two help functions. */
ssize_t btree_pwrite(struct btree *bt, const void *buf, uint32_t nbytes,
                     uint64_t offset)
{
    return bt->vfs->pwrite(bt->vfs_handle,buf,nbytes,offset);
}

ssize_t btree_pread(struct btree *bt, void *buf, uint32_t nbytes,
                    uint64_t offset)
{
    return bt->vfs->pread(bt->vfs_handle,buf,nbytes,offset);
}

/* We want to be able to write and read 32 and 64 integers easily and in a
 * platform / endianess agnostic way. */
ssize_t btree_pwrite_u32(struct btree *bt, uint32_t val, uint64_t offset) {
    unsigned char buf[4];

    btree_u32_to_big(buf,val);
    return btree_pwrite(bt,buf,sizeof(buf),offset);
}

int btree_pwrite_u64(struct btree *bt, uint64_t val, uint64_t offset) {
    unsigned char buf[8];

    btree_u64_to_big(buf,val);
    return btree_pwrite(bt,buf,sizeof(buf),offset);
}

int btree_pread_u32(struct btree *bt, uint32_t *val, uint64_t offset) {
    unsigned char buf[4];

    if (btree_pread(bt,buf,sizeof(buf),offset) == -1) return -1;
    *val = btree_u32_from_big(buf);
    return 0;
}

int btree_pread_u64(struct btree *bt, uint64_t *val, uint64_t offset) {
    unsigned char buf[8];

    if (btree_pread(bt,buf,sizeof(buf),offset) == -1) return -1;
    *val = btree_u64_from_big(buf);
    return 0;
}

void btree_sync(struct btree *bt) {
    if (bt->flags & BTREE_FLAG_USE_WRITE_BARRIER)
        bt->vfs->sync(bt->vfs_handle);
}

/* ---------------------------- BTREE operations ---------------------------- */

void btree_set_flags(struct btree *bt, int flags) {
    bt->flags |= flags;
}

void btree_clear_flags(struct btree *bt, int flags) {
    bt->flags &= ~flags;
}

/* Open a btree. On error NULL is returned, and errno is set accordingly.
 * Flags modify the behavior of the call:
 *
 * BTREE_CREAT: create the btree if it does not exist. */
struct btree *btree_open(struct btree_vfs *vfs, char *path, int flags) {
    struct btree *bt = NULL;
    struct timeval tv;
    int j, mkroot = 0;

    /* Initialize a new btree structure */
    if ((bt = malloc(sizeof(*bt))) == NULL) {
        errno = ENOMEM;
        return NULL;
    }
    bt->vfs = vfs ? vfs : &bvfs_unistd;
    bt->vfs_handle = NULL;
    bt->flags = BTREE_FLAG_USE_WRITE_BARRIER;
    for (j = 0; j < BTREE_FREELIST_COUNT; j++) {
        bt->freelist[j].numblocks = 0;
        bt->freelist[j].blocks = NULL;
        bt->freelist[j].last_items = 0;
    }

    /* Try opening the specified btree */
    bt->vfs_handle = bt->vfs->open(path,0);
    if (bt->vfs_handle == NULL) {
        if (!(flags & BTREE_CREAT)) goto err;
        /* Create the btree */
        if ((bt->vfs_handle = bt->vfs->open(path,flags)) == NULL) goto err;
        if (btree_create(bt) == -1) goto err;
        mkroot = 1; /* Create the root node before returing */
    }

    /* There are things about our btree that we always take in memory,
     * like all the free list block pointers and so forth.
     * Once we open the btree, we need to load this data into memory. */
    if (btree_read_metadata(bt) == -1) goto err;
    gettimeofday(&tv,NULL);
    bt->mark = (uint32_t) random() ^ tv.tv_sec ^ tv.tv_usec;

    /* Write the root node if needed (only when DB is created) */
    if (mkroot) {
        struct btree_node *root;
        uint64_t rootptr;

        /* Allocate space for the root */
        if ((rootptr = btree_alloc(bt,BTREE_NODE_SIZE)) == 0) goto err;

        /* Create a fresh root node and write it on disk */
        if ((root = btree_create_node()) == NULL) goto err;
        root->isleaf = 1; /* Our first node is a leaf */
        if (btree_write_node(bt,root,rootptr) == -1) {
            btree_free_node(root);
            goto err;
        }
        btree_free_node(root);
        btree_sync(bt);

        /* Write the root node pointer. */
        if (btree_pwrite_u64(bt,rootptr,BTREE_HDR_ROOTPTR_POS) == -1) goto err;
        bt->rootptr = rootptr;
        btree_sync(bt);
    }
    return bt;

err:
    btree_close(bt);
    return NULL;
}

/* Close a btree, even one that was unsuccesfull opened, so that
 * btree_open() can use this function for cleanup on error. */
void btree_close(struct btree *bt) {
    int j;

    if (!bt) return;
    if (bt->vfs_handle) bt->vfs->close(bt->vfs_handle);
    for (j = 0; j < BTREE_FREELIST_COUNT; j++)
        free(bt->freelist[j].blocks);
    free(bt);
}

#include <stdio.h>

/* Create a new btree, populating the header, free lists.
 * Note that this function is not exported, as callers should create a new
 * btree using open with the BTREE_CREAT flag. */
int btree_create(struct btree *bt) {
    int size, j;
    uint64_t filesize, freeoff;

    /* Make room for all the objects we have in the header */
    if (bt->vfs->getsize(bt->vfs_handle,&filesize) == -1) return -1;
    assert(filesize == 0);

    /* header: magic, version, free, freeoff */
    size = 8*4;
    /* Then we have our root free lists */
    size += BTREE_FREELIST_COUNT * BTREE_FREELIST_BLOCK_SIZE;
    /* And finally our root node pointer and actual node */
    size += 8; /* root pointer */
    size += BTREE_NODE_SIZE; /* root node */
    if (bt->vfs->resize(bt->vfs_handle,size) == -1) return -1;

    /* Now we have enough space to actually build the btree header,
     * free lists, and root node. */

    /* Magic and version */
    if (btree_pwrite(bt,"REDBTREE00000000",16,0) == -1) return -1;

    /* Free and Freeoff */
    if (btree_pwrite_u64(bt,0,BTREE_HDR_FREE_POS) == -1) return -1;
    freeoff = 32+BTREE_FREELIST_BLOCK_SIZE*BTREE_FREELIST_COUNT+8+BTREE_NODE_SIZE;
    if (btree_pwrite_u64(bt,freeoff,BTREE_HDR_FREEOFF_POS) == -1) return -1;

    /* Free lists */
    for (j = 0; j < BTREE_FREELIST_COUNT; j++) {
        uint64_t off = 32+BTREE_FREELIST_BLOCK_SIZE*j;

        /* next and prev pointers are set to zero, as this is the first
         * and sole block for this size. */
        if (btree_pwrite_u64(bt,0,off) == -1) return -1;
        if (btree_pwrite_u64(bt,0,off+8) == -1) return -1;
        /* Set count as zero, as we have no entry inside this block */
        if (btree_pwrite_u32(bt,0,off+16) == -1) return -1;
    }
    return 0;
}

int btree_read_metadata(struct btree *bt) {
    int j;

    /* TODO: Check signature and version. */
    /* Read free space and offset information */
    if (btree_pread_u64(bt,&bt->free,BTREE_HDR_FREE_POS) == -1) return -1;
    if (btree_pread_u64(bt,&bt->freeoff,BTREE_HDR_FREEOFF_POS) == -1) return -1;
    /* TODO: check that they makes sense considered the file size. */
    /* Read root node pointer */
    if (btree_pread_u64(bt,&bt->rootptr,BTREE_HDR_ROOTPTR_POS) == -1) return -1;
    printf("Root node is at %llu\n", bt->rootptr);
    /* Read free lists information */
    for (j = 0; j < BTREE_FREELIST_COUNT; j++) {
        uint64_t ptr = 32+BTREE_FREELIST_BLOCK_SIZE*j;
        uint64_t nextptr, numitems;

        // printf("Load metadata for freelist %d\n", j);
        do {
            struct btree_freelist *fl = &bt->freelist[j];

            if (btree_pread_u64(bt,&nextptr,ptr+sizeof(uint64_t)) == -1)
                return -1;
            if (btree_pread_u64(bt,&numitems,ptr+sizeof(uint64_t)*2) == -1)
                return -1;
            // printf("  block %lld: %lld items (next: %lld)\n", ptr, numitems,
            //    nextptr);
            fl->blocks = realloc(fl->blocks,sizeof(uint64_t)*(fl->numblocks+1));
            if (fl->blocks == NULL) return -1;
            fl->blocks[fl->numblocks] = ptr;
            fl->numblocks++;
            fl->last_items = numitems;
            ptr = nextptr;
        } while(ptr);
    }
    return 0;
}

/* Create a new node in memory */
struct btree_node *btree_create_node(void) {
    struct btree_node *n = calloc(1,sizeof(*n));

    return n;
}

void btree_free_node(struct btree_node *n) {
    free(n);
}

/* Write a node on disk at the specified offset. Returns 0 on success.
 * On error -1 is returne and errno set accordingly. */
int btree_write_node(struct btree *bt, struct btree_node *n, uint64_t offset) {
    unsigned char buf[BTREE_NODE_SIZE];
    unsigned char *p = buf;
    int j;

    bt->mark++;
    btree_u32_to_big(p,bt->mark); p += 4; /* start mark */
    btree_u32_to_big(p,n->numkeys); p += 4; /* number of keys */
    btree_u32_to_big(p,n->isleaf); p += 4; /* is a leaf? */
    btree_u32_to_big(p,0); p += 4; /* unused field, needed for alignment */
    memcpy(p,n->keys,sizeof(n->keys)); p += sizeof(n->keys); /* keys */
    /* values */
    for (j = 0; j < BTREE_MAX_KEYS; j++) {
        btree_u64_to_big(p,n->values[j]);
        p += 8;
    }
    /* children */
    for (j = 0; j <= BTREE_MAX_KEYS; j++) {
        btree_u64_to_big(p,n->children[j]);
        p += 8;
    }
    btree_u32_to_big(p,bt->mark); p += 4; /* end mark */
    return btree_pwrite(bt,buf,sizeof(buf),offset);
}

/* Read a node from the specified offset.
 * On success the in memory representation of the node is returned as a
 * btree_node structure (to be freed with btree_free_node). On error
 * NULL is returned and errno set accordingly.
 *
 * If data on disk is corrupted errno is set to EFAULT. */
struct btree_node *btree_read_node(struct btree *bt, uint64_t offset) {
    unsigned char buf[BTREE_NODE_SIZE], *p;
    struct btree_node *n;
    int j;

    if (btree_pread(bt,buf,sizeof(buf),offset) == -1) return NULL;
    /* Verify start/end marks */
    if (memcmp(buf,buf+BTREE_NODE_SIZE-4,4)) {
        errno = EFAULT;
        return NULL;
    }
    if ((n = btree_create_node()) == NULL) return NULL;

    p = buf+4;
    n->numkeys = btree_u32_from_big(p); p += 4; /* number of keys */
    n->isleaf = btree_u32_from_big(p); p += 4; /* is a leaf? */
    p += 4; /* unused field, needed for alignment */
    memcpy(n->keys,p,sizeof(n->keys)); p += sizeof(n->keys); /* keys */
    /* values */
    for (j = 0; j < BTREE_MAX_KEYS; j++) {
        n->values[j] = btree_u64_from_big(p);
        p += 8;
    }
    /* children */
    for (j = 0; j <= BTREE_MAX_KEYS; j++) {
        n->children[j] = btree_u64_from_big(p);
        p += 8;
    }
    return n;
}

/* ------------------------- disk space allocator --------------------------- */

/* Compute logarithm in base two of 'n', with 'n' being a power of two.
 * Probably you can just check the latest 1 bit set, but here it's not
 * a matter of speed as we are dealing with the disk every time we call
 * this function. */
int btree_log_two(uint32_t n) {
    int log = -1;

    while(n) {
        log++;
        n /= 2;
    }
    return log;
}

int btree_alloc_freelist(struct btree *bt, uint32_t realsize, uint64_t *ptr) {
    int exp = btree_log_two(realsize);
    int fli = btree_freelist_index_by_exp(exp);
    struct btree_freelist *fl = &bt->freelist[fli];
    uint64_t block, lastblock = 0, p;

    if (fl->last_items == 0 && fl->numblocks == 1) {
        *ptr = 0;
        return 0;
    }

    /* Last block is empty? Remove it */
    if (fl->last_items == 0) {
        uint64_t prevblock, *oldptr;

        assert(fl->numblocks > 1);
        /* Set prevblock next pointer to NULL */
        prevblock = fl->blocks[fl->numblocks-2];
        if (btree_pwrite_u64(bt,0,prevblock+sizeof(uint64_t)) == -1) return -1;
        btree_sync(bt);
        /* Fix our memory representaiton of freelist */
        lastblock = fl->blocks[fl->numblocks-1];
        fl->numblocks--;
        /* The previous item must be full, so we set the new number
         * of items to the max. */
        fl->last_items = BTREE_FREELIST_BLOCK_ITEMS;
        /* Realloc the block as we have one element less. */
        oldptr = fl->blocks;
        fl->blocks = realloc(fl->blocks,sizeof(uint64_t)*fl->numblocks);
        if (fl->blocks == NULL) {
            /* Out of memory. The realloc failed, but note that while this
             * is a leak as the block remains larger than needed we still
             * have a valid in memory representation. */
            fl->blocks = oldptr;
            return -1;
        }
    }

    /* There was a block to remove, but this block is the same size
     * of the allocation required? Just return it. */
    if (lastblock && exp == BTREE_FREELIST_SIZE_EXP) {
        *ptr = lastblock;
        return 0;
    } else {
        btree_free(bt,lastblock);
    }

    /* Get an element from the current block, and return it to the
     * caller. */
    block = fl->blocks[fl->numblocks-1];
    if (btree_pread_u64(bt,&p,block+((2+fl->last_items)*sizeof(uint64_t))) == -1) return -1;
    fl->last_items--;
    if (btree_pwrite_u64(bt,fl->last_items,block+(2*sizeof(uint64_t))) == -1) return -1;
    btree_sync(bt);
    *ptr = p+sizeof(uint64_t);
    return 0;
}

/* Return the next power of two that is able to hold size+1 bytes.
 * The byte we add is used to save the exponent of two as the first byte
 * so that for btree_free() can check the block size. */
uint32_t btree_alloc_realsize(uint32_t size) {
    uint32_t realsize;

    realsize = 16; /* We don't allocate nothing that is smaller than 16 bytes */
    while (realsize < (size+sizeof(uint64_t))) realsize *= 2;
    return realsize;
}

/* Allocate some piece of data on disk. Returns the offset to the newly
 * allocated space. If the allocation can't be performed, 0 is returned. */
uint64_t btree_alloc(struct btree *bt, uint32_t size) {
    uint64_t ptr;
    uint32_t realsize;

    printf("ALLOCATIING %lu\n", (unsigned long) size);

    /* Don't allow allocations bigger than 2GB */
    if (size > (unsigned)(1<<31)) {
        errno = EINVAL;
        return 0;
    }
    realsize = btree_alloc_realsize(size);

    /* Search for free space in the free lists */
    if (btree_alloc_freelist(bt,realsize,&ptr) == -1) return 0;
    if (ptr) {
        uint64_t oldsize;
        /* Got an element from the free list. Fix the size header if needed. */
        if (btree_pread_u64(bt,&oldsize,ptr-sizeof(uint64_t)) == -1) return 0;
        if (oldsize != size) {
            if (btree_pwrite_u64(bt,size,ptr-sizeof(uint64_t)) == -1)
                return 0;
            btree_sync(bt);
        }
        return ptr;
    }

    /* We have to perform a real allocation.
     * If we don't have room at the end of the file, create some space. */
    if (bt->free < realsize) {
        uint64_t currsize = bt->freeoff + bt->free;
        if (bt->vfs->resize(bt->vfs_handle,currsize+BTREE_PREALLOC_SIZE) == -1)
            return 0;
        bt->free += BTREE_PREALLOC_SIZE;
    }

    /* Allocate it moving the header pointers and free space count */
    ptr = bt->freeoff;
    bt->free -= realsize;
    bt->freeoff += realsize;

    if (btree_pwrite_u64(bt,bt->free,BTREE_HDR_FREE_POS) == -1) return -1;
    if (btree_pwrite_u64(bt,bt->freeoff,BTREE_HDR_FREEOFF_POS) == -1) return -1;

    /* Write the size header in the new allocated space */
    if (btree_pwrite_u64(bt,size,ptr) == -1) return -1;

    /* A final fsync() as a write barrier */
    btree_sync(bt);
    return ptr+sizeof(uint64_t);
}

/* Given an on disk pointer returns the length of the original allocation
 * (not the size of teh chunk itself as power of two, but the original
 * argument passed to btree_alloc function).
 *
 * On success 0 is returned and the size parameter populated, otherwise
 * -1 is returned and errno set accordingly. */
int btree_alloc_size(struct btree *bt, uint32_t *size, uint64_t ptr) {
    uint64_t s;

    if (btree_pread_u64(bt,&s,ptr-8) == -1) return -1;
    *size = (uint32_t) s;
    return 0;
}

/* Return the free list slot index given the power of two exponent representing
 * the size of the free list allocations. */
int btree_freelist_index_by_exp(int exponent) {
    assert(exponent > 1 && exponent < 32);
    return exponent-4;
}

/* Release allocated memory, putting the pointer in the right free list.
 * On success 0 is returned. On error -1. */
int btree_free(struct btree *bt, uint64_t ptr) {
    uint64_t size;
    uint32_t realsize;
    int fli, exp;
    struct btree_freelist *fl;

    if (btree_pread_u64(bt,&size,ptr-sizeof(uint64_t)) == -1) return -1;
    realsize = btree_alloc_realsize(size);
    exp = btree_log_two(realsize);
    printf("Free %llu bytes (realsize: %llu)\n", size, (uint64_t) realsize);

    fli = btree_freelist_index_by_exp(exp);
    fl = &bt->freelist[fli];

    /* We need special handling when freeing an allocation that is the same
     * size of the freelist block, and the latest free list block for that size
     * is full. Without this special handling what happens is that we need
     * to allocate a new block of the same size to make space, but doing so
     * would result in an element removed from the latest block, so after we
     * link the new block we have the previous block that is not full.
     *
     * Check BTREE.txt in this source distribution for more information. */
    if (fl->last_items == BTREE_FREELIST_BLOCK_ITEMS &&
        exp == BTREE_FREELIST_SIZE_EXP)
    {
        /* Just use the freed allocation as the next free block */
        fl->blocks = realloc(fl->blocks,sizeof(uint64_t)*(fl->numblocks+1));
        if (fl->blocks == NULL) return -1;
        fl->blocks[fl->numblocks] = ptr;
        fl->numblocks++;
        fl->last_items = 0;
        /* Init block setting items count, next pointer, prev pointer. */
        btree_pwrite_u64(bt,0,ptr+sizeof(uint64_t)); /* next */
        btree_pwrite_u64(bt,fl->blocks[fl->numblocks-2],ptr); /* prev */
        btree_pwrite_u64(bt,0,ptr+sizeof(uint64_t)*2); /* numitems */
        btree_sync(bt); /* Make sure it's ok before linking it to prev block */
        /* Link this new block to the free list blocks updating next pointer
         * of the previous block. */
        btree_pwrite_u64(bt,ptr,fl->blocks[fl->numblocks-2]+sizeof(uint64_t));
        btree_sync(bt);
    } else {
        /* Allocate a new block if needed */
        if (fl->last_items == BTREE_FREELIST_BLOCK_ITEMS) {
            uint64_t newblock;

            newblock = btree_alloc(bt,BTREE_FREELIST_BLOCK_SIZE);
            if (newblock == 0) return -1;

            fl->blocks = realloc(fl->blocks,sizeof(uint64_t)*(fl->numblocks+1));
            if (fl->blocks == NULL) return -1;
            fl->blocks[fl->numblocks] = newblock;
            fl->numblocks++;
            fl->last_items = 0;
            /* Init block setting items count, next pointer, prev pointer. */
            btree_pwrite_u64(bt,0,newblock+sizeof(uint64_t)); /* next */
            btree_pwrite_u64(bt,fl->blocks[fl->numblocks-2],newblock);/* prev */
            btree_pwrite_u64(bt,0,newblock+sizeof(uint64_t)*2); /* numitems */
            btree_sync(bt); /* Make sure it's ok before linking it. */
            /* Link this new block to the free list blocks updating next pointer
             * of the previous block. */
            btree_pwrite_u64(bt,newblock,fl->blocks[fl->numblocks-2]+sizeof(uint64_t));
            btree_sync(bt);
        }
        /* Add the item */
        fl->last_block[fl->last_items] = ptr-sizeof(uint64_t);
        fl->last_items++;
        /* Write the pointer in the block first */
        printf("Write freelist item about ptr %llu at %llu\n",
            ptr, fl->blocks[fl->numblocks-1]+(sizeof(uint64_t)*3)
            +(sizeof(uint64_t)*(fl->last_items-1)));
        btree_pwrite_u64(bt,ptr-sizeof(uint64_t),fl->blocks[fl->numblocks-1]+(sizeof(uint64_t)*3)+(sizeof(uint64_t)*(fl->last_items-1)));
        btree_sync(bt);
        /* Then write the items count. */
        printf("Write the new count for block %lld: %lld at %lld\n",
            fl->blocks[fl->numblocks-1],
            (uint64_t) fl->last_items,
            fl->blocks[fl->numblocks-1]+sizeof(uint64_t)*2);
        btree_pwrite_u64(bt,fl->last_items,fl->blocks[fl->numblocks-1]+sizeof(uint64_t)*2);
        btree_sync(bt);
    }
    return 0;
}

/* --------------------------- btree operations  ---------------------------- */

int btree_node_is_full(struct btree_node *n) {
    return n->numkeys == BTREE_MAX_KEYS;
}

/* Add a key at the specified position 'i' inside an in-memory node.
 * All the other keys starting from the old key at position 'i' are
 * shifted one position to the right.
 *
 * Note: this function does not change the position of the children as it
 * is intented to be used only on leafs. */
void btree_node_insert_key_at(struct btree_node *n, int i, unsigned char *key, uint64_t valoff) {
    void *p;
    
    p = n->keys + (i*BTREE_HASHED_KEY_LEN);
    memmove(p+BTREE_HASHED_KEY_LEN,p,(n->numkeys-i)*BTREE_HASHED_KEY_LEN);
    memmove(n->values+i+1,n->values+i,(n->numkeys-i)*8);
    memcpy(p,key,BTREE_HASHED_KEY_LEN);
    n->values[i] = valoff;
    n->numkeys++;
}

/* Insert a key (and associated value) into a non full node.
 * If the node is a leaf the key can be inserted in the current node otherwise
 * we need to walk the three, possibly splitting full nodes as we descend.
 *
 * The nodeptr is the offset of the node we want to insert into.
 *
 * Pointedby is the offset on disk inside the parent of the node pointed by
 * 'nodeptr'. As we always write new full nodes instead of modifying old ones
 * in order to be more crash proof, we need to update the pointer in the
 * parent node when everything is ready.
 *
 * The function returns 0 on success, and -1 on error.
 * On error errno is set accordingly, and may also assume the following values:
 *
 * EFAULT if the btree seems corrupted.
 * EEXIST if the key already exists.
 */
int btree_add_nonfull(struct btree *bt, uint64_t nodeptr, uint64_t pointedby, unsigned char *key, unsigned char *val, size_t vlen, int replace) {
    struct btree_node *n = NULL;
    int i, found = 0;

    if ((n = btree_read_node(bt,nodeptr)) == NULL) return -1;
    i = n->numkeys-1;

    /* Seek to the right position in the current node */
    while(1) {
        int cmp;

        if (i < 0) break;
        cmp = memcmp(key,n->keys+i*BTREE_HASHED_KEY_LEN,BTREE_HASHED_KEY_LEN);
        if (cmp == 0) {
            found = 1; /* the key is already present in the btree */
            break;
        }
        if (cmp >= 0) break;
        i--;
    }

    /* Key already present? Replace it with the new value if replace is true
     * otherwise return an error. */
    if (found) {
        if (!replace) {
            errno = EBUSY;
            return -1;
        } else {
            uint64_t oldvaloff = n->values[i];
            uint64_t newvaloff;

            if ((newvaloff = btree_alloc(bt,vlen)) == 0) goto err;
            if (btree_pwrite(bt,val,vlen,newvaloff) == -1) goto err;
            btree_sync(bt);
            /* Overwrite the pointer to the old value off with the new one. */
            if (btree_pwrite_u64(bt,newvaloff,nodeptr+16+(BTREE_HASHED_KEY_LEN*BTREE_MAX_KEYS)+(8*i)) == -1) goto err;
            /* Finally we can free the old value, and the in memory node. */
            btree_free(bt,oldvaloff);
            btree_free_node(n);
            return 0;
        }
    }

    if (n->isleaf) {
        uint64_t newoff; /* New node offset */
        uint64_t valoff; /* Value offset on disk */

        /* Write the value on disk */
        if ((valoff = btree_alloc(bt,vlen)) == 0) goto err;
        if (btree_pwrite(bt,val,vlen,valoff) == -1) goto err;
        /* Insert the new key in place, and a pointer to the value. */
        btree_node_insert_key_at(n,i+1,key,valoff);
        /* Write the modified node to disk */
        if ((newoff = btree_alloc(bt,BTREE_NODE_SIZE)) == 0) goto err;
        if (btree_write_node(bt,n,newoff) == -1) goto err;
        /* Update the pointer pointing to this node with the new node offset. */
        if (btree_pwrite_u64(bt,newoff,pointedby) == -1) goto err;
        if (pointedby == BTREE_HDR_ROOTPTR_POS) bt->rootptr = newoff;
        /* Free the old node on disk */
        if (btree_free(bt,nodeptr) == -1) goto err;
        btree_free_node(n);
    } else {
        struct btree_node *child;
        uint64_t newnode;

        i++;
        if ((child = btree_read_node(bt,n->children[i])) == NULL) return -1;
        if (btree_node_is_full(child)) {
            if (btree_split_child(bt,pointedby,nodeptr,i,n->children[i],
                &newnode) == -1)
            {
                btree_free_node(child);
                goto err;
            }
        } else {
            pointedby = nodeptr+16+BTREE_HASHED_KEY_LEN*BTREE_MAX_KEYS+8*BTREE_MAX_KEYS+8*i;
            newnode = n->children[i];
            /* Fixme, here we can set 'n' to 'child' and tail-recurse with
             * a goto, to avoid re-reading the same node again. */
        }
        btree_free_node(n);
        btree_free_node(child);
        return btree_add_nonfull(bt,newnode,pointedby,key,val,vlen,replace);
    }
    return 0;

err:
    btree_free_node(n);
    return -1;
}

/* Split child, that is the i-th child of parent.
 * We'll write three new nodes, two to split the original child in two nodes
 * and one containing the updated parent.
 * Finally we'll set 'pointedby' to the offset of the new parent. So
 * pointedby must point to the offset where the parent is referenced on disk,
 * that is the root pointer heeader if it's the root node, or the right offset
 * inside its parent (that is, the parent of the parent). */
int btree_split_child(struct btree *bt, uint64_t pointedby, uint64_t parentoff,
                      int i, uint64_t childoff, uint64_t *newparent)
{
    struct btree_node *lnode = NULL, *rnode = NULL;
    struct btree_node *child = NULL, *parent = NULL;
    int halflen = (BTREE_MAX_KEYS-1)/2;
    uint64_t loff, roff, poff; /* new left, right, parent nodes offets. */

    /* Read parent and child from disk.
     * Also creates new nodes in memory, lnode and rnode, that will be
     * the nodes produced splitting the child into two nodes. */
    if ((parent = btree_read_node(bt,parentoff)) == NULL) goto err;
    if ((child = btree_read_node(bt,childoff)) == NULL) goto err;
    if ((lnode = btree_create_node()) == NULL) goto err;
    if ((rnode = btree_create_node()) == NULL) goto err;
    /* Two fundamental conditions that must be always true */
    assert(child->numkeys == BTREE_MAX_KEYS);
    assert(parent->numkeys != BTREE_MAX_KEYS);
    /* Split the child into lnode and rnode */
    memcpy(lnode->keys,child->keys,BTREE_HASHED_KEY_LEN*halflen);
    memcpy(lnode->values,child->values,8*halflen);
    memcpy(lnode->children,child->children,8*(halflen+1));
    lnode->numkeys = halflen;
    lnode->isleaf = child->isleaf;
    /* And the rnode */
    memcpy(rnode->keys,child->keys+BTREE_HASHED_KEY_LEN*(halflen+1),
                                   BTREE_HASHED_KEY_LEN*halflen);
    memcpy(rnode->values,child->values+halflen+1,8*halflen);
    memcpy(rnode->children,child->children+halflen+1,8*(halflen+1));
    rnode->numkeys = halflen;
    rnode->isleaf = child->isleaf;
    /* Save left and right children on disk */
    if ((loff = btree_alloc(bt,BTREE_NODE_SIZE)) == 0) goto err;
    if ((roff = btree_alloc(bt,BTREE_NODE_SIZE)) == 0) goto err;
    if (btree_write_node(bt,lnode,loff) == -1) goto err;
    if (btree_write_node(bt,rnode,roff) == -1) goto err;

    /* Now fix the parent node:
     * let's move the child's median key into the parent.
     * Shift the current keys, values, and child pointers. */
    memmove(parent->keys+BTREE_HASHED_KEY_LEN*(i+1),
            parent->keys+BTREE_HASHED_KEY_LEN*i,
            (parent->numkeys-i)*BTREE_HASHED_KEY_LEN);
    memmove(parent->values+i+1,parent->values+i,(parent->numkeys-i)*8);
    memmove(parent->children+i+2,parent->children+i+1,(parent->numkeys-i)*8);
    /* Set the key and left and right children */
    memcpy(parent->keys+BTREE_HASHED_KEY_LEN*i,
           child->keys+BTREE_HASHED_KEY_LEN*halflen,BTREE_HASHED_KEY_LEN);
    parent->values[i] = child->values[halflen];
    parent->children[i] = loff;
    parent->children[i+1] = roff;
    parent->numkeys++;
    /* Write the parent on disk */
    if ((poff = btree_alloc(bt,BTREE_NODE_SIZE)) == 0) goto err;
    if (btree_write_node(bt,parent,poff) == -1) goto err;
    if (newparent) *newparent = poff;
    /* Now link the new nodes to the old btree */
    btree_sync(bt); /* Make sure the nodes are flushed */
    if (btree_pwrite_u64(bt,poff,pointedby) == -1) goto err;
    if (pointedby == BTREE_HDR_ROOTPTR_POS) bt->rootptr = poff;
    /* Finally reclaim the space used by the old nodes */
    btree_free(bt,parentoff);
    btree_free(bt,childoff);

    btree_free_node(lnode);
    btree_free_node(rnode);
    btree_free_node(parent);
    btree_free_node(child);
    return 0;

err:
    btree_free_node(lnode);
    btree_free_node(rnode);
    btree_free_node(parent);
    btree_free_node(child);
    return -1;
}

int btree_add(struct btree *bt, unsigned char *key, unsigned char *val, size_t vlen, int replace) {
    struct btree_node *root;

    if ((root = btree_read_node(bt,bt->rootptr)) == NULL) return -1;

    if (btree_node_is_full(root)) {
        uint64_t rootptr;

        /* Root is full. Split it. */
        btree_free_node(root);
        root = NULL;
        /* Create a fresh node on disk: will be our new root. */
        if ((root = btree_create_node()) == NULL) return -1;
        if ((rootptr = btree_alloc(bt,BTREE_NODE_SIZE)) == 0) goto err;
        if (btree_write_node(bt,root,rootptr) == -1) goto err;
        btree_free_node(root);
        /* Split it */
        if (btree_split_child(bt,BTREE_HDR_ROOTPTR_POS,rootptr,0,bt->rootptr,NULL) == -1) goto err;
    } else {
        btree_free_node(root);
    }
    return btree_add_nonfull(bt,bt->rootptr,BTREE_HDR_ROOTPTR_POS,key,val,vlen,replace);

err:
    btree_free_node(root);
    return -1;
}

/* Find a record by key.
 * The function seraches for the specified key. If the key is found
 * 0 is returned, and *voff is set to the offset of the value on disk.
 * 
 * On error -1 is returned and errno set accordingly.
 * 
 * Non existing key is considered an error with errno = ENOENT. */
int btree_find(struct btree *bt, unsigned char *key, uint64_t *voff) {
    struct btree_node *n;
    uint64_t nptr = bt->rootptr;
    unsigned int j;

    while(1) {
        int cmp;

        if ((n = btree_read_node(bt,nptr)) == NULL) return -1;
        for (j = 0; j < n->numkeys; j++) {
            cmp = memcmp(key,n->keys+BTREE_HASHED_KEY_LEN*j,
                BTREE_HASHED_KEY_LEN);
            if (cmp <= 0) break;
        }
        if (j < n->numkeys && cmp == 0) {
            if (voff) *voff = n->values[j];
            btree_free_node(n);
            return 0;
        }
        if (n->isleaf || n->children[j] == 0) {
            btree_free_node(n);
            errno = ENOENT;
            return -1;
        }
        nptr = n->children[j];
        btree_free_node(n);
    }
}

/* Just a debugging function to check what's inside the whole btree... */
void btree_walk_rec(struct btree *bt, uint64_t nodeptr, int level) {
    struct btree_node *n;
    unsigned int j;
    
    n = btree_read_node(bt,nodeptr);
    if (n == NULL) {
        printf("Error walking the btree: %s\n", strerror(errno));
        return;
    }
    for (j = 0; j < n->numkeys; j++) {
        char *data;
        uint32_t datalen;
        int k;

        if (n->children[j] != 0) {
            btree_walk_rec(bt,n->children[j],level+1);
        }
        for (k = 0; k < level; k++) printf(" ");
        printf("(@%llu) Key %20s: ", nodeptr, n->keys+(j*BTREE_HASHED_KEY_LEN));
        btree_alloc_size(bt,&datalen,n->values[j]);
        data = malloc(datalen+1);
        btree_pread(bt,data,datalen,n->values[j]);
        data[datalen] = '\0';
        printf("@%llu    %lu bytes: %s\n",
            n->values[j],
            (unsigned long)datalen, data);
        free(data);
    }
    if (n->children[j] != 0) {
        btree_walk_rec(bt,n->children[j], level+1);
    }
}

void btree_walk(struct btree *bt, uint64_t nodeptr) {
    btree_walk_rec(bt,nodeptr,0);
}
