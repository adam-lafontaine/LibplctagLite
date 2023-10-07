/***************************************************************************
 *   Copyright (C) 2020 by Kyle Hayes                                      *
 *   Author Kyle Hayes  kyle.hayes@gmail.com                               *
 *                                                                         *
 * This software is available under either the Mozilla Public License      *
 * version 2.0 or the GNU LGPL version 2 (or later) license, whichever     *
 * you choose.                                                             *
 *                                                                         *
 * MPL 2.0:                                                                *
 *                                                                         *
 *   This Source Code Form is subject to the terms of the Mozilla Public   *
 *   License, v. 2.0. If a copy of the MPL was not distributed with this   *
 *   file, You can obtain one at http://mozilla.org/MPL/2.0/.              *
 *                                                                         *
 *                                                                         *
 * LGPL 2:                                                                 *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU Library General Public License as       *
 *   published by the Free Software Foundation; either version 2 of the    *
 *   License, or (at your option) any later version.                       *
 *                                                                         *
 *   This program is distributed in the hope that it will be useful,       *
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of        *
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         *
 *   GNU General Public License for more details.                          *
 *                                                                         *
 *   You should have received a copy of the GNU Library General Public     *
 *   License along with this program; if not, write to the                 *
 *   Free Software Foundation, Inc.,                                       *
 *   59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.             *
 ***************************************************************************/

//#define LIBPLCTAGDLL_EXPORTS 1

#include <ctype.h>
#include <float.h>
#include <inttypes.h>
#include <limits.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <string.h>

#include "libplctag_internal.h"

#ifdef __cplusplus
extern "C" {
#endif




#ifndef __LIB_LIB_C__
#define __LIB_LIB_C__

#define INITIAL_TAG_TABLE_SIZE (201)

#define TAG_ID_MASK (0xFFFFFFF)

#define MAX_TAG_MAP_ATTEMPTS (50)

/* these are only internal to the file */

static volatile int32_t next_tag_id = 10; /* MAGIC */
static volatile hashtable_p tags = NULL;
static mutex_p tag_lookup_mutex = NULL;

static atomic_int library_terminating = {0};
static thread_p tag_tickler_thread = NULL;
static cond_p tag_tickler_wait = NULL;
#define TAG_TICKLER_TIMEOUT_MS  (100)
#define TAG_TICKLER_TIMEOUT_MIN_MS (10)
static int64_t tag_tickler_wait_timeout_end = 0;

//static mutex_p global_library_mutex = NULL;



/* helper functions. */
static plc_tag_p lookup_tag(int32_t id);
static int add_tag_lookup(plc_tag_p tag);
static int tag_id_inc(int id);
static THREAD_FUNC(tag_tickler_func);
static int set_tag_byte_order(plc_tag_p tag, attr attribs);
static int check_byte_order_str(const char *byte_order, int length);
// static int get_string_count_size_unsafe(plc_tag_p tag, int offset);
static int get_string_length_unsafe(plc_tag_p tag, int offset);
// static int get_string_capacity_unsafe(plc_tag_p tag, int offset);
// static int get_string_padding_unsafe(plc_tag_p tag, int offset);
// static int get_string_total_length_unsafe(plc_tag_p tag, int offset);
// static int get_string_byte_swapped_index_unsafe(plc_tag_p tag, int offset, int char_index);

#ifdef LIPLCTAGDLL_EXPORTS
    #if defined(_WIN32) || (defined(_WIN64)
#include <process.h>
BOOL WINAPI DllMain(HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpvReserved)
{
    switch(fdwReason) {
        case DLL_PROCESS_ATTACH:
            //fprintf(stderr, "DllMain called with DLL_PROCESS_ATTACH\n");
            break;

        case DLL_PROCESS_DETACH:
            //fprintf(stderr, "DllMain called with DLL_PROCESS_DETACH\n");
            plc_tag_shutdown();
            break;

        case DLL_THREAD_ATTACH:
            //fprintf(stderr, "DllMain called with DLL_THREAD_ATTACH\n");
            break;

        case DLL_THREAD_DETACH:
            //fprintf(stderr, "DllMain called with DLL_THREAD_DETACH\n");
            break;

        default:
            //fprintf(stderr, "DllMain called with unexpected code %d!\n", fdwReason);
            break;
    }

    return TRUE;
}
    #endif
#endif

/*
 * Initialize the library.  This is called in a threadsafe manner and
 * only called once.
 */

int lib_init(void)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting.");

    atomic_set(&library_terminating, 0);

    pdebug(DEBUG_INFO,"Setting up global library data.");

    pdebug(DEBUG_INFO,"Creating tag hashtable.");
    if((tags = hashtable_create(INITIAL_TAG_TABLE_SIZE)) == NULL) { /* MAGIC */
        pdebug(DEBUG_ERROR, "Unable to create tag hashtable!");
        return PLCTAG_ERR_NO_MEM;
    }

    pdebug(DEBUG_INFO,"Creating tag hashtable mutex.");
    rc = mutex_create((mutex_p *)&tag_lookup_mutex);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to create tag hashtable mutex!");
    }

    pdebug(DEBUG_INFO,"Creating tag condition variable.");
    rc = cond_create((cond_p *)&tag_tickler_wait);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to create tag condition var!");
    }

    pdebug(DEBUG_INFO,"Creating tag tickler thread.");
    rc = thread_create(&tag_tickler_thread, tag_tickler_func, 32*1024, NULL);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to create tag tickler thread!");
    }

    pdebug(DEBUG_INFO,"Done.");

    return rc;
}




void lib_teardown(void)
{
    pdebug(DEBUG_INFO,"Tearing down library.");

    atomic_set(&library_terminating, 1);

    if(tag_tickler_wait) {
        pdebug(DEBUG_INFO, "Signaling tag tickler condition var.");
        cond_signal(tag_tickler_wait);
    }

    if(tag_tickler_thread) {
        pdebug(DEBUG_INFO,"Tearing down tag tickler thread.");
        thread_join(tag_tickler_thread);
        thread_destroy(&tag_tickler_thread);
        tag_tickler_thread = NULL;
    }

    if(tag_tickler_wait) {
        pdebug(DEBUG_INFO, "Tearing down tag tickler condition var.");
        cond_destroy(&tag_tickler_wait);
        tag_tickler_wait = NULL;
    }

    if(tag_lookup_mutex) {
        pdebug(DEBUG_INFO,"Tearing down tag lookup mutex.");
        mutex_destroy(&tag_lookup_mutex);
        tag_lookup_mutex = NULL;
    }

    if(tags) {
        pdebug(DEBUG_INFO, "Destroying tag hashtable.");
        hashtable_destroy(tags);
        tags = NULL;
    }

    atomic_set(&library_terminating, 0);

    pdebug(DEBUG_INFO,"Done.");
}


int plc_tag_tickler_wake_impl(const char *func, int line_num)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_DETAIL, "Starting. Called from %s:%d.", func, line_num);

    if(!tag_tickler_wait) {
        pdebug(DEBUG_WARN, "Called from %s:%d when tag tickler condition var is NULL!", func, line_num);
        return PLCTAG_ERR_NULL_PTR;
    }

    rc = cond_signal(tag_tickler_wait);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Error %s trying to signal condition variable in call from %s:%d", plc_tag_decode_error(rc), func, line_num);
        return rc;
    }

    pdebug(DEBUG_DETAIL, "Done. Called from %s:%d.", func, line_num);

    return rc;
}



int plc_tag_generic_wake_tag_impl(const char *func, int line_num, plc_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_DETAIL, "Starting. Called from %s:%d.", func, line_num);

    if(!tag) {
        pdebug(DEBUG_WARN, "Called from %s:%d when tag is NULL!", func, line_num);
        return PLCTAG_ERR_NULL_PTR;
    }

    if(!tag->tag_cond_wait) {
        pdebug(DEBUG_WARN, "Called from %s:%d when tag condition var is NULL!", func, line_num);
        return PLCTAG_ERR_NULL_PTR;
    }

    rc = cond_signal(tag->tag_cond_wait);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Error %s trying to signal condition variable in call from %s:%d", plc_tag_decode_error(rc), func, line_num);
        return rc;
    }

    pdebug(DEBUG_DETAIL, "Done. Called from %s:%d.", func, line_num);

    return rc;
}



/*
 * plc_tag_generic_tickler
 *
 * This implements the protocol-independent tickling functions such as handling
 * automatic tag operations and callbacks.
 */

void plc_tag_generic_tickler(plc_tag_p tag)
{
    if(tag) {
        debug_set_tag_id(tag->tag_id);

        pdebug(DEBUG_DETAIL, "Tickling tag %d.", tag->tag_id);

        /* if this tag has automatic reads, we need to check that state too. */
        if(tag->auto_sync_read_ms > 0) {
            int64_t current_time = time_ms();

            // /* spread these out randomly to avoid too much clustering. */
            // if(tag->auto_sync_next_read == 0) {
            //     tag->auto_sync_next_read = current_time - (rand() % tag->auto_sync_read_ms);
            // }

            /* do we need to read? */
            if(tag->auto_sync_next_read < current_time) {
                /* make sure that we do not have an outstanding read. */
                if(!tag->read_in_flight) {
                    int64_t periods = 0;

                    pdebug(DEBUG_DETAIL, "Triggering automatic read start.");

                    tag->read_in_flight = 1;

                    if(tag->vtable->read) {
                        tag->status = (int8_t)tag->vtable->read(tag);
                    }

                    /*
                    * schedule the next read.
                    *
                    * Note that there will be some jitter.  In that case we want to skip
                    * to the next read time that is a whole multiple of the read period.
                    *
                    * This keeps the jitter from slowly moving the polling cycle.
                    *
                    * Round up to the next period.
                    */
                    periods = (current_time - tag->auto_sync_next_read + (tag->auto_sync_read_ms - 1))/tag->auto_sync_read_ms;

                    /* warn if we need to skip more than one period. */
                    if(periods > 1) {
                        pdebug(DEBUG_WARN, "Skipping %" PRId64 " periods of %" PRId32 "ms.", periods, tag->auto_sync_read_ms);
                    }

                    tag->auto_sync_next_read += (periods * tag->auto_sync_read_ms);
                    pdebug(DEBUG_DETAIL, "Scheduling next read at time %" PRId64 ".", tag->auto_sync_next_read);
                } else {
                    //pdebug(DEBUG_SPEW, "Unable to start read tag->read_in_flight=%d, tag->tag_is_dirty=%d, tag->write_in_flight=%d!", tag->read_in_flight, tag->tag_is_dirty, tag->write_in_flight);
                }
            }
        }
    } else {
        pdebug(DEBUG_WARN, "Called with null tag pointer!");
    }

    pdebug(DEBUG_DETAIL, "Done.");

    debug_set_tag_id(0);
}



int plc_tag_generic_init_tag(plc_tag_p tag, attr attribs)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting.");

    /* get the connection group ID here rather than in each PLC specific tag type. */
    tag->connection_group_id = attr_get_int(attribs, "connection_group_id", 0);
    if(tag->connection_group_id < 0 || tag->connection_group_id > 32767) {
        pdebug(DEBUG_WARN, "Connection group ID must be between 0 and 32767, inclusive, but was %d!", tag->connection_group_id);
        return PLCTAG_ERR_OUT_OF_BOUNDS;
    }

    rc = mutex_create(&(tag->ext_mutex));
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to create tag external mutex!");
        return PLCTAG_ERR_CREATE;
    }

    rc = mutex_create(&(tag->api_mutex));
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to create tag API mutex!");
        return PLCTAG_ERR_CREATE;
    }

    rc = cond_create(&(tag->tag_cond_wait));
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to create tag condition variable!");
        return PLCTAG_ERR_CREATE;
    }

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}





THREAD_FUNC(tag_tickler_func)
{
    (void)arg;

    debug_set_tag_id(0);

    pdebug(DEBUG_INFO, "Starting.");

    while(!atomic_get(&library_terminating)) {
        int max_index = 0;
        int64_t timeout_wait_ms = TAG_TICKLER_TIMEOUT_MS;

        /* what is the maximum time we will wait until */
        tag_tickler_wait_timeout_end = time_ms() + timeout_wait_ms;

        critical_block(tag_lookup_mutex) {
            max_index = hashtable_capacity(tags);
        }

        for(int i=0; i < max_index; i++) {
            plc_tag_p tag = NULL;

            critical_block(tag_lookup_mutex) {
                /* look up the max index again. it may have changed. */
                max_index = hashtable_capacity(tags);

                if(i < max_index) {
                    tag = (plc_tag_p)hashtable_get_index(tags, i);

                    if(tag) {
                        debug_set_tag_id(tag->tag_id);
                        tag = (plc_tag_p) rc_inc(tag);
                    }
                } else {
                    debug_set_tag_id(0);
                    tag = NULL;
                }
            }

            if(tag) {
                debug_set_tag_id(tag->tag_id);

                if(!tag->skip_tickler) {
                    pdebug(DEBUG_DETAIL, "Tickling tag %d.", tag->tag_id);

                    /* try to hold the tag API mutex while all this goes on. */
                    if(mutex_try_lock(tag->api_mutex) == PLCTAG_STATUS_OK) {
                        plc_tag_generic_tickler(tag);

                        /* call the tickler function if we can. */
                        if(tag->vtable->tickler) {
                            /* call the tickler on the tag. */
                            tag->vtable->tickler(tag);

                            if(tag->read_complete) {
                                tag->read_complete = 0;
                                tag->read_in_flight = 0;

                                /* wake immediately */
                                plc_tag_tickler_wake();
                                cond_signal(tag->tag_cond_wait);
                            }
                        }

                        /* wake up earlier if the time until the next read wake up is sooner. */
                        if(tag->auto_sync_next_read && tag->auto_sync_next_read < tag_tickler_wait_timeout_end) {
                            tag_tickler_wait_timeout_end = tag->auto_sync_next_read;
                        }

                        /* we are done with the tag API mutex now. */
                        mutex_unlock(tag->api_mutex);

                    } else {
                        pdebug(DEBUG_DETAIL, "Skipping tag as it is already locked.");
                    }

                } else {
                    pdebug(DEBUG_DETAIL, "Tag has its own tickler.");
                }

                // pdebug(DEBUG_DETAIL, "Current time %" PRId64 ".", time_ms());
                // pdebug(DEBUG_DETAIL, "Time to wake %" PRId64 ".", tag_tickler_wait_timeout_end);
                // pdebug(DEBUG_DETAIL, "Auto read time %" PRId64 ".", tag->auto_sync_next_read);
                // pdebug(DEBUG_DETAIL, "Auto write time %" PRId64 ".", tag->auto_sync_next_write);

                debug_set_tag_id(0);
            }

            if(tag) {
                rc_dec(tag);
            }

            debug_set_tag_id(0);
        }

        if(tag_tickler_wait) {
            int64_t time_to_wait = tag_tickler_wait_timeout_end - time_ms();
            int wait_rc = PLCTAG_STATUS_OK;

            if(time_to_wait < TAG_TICKLER_TIMEOUT_MIN_MS) {
                time_to_wait = TAG_TICKLER_TIMEOUT_MIN_MS;
            }

            if(time_to_wait > 0) {
                wait_rc = cond_wait(tag_tickler_wait, (int)time_to_wait);
                if(wait_rc == PLCTAG_ERR_TIMEOUT) {
                    pdebug(DEBUG_DETAIL, "Tag tickler thread timed out waiting for something to do.");
                }
            } else {
                pdebug(DEBUG_DETAIL, "Not waiting as time to wake is in the past.");
            }
        }
    }

    debug_set_tag_id(0);

    pdebug(DEBUG_INFO,"Terminating.");

    THREAD_RETURN(0);
}


/**************************************************************************
 ***************************  API Functions  ******************************
 **************************************************************************/


/*
 * plc_tag_decode_error()
 *
 * This takes an integer error value and turns it into a printable string.
 *
 * TODO - this should produce better errors than this!
 */



LIB_EXPORT const char *plc_tag_decode_error(int rc)
{
    switch(rc) {
    case PLCTAG_STATUS_PENDING:
        return "PLCTAG_STATUS_PENDING";
    case PLCTAG_STATUS_OK:
        return "PLCTAG_STATUS_OK";
    case PLCTAG_ERR_ABORT:
        return "PLCTAG_ERR_ABORT";
    case PLCTAG_ERR_BAD_CONFIG:
        return "PLCTAG_ERR_BAD_CONFIG";
    case PLCTAG_ERR_BAD_CONNECTION:
        return "PLCTAG_ERR_BAD_CONNECTION";
    case PLCTAG_ERR_BAD_DATA:
        return "PLCTAG_ERR_BAD_DATA";
    case PLCTAG_ERR_BAD_DEVICE:
        return "PLCTAG_ERR_BAD_DEVICE";
    case PLCTAG_ERR_BAD_GATEWAY:
        return "PLCTAG_ERR_BAD_GATEWAY";
    case PLCTAG_ERR_BAD_PARAM:
        return "PLCTAG_ERR_BAD_PARAM";
    case PLCTAG_ERR_BAD_REPLY:
        return "PLCTAG_ERR_BAD_REPLY";
    case PLCTAG_ERR_BAD_STATUS:
        return "PLCTAG_ERR_BAD_STATUS";
    case PLCTAG_ERR_CLOSE:
        return "PLCTAG_ERR_CLOSE";
    case PLCTAG_ERR_CREATE:
        return "PLCTAG_ERR_CREATE";
    case PLCTAG_ERR_DUPLICATE:
        return "PLCTAG_ERR_DUPLICATE";
    case PLCTAG_ERR_ENCODE:
        return "PLCTAG_ERR_ENCODE";
    case PLCTAG_ERR_MUTEX_DESTROY:
        return "PLCTAG_ERR_MUTEX_DESTROY";
    case PLCTAG_ERR_MUTEX_INIT:
        return "PLCTAG_ERR_MUTEX_INIT";
    case PLCTAG_ERR_MUTEX_LOCK:
        return "PLCTAG_ERR_MUTEX_LOCK";
    case PLCTAG_ERR_MUTEX_UNLOCK:
        return "PLCTAG_ERR_MUTEX_UNLOCK";
    case PLCTAG_ERR_NOT_ALLOWED:
        return "PLCTAG_ERR_NOT_ALLOWED";
    case PLCTAG_ERR_NOT_FOUND:
        return "PLCTAG_ERR_NOT_FOUND";
    case PLCTAG_ERR_NOT_IMPLEMENTED:
        return "PLCTAG_ERR_NOT_IMPLEMENTED";
    case PLCTAG_ERR_NO_DATA:
        return "PLCTAG_ERR_NO_DATA";
    case PLCTAG_ERR_NO_MATCH:
        return "PLCTAG_ERR_NO_MATCH";
    case PLCTAG_ERR_NO_MEM:
        return "PLCTAG_ERR_NO_MEM";
    case PLCTAG_ERR_NO_RESOURCES:
        return "PLCTAG_ERR_NO_RESOURCES";
    case PLCTAG_ERR_NULL_PTR:
        return "PLCTAG_ERR_NULL_PTR";
    case PLCTAG_ERR_OPEN:
        return "PLCTAG_ERR_OPEN";
    case PLCTAG_ERR_OUT_OF_BOUNDS:
        return "PLCTAG_ERR_OUT_OF_BOUNDS";
    case PLCTAG_ERR_READ:
        return "PLCTAG_ERR_READ";
    case PLCTAG_ERR_REMOTE_ERR:
        return "PLCTAG_ERR_REMOTE_ERR";
    case PLCTAG_ERR_THREAD_CREATE:
        return "PLCTAG_ERR_THREAD_CREATE";
    case PLCTAG_ERR_THREAD_JOIN:
        return "PLCTAG_ERR_THREAD_JOIN";
    case PLCTAG_ERR_TIMEOUT:
        return "PLCTAG_ERR_TIMEOUT";
    case PLCTAG_ERR_TOO_LARGE:
        return "PLCTAG_ERR_TOO_LARGE";
    case PLCTAG_ERR_TOO_SMALL:
        return "PLCTAG_ERR_TOO_SMALL";
    case PLCTAG_ERR_UNSUPPORTED:
        return "PLCTAG_ERR_UNSUPPORTED";
    case PLCTAG_ERR_WINSOCK:
        return "PLCTAG_ERR_WINSOCK";
    case PLCTAG_ERR_WRITE:
        return "PLCTAG_ERR_WRITE";
    case PLCTAG_ERR_PARTIAL:
        return "PLCTAG_ERR_PARTIAL";
    case PLCTAG_ERR_BUSY:
        return "PLCTAG_ERR_BUSY";

    default:
        return "Unknown error.";
        break;
    }

    return "Unknown error.";
}





/*
 * Set the debug level.
 *
 * This function takes values from the defined debug levels.  It sets
 * the debug level to the passed value.  Higher numbers output increasing amounts
 * of information.   Input values not defined will be ignored.
 */

LIB_EXPORT void plc_tag_set_debug_level(int debug_level)
{
	if (debug_level >= PLCTAG_DEBUG_NONE && debug_level <= PLCTAG_DEBUG_SPEW) {
		set_debug_level(debug_level);
	}
}




/*
 * Check that the library supports the required API version.
 *
 * PLCTAG_STATUS_OK is returned if the version matches.  If it does not,
 * PLCTAG_ERR_UNSUPPORTED is returned.
 */

LIB_EXPORT int plc_tag_check_lib_version(int req_major, int req_minor, int req_patch)
{
    /* encode these with 16-bits per version part. */
    uint64_t lib_encoded_version = (((uint64_t)version_major) << 32u)
                                 + (((uint64_t)version_minor) << 16u)
                                   + (uint64_t)version_patch;

    uint64_t req_encoded_version = (((uint64_t)req_major) << 32u)
                                 + (((uint64_t)req_minor) << 16u)
                                   + (uint64_t)req_patch;

    if(version_major == (uint64_t)req_major && lib_encoded_version >= req_encoded_version) {
        return PLCTAG_STATUS_OK;
    } else {
        return PLCTAG_ERR_UNSUPPORTED;
    }
}






/*
 * plc_tag_create()
 *
 * This is where the dispatch occurs to the protocol specific implementation.
 */


LIB_EXPORT int32_t plc_tag_create(const char* attrib_str, int timeout)
{
    plc_tag_p tag = PLC_TAG_P_NULL;
    int id = PLCTAG_ERR_OUT_OF_BOUNDS;
    attr attribs = NULL;
    int rc = PLCTAG_STATUS_OK;
    int read_cache_ms = 0;
	int debug_level = -1;

    /* we are creating a tag, there is no ID yet. */
    debug_set_tag_id(0);

    pdebug(DEBUG_INFO,"Starting");

    /* check to see if the library is terminating. */
    if(atomic_get(&library_terminating)) {
        pdebug(DEBUG_WARN, "The plctag library is in the process of shutting down!");
        return PLCTAG_ERR_NOT_ALLOWED;
    }

    /* make sure that all modules are initialized. */
    if((rc = initialize_modules()) != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR,"Unable to initialize the internal library state!");
        return rc;
    }

    /* check the arguments */

    if(timeout < 0) {
        pdebug(DEBUG_WARN, "Timeout must not be negative!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    if(!attrib_str || str_length(attrib_str) == 0) {
        pdebug(DEBUG_WARN,"Tag attribute string is null or zero length!");
        return PLCTAG_ERR_TOO_SMALL;
    }

    attribs = attr_create_from_str(attrib_str);
    if(!attribs) {
        pdebug(DEBUG_WARN,"Unable to parse attribute string!");
        return PLCTAG_ERR_BAD_DATA;
    }

    /* set debug level */
	debug_level = attr_get_int(attribs, "debug", -1);
	if (debug_level > DEBUG_NONE) {
		set_debug_level(debug_level);
	}

    tag = ab_tag_create(attribs);

    if(!tag) {
        pdebug(DEBUG_WARN, "Tag creation failed, skipping mutex creation and other generic setup.");
        attr_destroy(attribs);
        return PLCTAG_ERR_CREATE;
    }

    if(tag->status != PLCTAG_STATUS_OK && tag->status != PLCTAG_STATUS_PENDING) {
        int tag_status = tag->status;

        pdebug(DEBUG_WARN, "Warning, %s error found while creating tag!", plc_tag_decode_error(tag_status));

        attr_destroy(attribs);
        rc_dec(tag);

        return tag_status;
    }

    /* set up the read cache config. */
    read_cache_ms = attr_get_int(attribs,"read_cache_ms",0);
    if(read_cache_ms < 0) {
        pdebug(DEBUG_WARN, "read_cache_ms value must be positive, using zero.");
        read_cache_ms = 0;
    }

    tag->read_cache_expire = (int64_t)0;
    tag->read_cache_ms = (int64_t)read_cache_ms;

    /* set up any automatic read/write */
    tag->auto_sync_read_ms = attr_get_int(attribs, "auto_sync_read_ms", 0);
    if(tag->auto_sync_read_ms < 0) {
        pdebug(DEBUG_WARN, "auto_sync_read_ms value must be positive!");
        attr_destroy(attribs);
        rc_dec(tag);
        return PLCTAG_ERR_BAD_PARAM;
    } else if(tag->auto_sync_read_ms > 0) {
        /* how many periods did we already pass? */
        // int64_t periods = (time_ms() / tag->auto_sync_read_ms);
        // tag->auto_sync_next_read = (periods + 1) * tag->auto_sync_read_ms;
        /* start some time in the future, but with random jitter. */
        tag->auto_sync_next_read = time_ms() + (rand() % tag->auto_sync_read_ms);
    }

    /* set up the tag byte order if there are any overrides. */
    rc = set_tag_byte_order(tag, attribs);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to correctly set tag data byte order: %s!", plc_tag_decode_error(rc));
        attr_destroy(attribs);
        rc_dec(tag);
        return rc;
    }

    /*
     * Release memory for attributes
     */
    attr_destroy(attribs);

    /* map the tag to a tag ID */
    id = add_tag_lookup(tag);

    /* if the mapping failed, then punt */
    if(id < 0) {
        pdebug(DEBUG_ERROR, "Unable to map tag %p to lookup table entry, rc=%s", tag, plc_tag_decode_error(id));
        rc_dec(tag);
        return id;
    }

    /* save this for later. */
    tag->tag_id = id;

    debug_set_tag_id(id);

    pdebug(DEBUG_INFO, "Returning mapped tag ID %d", id);

    /* get the tag status. */
    rc = tag->vtable->status(tag);

    /* check to see if there was an error during tag creation. */
    if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
        pdebug(DEBUG_WARN, "Error %s while trying to create tag!", plc_tag_decode_error(rc));
        if(tag->vtable->abort) {
            tag->vtable->abort(tag);
        }

        /* remove the tag from the hashtable. */
        critical_block(tag_lookup_mutex) {
            hashtable_remove(tags, (int64_t)tag->tag_id);
        }

        rc_dec(tag);
        return rc;
    }

    pdebug(DEBUG_DETAIL, "Tag status after creation is %s.", plc_tag_decode_error(rc));

    /*
    * if there is a timeout, then wait until we get
    * an error or we timeout.
    */
    if(timeout > 0 && rc == PLCTAG_STATUS_PENDING) {
        int64_t start_time = time_ms();
        int64_t end_time = start_time + timeout;

        /* wake up the tickler in case it is needed to create the tag. */
        plc_tag_tickler_wake();

        /* we loop as long as we have time left to wait. */
        do {
            int64_t timeout_left = end_time - time_ms();

            /* clamp the timeout left to non-negative int range. */
            if(timeout_left < 0) {
                timeout_left = 0;
            }

            if(timeout_left > INT_MAX) {
                timeout_left = 100; /* MAGIC, only wait 100ms in this weird case. */
            }

            /* wait for something to happen */
            rc = cond_wait(tag->tag_cond_wait, (int)timeout_left);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Error %s while waiting for tag creation to complete!", plc_tag_decode_error(rc));
                if(tag->vtable->abort) {
                    tag->vtable->abort(tag);
                }

                /* remove the tag from the hashtable. */
                critical_block(tag_lookup_mutex) {
                    hashtable_remove(tags, (int64_t)tag->tag_id);
                }

                rc_dec(tag);
                return rc;
            }

            /* get the tag status. */
            rc = tag->vtable->status(tag);

            /* check to see if there was an error during tag creation. */
            if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
                pdebug(DEBUG_WARN, "Error %s while trying to create tag!", plc_tag_decode_error(rc));
                if(tag->vtable->abort) {
                    tag->vtable->abort(tag);
                }

                /* remove the tag from the hashtable. */
                critical_block(tag_lookup_mutex) {
                    hashtable_remove(tags, (int64_t)tag->tag_id);
                }

                rc_dec(tag);
                return rc;
            }
        } while(rc == PLCTAG_STATUS_PENDING && time_ms() > end_time);

        /* clear up any remaining flags.  This should be refactored. */
        tag->read_in_flight = 0;

        pdebug(DEBUG_INFO,"tag set up elapsed time %" PRId64 "ms",(time_ms()-start_time));
    }

    pdebug(DEBUG_INFO,"Done.");

    return id;
}




/*
 * plc_tag_shutdown
 *
 * Some systems may not be able to call atexit() handlers.  In those cases, wrappers should
 * call this function before unloading the library or terminating.   Most OSes will cleanly
 * recover all system resources when a process is terminated and this will not be necessary.
 */

LIB_EXPORT void plc_tag_shutdown(void)
{
    int tag_table_entries = 0;

    pdebug(DEBUG_INFO, "Starting.");

    /* terminate anything waiting on the library and prevent any tags from being created. */
    atomic_set(&library_terminating, 1);

    /* close all tags. */
    pdebug(DEBUG_DETAIL, "Closing all tags.");

    critical_block(tag_lookup_mutex) {
        tag_table_entries = hashtable_capacity(tags);
    }

    for(int i=0; i<tag_table_entries; i++) {
        plc_tag_p tag = NULL;

        critical_block(tag_lookup_mutex) {
            tag_table_entries = hashtable_capacity(tags);

            if(i<tag_table_entries && tag_table_entries >= 0) {
                tag = (plc_tag_p)hashtable_get_index(tags, i);

                /* make sure the tag does not go away while we are using the pointer. */
                if(tag) {
                    /* this returns NULL if the existing ref-count is zero. */
                    tag = (plc_tag_p) rc_inc(tag);
                }
            }
        }

        /* do this outside the mutex. */
        if(tag) {
            debug_set_tag_id(tag->tag_id);
            pdebug(DEBUG_DETAIL, "Destroying tag %" PRId32 ".", tag->tag_id);
            plc_tag_destroy(tag->tag_id);
            rc_dec(tag);
        }
    }

    pdebug(DEBUG_DETAIL, "All tags closed.");

    pdebug(DEBUG_DETAIL, "Cleaning up library resources.");

    destroy_modules();

    /* Clear the termination flag in case we want to start up again. */
    atomic_set(&library_terminating, 0);

    pdebug(DEBUG_INFO, "Done.");
}




/*
 * plc_tag_abort()
 *
 * This function calls through the vtable in the passed tag to call
 * the protocol-specific implementation.
 *
 * The implementation must do whatever is necessary to abort any
 * ongoing IO.
 *
 * The status of the operation is returned.
 */

int plc_tag_abort(int32_t id)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_INFO, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    critical_block(tag->api_mutex) {
        /* who knows what state the tag data is in.  */
        tag->read_cache_expire = (uint64_t)0;

        if(!tag->vtable || !tag->vtable->abort) {
            pdebug(DEBUG_WARN,"Tag does not have a abort function!");
            rc = PLCTAG_ERR_NOT_IMPLEMENTED;
            break;
        }

        /* this may be synchronous. */
        rc = tag->vtable->abort(tag);

        tag->read_in_flight = 0;
        tag->read_complete = 0;
    }

    /* release the kraken... or tickler */
    plc_tag_tickler_wake();

    rc_dec(tag);

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}




/*
 * plc_tag_destroy()
 *
 * Remove all implementation specific details about a tag and clear its
 * memory.
 */


LIB_EXPORT int plc_tag_destroy(int32_t tag_id)
{
    plc_tag_p tag = NULL;

    debug_set_tag_id((int)tag_id);

    pdebug(DEBUG_INFO, "Starting.");

    if(tag_id <= 0 || tag_id >= TAG_ID_MASK) {
        pdebug(DEBUG_WARN, "Called with zero or invalid tag!");
        return PLCTAG_ERR_NULL_PTR;
    }

    critical_block(tag_lookup_mutex) {
        tag = (plc_tag_p)hashtable_remove(tags, tag_id);
    }

    if(!tag) {
        pdebug(DEBUG_WARN, "Called with non-existent tag!");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* abort anything in flight */
    pdebug(DEBUG_DETAIL, "Aborting any in-flight operations.");

    critical_block(tag->api_mutex) {
        if(!tag->vtable || !tag->vtable->abort) {
            pdebug(DEBUG_WARN,"Tag does not have a abort function!");
        } else {
            /* Force a clean up. */
            tag->vtable->abort(tag);
        }
    }

    /* wake the tickler */
    plc_tag_tickler_wake();

    /* release the reference outside the mutex. */
    rc_dec(tag);

    pdebug(DEBUG_INFO, "Done.");

    debug_set_tag_id(0);

    return PLCTAG_STATUS_OK;
}





/*
 * plc_tag_read()
 *
 * This function calls through the vtable in the passed tag to call
 * the protocol-specific implementation.  That starts the read operation.
 * If there is a timeout passed, then this routine waits for either
 * a timeout or an error.
 *
 * The status of the operation is returned.
 */

LIB_EXPORT int plc_tag_read(int32_t id, int timeout)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);
    int is_done = 0;

    pdebug(DEBUG_INFO, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    if(timeout < 0) {
        pdebug(DEBUG_WARN, "Timeout must not be negative!");
        rc_dec(tag);
        return PLCTAG_ERR_BAD_PARAM;
    }

    critical_block(tag->api_mutex) {

        /* check read cache, if not expired, return existing data. */
        if(tag->read_cache_expire > time_ms()) {
            pdebug(DEBUG_INFO, "Returning cached data.");
            rc = PLCTAG_STATUS_OK;
            is_done = 1;
            break;
        }

        if(tag->read_in_flight) {
            pdebug(DEBUG_WARN, "An operation is already in flight!");
            rc = PLCTAG_ERR_BUSY;
            is_done = 1;
            break;
        }

        tag->read_in_flight = 1;
        tag->status = PLCTAG_STATUS_PENDING;

        /* clear the condition var */
        cond_clear(tag->tag_cond_wait);

        /* the protocol implementation does not do the timeout. */
        rc = tag->vtable->read(tag);

        /* if not pending then check for success or error. */
        if(rc != PLCTAG_STATUS_PENDING) {
            if(rc != PLCTAG_STATUS_OK) {
                /* not pending and not OK, so error. Abort and clean up. */

                pdebug(DEBUG_WARN,"Response from read command returned error %s!", plc_tag_decode_error(rc));

                if(tag->vtable->abort) {
                    tag->vtable->abort(tag);
                }
            }

            tag->read_in_flight = 0;
            is_done = 1;
            break;
        }
    }

    /*
     * if there is a timeout, then wait until we get
     * an error or we timeout.
     */
    if(!is_done && timeout > 0) {
        int64_t start_time = time_ms();
        int64_t end_time = start_time + timeout;

        /* wake up the tickler in case it is needed to read the tag. */
        plc_tag_tickler_wake();

        /* we loop as long as we have time left to wait. */
        do {
            int64_t timeout_left = end_time - time_ms();

            /* clamp the timeout left to non-negative int range. */
            if(timeout_left < 0) {
                timeout_left = 0;
            }

            if(timeout_left > INT_MAX) {
                timeout_left = 100; /* MAGIC, only wait 100ms in this weird case. */
            }

            /* wait for something to happen */
            rc = cond_wait(tag->tag_cond_wait, (int)timeout_left);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Error %s while waiting for tag read to complete!", plc_tag_decode_error(rc));
                plc_tag_abort(id);

                break;
            }

            /* get the tag status. */
            rc = plc_tag_status(id);

            /* check to see if there was an error during tag read. */
            if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
                pdebug(DEBUG_WARN, "Error %s while trying to read tag!", plc_tag_decode_error(rc));
                plc_tag_abort(id);
            }
        } while(rc == PLCTAG_STATUS_PENDING && time_ms() < end_time);

        /* the read is not in flight anymore. */
        critical_block(tag->api_mutex) {
            tag->read_in_flight = 0;
            tag->read_complete = 0;
            is_done = 1;
        }

        pdebug(DEBUG_INFO,"elapsed time %" PRId64 "ms", (time_ms()-start_time));
    }

    if(rc == PLCTAG_STATUS_OK) {
        /* set up the cache time.  This works when read_cache_ms is zero as it is already expired. */
        tag->read_cache_expire = time_ms() + tag->read_cache_ms;
    }

    rc_dec(tag);

    pdebug(DEBUG_INFO, "Done");

    return rc;
}





/*
 * plc_tag_status
 *
 * Return the current status of the tag.  This will be PLCTAG_STATUS_PENDING if there is
 * an uncompleted IO operation.  It will be PLCTAG_STATUS_OK if everything is fine.  Other
 * errors will be returned as appropriate.
 *
 * This is a function provided by the underlying protocol implementation.
 */

LIB_EXPORT int plc_tag_status(int32_t id)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    /* check the ID.  It might be an error status from creating the tag. */
    if(!tag) {
        if(id < 0) {
            pdebug(DEBUG_WARN, "Called with an error status %s!", plc_tag_decode_error(id));
            return id;
        } else {
            pdebug(DEBUG_WARN,"Tag not found.");
            return PLCTAG_ERR_NOT_FOUND;
        }
    }

    critical_block(tag->api_mutex) {
        if(tag && tag->vtable->tickler) {
            tag->vtable->tickler(tag);
        }

        rc = tag->vtable->status(tag);

        if(rc == PLCTAG_STATUS_OK) {
            if(tag->read_in_flight) {
                rc = PLCTAG_STATUS_PENDING;
            }
        }
    }

    rc_dec(tag);

    pdebug(DEBUG_SPEW, "Done with rc=%s.", plc_tag_decode_error(rc));

    return rc;
}


/*
 * Tag data accessors.
 */




LIB_EXPORT int plc_tag_get_int_attribute(int32_t id, const char *attrib_name, int default_value)
{
    int res = default_value;
    plc_tag_p tag = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    /* FIXME - this should set the tag status if there is a tag. */
    if(!attrib_name || str_length(attrib_name) == 0) {
        pdebug(DEBUG_WARN, "Attribute name must not be null or zero-length!");
        return default_value;
    }

    /* get library attributes */
    if(id == 0) {
        if(str_cmp_i(attrib_name, "version_major") == 0) {
            res = (int)version_major;
        } else if(str_cmp_i(attrib_name, "version_minor") == 0) {
            res = (int)version_minor;
        } else if(str_cmp_i(attrib_name, "version_patch") == 0) {
            res = (int)version_patch;
        } else if(str_cmp_i(attrib_name, "debug") == 0) {
            res = (int)get_debug_level();
        } else if(str_cmp_i(attrib_name, "debug_level") == 0) {
            pdebug(DEBUG_WARN, "Deprecated attribute \"debug_level\" used, use \"debug\" instead.");
            res = (int)get_debug_level();
        } else {
            pdebug(DEBUG_WARN, "Attribute \"%s\" is not supported at the library level!");
            res = default_value;
        }
    } else {
        tag = lookup_tag(id);

        if(!tag) {
            pdebug(DEBUG_WARN,"Tag not found.");
            return default_value;
        }

        critical_block(tag->api_mutex) {
            /* match the generic ones first. */
            if(str_cmp_i(attrib_name, "size") == 0) {
                tag->status = PLCTAG_STATUS_OK;
                res = (int)tag->size;
            } else if(str_cmp_i(attrib_name, "read_cache_ms") == 0) {
                /* FIXME - what happens if this overflows? */
                tag->status = PLCTAG_STATUS_OK;
                res = (int)tag->read_cache_ms;
            } else if(str_cmp_i(attrib_name, "auto_sync_read_ms") == 0) {
                tag->status = PLCTAG_STATUS_OK;
                res = (int)tag->auto_sync_read_ms;
            } else if(str_cmp_i(attrib_name, "bit_num") == 0) {
                tag->status = PLCTAG_STATUS_OK;
                res = (int)(unsigned int)(tag->bit);
            } else if(str_cmp_i(attrib_name, "connection_group_id") == 0) {
                pdebug(DEBUG_DETAIL, "Getting the connection_group_id for tag %" PRId32 ".", id);
                tag->status = PLCTAG_STATUS_OK;
                res = tag->connection_group_id;
            } else {
                if(tag->vtable->get_int_attrib) {
                    res = tag->vtable->get_int_attrib(tag, attrib_name, default_value);
                }
            }
        }

        rc_dec(tag);
    }

    pdebug(DEBUG_SPEW, "Done.");

    return res;
}



LIB_EXPORT int plc_tag_set_int_attribute(int32_t id, const char *attrib_name, int new_value)
{
    int res = PLCTAG_ERR_NOT_FOUND;
    plc_tag_p tag = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!attrib_name || str_length(attrib_name) == 0) {
        pdebug(DEBUG_WARN, "Attribute name must not be null or zero-length!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* get library attributes */
    if(id == 0) {
        if(str_cmp_i(attrib_name, "debug") == 0) {
            if(new_value >= DEBUG_ERROR && new_value < DEBUG_SPEW) {
                set_debug_level(new_value);
                res = PLCTAG_STATUS_OK;
            } else {
                res = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        } else if(str_cmp_i(attrib_name, "debug_level") == 0) {
            pdebug(DEBUG_WARN, "Deprecated attribute \"debug_level\" used, use \"debug\" instead.");
            if(new_value >= DEBUG_ERROR && new_value < DEBUG_SPEW) {
                set_debug_level(new_value);
                res = PLCTAG_STATUS_OK;
            } else {
                res = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        } else {
            pdebug(DEBUG_WARN, "Attribute \"%s\" is not support at the library level!", attrib_name);
            return PLCTAG_ERR_UNSUPPORTED;
        }
    } else {
        tag = lookup_tag(id);

        if(!tag) {
            pdebug(DEBUG_WARN,"Tag not found.");
            return PLCTAG_ERR_NOT_FOUND;
        }

        critical_block(tag->api_mutex) {
            /* match the generic ones first. */
            if(str_cmp_i(attrib_name, "read_cache_ms") == 0) {
                if(new_value >= 0) {
                    /* expire the cache. */
                    tag->read_cache_expire = (int64_t)0;
                    tag->read_cache_ms = (int64_t)new_value;
                    tag->status = PLCTAG_STATUS_OK;
                    res = PLCTAG_STATUS_OK;
                } else {
                    tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                    res = PLCTAG_ERR_OUT_OF_BOUNDS;
                }
            } else if(str_cmp_i(attrib_name, "auto_sync_read_ms") == 0) {
                if(new_value >= 0) {
                    tag->auto_sync_read_ms = new_value;
                    tag->status = PLCTAG_STATUS_OK;
                    res = PLCTAG_STATUS_OK;
                } else {
                    pdebug(DEBUG_WARN, "auto_sync_read_ms must be greater than or equal to zero!");
                    tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                    res = PLCTAG_ERR_OUT_OF_BOUNDS;
                }
            } else {
                if(tag->vtable->set_int_attrib) {
                    res = tag->vtable->set_int_attrib(tag, attrib_name, new_value);
                    tag->status = (int8_t)res;
                }
            }
        }
    }

    rc_dec(tag);

    pdebug(DEBUG_SPEW, "Done.");

    return res;
}




LIB_EXPORT int plc_tag_get_size(int32_t id)
{
    int result = 0;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    critical_block(tag->api_mutex) {
        result = tag->size;
        tag->status = PLCTAG_STATUS_OK;
    }

    rc_dec(tag);

    pdebug(DEBUG_SPEW, "Done.");

    return result;
}



LIB_EXPORT int plc_tag_set_size(int32_t id, int new_size)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_DETAIL, "Starting with new size %d.", new_size);

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    if(new_size < 0) {
        pdebug(DEBUG_WARN, "Illegal new size %d bytes for tag is illegal.  Tag size must be positive.");
        rc_dec(tag);
        return PLCTAG_ERR_BAD_PARAM;
    }

    critical_block(tag->api_mutex) {
        uint8_t *new_data = (uint8_t*)mem_realloc(tag->data, new_size);

        if(new_data) {
            /* return the old size */
            rc = tag->size;
            tag->data = new_data;
            tag->size = new_size;
            tag->status = PLCTAG_STATUS_OK;
        } else {
            rc = (int)PLCTAG_ERR_NO_MEM;
            tag->status = (int8_t)rc;
        }
    }

    rc_dec(tag);

    if(rc >= 0) {
        pdebug(DEBUG_DETAIL, "Done with old size %d.", rc);
    } else {
        pdebug(DEBUG_WARN, "Tag buffer resize failed with error %s!", plc_tag_decode_error(rc));
    }

    return rc;
}





LIB_EXPORT int plc_tag_get_bit(int32_t id, int offset_bit)
{
    int res = PLCTAG_ERR_OUT_OF_BOUNDS;
    int real_offset = offset_bit;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN, "Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    /* if this is a single bit, then make sure the offset is the tag bit. */
    if(tag->is_bit) {
        real_offset = tag->bit;
    } else {
        real_offset = offset_bit;
    }

    pdebug(DEBUG_SPEW, "selecting bit %d with offset %d in byte %d (%x).", real_offset, (real_offset % 8), (real_offset / 8), tag->data[real_offset / 8]);

    critical_block(tag->api_mutex) {
        if((real_offset >= 0) && ((real_offset / 8) < tag->size)) {
            res = !!(((1 << (real_offset % 8)) & 0xFF) & (tag->data[real_offset / 8]));
            tag->status = PLCTAG_STATUS_OK;
        } else {
            pdebug(DEBUG_WARN, "Data offset out of bounds!");
            res = PLCTAG_ERR_OUT_OF_BOUNDS;
            tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
        }
    }

    rc_dec(tag);

    return res;
}



LIB_EXPORT uint64_t plc_tag_get_uint64(int32_t id, int offset)
{
    uint64_t res = UINT64_MAX;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(uint64_t)) <= tag->size)) {
                res =   ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[0]]) << 0 ) +
                        ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[1]]) << 8 ) +
                        ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[2]]) << 16) +
                        ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[3]]) << 24) +
                        ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[4]]) << 32) +
                        ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[5]]) << 40) +
                        ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[6]]) << 48) +
                        ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[7]]) << 56);

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = (unsigned int)rc;
        }
    }

    rc_dec(tag);

    return res;
}


LIB_EXPORT int64_t plc_tag_get_int64(int32_t id, int offset)
{
    int64_t res = INT64_MIN;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(int64_t)) <= tag->size)) {
                res = (int64_t)(((uint64_t)(tag->data[offset + tag->byte_order->int64_order[0]]) << 0 ) +
                                ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[1]]) << 8 ) +
                                ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[2]]) << 16) +
                                ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[3]]) << 24) +
                                ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[4]]) << 32) +
                                ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[5]]) << 40) +
                                ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[6]]) << 48) +
                                ((uint64_t)(tag->data[offset + tag->byte_order->int64_order[7]]) << 56));

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = rc;
        }
    }

    rc_dec(tag);

    return res;
}


LIB_EXPORT uint32_t plc_tag_get_uint32(int32_t id, int offset)
{
    uint32_t res = UINT32_MAX;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(uint32_t)) <= tag->size)) {
                res =   ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[0]]) << 0 ) +
                        ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[1]]) << 8 ) +
                        ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[2]]) << 16) +
                        ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[3]]) << 24);

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = (unsigned int)rc;
        }
    }

    rc_dec(tag);

    return res;
}


LIB_EXPORT int32_t  plc_tag_get_int32(int32_t id, int offset)
{
    int32_t res = INT32_MIN;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(int32_t)) <= tag->size)) {
                res = (int32_t)(((uint32_t)(tag->data[offset + tag->byte_order->int32_order[0]]) << 0 ) +
                                ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[1]]) << 8 ) +
                                ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[2]]) << 16) +
                                ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[3]]) << 24));

                tag->status = PLCTAG_STATUS_OK;
            }  else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = (int32_t)rc;
        }
    }

    rc_dec(tag);

    return res;
}


LIB_EXPORT uint16_t plc_tag_get_uint16(int32_t id, int offset)
{
    uint16_t res = UINT16_MAX;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(uint16_t)) <= tag->size)) {
                res =   (uint16_t)(((uint16_t)(tag->data[offset + tag->byte_order->int16_order[0]]) << 0 ) +
                                   ((uint16_t)(tag->data[offset + tag->byte_order->int16_order[1]]) << 8 ));

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = (uint16_t)(unsigned int)rc;
        }
    }

    rc_dec(tag);

    return res;
}


LIB_EXPORT int16_t  plc_tag_get_int16(int32_t id, int offset)
{
    int16_t res = INT16_MIN;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(int16_t)) <= tag->size)) {
                res =   (int16_t)(uint16_t)(((uint16_t)(tag->data[offset + tag->byte_order->int16_order[0]]) << 0 ) +
                                            ((uint16_t)(tag->data[offset + tag->byte_order->int16_order[1]]) << 8 ));
                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = (int16_t)(uint16_t)(unsigned int)rc;
        }
    }

    rc_dec(tag);

    return res;
}


LIB_EXPORT uint8_t plc_tag_get_uint8(int32_t id, int offset)
{
    uint8_t res = UINT8_MAX;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(uint8_t)) <= tag->size)) {
                res = tag->data[offset];
                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = (uint8_t)(unsigned int)rc;
        }
    }

    rc_dec(tag);

    return res;
}


LIB_EXPORT int8_t plc_tag_get_int8(int32_t id, int offset)
{
    int8_t res = INT8_MIN;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && (offset + ((int)sizeof(uint8_t)) <= tag->size)) {
                res =   (int8_t)tag->data[offset];
                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        int rc = plc_tag_get_bit(id, tag->bit);

        /* make sure the response is good. */
        if(rc >= 0) {
            res = (int8_t)(unsigned int)rc;
        }
    }

    rc_dec(tag);

    return res;
}


LIB_EXPORT double plc_tag_get_float64(int32_t id, int offset)
{
    double res = DBL_MIN;
    int rc = PLCTAG_STATUS_OK;
    uint64_t ures = 0;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(tag->is_bit) {
        pdebug(DEBUG_WARN, "Getting float64 value is unsupported on a bit tag!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        rc_dec(tag);
        return res;
    }

    critical_block(tag->api_mutex) {
        if((offset >= 0) && (offset + ((int)sizeof(double)) <= tag->size)) {
            ures =  ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[0]]) << 0 ) +
                    ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[1]]) << 8 ) +
                    ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[2]]) << 16) +
                    ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[3]]) << 24) +
                    ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[4]]) << 32) +
                    ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[5]]) << 40) +
                    ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[6]]) << 48) +
                    ((uint64_t)(tag->data[offset + tag->byte_order->float64_order[7]]) << 56);

            tag->status = PLCTAG_STATUS_OK;
            rc = PLCTAG_STATUS_OK;
        } else {
            pdebug(DEBUG_WARN, "Data offset out of bounds!");
            tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            rc = PLCTAG_ERR_OUT_OF_BOUNDS;
        }
    }

    if(rc == PLCTAG_STATUS_OK) {
        /* copy the data */
        mem_copy(&res,&ures,sizeof(res));
    } else {
        res = DBL_MIN;
    }

    rc_dec(tag);

    return res;
}


LIB_EXPORT float plc_tag_get_float32(int32_t id, int offset)
{
    float res = FLT_MIN;
    int rc = PLCTAG_STATUS_OK;
    uint32_t ures = 0;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return res;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return res;
    }

    if(tag->is_bit) {
        pdebug(DEBUG_WARN, "Getting float32 value is unsupported on a bit tag!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        rc_dec(tag);
        return res;
    }

    critical_block(tag->api_mutex) {
        if((offset >= 0) && (offset + ((int)sizeof(float)) <= tag->size)) {
            ures =  (uint32_t)(((uint32_t)(tag->data[offset + tag->byte_order->float32_order[0]]) << 0 ) +
                               ((uint32_t)(tag->data[offset + tag->byte_order->float32_order[1]]) << 8 ) +
                               ((uint32_t)(tag->data[offset + tag->byte_order->float32_order[2]]) << 16) +
                               ((uint32_t)(tag->data[offset + tag->byte_order->float32_order[3]]) << 24));

            tag->status = PLCTAG_STATUS_OK;
            rc = PLCTAG_STATUS_OK;
        } else {
            pdebug(DEBUG_WARN, "Data offset out of bounds!");
            tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            rc = PLCTAG_ERR_OUT_OF_BOUNDS;
        }
    }

    if(rc == PLCTAG_STATUS_OK) {
        /* copy the data */
        mem_copy(&res,&ures,sizeof(res));
    } else {
        res = FLT_MIN;
    }

    rc_dec(tag);

    return res;
}


LIB_EXPORT int plc_tag_get_string(int32_t tag_id, int string_start_offset, char *buffer, int buffer_length)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(tag_id);
    int max_len = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* are strings defined for this tag? */
    if(!tag->byte_order || !tag->byte_order->str_is_defined) {
        pdebug(DEBUG_WARN,"Tag has no definitions for strings!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        rc_dec(tag);
        return PLCTAG_ERR_UNSUPPORTED;
    }

    /* is there data? */
    if(!tag->data) {
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        rc_dec(tag);
        return PLCTAG_ERR_NO_DATA;
    }

    if(tag->is_bit) {
        pdebug(DEBUG_WARN, "Getting a string value from a bit tag is not supported!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        rc_dec(tag);
        return PLCTAG_ERR_UNSUPPORTED;
    }

    /* set all buffer bytes to zero. */
    mem_set(buffer, 0, buffer_length);

    critical_block(tag->api_mutex) {
        int string_length = get_string_length_unsafe(tag, string_start_offset);

        /* determine the maximum number of characters/bytes to copy. */
        if(buffer_length < string_length) {
            pdebug(DEBUG_WARN, "Buffer length, %d, is less than the string length, %d!", buffer_length, string_length);
            max_len = buffer_length;
        } else {
            max_len = string_length;
        }

        /* check the amount of space. */
        if(string_start_offset + (int)tag->byte_order->str_count_word_bytes + max_len <= tag->size) {
            for(int i = 0; i < max_len; i++) {
                size_t char_index = (((size_t)(unsigned int)i) ^ (tag->byte_order->str_is_byte_swapped)) /* byte swap if necessary */
                                  + (size_t)(unsigned int)string_start_offset
                                  + (size_t)(unsigned int)(tag->byte_order->str_count_word_bytes);
                buffer[i] = (char)tag->data[char_index];
            }

            tag->status = PLCTAG_STATUS_OK;
            rc = PLCTAG_STATUS_OK;
        } else {
            pdebug(DEBUG_WARN, "Data offset out of bounds!");
            tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
            rc = PLCTAG_ERR_OUT_OF_BOUNDS;
        }
    }

    rc_dec(tag);

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}


LIB_EXPORT int plc_tag_get_string_capacity(int32_t id, int string_start_offset)
{
    int string_capacity = 0;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* are strings defined for this tag? */
    if(!tag->byte_order || !tag->byte_order->str_is_defined) {
        rc_dec(tag);
        pdebug(DEBUG_WARN,"Tag has no definitions for strings!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        return PLCTAG_ERR_UNSUPPORTED;
    }

    /* is there data? */
    if(!tag->data) {
        rc_dec(tag);
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        return PLCTAG_ERR_NO_DATA;
    }

    if(tag->is_bit) {
        rc_dec(tag);
        pdebug(DEBUG_WARN, "Getting string capacity from a bit tag is not supported!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        return PLCTAG_ERR_UNSUPPORTED;
    }

    critical_block(tag->api_mutex) {
        string_capacity = (tag->byte_order->str_max_capacity ? (int)(tag->byte_order->str_max_capacity) : get_string_length_unsafe(tag, string_start_offset));
    }

    rc_dec(tag);

    pdebug(DEBUG_SPEW, "Done.");

    return string_capacity;
}



LIB_EXPORT int plc_tag_get_string_length(int32_t id, int string_start_offset)
{
    int string_length = 0;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* are strings defined for this tag? */
    if(!tag->byte_order || !tag->byte_order->str_is_defined) {
        rc_dec(tag);
        pdebug(DEBUG_WARN,"Tag has no definitions for strings!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        return PLCTAG_ERR_UNSUPPORTED;
    }

    /* is there data? */
    if(!tag->data) {
        rc_dec(tag);
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        return PLCTAG_ERR_NO_DATA;
    }

    if(tag->is_bit) {
        rc_dec(tag);
        pdebug(DEBUG_WARN, "Getting string length from a bit tag is not supported!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        return PLCTAG_ERR_UNSUPPORTED;
    }

    critical_block(tag->api_mutex) {
        string_length = get_string_length_unsafe(tag, string_start_offset);
    }

    rc_dec(tag);

    pdebug(DEBUG_SPEW, "Done.");

    return string_length;
}




LIB_EXPORT int plc_tag_get_string_total_length(int32_t id, int string_start_offset)
{
    int total_length = 0;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* are strings defined for this tag? */
    if(!tag->byte_order || !tag->byte_order->str_is_defined) {
        rc_dec(tag);
        pdebug(DEBUG_WARN,"Tag has no definitions for strings!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        return PLCTAG_ERR_UNSUPPORTED;
    }

    /* is there data? */
    if(!tag->data) {
        rc_dec(tag);
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        return PLCTAG_ERR_NO_DATA;
    }

    if(tag->is_bit) {
        rc_dec(tag);
        pdebug(DEBUG_WARN, "Getting a string total length from a bit tag is not supported!");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        return PLCTAG_ERR_UNSUPPORTED;
    }

    critical_block(tag->api_mutex) {
        total_length = (int)(tag->byte_order->str_count_word_bytes)
                     + (tag->byte_order->str_is_fixed_length ? (int)(tag->byte_order->str_max_capacity) : get_string_length_unsafe(tag, string_start_offset))
                     + (tag->byte_order->str_is_zero_terminated ? (int)1 : (int)0)
                     + (int)(tag->byte_order->str_pad_bytes);
    }

    rc_dec(tag);

    pdebug(DEBUG_SPEW, "Done.");

    return total_length;
}


LIB_EXPORT int plc_tag_get_raw_bytes(int32_t id, int offset, uint8_t *buffer, int buffer_size)
{
    int rc = PLCTAG_STATUS_OK;
    plc_tag_p tag = lookup_tag(id);

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_WARN,"Tag not found.");
        return PLCTAG_ERR_NOT_FOUND;
    }

    /* is there data? */
    if(!tag->data) {
        rc_dec(tag);
        pdebug(DEBUG_WARN,"Tag has no data!");
        tag->status = PLCTAG_ERR_NO_DATA;
        return PLCTAG_ERR_NO_DATA;
    }

    if(!buffer) {
        pdebug(DEBUG_WARN,"Buffer is null!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if(buffer_size <= 0) {
        pdebug(DEBUG_WARN,"The buffer must have some capacity for data.");
        return PLCTAG_ERR_BAD_PARAM;
    }

    if(!tag->is_bit) {
        critical_block(tag->api_mutex) {
            if((offset >= 0) && ((offset + buffer_size) <= tag->size)) {
                int i;
                for (i=0;i<buffer_size;i++) {
                    buffer[i] = tag->data[offset + i];
                }

                tag->status = PLCTAG_STATUS_OK;
            } else {
                pdebug(DEBUG_WARN, "Data offset out of bounds!");
                tag->status = PLCTAG_ERR_OUT_OF_BOUNDS;
                rc = PLCTAG_ERR_OUT_OF_BOUNDS;
            }
        }
    } else {
        pdebug(DEBUG_WARN,"Trying to read a list of values from a Tag bit.");
        tag->status = PLCTAG_ERR_UNSUPPORTED;
        rc = PLCTAG_ERR_UNSUPPORTED;
    }

    rc_dec(tag);

    return rc;
}




/*****************************************************************************************************
 *****************************  Support routines for extra indirection *******************************
 ****************************************************************************************************/

int set_tag_byte_order(plc_tag_p tag, attr attribs)

{
    int use_default = 1;

    pdebug(DEBUG_INFO, "Starting.");

    /* the default values are already set in the tag. */

    /* check for overrides. */
    if(attr_get_str(attribs,  "int16_byte_order", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs,  "int32_byte_order", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs,  "int64_byte_order", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs,  "float32_byte_order", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs,  "float64_byte_order", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_is_counted", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_is_fixed_length", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_is_zero_terminated", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_is_byte_swapped", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_count_word_bytes", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_max_capacity", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_total_length", NULL) != NULL) {
        use_default = 0;
    }

    if(attr_get_str(attribs, "str_pad_bytes", NULL) != NULL) {
        use_default = 0;
    }

    /* if we need to override something, build a new byte order structure. */
    if(!use_default) {
        const char *byte_order_str = NULL;
        int str_param = 0;
        int rc = PLCTAG_STATUS_OK;
        tag_byte_order_t *new_byte_order = (tag_byte_order_t*)mem_alloc((int)(unsigned int)sizeof(*(tag->byte_order)));

        if(!new_byte_order) {
            pdebug(DEBUG_WARN, "Unable to allocate byte order struct for tag!");
            return PLCTAG_ERR_NO_MEM;
        }

        /* copy the defaults. */
        *new_byte_order = *(tag->byte_order);

        /* replace the old byte order. */
        tag->byte_order = new_byte_order;

        /* mark it as allocated so that we free it later. */
        tag->byte_order->is_allocated = 1;

        /* 16-bit ints. */
        byte_order_str = attr_get_str(attribs, "int16_byte_order", NULL);
        if(byte_order_str) {
            pdebug(DEBUG_DETAIL, "Override byte order int16_byte_order=%s", byte_order_str);

            rc = check_byte_order_str(byte_order_str, 2);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Byte order string int16_byte_order, \"%s\", is illegal or malformed.", byte_order_str);
                return rc;
            }

            /* strange gyrations to make the compiler happy.   MSVC will probably complain. */
            tag->byte_order->int16_order[0] = (int)(unsigned int)(((unsigned int)byte_order_str[0] - (unsigned int)('0')) & 0x01);
            tag->byte_order->int16_order[1] = (int)(unsigned int)(((unsigned int)byte_order_str[1] - (unsigned int)('0')) & 0x01);
        }

        /* 32-bit ints. */
        byte_order_str = attr_get_str(attribs, "int32_byte_order", NULL);
        if(byte_order_str) {
            pdebug(DEBUG_DETAIL, "Override byte order int32_byte_order=%s", byte_order_str);

            rc = check_byte_order_str(byte_order_str, 4);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Byte order string int32_byte_order, \"%s\", is illegal or malformed.", byte_order_str);
                return rc;
            }

            tag->byte_order->int32_order[0] = (int)(unsigned int)(((unsigned int)byte_order_str[0] - (unsigned int)('0')) & 0x03);
            tag->byte_order->int32_order[1] = (int)(unsigned int)(((unsigned int)byte_order_str[1] - (unsigned int)('0')) & 0x03);
            tag->byte_order->int32_order[2] = (int)(unsigned int)(((unsigned int)byte_order_str[2] - (unsigned int)('0')) & 0x03);
            tag->byte_order->int32_order[3] = (int)(unsigned int)(((unsigned int)byte_order_str[3] - (unsigned int)('0')) & 0x03);
        }

        /* 64-bit ints. */
        byte_order_str = attr_get_str(attribs, "int64_byte_order", NULL);
        if(byte_order_str) {
            pdebug(DEBUG_DETAIL, "Override byte order int64_byte_order=%s", byte_order_str);

            rc = check_byte_order_str(byte_order_str, 8);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Byte order string int64_byte_order, \"%s\", is illegal or malformed.", byte_order_str);
                return rc;
            }

            tag->byte_order->int64_order[0] = (int)(unsigned int)(((unsigned int)byte_order_str[0] - (unsigned int)('0')) & 0x07);
            tag->byte_order->int64_order[1] = (int)(unsigned int)(((unsigned int)byte_order_str[1] - (unsigned int)('0')) & 0x07);
            tag->byte_order->int64_order[2] = (int)(unsigned int)(((unsigned int)byte_order_str[2] - (unsigned int)('0')) & 0x07);
            tag->byte_order->int64_order[3] = (int)(unsigned int)(((unsigned int)byte_order_str[3] - (unsigned int)('0')) & 0x07);
            tag->byte_order->int64_order[4] = (int)(unsigned int)(((unsigned int)byte_order_str[4] - (unsigned int)('0')) & 0x07);
            tag->byte_order->int64_order[5] = (int)(unsigned int)(((unsigned int)byte_order_str[5] - (unsigned int)('0')) & 0x07);
            tag->byte_order->int64_order[6] = (int)(unsigned int)(((unsigned int)byte_order_str[6] - (unsigned int)('0')) & 0x07);
            tag->byte_order->int64_order[7] = (int)(unsigned int)(((unsigned int)byte_order_str[7] - (unsigned int)('0')) & 0x07);
        }

        /* 32-bit floats. */
        byte_order_str = attr_get_str(attribs, "float32_byte_order", NULL);
        if(byte_order_str) {
            pdebug(DEBUG_DETAIL, "Override byte order float32_byte_order=%s", byte_order_str);

            rc = check_byte_order_str(byte_order_str, 4);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Byte order string float32_byte_order, \"%s\", is illegal or malformed.", byte_order_str);
                return rc;
            }

            tag->byte_order->float32_order[0] = (int)(unsigned int)(((unsigned int)byte_order_str[0] - (unsigned int)('0')) & 0x03);
            tag->byte_order->float32_order[1] = (int)(unsigned int)(((unsigned int)byte_order_str[1] - (unsigned int)('0')) & 0x03);
            tag->byte_order->float32_order[2] = (int)(unsigned int)(((unsigned int)byte_order_str[2] - (unsigned int)('0')) & 0x03);
            tag->byte_order->float32_order[3] = (int)(unsigned int)(((unsigned int)byte_order_str[3] - (unsigned int)('0')) & 0x03);
        }

        /* 64-bit floats */
        byte_order_str = attr_get_str(attribs, "float64_byte_order", NULL);
        if(byte_order_str) {
            pdebug(DEBUG_DETAIL, "Override byte order float64_byte_order=%s", byte_order_str);

            rc = check_byte_order_str(byte_order_str, 8);
            if(rc != PLCTAG_STATUS_OK) {
                pdebug(DEBUG_WARN, "Byte order string float64_byte_order, \"%s\", is illegal or malformed.", byte_order_str);
                return rc;
            }

            tag->byte_order->float64_order[0] = (int)(unsigned int)(((unsigned int)byte_order_str[0] - (unsigned int)('0')) & 0x07);
            tag->byte_order->float64_order[1] = (int)(unsigned int)(((unsigned int)byte_order_str[1] - (unsigned int)('0')) & 0x07);
            tag->byte_order->float64_order[2] = (int)(unsigned int)(((unsigned int)byte_order_str[2] - (unsigned int)('0')) & 0x07);
            tag->byte_order->float64_order[3] = (int)(unsigned int)(((unsigned int)byte_order_str[3] - (unsigned int)('0')) & 0x07);
            tag->byte_order->float64_order[4] = (int)(unsigned int)(((unsigned int)byte_order_str[4] - (unsigned int)('0')) & 0x07);
            tag->byte_order->float64_order[5] = (int)(unsigned int)(((unsigned int)byte_order_str[5] - (unsigned int)('0')) & 0x07);
            tag->byte_order->float64_order[6] = (int)(unsigned int)(((unsigned int)byte_order_str[6] - (unsigned int)('0')) & 0x07);
            tag->byte_order->float64_order[7] = (int)(unsigned int)(((unsigned int)byte_order_str[7] - (unsigned int)('0')) & 0x07);
        }

        /* string information. */

        /* is the string counted? */
        if(attr_get_str(attribs, "str_is_counted", NULL)) {
            str_param = attr_get_int(attribs, "str_is_counted", 0);
            if(str_param == 1 || str_param == 0) {
                tag->byte_order->str_is_counted = (str_param ? 1 : 0);
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_is_counted must be missing, zero (0) or one (1)!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* is the string a fixed length? */
        if(attr_get_str(attribs, "str_is_fixed_length", NULL)) {
            str_param = attr_get_int(attribs, "str_is_fixed_length", 0);
            if(str_param == 1 || str_param == 0) {
                tag->byte_order->str_is_fixed_length = (str_param ? 1: 0);
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_is_fixed_length must be missing, zero (0) or one (1)!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* is the string zero terminated? */
        if(attr_get_str(attribs, "str_is_zero_terminated", NULL)) {
            str_param = attr_get_int(attribs, "str_is_zero_terminated", 0);
            if(str_param == 1 || str_param == 0) {
                tag->byte_order->str_is_zero_terminated = (str_param ? 1 : 0);
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_is_zero_terminated must be missing, zero (0) or one (1)!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* is the string byteswapped like PLC/5? */
        if(attr_get_str(attribs, "str_is_byte_swapped", NULL)) {
            str_param = attr_get_int(attribs, "str_is_byte_swapped", 0);
            if(str_param == 1 || str_param == 0) {
                tag->byte_order->str_is_byte_swapped = (str_param ? 1 : 0);
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_is_byte_swapped must be missing, zero (0) or one (1)!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* main string parameters. */

        /* how many bytes is the string count word? */
        if(attr_get_str(attribs, "str_count_word_bytes", NULL)) {
            str_param = attr_get_int(attribs, "str_count_word_bytes", 0);
            if(str_param == 0 || str_param == 1 || str_param == 2 || str_param == 4 || str_param == 8) {
                tag->byte_order->str_count_word_bytes = (unsigned int)str_param;
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_count_word_bytes must be missing, 0, 1, 2, 4, or 8!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* What is the string maximum capacity */
        if(attr_get_str(attribs, "str_max_capacity", NULL)) {
            str_param = attr_get_int(attribs, "str_max_capacity", 0);
            if(str_param >= 0) {
                tag->byte_order->str_max_capacity = (unsigned int)str_param;
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_max_capacity must be missing, 0, or positive!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* What is the string total length */
        if(attr_get_str(attribs, "str_total_length", NULL)) {
            str_param = attr_get_int(attribs, "str_total_length", 0);
            if(str_param >= 0) {
                tag->byte_order->str_total_length = (unsigned int)str_param;
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_total_length must be missing, 0, or positive!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* What is the string padding length */
        if(attr_get_str(attribs, "str_pad_bytes", NULL)) {
            str_param = attr_get_int(attribs, "str_pad_bytes", 0);
            if(str_param >= 0) {
                tag->byte_order->str_pad_bytes = (unsigned int)str_param;
            } else {
                pdebug(DEBUG_WARN, "Tag string attribute str_pad_bytes must be missing, 0, or positive!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* now make sure that the combination of settings works. */

        /* if we have a counted string, we need the count! */
        if(tag->byte_order->str_is_counted) {
            if(tag->byte_order->str_count_word_bytes == 0) {
                pdebug(DEBUG_WARN, "If a string definition is counted, you must use both \"str_is_counted\" and \"str_count_word_bytes\" parameters!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* if we have a fixed length string, we need to know what the length is! */
        if(tag->byte_order->str_is_fixed_length) {
            if(tag->byte_order->str_total_length == 0) {
                pdebug(DEBUG_WARN, "If a string definition is fixed length, you must use both \"str_is_fixed_length\" and \"str_total_length\" parameters!");
                return PLCTAG_ERR_BAD_PARAM;
            }
        }

        /* check the total length. */
        if(tag->byte_order->str_total_length > 0
          && (tag->byte_order->str_is_zero_terminated
            + tag->byte_order->str_max_capacity
            + tag->byte_order->str_count_word_bytes
            + tag->byte_order->str_pad_bytes)
          > tag->byte_order->str_total_length)
        {
            pdebug(DEBUG_WARN, "Tag string total length must be at least the sum of the other string components!");
            return PLCTAG_ERR_BAD_PARAM;
        }

        /* Do we have enough of a definition for a string? */
        /* FIXME - This is probably not enough checking! */
        if(tag->byte_order->str_is_counted || tag->byte_order->str_is_zero_terminated) {
            tag->byte_order->str_is_defined = 1;
        } else {
            pdebug(DEBUG_WARN, "Insufficient definitions found to support strings!");
        }
    }

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_OK;
}

int check_byte_order_str(const char *byte_order, int length)
{
    int taken[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    int byte_order_len = str_length(byte_order);

    pdebug(DEBUG_DETAIL, "Starting.");

    /* check the size. */
    if(byte_order_len != length) {
        pdebug(DEBUG_WARN, "Byte order string, \"%s\", must be %d characters long!", byte_order, length);
        return (byte_order_len < length ? PLCTAG_ERR_TOO_SMALL : PLCTAG_ERR_TOO_LARGE);
    }

    /* check each character. */
    for(int i=0; i < byte_order_len; i++) {
        int val = 0;

        if(!isdigit(byte_order[i]) || byte_order[i] < '0' || byte_order[i] > '7') {
            pdebug(DEBUG_WARN, "Byte order string, \"%s\", must be only characters from '0' to '7'!", byte_order);
            return PLCTAG_ERR_BAD_DATA;
        }

        /* get the numeric value. */
        val = byte_order[i] - '0';

        if(val < 0 || val > (length-1)) {
            pdebug(DEBUG_WARN, "Byte order string, \"%s\", must only values from 0 to %d!", byte_order, (length - 1));
            return PLCTAG_ERR_BAD_DATA;
        }

        if(taken[val]) {
            pdebug(DEBUG_WARN, "Byte order string, \"%s\", must use each digit exactly once!", byte_order);
            return PLCTAG_ERR_BAD_DATA;
        }

        taken[val] = 1;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}




plc_tag_p lookup_tag(int32_t tag_id)
{
    plc_tag_p tag = NULL;

    critical_block(tag_lookup_mutex) {
        tag = (plc_tag_p)hashtable_get(tags, (int64_t)tag_id);

        if(tag) {
            debug_set_tag_id(tag->tag_id);
        } else {
            /* TODO - remove this. */
            pdebug(DEBUG_WARN, "Tag with ID %d not found.", tag_id);
        }

        if(tag && tag->tag_id == tag_id) {
            pdebug(DEBUG_SPEW, "Found tag %p with id %d.", tag, tag->tag_id);
            tag = (plc_tag_p)rc_inc(tag);
        } else {
            debug_set_tag_id(0);
            tag = NULL;
        }
    }

    return tag;
}



int tag_id_inc(int id)
{
    if(id <= 0) {
        pdebug(DEBUG_ERROR, "Incoming ID is not valid! Got %d",id);
        /* try to correct. */
        id = (TAG_ID_MASK/2);
    }

    id = (id + 1) & TAG_ID_MASK;

    if(id == 0) {
        id = 1; /* skip zero intentionally! Can't return an ID of zero because it looks like a NULL pointer */
    }

    return id;
}



int add_tag_lookup(plc_tag_p tag)
{
    int rc = PLCTAG_ERR_NOT_FOUND;
    int new_id = 0;

    pdebug(DEBUG_DETAIL, "Starting.");

    critical_block(tag_lookup_mutex) {
        int attempts = 0;

        /* only get this when we hold the mutex. */
        new_id = next_tag_id;

        do {
            new_id = tag_id_inc(new_id);

            if(new_id <=0) {
                pdebug(DEBUG_WARN,"ID %d is illegal!", new_id);
                attempts = MAX_TAG_MAP_ATTEMPTS;
                break;
            }

            pdebug(DEBUG_SPEW,"Trying new ID %d.", new_id);

            if(!hashtable_get(tags,(int64_t)new_id)) {
                pdebug(DEBUG_DETAIL,"Found unused ID %d", new_id);
                break;
            }

            attempts++;
        } while(attempts < MAX_TAG_MAP_ATTEMPTS);

        if(attempts < MAX_TAG_MAP_ATTEMPTS) {
            rc = hashtable_put(tags, (int64_t)new_id, tag);
        } else {
            rc = PLCTAG_ERR_NO_RESOURCES;
        }

        next_tag_id = new_id;
    }

    if(rc != PLCTAG_STATUS_OK) {
        new_id = rc;
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return new_id;
}





/*
 * get the string length depending on the PLC string type.
 *
 * This is called in other functions so is separated out.
 *
 * This must be called with the tag API mutex held!
 */

int get_string_length_unsafe(plc_tag_p tag, int offset)
{
    int string_length = 0;

    if(tag->byte_order->str_is_counted) {
        switch(tag->byte_order->str_count_word_bytes) {
            case 1:
                string_length = (int)(unsigned int)(tag->data[offset]);
                break;

            case 2:
                string_length = (int16_t)(uint16_t)(((uint16_t)(tag->data[offset + tag->byte_order->int16_order[0]]) << 0 ) +
                                                    ((uint16_t)(tag->data[offset + tag->byte_order->int16_order[1]]) << 8 ));
                break;

            case 4:
                string_length = (int32_t)(((uint32_t)(tag->data[offset + tag->byte_order->int32_order[0]]) << 0 ) +
                                          ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[1]]) << 8 ) +
                                          ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[2]]) << 16) +
                                          ((uint32_t)(tag->data[offset + tag->byte_order->int32_order[3]]) << 24));
                break;

            default:
                pdebug(DEBUG_WARN, "Unsupported string count word size, %d bytes!", tag->byte_order->str_count_word_bytes);
                return 0; /* FIXME - this should be an error code. */
                break;
        }

    } else {
        if(tag->byte_order->str_is_zero_terminated) {
            /* slow, but hopefully correct. */

            /*
             * note that this will count the correct length of a string that runs up against
             * the end of the tag buffer.
             */
            for(int i = offset + (int)(tag->byte_order->str_count_word_bytes); i < tag->size; i++) {
                size_t char_index = (((size_t)(unsigned int)string_length) ^ (tag->byte_order->str_is_byte_swapped)) /* byte swap if necessary */
                                + (size_t)(unsigned int)offset
                                + (size_t)(unsigned int)(tag->byte_order->str_count_word_bytes);

                if(tag->data[char_index] == (uint8_t)0) {
                    /* found the end. */
                    break;
                }

                string_length++;
            }
        } else {
            /* it is not counted or zero terminated, so it is not supported. */
            pdebug(DEBUG_WARN, "Unsupported string length type.   Must be counted or zero-terminated!");
            return 0; /* FIXME this should be an error code. */
        }
    }

    return string_length;
}


#endif // __LIB_LIB_C__


#ifndef __LIB_INIT_C__
#define __LIB_INIT_C__



static lock_t library_initialization_lock = LOCK_INIT;
static volatile int library_initialized = 0;
static volatile mutex_p lib_mutex = NULL;


/*
 * destroy_modules() is called when the main process exits.
 *
 * Modify this for any PLC/protocol that needs to have something
 * torn down at the end.
 */

void destroy_modules(void)
{
    ab_teardown();

    lib_teardown();

    spin_block(&library_initialization_lock) {
        if(lib_mutex != NULL) {
            /* FIXME casting to get rid of volatile is WRONG */
            mutex_destroy((mutex_p *)&lib_mutex);
            lib_mutex = NULL;
        }
    }

    library_initialized = 0;
}



/*
 * initialize_modules() is called the first time any kind of tag is
 * created.  It will be called before the tag creation routines are
 * run.
 */


int initialize_modules(void)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting.");

    /*
     * Try to keep busy waiting to a minimum.
     * If there is no mutex set up, then create one.
     * Only one thread allowed at a time through this gate.
     */
    spin_block(&library_initialization_lock) {
        if(lib_mutex == NULL) {
            pdebug(DEBUG_INFO, "Creating library mutex.");
            /* FIXME - casting to get rid of volatile is WRONG */
            rc = mutex_create((mutex_p *)&lib_mutex);
        }
    }

    /* check the status outside the lock. */
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to initialize library mutex!  Error %s!", plc_tag_decode_error(rc));
        return rc;
    } else {
        /*
        * guard library initialization with a mutex.
        *
        * This prevents busy waiting as would happen with just a spin lock.
        */
        critical_block(lib_mutex) {
            if(!library_initialized) {
                /* initialize a random seed value. */
                srand((unsigned int)time_ms());

                pdebug(DEBUG_INFO,"Initializing library modules.");
                rc = lib_init();

                pdebug(DEBUG_INFO,"Initializing AB module.");
                if(rc == PLCTAG_STATUS_OK) {
                    rc = ab_init();
                }

                /* hook the destructor */
                atexit(plc_tag_shutdown);

                /* do this last */
                library_initialized = 1;

                pdebug(DEBUG_INFO,"Done initializing library modules.");
            }
        }
    }

    pdebug(DEBUG_INFO, "Done.");

    return rc;
}


#endif // __LIB_INIT_C__


#ifndef __UTIL_ATOMIC_INT_C__
#define __UTIL_ATOMIC_INT_C__


void atomic_init(atomic_int* a, int new_val)
{
    a->lock = LOCK_INIT;
    a->val = new_val;
}



int atomic_get(atomic_int* a)
{
    int val = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    spin_block(&a->lock) {
        val = a->val;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return val;
}



int atomic_set(atomic_int* a, int new_val)
{
    int old_val = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    spin_block(&a->lock) {
        old_val = a->val;
        a->val = new_val;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return old_val;
}



int atomic_add(atomic_int* a, int other)
{
    int old_val = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    spin_block(&a->lock) {
        old_val = a->val;
        a->val += other;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return old_val;
}


int atomic_compare_and_set(atomic_int* a, int old_val, int new_val)
{
    int ret_val = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    spin_block(&a->lock) {
        ret_val = a->val;

        if (ret_val == old_val) {
            a->val = new_val;
        }
    }

    pdebug(DEBUG_SPEW, "Done.");

    return ret_val;
}

#endif // __UTIL_ATOMIC_INT_C__


#ifndef __UTIL_ATTR_C__
#define __UTIL_ATTR_C__

struct attr_entry_t {
    attr_entry next;
    char* name;
    char* val;
};

struct attr_t {
    attr_entry head;
};





/*
 * find_entry
 *
 * A helper function to find the attr_entry that has the
 * passed name.
 */

attr_entry find_entry(attr a, const char* name)
{
    attr_entry e;

    if (!a)
        return NULL;

    e = a->head;

    if (!e)
        return NULL;

    while (e) {
        if (str_cmp(e->name, name) == 0) {
            return e;
        }

        e = e->next;
    }

    return NULL;
}


/*
 * attr_create
 *
 * Create a new attr structure and return a pointer to it.
 */
extern attr attr_create()
{
    return (attr)mem_alloc(sizeof(struct attr_t));
}









/*
 * attr_create_from_str
 *
 * Parse the passed string into an attr structure and return a pointer to a list of
 * attr_entry structs.
 *
 * Attribute strings are formatted much like URL arguments:
 * foo=bar&blah=humbug&blorg=42&test=one
 * You cannot, currently, have an "=" or "&" character in the value for an
 * attribute.
 */
extern attr attr_create_from_str(const char* attr_str)
{
    attr res = NULL;
    char** kv_pairs = NULL;

    pdebug(DEBUG_DETAIL, "Starting.");

    if (!str_length(attr_str)) {
        pdebug(DEBUG_WARN, "Attribute string needs to be longer than zero characters!");
        return NULL;
    }

    /* split the string on "&" */
    kv_pairs = str_split(attr_str, "&");
    if (!kv_pairs) {
        pdebug(DEBUG_WARN, "No key-value pairs!");
        return NULL;
    }

    /* set up the attribute list head */
    res = attr_create();
    if (!res) {
        pdebug(DEBUG_ERROR, "Unable to allocate memory for attribute list!");
        mem_free(kv_pairs);
        return NULL;
    }

    /* loop over each key-value pair */
    for (char** kv_pair = kv_pairs; *kv_pair; kv_pair++) {
        /* find the position of the '=' character */
        char* separator = strchr(*kv_pair, '=');
        char* key = *kv_pair;
        char* value = separator;

        pdebug(DEBUG_DETAIL, "Key-value pair \"%s\".", *kv_pair);

        if (separator == NULL) {
            pdebug(DEBUG_WARN, "Attribute string \"%s\" has invalid key-value pair near \"%s\"!", attr_str, *kv_pair);
            mem_free(kv_pairs);
            attr_destroy(res);
            return NULL;
        }

        /* value points to the '=' character.  Step past that for the value. */
        value++;

        /* cut the string at the separator. */
        *separator = (char)0;

        pdebug(DEBUG_DETAIL, "Key-value pair before trimming \"%s\":\"%s\".", key, value);

        /* skip leading spaces in the key */
        while (*key == ' ') {
            key++;
        }

        /* zero out all trailing spaces in the key */
        for (int i = str_length(key) - 1; i > 0 && key[i] == ' '; i--) {
            key[i] = (char)0;
        }

        pdebug(DEBUG_DETAIL, "Key-value pair after trimming \"%s\":\"%s\".", key, value);

        /* check the string lengths */

        if (str_length(key) <= 0) {
            pdebug(DEBUG_WARN, "Attribute string \"%s\" has invalid key-value pair near \"%s\"!  Key must not be zero length!", attr_str, *kv_pair);
            mem_free(kv_pairs);
            attr_destroy(res);
            return NULL;
        }

        if (str_length(value) <= 0) {
            pdebug(DEBUG_WARN, "Attribute string \"%s\" has invalid key-value pair near \"%s\"!  Value must not be zero length!", attr_str, *kv_pair);
            mem_free(kv_pairs);
            attr_destroy(res);
            return NULL;
        }

        /* add the key-value pair to the attribute list */
        if (attr_set_str(res, key, value)) {
            pdebug(DEBUG_WARN, "Unable to add key-value pair \"%s\":\"%s\" to attribute list!", key, value);
            mem_free(kv_pairs);
            attr_destroy(res);
            return NULL;
        }
    }

    if (kv_pairs) {
        mem_free(kv_pairs);
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return res;
}





/*
 * attr_set
 *
 * Set/create a new string attribute
 */
extern int attr_set_str(attr attrs, const char* name, const char* val)
{
    attr_entry e;

    if (!attrs) {
        return 1;
    }

    /* does the entry exist? */
    e = find_entry(attrs, name);

    /* if we had a match, then delete the existing value and add in the
     * new one.
     *
     * If we had no match, then e is NULL and we need to create a new one.
     */
    if (e) {
        /* we had a match, free any existing value */
        if (e->val) {
            mem_free(e->val);
        }

        /* set up the new value */
        e->val = str_dup(val);
        if (!e->val) {
            /* oops! */
            return 1;
        }
    }
    else {
        /* no match, need a new entry */
        e = (attr_entry)mem_alloc(sizeof(struct attr_entry_t));

        if (e) {
            e->name = str_dup(name);

            if (!e->name) {
                mem_free(e);
                return 1;
            }

            e->val = str_dup(val);

            if (!e->val) {
                mem_free(e->name);
                mem_free(e);
                return 1;
            }

            /* link it in the list */
            e->next = attrs->head;
            attrs->head = e;
        }
        else {
            /* allocation failed */
            return 1;
        }
    }

    return 0;
}



extern int attr_set_int(attr attrs, const char* name, int val)
{
    char buf[64];

    snprintf_platform(buf, sizeof buf, "%d", val);

    return attr_set_str(attrs, name, buf);
}



extern int attr_set_float(attr attrs, const char* name, float val)
{
    char buf[64];

    snprintf_platform(buf, sizeof buf, "%f", val);

    return attr_set_str(attrs, name, buf);
}





/*
 * attr_get
 *
 * Walk the list of attrs and return the value found with the passed name.
 * If the name is not found, return the passed default value.
 */
extern const char* attr_get_str(attr attrs, const char* name, const char* def)
{
    attr_entry e;

    if (!attrs) {
        return def;
    }

    e = find_entry(attrs, name);

    /* only return a value if there is one. */
    if (e) {
        return e->val;
    }
    else {
        return def;
    }
}


extern int attr_get_int(attr attrs, const char* name, int def)
{
    int res;
    int rc;

    const char* str_val = attr_get_str(attrs, name, NULL);

    if (!str_val) {
        return def;
    }

    rc = str_to_int(str_val, &res);

    if (rc) {
        /* format error? */
        return def;
    }
    else {
        return res;
    }
}


extern float attr_get_float(attr attrs, const char* name, float def)
{
    float res;
    int rc;

    const char* str_val = attr_get_str(attrs, name, NULL);

    if (!str_val) {
        return def;
    }

    rc = str_to_float(str_val, &res);

    if (rc) {
        /* format error? */
        return def;
    }
    else {
        return res;
    }
}


extern int attr_remove(attr attrs, const char* name)
{
    attr_entry e, p;

    if (!attrs)
        return 0;

    e = attrs->head;

    /* no such entry, return */
    if (!e)
        return 0;

    /* loop to find the entry */
    p = NULL;

    while (e) {
        if (str_cmp(e->name, name) == 0) {
            break;
        }

        p = e;
        e = e->next;
    }

    if (e) {
        /* unlink the node */
        if (!p) {
            attrs->head = e->next;
        }
        else {
            p->next = e->next;
        }

        if (e->name) {
            mem_free(e->name);
        }

        if (e->val) {
            mem_free(e->val);
        }

        mem_free(e);
    } /* else not found */

    return 0;
}


/*
 * attr_delete
 *
 * Destroy and free all memory for an attribute list.
 */
extern void attr_destroy(attr a)
{
    attr_entry e, p;

    if (!a)
        return;

    e = a->head;

    /* walk down the entry list and free as we go. */
    while (e) {
        if (e->name) {
            mem_free(e->name);
        }

        if (e->val) {
            mem_free(e->val);
        }

        p = e;
        e = e->next;

        mem_free(p);
    }

    mem_free(a);
}

#endif // __UTIL_ATTR_C__


#ifndef __UTIL_DEBUG_C__
#define __UTIL_DEBUG_C__

/*
 * Debugging support.
 */


static int global_debug_level = DEBUG_NONE;
static lock_t thread_num_lock = LOCK_INIT;
static volatile uint32_t thread_num = 1;
static lock_t logger_callback_lock = LOCK_INIT;
static void (* volatile log_callback_func)(int32_t tag_id, int debug_level, const char* message);


/*
 * Keep the thread ID and the tag ID thread local.
 */

static THREAD_LOCAL uint32_t this_thread_num = 0;
static THREAD_LOCAL int32_t tag_id = 0;


// /* only output the version once */
// static lock_t printed_version = LOCK_INIT;



int set_debug_level(int level)
{
    int old_level = global_debug_level;

    global_debug_level = level;

    return old_level;
}


int get_debug_level(void)
{
    return global_debug_level;
}



void debug_set_tag_id(int32_t t_id)
{
    tag_id = t_id;
}



static uint32_t get_thread_id()
{
    if (!this_thread_num) {
        spin_block(&thread_num_lock) {
            this_thread_num = thread_num;
            thread_num++;
        }
    }

    return this_thread_num;
}

// static int make_prefix(char *prefix_buf, int prefix_buf_size)
// {
//     struct tm t;
//     time_t epoch;
//     int64_t epoch_ms;
//     int remainder_ms;
//     int rc = PLCTAG_STATUS_OK;

//     /* make sure we have room, MAGIC */
//     if(prefix_buf_size < 37) {
//         return PLCTAG_ERR_TOO_SMALL;
//     }

//     /* build the prefix */

//     /* get the time parts */
//     epoch_ms = time_ms();
//     epoch = (time_t)(epoch_ms/1000);
//     remainder_ms = (int)(epoch_ms % 1000);

//     /* FIXME - should capture error return! */
//     localtime_r(&epoch,&t);

//     /* create the prefix and format for the file entry. */
//     rc = snprintf(prefix_buf, (size_t)prefix_buf_size,"%04d-%02d-%02d %02d:%02d:%02d.%03d thread(%u) tag(%d)",
//                   t.tm_year+1900,t.tm_mon,t.tm_mday,t.tm_hour,t.tm_min,t.tm_sec,remainder_ms, get_thread_id(), tag_id);

//     /* enforce zero string termination */
//     if(rc > 1 && rc < prefix_buf_size) {
//         prefix_buf[rc] = 0;
//     } else {
//         prefix_buf[prefix_buf_size - 1] = 0;
//     }

//     return rc;
// }


static const char* debug_level_name[DEBUG_END] = { "NONE", "ERROR", "WARN", "INFO", "DETAIL", "SPEW" };

extern void pdebug_impl(const char* func, int line_num, int debug_level, const char* templ, ...)
{
    va_list va;
    struct tm t;
    time_t epoch;
    int64_t epoch_ms;
    int remainder_ms;
    char prefix[1000]; /* MAGIC */
    int prefix_size = 0;
    char output[1000];
    //int output_size = 0;

    /* build the prefix */
    // prefix_size = make_prefix(prefix,(int)sizeof(prefix));  /* don't exceed a size that int can express! */
    // if(prefix_size <= 0) {
    //     return;
    // }

    /* get the time parts */
    epoch_ms = time_ms();
    epoch = (time_t)(epoch_ms / 1000);
    remainder_ms = (int)(epoch_ms % 1000);

    /* FIXME - should capture error return! */
    localtime_r(&epoch, &t);

    /* print only once */
    /* FIXME - this may not be safe. */
    // if(!printed_version && debug_level >= DEBUG_INFO) {
    //     if(lock_acquire_try((lock_t*)&printed_version)) {
    //         /* create the output string template */
    //         fprintf(stderr,"%s INFO libplctag version %s, debug level %d.\n",prefix, VERSION, get_debug_level());
    //     }
    // }

    /* build the output string template */
    prefix_size += snprintf(prefix, sizeof(prefix), "%04d-%02d-%02d %02d:%02d:%02d.%03d thread(%u) tag(%" PRId32 ") %s %s:%d %s\n",
        t.tm_year + 1900,
        t.tm_mon + 1, /* month is 0-11? */
        t.tm_mday,
        t.tm_hour,
        t.tm_min,
        t.tm_sec,
        remainder_ms,
        get_thread_id(),
        tag_id,
        debug_level_name[debug_level],
        func,
        line_num,
        templ);

    /* make sure it is zero terminated */
    prefix[sizeof(prefix) - 1] = 0;

    /* print it out. */
    va_start(va, templ);

    /* FIXME - check the output size */
    /*output_size = */vsnprintf(output, sizeof(output), prefix, va);
    if (log_callback_func) {
        log_callback_func(tag_id, debug_level, output);
    }
    else {
        fputs(output, stderr);
    }

    va_end(va);
}




#define COLUMNS (16)

void pdebug_dump_bytes_impl(const char* func, int line_num, int debug_level, uint8_t* data, int count)
{
    int max_row, row, column;
    char row_buf[(COLUMNS * 3) + 5 + 1];

    /* determine the number of rows we will need to print. */
    max_row = (count + (COLUMNS - 1)) / COLUMNS;

    for (row = 0; row < max_row; row++) {
        int offset = (row * COLUMNS);
        int row_offset = 0;

        /* print the offset in the packet */
        row_offset = snprintf(&row_buf[0], sizeof(row_buf), "%05d", offset);

        for (column = 0; column < COLUMNS && ((row * COLUMNS) + column) < count && row_offset < (int)sizeof(row_buf); column++) {
            offset = (row * COLUMNS) + column;
            row_offset += snprintf(&row_buf[row_offset], sizeof(row_buf) - (size_t)row_offset, " %02x", data[offset]);
        }

        /* terminate the row string*/
        row_buf[sizeof(row_buf) - 1] = 0; /* just in case */

        /* output it, finally */
        pdebug_impl(func, line_num, debug_level, row_buf);
    }
}


int debug_register_logger(void (*log_callback_func_arg)(int32_t tag_id, int debug_level, const char* message))
{
    int rc = PLCTAG_STATUS_OK;

    spin_block(&logger_callback_lock) {
        if (!log_callback_func) {
            log_callback_func = log_callback_func_arg;
        }
        else {
            rc = PLCTAG_ERR_DUPLICATE;
        }
    }

    return rc;
}


int debug_unregister_logger(void)
{
    int rc = PLCTAG_STATUS_OK;

    spin_block(&logger_callback_lock) {
        if (log_callback_func) {
            log_callback_func = NULL;
        }
        else {
            rc = PLCTAG_ERR_NOT_FOUND;
        }
    }

    return rc;
}

#endif // __UTIL_DEBUG_C__


#ifndef __UTIL_HASH_C__
#define __UTIL_HASH_C__

/*
--------------------------------------------------------------------
lookup2.c, by Bob Jenkins, December 1996, Public Domain.
hash(), hash2(), hash3, and mix() are externally useful functions.
Routines to test the hash are included if SELF_TEST is defined.
You can use this free for any purpose.  It has no warranty.

Obsolete.  Use lookup3.c instead, it is faster and more thorough.
--------------------------------------------------------------------
*/
#define SELF_TEST

#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
typedef uint32_t ub4;   /* unsigned 4-byte quantities */
typedef uint8_t ub1;

#define hashsize(n) ((ub4)1<<(n))
#define hashmask(n) (hashsize(n)-1)

/*
--------------------------------------------------------------------
mix -- mix 3 32-bit values reversibly.
For every delta with one or two bit set, and the deltas of all three
  high bits or all three low bits, whether the original value of a,b,c
  is almost all zero or is uniformly distributed,
* If mix() is run forward or backward, at least 32 bits in a,b,c
  have at least 1/4 probability of changing.
* If mix() is run forward, every bit of c will change between 1/3 and
  2/3 of the time.  (Well, 22/100 and 78/100 for some 2-bit deltas.)
mix() was built out of 36 single-cycle latency instructions in a
  structure that could supported 2x parallelism, like so:
      a -= b;
      a -= c; x = (c>>13);
      b -= c; a ^= x;
      b -= a; x = (a<<8);
      c -= a; b ^= x;
      c -= b; x = (b>>13);
      ...
  Unfortunately, superscalar Pentiums and Sparcs can't take advantage
  of that parallelism.  They've also turned some of those single-cycle
  latency instructions into multi-cycle latency instructions.  Still,
  this is the fastest good hash I could find.  There were about 2^^68
  to choose from.  I only looked at a billion or so.
--------------------------------------------------------------------
*/
#define mix(a,b,c) \
    { \
        a -= b; a -= c; a ^= (c>>13); \
        b -= c; b -= a; b ^= (a<<8); \
        c -= a; c -= b; c ^= (b>>13); \
        a -= b; a -= c; a ^= (c>>12);  \
        b -= c; b -= a; b ^= (a<<16); \
        c -= a; c -= b; c ^= (b>>5); \
        a -= b; a -= c; a ^= (c>>3);  \
        b -= c; b -= a; b ^= (a<<10); \
        c -= a; c -= b; c ^= (b>>15); \
    }

/* same, but slower, works on systems that might have 8 byte ub4's */
#define mix2(a,b,c) \
    { \
        a -= b; a -= c; a ^= (c>>13); \
        b -= c; b -= a; b ^= (a<< 8); \
        c -= a; c -= b; c ^= ((b&0xffffffff)>>13); \
        a -= b; a -= c; a ^= ((c&0xffffffff)>>12); \
        b -= c; b -= a; b = (b ^ (a<<16)) & 0xffffffff; \
        c -= a; c -= b; c = (c ^ (b>> 5)) & 0xffffffff; \
        a -= b; a -= c; a = (a ^ (c>> 3)) & 0xffffffff; \
        b -= c; b -= a; b = (b ^ (a<<10)) & 0xffffffff; \
        c -= a; c -= b; c = (c ^ (b>>15)) & 0xffffffff; \
    }

/*
--------------------------------------------------------------------
hash() -- hash a variable-length key into a 32-bit value
  k     : the key (the unaligned variable-length array of bytes)
  len   : the length of the key, counting by bytes
  level : can be any 4-byte value
Returns a 32-bit value.  Every bit of the key affects every bit of
the return value.  Every 1-bit and 2-bit delta achieves avalanche.
About 36+6len instructions.

The best hash table sizes are powers of 2.  There is no need to do
mod a prime (mod is sooo slow!).  If you need less than 32 bits,
use a bitmask.  For example, if you need only 10 bits, do
  h = (h & hashmask(10));
In which case, the hash table should have hashsize(10) elements.

If you are hashing n strings (ub1 **)k, do it like this:
  for (i=0, h=0; i<n; ++i) h = hash( k[i], len[i], h);

By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.  You may use this
code any way you wish, private, educational, or commercial.  It's free.

See http://burtleburtle.net/bob/hash/evahash.html
Use for hash table lookup, or anything where one collision in 2^32 is
acceptable.  Do NOT use for cryptographic purposes.
--------------------------------------------------------------------
*/

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4061)
#endif

uint32_t hash(uint8_t* k, size_t length, uint32_t initval)
{
    uint32_t a, b, c, len;

    /* Set up the internal state */
    len = (uint32_t)length;
    a = b = 0x9e3779b9;  /* the golden ratio; an arbitrary value */
    c = initval;           /* the previous hash value */

    /*---------------------------------------- handle most of the key */
    while (len >= 12) {
        a += (uint32_t)((k[0] + ((ub4)k[1] << 8) + ((ub4)k[2] << 16) + ((ub4)k[3] << 24)));
        b += (uint32_t)((k[4] + ((ub4)k[5] << 8) + ((ub4)k[6] << 16) + ((ub4)k[7] << 24)));
        c += (uint32_t)((k[8] + ((ub4)k[9] << 8) + ((ub4)k[10] << 16) + ((ub4)k[11] << 24)));
        mix(a, b, c);
        k += 12;
        len -= 12;
    }

    /*------------------------------------- handle the last 11 bytes */
    c += (uint32_t)length;
    switch (len) {            /* all the case statements fall through */
    case 11:
        c += ((ub4)k[10] << 24);
        /* Falls through. */
    case 10:
        c += ((ub4)k[9] << 16);
        /* Falls through. */
    case 9:
        c += ((ub4)k[8] << 8);
        /* the first byte of c is reserved for the length */
        /* Falls through. */
    case 8:
        b += ((ub4)k[7] << 24);
        /* Falls through. */
    case 7:
        b += ((ub4)k[6] << 16);
        /* Falls through. */
    case 6:
        b += ((ub4)k[5] << 8);
        /* Falls through. */
    case 5:
        b += k[4];
        /* Falls through. */
    case 4:
        a += ((ub4)k[3] << 24);
        /* Falls through. */
    case 3:
        a += ((ub4)k[2] << 16);
        /* Falls through. */
    case 2:
        a += ((ub4)k[1] << 8);
        /* Falls through. */
    case 1:
        a += k[0];
        /* case 0: nothing left to add */
        /* Falls through. */
    }
    mix(a, b, c);
    /*-------------------------------------------- report the result */
    return c;
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif // __UTIL_HASH_C__


#ifndef __UTIL_HASHTABLE_C__
#define __UTIL_HASHTABLE_C__

/*
 * This implements a simple linear probing hash table.
 *
 * Note that it will readjust its size if enough entries are made.
 */

#define MAX_ITERATIONS  (10)
#define MAX_INCREMENT (10000)

struct hashtable_entry_t {
    void* data;
    int64_t key;
};

struct hashtable_t {
    int total_entries;
    int used_entries;
    uint32_t hash_salt;
    struct hashtable_entry_t* entries;
};


typedef struct hashtable_entry_t* hashtable_entry_p;

//static int next_highest_prime(int x);
static int find_key(hashtable_p table, int64_t key);
static int find_empty(hashtable_p table, int64_t key);
static int expand_table(hashtable_p table);


hashtable_p hashtable_create(int initial_capacity)
{
    hashtable_p tab = NULL;

    pdebug(DEBUG_INFO, "Starting");

    if (initial_capacity <= 0) {
        pdebug(DEBUG_WARN, "Size is less than or equal to zero!");
        return NULL;
    }

    tab = (hashtable_p)mem_alloc(sizeof(struct hashtable_t));
    if (!tab) {
        pdebug(DEBUG_ERROR, "Unable to allocate memory for hash table!");
        return NULL;
    }

    tab->total_entries = initial_capacity;
    tab->used_entries = 0;
    tab->hash_salt = (uint32_t)(time_ms()) + (uint32_t)(intptr_t)(tab);

    tab->entries = (struct hashtable_entry_t*)mem_alloc(initial_capacity * (int)sizeof(struct hashtable_entry_t));
    if (!tab->entries) {
        pdebug(DEBUG_ERROR, "Unable to allocate entry array!");
        hashtable_destroy(tab);
        return NULL;
    }

    pdebug(DEBUG_INFO, "Done");

    return tab;
}


void* hashtable_get(hashtable_p table, int64_t key)
{
    int index = 0;
    void* result = NULL;

    pdebug(DEBUG_SPEW, "Starting");

    if (!table) {
        pdebug(DEBUG_WARN, "Hashtable pointer null or invalid.");
        return NULL;
    }

    index = find_key(table, key);
    if (index != PLCTAG_ERR_NOT_FOUND) {
        result = table->entries[index].data;
        pdebug(DEBUG_SPEW, "found data %p", result);
    }
    else {
        pdebug(DEBUG_SPEW, "key not found!");
    }

    pdebug(DEBUG_SPEW, "Done");

    return result;
}


int hashtable_put(hashtable_p table, int64_t key, void* data)
{
    int rc = PLCTAG_STATUS_OK;
    int index = 0;

    pdebug(DEBUG_SPEW, "Starting");

    if (!table) {
        pdebug(DEBUG_WARN, "Hashtable pointer null or invalid.");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* try to find a slot to put the new entry */
    index = find_empty(table, key);
    while (index == PLCTAG_ERR_NOT_FOUND) {
        rc = expand_table(table);
        if (rc != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Unable to expand table to make entry unique!");
            return rc;
        }

        index = find_empty(table, key);
    }

    pdebug(DEBUG_SPEW, "Putting value at index %d", index);

    table->entries[index].key = key;
    table->entries[index].data = data;
    table->used_entries++;

    pdebug(DEBUG_SPEW, "Done.");

    return PLCTAG_STATUS_OK;
}


void* hashtable_get_index(hashtable_p table, int index)
{
    if (!table) {
        pdebug(DEBUG_WARN, "Hashtable pointer null or invalid");
        return NULL;
    }

    if (index < 0 || index >= table->total_entries) {
        pdebug(DEBUG_WARN, "Out of bounds index!");
        return NULL;
    }

    return table->entries[index].data;
}



int hashtable_capacity(hashtable_p table)
{
    if (!table) {
        pdebug(DEBUG_WARN, "Hashtable pointer null or invalid");
        return PLCTAG_ERR_NULL_PTR;
    }

    return table->total_entries;
}



int hashtable_entries(hashtable_p table)
{
    if (!table) {
        pdebug(DEBUG_WARN, "Hashtable pointer null or invalid");
        return PLCTAG_ERR_NULL_PTR;
    }

    return table->used_entries;
}



int hashtable_on_each(hashtable_p table, int (*callback_func)(hashtable_p table, int64_t key, void* data, void* context), void* context_arg)
{
    int rc = PLCTAG_STATUS_OK;

    if (!table) {
        pdebug(DEBUG_WARN, "Hashtable pointer null or invalid");
    }

    for (int i = 0; i < table->total_entries && rc == PLCTAG_STATUS_OK; i++) {
        if (table->entries[i].data) {
            rc = callback_func(table, table->entries[i].key, table->entries[i].data, context_arg);
        }
    }

    return rc;
}



void* hashtable_remove(hashtable_p table, int64_t key)
{
    int index = 0;
    void* result = NULL;

    pdebug(DEBUG_DETAIL, "Starting");

    if (!table) {
        pdebug(DEBUG_WARN, "Hashtable pointer null or invalid.");
        return result;
    }

    index = find_key(table, key);
    if (index == PLCTAG_ERR_NOT_FOUND) {
        pdebug(DEBUG_SPEW, "Not found.");
        return result;
    }

    result = table->entries[index].data;
    table->entries[index].key = 0;
    table->entries[index].data = NULL;
    table->used_entries--;

    pdebug(DEBUG_DETAIL, "Done");

    return result;
}




int hashtable_destroy(hashtable_p table)
{
    pdebug(DEBUG_INFO, "Starting");

    if (!table) {
        pdebug(DEBUG_WARN, "Called with null pointer!");
        return PLCTAG_ERR_NULL_PTR;
    }

    mem_free(table->entries);
    table->entries = NULL;

    mem_free(table);

    pdebug(DEBUG_INFO, "Done");

    return PLCTAG_STATUS_OK;
}





/***********************************************************************
 *************************** Helper Functions **************************
 **********************************************************************/


#define KEY_TO_INDEX(t, k) (uint32_t)((hash((uint8_t*)&k, sizeof(k), t->hash_salt)) % (uint32_t)(t->total_entries))


int find_key(hashtable_p table, int64_t key)
{
    uint32_t initial_index = KEY_TO_INDEX(table, key);
    int index = 0;
    int iteration = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    /*
     * search for the hash value.
     *
     * Note: do NOT stop if we find a NULL entry.  That might be the result
     * of a removed entry and there could still be entries past that point
     * that are for the initial slot.
     */
    for (iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
        index = ((int)initial_index + iteration) % table->total_entries;

        if (table->entries[index].key == key) {
            break;
        }
    }

    if (iteration >= MAX_ITERATIONS) {
        /* FIXME - does not work on Windows. */
        //pdebug(DEBUG_SPEW, "Key %ld not found.", key);
        return PLCTAG_ERR_NOT_FOUND;
    }
    else {
        //pdebug(DEBUG_SPEW, "Key %d found at index %d.", (int)table->entries[index].key, index);
    }

    pdebug(DEBUG_SPEW, "Done.");

    return index;
}




int find_empty(hashtable_p table, int64_t key)
{
    uint32_t initial_index = KEY_TO_INDEX(table, key);
    int index = 0;
    int iteration = 0;

    pdebug(DEBUG_SPEW, "Starting.");

    /* search for the hash value. */
    for (iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
        index = ((int)initial_index + iteration) % table->total_entries;

        pdebug(DEBUG_SPEW, "Trying index %d for key %ld.", index, key);
        if (table->entries[index].data == NULL) {
            break;
        }
    }

    if (iteration >= MAX_ITERATIONS) {
        pdebug(DEBUG_SPEW, "No empty entry found in %d iterations!", MAX_ITERATIONS);
        return PLCTAG_ERR_NOT_FOUND;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return index;
}




int expand_table(hashtable_p table)
{
    struct hashtable_t new_table;
    int total_entries = table->total_entries;
    int index = PLCTAG_ERR_NOT_FOUND;

    pdebug(DEBUG_SPEW, "Starting.");

    pdebug(DEBUG_SPEW, "Table using %d entries of %d.", table->used_entries, table->total_entries);

    do {
        /* double entries unless already at max doubling, then increment. */
        total_entries += (total_entries < MAX_INCREMENT ? total_entries : MAX_INCREMENT);

        new_table.total_entries = total_entries;
        new_table.used_entries = 0;
        new_table.hash_salt = table->hash_salt;

        pdebug(DEBUG_SPEW, "trying new size = %d", total_entries);

        new_table.entries = (struct hashtable_entry_t*)mem_alloc(total_entries * (int)sizeof(struct hashtable_entry_t));
        if (!new_table.entries) {
            pdebug(DEBUG_ERROR, "Unable to allocate new entry array!");
            return PLCTAG_ERR_NO_MEM;
        }

        /* copy the old entries.  Only copy ones that are used. */
        for (int i = 0; i < table->total_entries; i++) {
            if (table->entries[i].data) {
                index = find_empty(&new_table, table->entries[i].key);
                if (index == PLCTAG_ERR_NOT_FOUND) {
                    /* oops, still cannot insert all the entries! Try again. */
                    pdebug(DEBUG_DETAIL, "Unable to insert existing entry into expanded table. Retrying.");
                    mem_free(new_table.entries);
                    break;
                }
                else {
                    /* store the entry into the new table. */
                    new_table.entries[index] = table->entries[i];
                    new_table.used_entries++;
                }
            }
        }
    } while (index == PLCTAG_ERR_NOT_FOUND);

    /* success! */
    mem_free(table->entries);
    table->entries = new_table.entries;
    table->total_entries = new_table.total_entries;
    table->used_entries = new_table.used_entries;

    pdebug(DEBUG_SPEW, "Done.");

    return PLCTAG_STATUS_OK;
}

#endif // __UTIL_HASHTABLE_C__


#ifndef __UTIL_RC_C__
#define __UTIL_RC_C__

//~ #ifndef container_of
//~ #define container_of(ptr, type, member) ((type *)((char *)(1 ? (ptr) : &((type *)0)->member) - offsetof(type, member)))
//~ #endif




/*
 * Handle clean up functions.
 */

 //typedef struct cleanup_t *cleanup_p;
 //
 //struct cleanup_t {
 //    cleanup_p next;
 //    const char *function_name;
 //    int line_num;
 //    int extra_arg_count;
 //    void **extra_args;
 //    rc_cleanup_func cleanup_func;
 //    void *dummy[]; /* force alignment */
 //};


 /*
  * This is a rc struct that we use to make sure that we are able to align
  * the remaining part of the allocated block.
  */

struct refcount_t {
    lock_t lock;
    int count;
    const char* function_name;
    int line_num;
    //cleanup_p cleaners;
    rc_cleanup_func cleanup_func;

    /* FIXME - needed for alignment, this is a hack! */
    union {
        uint8_t dummy_u8;
        uint16_t dummy_u16;
        uint32_t dummy_u32;
        uint64_t dummy_u64;
        double dummy_double;
        void* dummy_ptr;
        void (*dummy_func)(void);
    } dummy_align[];
};


typedef struct refcount_t* refcount_p;

static void refcount_cleanup(refcount_p rc);
//static cleanup_p cleanup_entry_create(const char *func, int line_num, rc_cleanup_func cleaner, int extra_arg_count, va_list extra_args);
//static void cleanup_entry_destroy(cleanup_p entry);



/*
 * rc_alloc
 *
 * Create a reference counted control for the requested data size.  Return a strong
 * reference to the data.
 */
 //void *rc_alloc_impl(const char *func, int line_num, int data_size, int extra_arg_count, rc_cleanup_func cleaner_func, ...)
void* rc_alloc_impl(const char* func, int line_num, int data_size, rc_cleanup_func cleaner_func)
{
    refcount_p rc = NULL;
    //cleanup_p cleanup = NULL;
    //va_list extra_args;

    pdebug(DEBUG_INFO, "Starting, called from %s:%d", func, line_num);

    pdebug(DEBUG_SPEW, "Allocating %d-byte refcount struct", (int)sizeof(struct refcount_t));

    rc = (refcount_p)mem_alloc((int)sizeof(struct refcount_t) + data_size);
    if (!rc) {
        pdebug(DEBUG_WARN, "Unable to allocate refcount struct!");
        return NULL;
    }

    rc->count = 1;  /* start with a reference count. */
    rc->lock = LOCK_INIT;

    rc->cleanup_func = cleaner_func;

    /* store where we were called from for later. */
    rc->function_name = func;
    rc->line_num = line_num;

    pdebug(DEBUG_INFO, "Done");

    /* return the original address if successful otherwise NULL. */

    /* DEBUG */
    pdebug(DEBUG_DETAIL, "Returning memory pointer %p", (char*)(rc + 1));

    return (char*)(rc + 1);
}








/*
 * Increments the ref count if the reference is valid.
 *
 * It returns the original poiner if the passed pointer was valid.  It returns
 * NULL if the passed pointer was invalid.
 *
 * This is for usage like:
 * my_struct->some_field_ref = rc_inc(ref);
 */

void* rc_inc_impl(const char* func, int line_num, void* data)
{
    int count = 0;
    refcount_p rc = NULL;
    char* result = NULL;

    pdebug(DEBUG_SPEW, "Starting, called from %s:%d for %p", func, line_num, data);

    if (!data) {
        pdebug(DEBUG_SPEW, "Invalid pointer passed from %s:%d!", func, line_num);
        return result;
    }

    /* get the refcount structure. */
    rc = ((refcount_p)data) - 1;

    /* spin until we have ownership */
    spin_block(&rc->lock) {
        if (rc->count > 0) {
            rc->count++;
            count = rc->count;
            result = (char*)data;
        }
        else {
            count = rc->count;
            result = NULL;
        }
    }

    if (!result) {
        pdebug(DEBUG_SPEW, "Invalid ref count (%d) from call at %s line %d!  Unable to take strong reference.", count, func, line_num);
    }
    else {
        pdebug(DEBUG_SPEW, "Ref count is %d for %p.", count, data);
    }

    /* return the result pointer. */
    return result;
}



/*
 * Decrement the ref count.
 *
 * This is for usage like:
 * my_struct->some_field = rc_dec(rc_obj);
 *
 * Note that the final clean up function _MUST_ free the data pointer
 * passed to it.   It must clean up anything referenced by that data,
 * and the block itself using mem_free() or the appropriate function;
 */

void* rc_dec_impl(const char* func, int line_num, void* data)
{
    int count = 0;
    int invalid = 0;
    refcount_p rc = NULL;

    pdebug(DEBUG_SPEW, "Starting, called from %s:%d for %p", func, line_num, data);

    if (!data) {
        pdebug(DEBUG_SPEW, "Null reference passed from %s:%d!", func, line_num);
        return NULL;
    }

    /* get the refcount structure. */
    rc = ((refcount_p)data) - 1;

    /* do this sorta atomically */
    spin_block(&rc->lock) {
        if (rc->count > 0) {
            rc->count--;
            count = rc->count;
        }
        else {
            count = rc->count;
            invalid = 1;
        }
    }

    if (invalid) {
        pdebug(DEBUG_WARN, "Reference has invalid count %d!", count);
    }
    else {
        pdebug(DEBUG_SPEW, "Ref count is %d for %p.", count, data);

        /* clean up only if count is zero. */
        if (rc && count <= 0) {
            pdebug(DEBUG_DETAIL, "Calling cleanup functions due to call at %s:%d for %p.", func, line_num, data);

            refcount_cleanup(rc);
        }
    }

    return NULL;
}




void refcount_cleanup(refcount_p rc)
{
    pdebug(DEBUG_INFO, "Starting");
    if (!rc) {
        pdebug(DEBUG_WARN, "Refcount is NULL!");
        return;
    }

    /* call the clean up function */
    rc->cleanup_func((void*)(rc + 1));

    /* finally done. */
    mem_free(rc);

    pdebug(DEBUG_INFO, "Done.");
}

#endif // __UTIL_RC_C__


#ifndef __UTIL_VECTOR_C__
#define __UTIL_VECTOR_C__

struct vector_t {
    int len;
    int capacity;
    int max_inc;
    void** data;
};



static int ensure_capacity(vector_p vec, int capacity);


vector_p vector_create(int capacity, int max_inc)
{
    vector_p vec = NULL;

    pdebug(DEBUG_SPEW, "Starting");

    if (capacity <= 0) {
        pdebug(DEBUG_WARN, "Called with negative capacity!");
        return NULL;
    }

    if (max_inc <= 0) {
        pdebug(DEBUG_WARN, "Called with negative maximum size increment!");
        return NULL;
    }

    vec = (vector_p)mem_alloc((int)sizeof(struct vector_t));
    if (!vec) {
        pdebug(DEBUG_ERROR, "Unable to allocate memory for vector!");
        return NULL;
    }

    vec->len = 0;
    vec->capacity = capacity;
    vec->max_inc = max_inc;

    vec->data = (void**)mem_alloc(capacity * (int)sizeof(void*));
    if (!vec->data) {
        pdebug(DEBUG_ERROR, "Unable to allocate memory for vector data!");
        vector_destroy(vec);
        return NULL;
    }

    pdebug(DEBUG_SPEW, "Done");

    return vec;
}



int vector_length(vector_p vec)
{
    pdebug(DEBUG_SPEW, "Starting");

    /* check to see if the vector ref is valid */
    if (!vec) {
        pdebug(DEBUG_WARN, "Null pointer or invalid pointer to vector passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    pdebug(DEBUG_SPEW, "Done");

    return vec->len;
}



int vector_put(vector_p vec, int index, void* data)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW, "Starting");

    /* check to see if the vector ref is valid */
    if (!vec) {
        pdebug(DEBUG_WARN, "Null pointer or invalid pointer to vector passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    if (index < 0) {
        pdebug(DEBUG_WARN, "Index is negative!");
        return PLCTAG_ERR_OUT_OF_BOUNDS;
    }

    rc = ensure_capacity(vec, index + 1);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to ensure capacity!");
        return rc;
    }

    /* reference the new data. */
    vec->data[index] = data;

    /* adjust the length, if needed */
    if (index >= vec->len) {
        vec->len = index + 1;
    }

    pdebug(DEBUG_SPEW, "Done");

    return rc;
}


void* vector_get(vector_p vec, int index)
{
    pdebug(DEBUG_SPEW, "Starting");

    /* check to see if the vector ref is valid */
    if (!vec) {
        pdebug(DEBUG_WARN, "Null pointer or invalid pointer to vector passed!");
        return NULL;
    }

    if (index < 0 || index >= vec->len) {
        pdebug(DEBUG_WARN, "Index is out of bounds!");
        return NULL;
    }

    pdebug(DEBUG_SPEW, "Done");

    return vec->data[index];
}


void* vector_remove(vector_p vec, int index)
{
    void* result = NULL;

    pdebug(DEBUG_SPEW, "Starting");

    /* check to see if the vector ref is valid */
    if (!vec) {
        pdebug(DEBUG_WARN, "Null pointer or invalid pointer to vector passed!");
        return NULL;
    }

    if (index < 0 || index >= vec->len) {
        pdebug(DEBUG_WARN, "Index is out of bounds!");
        return NULL;
    }

    /* get the value in this slot before we overwrite it. */
    result = vec->data[index];

    /* move the rest of the data over this. */
    mem_move(&vec->data[index], &vec->data[index + 1], (int)((sizeof(void*) * (size_t)(vec->len - index - 1))));

    /* make sure that we do not have old data hanging around. */
    vec->data[vec->len - 1] = NULL;

    /* adjust the length to the new size */
    vec->len--;

    pdebug(DEBUG_SPEW, "Done");

    return result;
}



int vector_destroy(vector_p vec)
{
    pdebug(DEBUG_SPEW, "Starting.");

    if (!vec) {
        pdebug(DEBUG_WARN, "Null pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    mem_free(vec->data);
    mem_free(vec);

    pdebug(DEBUG_SPEW, "Done.");

    return PLCTAG_STATUS_OK;
}



/***********************************************************************
 *************** Private Helper Functions ******************************
 **********************************************************************/



int ensure_capacity(vector_p vec, int capacity)
{
    int new_inc = 0;
    void** new_data = NULL;

    if (!vec) {
        pdebug(DEBUG_WARN, "Null pointer or invalid pointer to vector passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* is there anything to do? */
    if (capacity <= vec->capacity) {
        /* release the reference */
        return PLCTAG_STATUS_OK;
    }

    /* calculate the new capacity
     *
     * Start by guessing 50% larger.  Clamp that against 1 at the
     * low end and the max increment passed when the vector was created.
     */
    new_inc = vec->capacity / 2;

    if (new_inc > vec->max_inc) {
        new_inc = vec->max_inc;
    }

    if (new_inc < 1) {
        new_inc = 1;
    }

    /* allocate the new data area */
    new_data = (void**)mem_alloc((int)((sizeof(void*) * (size_t)(vec->capacity + new_inc))));
    if (!new_data) {
        pdebug(DEBUG_ERROR, "Unable to allocate new data area!");
        return PLCTAG_ERR_NO_MEM;
    }

    mem_copy(new_data, vec->data, (int)((size_t)(vec->capacity) * sizeof(void*)));

    mem_free(vec->data);

    vec->data = new_data;

    vec->capacity += new_inc;

    return PLCTAG_STATUS_OK;
}

#endif // __UTIL_VECTOR_C__



#ifdef __cplusplus
}
#endif