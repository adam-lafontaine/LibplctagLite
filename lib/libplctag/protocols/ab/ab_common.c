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

#include <ctype.h>
#include <limits.h>
#include <float.h>

#include "ab_vtable.h"
#include "ab_byte_order.h"

#ifdef __cplusplus
extern "C" {
#endif


/*
 * Externally visible global variables
 */

//volatile ab_session_p sessions = NULL;
//volatile mutex_p global_session_mut = NULL;
//
//volatile vector_p read_group_tags = NULL;


/* request/response handling thread */
volatile thread_p io_handler_thread = NULL;

volatile int ab_protocol_terminating = 0;



/*
 * Generic Rockwell/Allen-Bradley protocol functions.
 *
 * These are the primary entry points into the AB protocol
 * stack.
 */


#define DEFAULT_NUM_RETRIES (5)
#define DEFAULT_RETRY_INTERVAL (300)


/* forward declarations*/
static int get_tag_data_type(ab_tag_p tag, attr attribs);

static void ab_tag_destroy(ab_tag_p tag);
static int default_abort(plc_tag_p tag);
static int default_read(plc_tag_p tag);
static int default_status(plc_tag_p tag);
static int default_tickler(plc_tag_p tag);
static int default_write(plc_tag_p tag);


/* vtables for different kinds of tags */
struct tag_vtable_t default_vtable = {
    default_abort,
    default_read,
    default_status,
    default_tickler,
    //default_write,
    //(tag_vtable_func)NULL, /* this is not portable! */

    /* attribute accessors */
    ab_get_int_attrib,
    ab_set_int_attrib
};


/*
 * Public functions.
 */


int ab_init(void)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO,"Initializing AB protocol library.");

    ab_protocol_terminating = 0;

    if((rc = session_startup()) != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to initialize session library!");
        return rc;
    }

    pdebug(DEBUG_INFO,"Finished initializing AB protocol library.");

    return rc;
}

/*
 * called when the whole program is going to terminate.
 */
void ab_teardown(void)
{
    pdebug(DEBUG_INFO,"Releasing global AB protocol resources.");

    if(io_handler_thread) {
        pdebug(DEBUG_INFO,"Terminating IO thread.");
        /* signal the IO thread to quit first. */
        ab_protocol_terminating = 1;

        /* wait for the thread to die */
        thread_join(io_handler_thread);
        thread_destroy((thread_p*)&io_handler_thread);
    } else {
        pdebug(DEBUG_INFO, "IO thread already stopped.");
    }

    pdebug(DEBUG_INFO,"Freeing session information.");

    session_teardown();

    ab_protocol_terminating = 0;

    pdebug(DEBUG_INFO,"Done.");
}



plc_tag_p ab_tag_create(attr attribs)
{
    ab_tag_p tag = AB_TAG_NULL;
    const char *path = NULL;
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO,"Starting.");

    /*
     * allocate memory for the new tag.  Do this first so that
     * we have a vehicle for returning status.
     */

    tag = (ab_tag_p)rc_alloc(sizeof(struct ab_tag_t), (rc_cleanup_func)ab_tag_destroy);
    if(!tag) {
        pdebug(DEBUG_ERROR,"Unable to allocate memory for AB EIP tag!");
        return (plc_tag_p)NULL;
    }

    pdebug(DEBUG_DETAIL, "tag=%p", tag);

    /*
     * we got far enough to allocate memory, set the default vtable up
     * in case we need to abort later.
     */

    tag->vtable = &default_vtable;

    /* set up the generic parts. */
    rc = plc_tag_generic_init_tag((plc_tag_p)tag, attribs);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Unable to initialize generic tag parts!");
        rc_dec(tag);
        return (plc_tag_p)NULL;
    }

    /* set up any required settings based on the cpu type. */
    /* default to requiring a connection and allowing packing. */
    tag->use_connected_msg = attr_get_int(attribs, "use_connected_msg", 1);
    tag->allow_packing = attr_get_int(attribs, "allow_packing", 1);

    /* make sure that the connection requirement is forced. */
    attr_set_int(attribs, "use_connected_msg", tag->use_connected_msg);

    /* get the connection path.  We need this to make a decision about the PLC. */
    path = attr_get_str(attribs,"path",NULL);

    /*
     * Find or create a session.
     *
     * All tags need sessions.  They are the TCP connection to the gateway PLC.
     */
    if(session_find_or_create(&tag->session, attribs) != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_INFO,"Unable to create session!");
        tag->status = PLCTAG_ERR_BAD_GATEWAY;
        return (plc_tag_p)tag;
    }

    pdebug(DEBUG_DETAIL, "using session=%p", tag->session);

    /* get the tag data type, or try. */
    rc = get_tag_data_type(tag, attribs);
    if(rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "Error %s getting tag element data type or handling special tag!", plc_tag_decode_error(rc));
        tag->status = (int8_t)rc;
        return (plc_tag_p)tag;
    }

    /* set up PLC-specific information. */
    pdebug(DEBUG_DETAIL, "Setting up Logix tag.");

    /* Logix tags need a path. */
    if (path == NULL) {
        pdebug(DEBUG_WARN, "A path is required for Logix-class PLCs!");
        tag->status = PLCTAG_ERR_BAD_PARAM;
        return (plc_tag_p)tag;
    }

    /* if we did not fill in the byte order elsewhere, fill it in now. */
    if (!tag->byte_order) {
        pdebug(DEBUG_DETAIL, "Using default Logix byte order.");
        //tag->byte_order = &logix_tag_byte_order;
        tag->byte_order = logix_tag_byte_order();
    }

    /* if this was not filled in elsewhere default to Logix */
    if (tag->vtable == &default_vtable || !tag->vtable) {
        pdebug(DEBUG_DETAIL, "Setting default Logix vtable.");
        //tag->vtable = &eip_cip_vtable;
        tag->vtable = eip_cip_vtable();
    }

    /* default to requiring a connection. */
    tag->use_connected_msg = attr_get_int(attribs, "use_connected_msg", 1);
    tag->allow_packing = attr_get_int(attribs, "allow_packing", 1);

    /* pass the connection requirement since it may be overridden above. */
    attr_set_int(attribs, "use_connected_msg", tag->use_connected_msg);

    /* get the element count, default to 1 if missing. */
    tag->elem_count = attr_get_int(attribs,"elem_count", 1);

    /* fill this in when we read the tag. */
        //tag->elem_size = 0;
    tag->size = 0;
    tag->data = NULL;

    /*
     * check the tag name, this is protocol specific.
     */

    if(!tag->special_tag && check_tag_name(tag, attr_get_str(attribs,"name",NULL)) != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_INFO,"Bad tag name!");
        tag->status = PLCTAG_ERR_BAD_PARAM;
        return (plc_tag_p)tag;
    }

    /* kick off a read to get the tag type and size. */
    if(!tag->special_tag && tag->vtable->read) {
        /* trigger the first read. */
        pdebug(DEBUG_DETAIL, "Kicking off initial read.");

        tag->first_read = 1;
        tag->read_in_flight = 1;
        tag->vtable->read((plc_tag_p)tag);
        //tag_raise_event((plc_tag_p)tag, PLCTAG_EVENT_READ_STARTED, tag->status);
    } else {
        pdebug(DEBUG_DETAIL, "Not kicking off initial read: tag is special or does not have read function.");
    }

    pdebug(DEBUG_DETAIL, "Using vtable %p.", tag->vtable);

    pdebug(DEBUG_INFO,"Done.");

    return (plc_tag_p)tag;
}


/*
 * determine the tag's data type and size.  Or at least guess it.
 */

int get_tag_data_type(ab_tag_p tag, attr attribs)
{
    int rc = PLCTAG_STATUS_OK;
    const char *elem_type = NULL;
    const char *tag_name = NULL;

    pdebug(DEBUG_DETAIL, "Starting.");

    /* look for the elem_type attribute. */
    elem_type = attr_get_str(attribs, "elem_type", NULL);

    if (elem_type) {
        if (str_cmp_i(elem_type, "lint") == 0 || str_cmp_i(elem_type, "ulint") == 0) {
            pdebug(DEBUG_DETAIL, "Found tag element type of 64-bit integer.");
            tag->elem_size = 8;
            tag->elem_type = AB_TYPE_INT64;
        }
        else if (str_cmp_i(elem_type, "dint") == 0 || str_cmp_i(elem_type, "udint") == 0) {
            pdebug(DEBUG_DETAIL, "Found tag element type of 32-bit integer.");
            tag->elem_size = 4;
            tag->elem_type = AB_TYPE_INT32;
        }
        else if (str_cmp_i(elem_type, "int") == 0 || str_cmp_i(elem_type, "uint") == 0) {
            pdebug(DEBUG_DETAIL, "Found tag element type of 16-bit integer.");
            tag->elem_size = 2;
            tag->elem_type = AB_TYPE_INT16;
        }
        else if (str_cmp_i(elem_type, "sint") == 0 || str_cmp_i(elem_type, "usint") == 0) {
            pdebug(DEBUG_DETAIL, "Found tag element type of 8-bit integer.");
            tag->elem_size = 1;
            tag->elem_type = AB_TYPE_INT8;
        }
        else if (str_cmp_i(elem_type, "bool") == 0) {
            pdebug(DEBUG_DETAIL, "Found tag element type of bit.");
            tag->elem_size = 1;
            tag->elem_type = AB_TYPE_BOOL;
        }
        else if (str_cmp_i(elem_type, "bool array") == 0) {
            pdebug(DEBUG_DETAIL, "Found tag element type of bool array.");
            tag->elem_size = 4;
            tag->elem_type = AB_TYPE_BOOL_ARRAY;
        }
        else if (str_cmp_i(elem_type, "real") == 0) {
            pdebug(DEBUG_DETAIL, "Found tag element type of 32-bit float.");
            tag->elem_size = 4;
            tag->elem_type = AB_TYPE_FLOAT32;
        }
        else if (str_cmp_i(elem_type, "lreal") == 0) {
            pdebug(DEBUG_DETAIL, "Found tag element type of 64-bit float.");
            tag->elem_size = 8;
            tag->elem_type = AB_TYPE_FLOAT64;
        }
        else if (str_cmp_i(elem_type, "string") == 0) {
            pdebug(DEBUG_DETAIL, "Fount tag element type of string.");
            tag->elem_size = 88;
            tag->elem_type = AB_TYPE_STRING;
        }
        else if (str_cmp_i(elem_type, "short string") == 0) {
            pdebug(DEBUG_DETAIL, "Found tag element type of short string.");
            tag->elem_size = 256; /* TODO - find the real length */
            tag->elem_type = AB_TYPE_SHORT_STRING;
        }
        else {
            pdebug(DEBUG_DETAIL, "Unknown tag type %s", elem_type);
            return PLCTAG_ERR_UNSUPPORTED;
        }
    }
    else
    {
        /*
         * We have two cases
         *      * tag listing, but only for CIP PLCs (but not for UDTs!).
         *      * no type, just elem_size.
         * Otherwise this is an error.
         */
        int elem_size = attr_get_int(attribs, "elem_size", 0);


        const char* tmp_tag_name = attr_get_str(attribs, "name", NULL);
        int special_tag_rc = PLCTAG_STATUS_OK;

        /* check for special tags. */
        if (str_str_cmp_i(tmp_tag_name, "@tags"))
        {
            special_tag_rc = setup_tag_listing_tag(tag, tmp_tag_name);
        }
        else if (str_str_cmp_i(tmp_tag_name, "@udt/")) 
        {
            special_tag_rc = setup_udt_tag(tag, tmp_tag_name);
        } 

        /* else not a special tag. */

        if (special_tag_rc != PLCTAG_STATUS_OK) {
            pdebug(DEBUG_WARN, "Error parsing tag listing name!");
            return special_tag_rc;
        }


        /* if we did not set an element size yet, set one. */
        if (tag->elem_size == 0) {
            if (elem_size > 0) {
                pdebug(DEBUG_INFO, "Setting element size to %d.", elem_size);
                tag->elem_size = elem_size;
            }
        }
        else {
            if (elem_size > 0) {
                pdebug(DEBUG_WARN, "Tag has elem_size and either is a tag listing or has elem_type, only use one!");
            }
        }
    }

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}


int default_abort(plc_tag_p tag)
{
    (void)tag;

    pdebug(DEBUG_WARN, "This should be overridden by a PLC-specific function!");

    return PLCTAG_ERR_NOT_IMPLEMENTED;
}


int default_read(plc_tag_p tag)
{
    (void)tag;

    pdebug(DEBUG_WARN, "This should be overridden by a PLC-specific function!");

    return PLCTAG_ERR_NOT_IMPLEMENTED;
}

int default_status(plc_tag_p tag)
{
    pdebug(DEBUG_WARN, "This should be overridden by a PLC-specific function!");

    if(tag) {
        return tag->status;
    } else {
        return PLCTAG_ERR_NOT_FOUND;
    }
}


int default_tickler(plc_tag_p tag)
{
    (void)tag;

    pdebug(DEBUG_WARN, "This should be overridden by a PLC-specific function!");

    return PLCTAG_STATUS_OK;
}



int default_write(plc_tag_p tag)
{
    (void)tag;

    pdebug(DEBUG_WARN, "This should be overridden by a PLC-specific function!");

    return PLCTAG_ERR_NOT_IMPLEMENTED;
}



/*
* ab_tag_abort
*
* This does the work of stopping any inflight requests.
* This is not thread-safe.  It must be called from a function
* that locks the tag's mutex or only from a single thread.
*/

int ab_tag_abort(ab_tag_p tag)
{
    pdebug(DEBUG_DETAIL, "Starting.");

    if(tag->req) {
        spin_block(&tag->req->lock) {
            tag->req->abort_request = 1;
        }

        tag->req = (ab_request_p)rc_dec(tag->req);
    } else {
        pdebug(DEBUG_DETAIL, "Called without a request in flight.");
    }

    tag->read_in_progress = 0;
    tag->offset = 0;

    pdebug(DEBUG_DETAIL, "Done.");

    return PLCTAG_STATUS_OK;
}




/*
 * ab_tag_status
 *
 * Generic status checker.   May be overridden by individual PLC types.
 */
int ab_tag_status(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    if (tag->read_in_progress) {
        return PLCTAG_STATUS_PENDING;
    }

    if(tag->session) {
        rc = tag->status;
    } else {
        /* this is not OK.  This is fatal! */
        rc = PLCTAG_ERR_CREATE;
    }

    return rc;
}






/*
 * ab_tag_destroy
 *
 * This blocks on the global library mutex.  This should
 * be fixed to allow for more parallelism.  For now, safety is
 * the primary concern.
 */

void ab_tag_destroy(ab_tag_p tag)
{
    ab_session_p session = NULL;

    pdebug(DEBUG_INFO, "Starting.");

    /* already destroyed? */
    if (!tag) {
        pdebug(DEBUG_WARN,"Tag pointer is null!");

        return;
    }

    /* abort anything in flight */
    ab_tag_abort(tag);

    session = tag->session;

    /* tags should always have a session.  Release it. */
    pdebug(DEBUG_DETAIL,"Getting ready to release tag session %p",tag->session);
    if(session) {
        pdebug(DEBUG_DETAIL, "Removing tag from session.");
        rc_dec(session);
        tag->session = NULL;
    } else {
        pdebug(DEBUG_WARN,"No session pointer!");
    }

    if(tag->ext_mutex) {
        mutex_destroy(&(tag->ext_mutex));
        tag->ext_mutex = NULL;
    }

    if(tag->api_mutex) {
        mutex_destroy(&(tag->api_mutex));
        tag->api_mutex = NULL;
    }

    if(tag->tag_cond_wait) {
        cond_destroy(&(tag->tag_cond_wait));
        tag->tag_cond_wait = NULL;
    }

    if(tag->byte_order && tag->byte_order->is_allocated) {
        mem_free(tag->byte_order);
        tag->byte_order = NULL;
    }

    if (tag->data) {
        mem_free(tag->data);
        tag->data = NULL;
    }

    pdebug(DEBUG_INFO,"Finished releasing all tag resources.");

    pdebug(DEBUG_INFO, "done");
}


int ab_get_int_attrib(plc_tag_p raw_tag, const char *attrib_name, int default_value)
{
    int res = default_value;
    ab_tag_p tag = (ab_tag_p)raw_tag;

    pdebug(DEBUG_SPEW, "Starting.");

    /* assume we have a match. */
    tag->status = PLCTAG_STATUS_OK;

    /* match the attribute. */
    if(str_cmp_i(attrib_name, "elem_size") == 0) {
        res = tag->elem_size;
    } else if(str_cmp_i(attrib_name, "elem_count") == 0) {
        res = tag->elem_count;
    } else if(str_cmp_i(attrib_name, "elem_type") == 0) {
        res = (int)(tag->elem_type);
    } else {
        pdebug(DEBUG_WARN, "Unsupported attribute name \"%s\"!", attrib_name);
        tag->status = PLCTAG_ERR_UNSUPPORTED;
    }

    return res;
}


int ab_set_int_attrib(plc_tag_p raw_tag, const char *attrib_name, int new_value)
{
    (void)attrib_name;
    (void)new_value;

    pdebug(DEBUG_WARN, "Unsupported attribute \"%s\"!", attrib_name);

    raw_tag->status  = PLCTAG_ERR_UNSUPPORTED;

    return PLCTAG_ERR_UNSUPPORTED;
}


int check_tag_name(ab_tag_p tag, const char* name)
{
    int rc = PLCTAG_STATUS_OK;

    if (!name) {
        pdebug(DEBUG_WARN,"No tag name parameter found!");
        return PLCTAG_ERR_BAD_PARAM;
    }

    /* attempt to parse the tag name */
    if ((rc = cip_encode_tag_name(tag, name)) != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN, "parse of CIP-style tag name %s failed!", name);

        return rc;
    }

    return PLCTAG_STATUS_OK;
}





/**
 * @brief Check the status of the read request
 * 
 * This function checks the request itself and updates the
 * tag if there are any failures or changes that need to be
 * made due to the request status.
 * 
 * The tag and the request must not be deleted out from underneath
 * this function.   Ideally both are held with write mutexes.
 * 
 * @return status of the request.
 * 
 */

int check_read_request_status(ab_tag_p tag, ab_request_p request)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!request) {
        tag->read_in_progress = 0;
        tag->offset = 0;

        pdebug(DEBUG_WARN,"Read in progress, but no request in flight!");

        return PLCTAG_ERR_READ;
    } 

    /* we now have a valid reference to the request. */

    /* request can be used by more than one thread at once. */
    spin_block(&request->lock) {
        if(!request->resp_received) {
            rc = PLCTAG_STATUS_PENDING;
            break;
        }

        /* check to see if it was an abort on the session side. */
        if(request->status != PLCTAG_STATUS_OK) {
            rc = request->status;
            request->abort_request = 1;

            pdebug(DEBUG_WARN,"Session reported failure of request: %s.", plc_tag_decode_error(rc));

            tag->read_in_progress = 0;
            tag->offset = 0;

            /* TODO - why is this here? */
            tag->size = tag->elem_count * tag->elem_size;

            break;
        }
    }

    if(rc != PLCTAG_STATUS_OK) {
        if(rc_is_error(rc)) {
            /* the request is dead, from session side. */
            tag->read_in_progress = 0;
            tag->offset = 0;

            tag->req = NULL; 
        }

        pdebug(DEBUG_DETAIL, "Read not ready with status %s.", plc_tag_decode_error(rc));

        return rc;
    }

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}



#ifdef __cplusplus
}
#endif


