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

#include "../../libplctag_internal.h"

#ifdef __cplusplus
extern "C" {
#endif


/* tag listing packet format is as follows for controller tags:

CIP Tag Info command
    uint8_t request_service    0x55
    uint8_t request_path_size  3 - 6 bytes
    uint8_t   0x20    get class
    uint8_t   0x6B    tag info/symbol class
    uint8_t   0x25    get instance (16-bit)
    uint8_t   0x00    padding
    uint8_t   0x00    instance byte 0
    uint8_t   0x00    instance byte 1
    uint16_t  0x04    number of attributes to get
    uint16_t  0x02    attribute #2 - symbol type
    uint16_t  0x07    attribute #7 - base type size (array element) in bytes
    uint16_t  0x08    attribute #8 - array dimensions (3xu32)
    uint16_t  0x01    attribute #1 - symbol name

*/

/* tag listing packet format is as follows for program tags:

CIP Tag Info command
    uint8_t request_service    0x55
    uint8_t request_path_size  N bytes
      uint8_t   0x91    Symbolic segment header
      uint8_t   name_length   Length in bytes.
      uint8_t   name[N] program name, i.e. 'PROGRAM:foobar'
      (uint8_t padding) optional if program name is odd length.
      uint8_t   0x20    get class
      uint8_t   0x6B    tag info/symbol class
      uint8_t   0x25    get instance (16-bit)
      uint8_t   0x00    padding
      uint8_t   0x00    instance byte 0
      uint8_t   0x00    instance byte 1
    uint16_t  0x04    number of attributes to get
    uint16_t  0x02    attribute #2 - symbol type
    uint16_t  0x07    attribute #7 - base type size (array element) in bytes
    uint16_t  0x08    attribute #8 - array dimensions (3xu32)
    uint16_t  0x01    attribute #1 - symbol name

*/

//
//START_PACK typedef struct {
//    uint8_t request_service;    /* AB_EIP_CMD_CIP_LIST_TAGS=0x55 */
//    uint8_t request_path_size;  /* 3 word = 6 bytes */
//    uint8_t request_path[4];    /* MAGIC
//                                    0x20    get class
//                                    0x6B    tag info/symbol class
//                                    0x25    get instance (16-bit)
//                                    0x00    padding
//                                    0x00    instance byte 0
//                                    0x00    instance byte 1
//                                */
//    uint16_le instance_id;      /* actually last two bytes above */
//    uint16_le num_attributes;   /* 0x04    number of attributes to get */
//    uint16_le requested_attributes[4];  /*
//                                            0x02 attribute #2 - symbol type
//                                            0x07 attribute #7 - base type size (array element) in bytes
//                                            0x08    attribute #8 - array dimensions (3xu32)
//                                            0x01    attribute #1 - symbol name
//                                        */
//
//} END_PACK tag_list_req_DEAD;

/*
 * This is a pseudo UDT structure for each tag entry when listing all the tags
 * in a PLC.
 */

START_PACK typedef struct {
        uint32_le instance_id;  /* monotonically increasing but not contiguous */
        uint16_le symbol_type;   /* type of the symbol. */
        uint16_le element_length; /* length of one array element in bytes. */
        uint32_le array_dims[3];  /* array dimensions. */
        uint16_le string_len;   /* string length count. */
        //uint8_t string_name[82]; /* MAGIC string name bytes (string_len of them, zero padded) */
} END_PACK tag_list_entry;



static int build_read_request_connected(ab_tag_p tag, int byte_offset);
//static int build_tag_list_request_connected(ab_tag_p tag);
static int build_read_request_unconnected(ab_tag_p tag, int byte_offset);
static int build_write_request_connected(ab_tag_p tag, int byte_offset);
static int build_write_request_unconnected(ab_tag_p tag, int byte_offset);
static int build_write_bit_request_connected(ab_tag_p tag);
static int build_write_bit_request_unconnected(ab_tag_p tag);
static int check_read_status_connected(ab_tag_p tag);
static int check_read_status_unconnected(ab_tag_p tag);
static int check_write_status_connected(ab_tag_p tag);
static int check_write_status_unconnected(ab_tag_p tag);
static int calculate_write_data_per_packet(ab_tag_p tag);

static int tag_read_start(ab_tag_p tag);
static int tag_tickler(ab_tag_p tag);
static int tag_write_start(ab_tag_p tag);

#if 0
/* define the exported vtable for this tag type. */
struct tag_vtable_t eip_cip_vtable = {
    .abort = (tag_vtable_func)ab_tag_abort, /* shared */
    .read = (tag_vtable_func)tag_read_start,
    .status = (tag_vtable_func)ab_tag_status, /* shared */
    .tickler = (tag_vtable_func)tag_tickler,
    .write = (tag_vtable_func)tag_write_start,
    .wake_plc = (tag_vtable_func)NULL, /* wake_plc */

    /* attribute accessors */
    .get_int_attrib = ab_get_int_attrib,
    .set_int_attrib = ab_set_int_attrib
};

/* default string types used for ControlLogix-class PLCs. */
tag_byte_order_t logix_tag_byte_order = {
    .is_allocated = 0,    

    .str_is_defined = 1,
    .str_is_counted = 1,
    .str_is_fixed_length = 1,
    .str_is_zero_terminated = 0,
    .str_is_byte_swapped = 0,

    .str_count_word_bytes = 4,
    .str_max_capacity = 82,
    .str_total_length = 88,
    .str_pad_bytes = 2,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {0,1,2,3},
    .float64_order = {0,1,2,3,4,5,6,7},
};

/* default string types used for Omron-NJ/NX PLCs. */
tag_byte_order_t omron_njnx_tag_byte_order = {
    .is_allocated = 0,

    .str_is_defined = 1,
    .str_is_counted = 1,
    .str_is_fixed_length = 0,
    .str_is_zero_terminated = 1,
    .str_is_byte_swapped = 0,

    .str_count_word_bytes = 2,
    .str_max_capacity = 0,
    .str_total_length = 0,
    .str_pad_bytes = 0,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {0,1,2,3},
    .float64_order = {0,1,2,3,4,5,6,7},
};


tag_byte_order_t logix_tag_listing_byte_order = {
    .is_allocated = 0,

    .str_is_defined = 1,
    .str_is_counted = 1,
    .str_is_fixed_length = 0,
    .str_is_zero_terminated = 0,
    .str_is_byte_swapped = 0,

    .str_count_word_bytes = 2,
    .str_max_capacity = 0,
    .str_total_length = 0,
    .str_pad_bytes = 0,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {0,1,2,3},
    .float64_order = {0,1,2,3,4,5,6,7},
};
#endif

static tag_vtable eip_cip_vtable_def = {
    .abort = (tag_vtable_func)ab_tag_abort, /* shared */
    .read = (tag_vtable_func)tag_read_start,
    .status = (tag_vtable_func)ab_tag_status, /* shared */
    .tickler = (tag_vtable_func)tag_tickler,

    //.write = (tag_vtable_func)tag_write_start,
    //.wake_plc = (tag_vtable_func)NULL, /* wake_plc */

    /* attribute accessors */
    .get_int_attrib = ab_get_int_attrib,
    .set_int_attrib = ab_set_int_attrib
};


/* default string types used for ControlLogix-class PLCs. */
static tag_byte_order_t logix_tag_byte_order_def = {
    .is_allocated = 0,    

    .str_is_defined = 1,
    .str_is_counted = 1,
    .str_is_fixed_length = 1,
    .str_is_zero_terminated = 0,
    .str_is_byte_swapped = 0,

    .str_count_word_bytes = 4,
    .str_max_capacity = 82,
    .str_total_length = 88,
    .str_pad_bytes = 2,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {0,1,2,3},
    .float64_order = {0,1,2,3,4,5,6,7},
};


/* default string types used for Omron-NJ/NX PLCs. */
static tag_byte_order_t omron_njnx_tag_byte_order_def = {
    .is_allocated = 0,

    .str_is_defined = 1,
    .str_is_counted = 1,
    .str_is_fixed_length = 0,
    .str_is_zero_terminated = 1,
    .str_is_byte_swapped = 0,

    .str_count_word_bytes = 2,
    .str_max_capacity = 0,
    .str_total_length = 0,
    .str_pad_bytes = 0,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {0,1,2,3},
    .float64_order = {0,1,2,3,4,5,6,7},
};


static tag_byte_order_t logix_tag_listing_byte_order_def = {
    .is_allocated = 0,

    .str_is_defined = 1,
    .str_is_counted = 1,
    .str_is_fixed_length = 0,
    .str_is_zero_terminated = 0,
    .str_is_byte_swapped = 0,

    .str_count_word_bytes = 2,
    .str_max_capacity = 0,
    .str_total_length = 0,
    .str_pad_bytes = 0,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {0,1,2,3},
    .float64_order = {0,1,2,3,4,5,6,7},
};



tag_vtable_p eip_cip_vtable()
{
    return &eip_cip_vtable_def;
}


tag_byte_order_p logix_tag_byte_order()
{
    return &logix_tag_byte_order_def;
}


tag_byte_order_p omron_njnx_tag_byte_order()
{
    return &omron_njnx_tag_byte_order_def;
}


tag_byte_order_p logix_tag_listing_byte_order()
{
    return &logix_tag_listing_byte_order_def;
}


/*************************************************************************
 **************************** API Functions ******************************
 ************************************************************************/


int tag_tickler(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_SPEW,"Starting.");

    if (tag->read_in_progress) {
        if(tag->use_connected_msg) {
            rc = check_read_status_connected(tag);
        } else {
            rc = check_read_status_unconnected(tag);
        }

        tag->status = (int8_t)rc;

        /* if the operation completed, make a note so that the callback will be called. */
        if(!tag->read_in_progress) {
            /* done! */
            if(tag->first_read) {
                tag->first_read = 0;
                tag_raise_event((plc_tag_p)tag, PLCTAG_EVENT_CREATED, (int8_t)rc);
            }

            tag->read_complete = 1;
        }

        pdebug(DEBUG_SPEW,"Done.  Read in progress.");

        return rc;
    }

    pdebug(DEBUG_SPEW, "Done.  No operation in progress.");

    return tag->status;
}





/*
 * tag_read_common_start
 *
 * This function must be called only from within one thread, or while
 * the tag's mutex is locked.
 *
 * The function starts the process of getting tag data from the PLC.
 */

int tag_read_start(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;

    pdebug(DEBUG_INFO, "Starting");

    if(tag->read_in_progress) {
        pdebug(DEBUG_WARN, "Read operation already in flight!");
        return PLCTAG_ERR_BUSY;
    }

    /* mark the tag read in progress */
    tag->read_in_progress = 1;

    /* i is the index of the first new request */
    if(tag->use_connected_msg) {
        // if(tag->tag_list) {
        //     rc = build_tag_list_request_connected(tag);
        // } else {
            rc = build_read_request_connected(tag, tag->offset);
        // }
    } else {
        rc = build_read_request_unconnected(tag, tag->offset);
    }

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_WARN,"Unable to build read request!");

        tag->read_in_progress = 0;

        return rc;
    }

    pdebug(DEBUG_INFO, "Done.");

    return PLCTAG_STATUS_PENDING;
}





int build_read_request_connected(ab_tag_p tag, int byte_offset)
{
    eip_cip_co_req* cip = NULL;
    uint8_t* data = NULL;
    ab_request_p req = NULL;
    int rc = PLCTAG_STATUS_OK;
    uint8_t read_cmd = AB_EIP_CMD_CIP_READ_FRAG;

    pdebug(DEBUG_INFO, "Starting.");

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);
    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    /* point the request struct at the buffer */
    cip = (eip_cip_co_req*)(req->data);

    /* point to the end of the struct */
    data = (req->data) + sizeof(eip_cip_co_req);

    /*
     * set up the embedded CIP read packet
     * The format is:
     *
     * uint8_t cmd
     * LLA formatted name
     * uint16_t # of elements to read
     */

    //embed_start = data;

    /* set up the CIP Read request */
    read_cmd = AB_EIP_CMD_CIP_READ_FRAG;

    *data = read_cmd;
    data++;

    /* copy the tag name into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* add the count of elements to read. */
    *((uint16_le*)data) = h2le16((uint16_t)(tag->elem_count));
    data += sizeof(uint16_le);

    if (read_cmd == AB_EIP_CMD_CIP_READ_FRAG) {
        /* add the byte offset for this request */
        *((uint32_le*)data) = h2le32((uint32_t)byte_offset);
        data += sizeof(uint32_le);
    }

    /* now we go back and fill in the fields of the static part */

    /* encap fields */
    cip->encap_command = h2le16(AB_EIP_CONNECTED_SEND); /* ALWAYS 0x0070 Unconnected Send*/

    /* router timeout */
    cip->router_timeout = h2le16(1); /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    cip->cpf_item_count = h2le16(2);                 /* ALWAYS 2 */
    cip->cpf_cai_item_type = h2le16(AB_EIP_ITEM_CAI);/* ALWAYS 0x00A1 connected address item */
    cip->cpf_cai_item_length = h2le16(4);            /* ALWAYS 4, size of connection ID*/
    cip->cpf_cdi_item_type = h2le16(AB_EIP_ITEM_CDI);/* ALWAYS 0x00B1 - connected Data Item */
    cip->cpf_cdi_item_length = h2le16((uint16_t)(data - (uint8_t*)(&cip->cpf_conn_seq_num))); /* REQ: fill in with length of remaining data. */

    /* set the size of the request */
    req->request_size = (int)(data - (req->data));

    /* set the session so that we know what session the request is aiming at */
    //req->session = tag->session;

    req->allow_packing = tag->allow_packing;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done");

    return PLCTAG_STATUS_OK;
}




int build_read_request_unconnected(ab_tag_p tag, int byte_offset)
{
    eip_cip_uc_req* cip;
    uint8_t* data;
    uint8_t* embed_start, *embed_end;
    ab_request_p req = NULL;
    int rc = PLCTAG_STATUS_OK;
    uint8_t read_cmd = AB_EIP_CMD_CIP_READ_FRAG;
    uint16_le tmp_uint16_le;

    pdebug(DEBUG_INFO, "Starting.");

    /* get a request buffer */
    rc = session_create_request(tag->session, tag->tag_id, &req);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to get new request.  rc=%d", rc);
        return rc;
    }

    /* point the request struct at the buffer */
    cip = (eip_cip_uc_req*)(req->data);

    /* point to the end of the struct */
    data = (req->data) + sizeof(eip_cip_uc_req);

    /*
     * set up the embedded CIP read packet
     * The format is:
     *
     * uint8_t cmd
     * LLA formatted name
     * uint16_t # of elements to read
     */

    embed_start = data;

    /* set up the CIP Read request */
    read_cmd = AB_EIP_CMD_CIP_READ_FRAG;

    *data = read_cmd;
    data++;

    /* copy the tag name into the request */
    mem_copy(data, tag->encoded_name, tag->encoded_name_size);
    data += tag->encoded_name_size;

    /* add the count of elements to read. */
    tmp_uint16_le = h2le16((uint16_t)(tag->elem_count));
    mem_copy(data, &tmp_uint16_le, (int)(unsigned int)sizeof(tmp_uint16_le));
    data += sizeof(tmp_uint16_le);

    /* add the byte offset for this request */
    if(read_cmd == AB_EIP_CMD_CIP_READ_FRAG) {
        /* FIXME - this may not work on some proessors. */
        *((uint32_le*)data) = h2le32((uint32_t)byte_offset);
        data += sizeof(uint32_le);
    }

    /* mark the end of the embedded packet */
    embed_end = data;

    /* Now copy in the routing information for the embedded message */
    /*
     * routing information.  Format:
     *
     * uint8_t path_size in 16-bit words
     * uint8_t reserved/pad (zero)
     * uint8_t[...] path (padded to even number of bytes)
     */
    if(tag->session->conn_path_size > 0) {
        *data = (tag->session->conn_path_size) / 2; /* in 16-bit words */
        data++;
        *data = 0; /* reserved/pad */
        data++;
        mem_copy(data, tag->session->conn_path, tag->session->conn_path_size);
        data += tag->session->conn_path_size;
    }

    /* now we go back and fill in the fields of the static part */

    /* encap fields */
    cip->encap_command = h2le16(AB_EIP_UNCONNECTED_SEND); /* ALWAYS 0x0070 Unconnected Send*/

    /* router timeout */
    cip->router_timeout = h2le16(1); /* one second timeout, enough? */

    /* Common Packet Format fields for unconnected send. */
    cip->cpf_item_count = h2le16(2);                  /* ALWAYS 2 */
    cip->cpf_nai_item_type = h2le16(AB_EIP_ITEM_NAI); /* ALWAYS 0 */
    cip->cpf_nai_item_length = h2le16(0);             /* ALWAYS 0 */
    cip->cpf_udi_item_type = h2le16(AB_EIP_ITEM_UDI); /* ALWAYS 0x00B2 - Unconnected Data Item */
    cip->cpf_udi_item_length = h2le16((uint16_t)(data - (uint8_t*)(&cip->cm_service_code))); /* REQ: fill in with length of remaining data. */

    /* CM Service Request - Connection Manager */
    cip->cm_service_code = AB_EIP_CMD_UNCONNECTED_SEND; /* 0x52 Unconnected Send */
    cip->cm_req_path_size = 2;                          /* 2, size in 16-bit words of path, next field */
    cip->cm_req_path[0] = 0x20;                         /* class */
    cip->cm_req_path[1] = 0x06;                         /* Connection Manager */
    cip->cm_req_path[2] = 0x24;                         /* instance */
    cip->cm_req_path[3] = 0x01;                         /* instance 1 */

    /* Unconnected send needs timeout information */
    cip->secs_per_tick = AB_EIP_SECS_PER_TICK; /* seconds per tick */
    cip->timeout_ticks = AB_EIP_TIMEOUT_TICKS; /* timeout = src_secs_per_tick * src_timeout_ticks */

    /* size of embedded packet */
    cip->uc_cmd_length = h2le16((uint16_t)(embed_end - embed_start));

    /* set the size of the request */
    req->request_size = (int)(data - (req->data));

    /* allow packing if the tag allows it. */
    req->allow_packing = tag->allow_packing;

    /* add the request to the session's list. */
    rc = session_add_request(tag->session, req);

    if (rc != PLCTAG_STATUS_OK) {
        pdebug(DEBUG_ERROR, "Unable to add request to session! rc=%d", rc);
        tag->req = (ab_request_p)rc_dec(req);
        return rc;
    }

    /* save the request for later */
    tag->req = req;

    pdebug(DEBUG_INFO, "Done");

    return PLCTAG_STATUS_OK;
}



/*
 * check_read_status_connected
 *
 * This routine checks for any outstanding requests and copies in data
 * that has arrived.  At the end of the request, it will clean up the request
 * buffers.  This is not thread-safe!  It should be called with the tag mutex
 * locked!
 */

static int check_read_status_connected(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    eip_cip_co_resp* cip_resp;
    uint8_t* data;
    uint8_t* data_end;
    int partial_data = 0;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_read_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Read request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    /* point to the data */
    cip_resp = (eip_cip_co_resp*)(request->data);

    /* point to the start of the data */
    data = (request->data) + sizeof(eip_cip_co_resp);

    /* point the end of the data */
    data_end = (request->data + le2h16(cip_resp->encap_length) + sizeof(eip_encap));

    /* check the status */
    do {
        ptrdiff_t payload_size = (data_end - data);

        if (le2h16(cip_resp->encap_command) != AB_EIP_CONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", cip_resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (le2h32(cip_resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(cip_resp->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        /*
         * FIXME
         *
         * It probably should not be necessary to check for both as setting the type to anything other
         * than fragmented is error-prone.
         */

        if (cip_resp->reply_service != (AB_EIP_CMD_CIP_READ_FRAG | AB_EIP_CMD_CIP_OK)
            && cip_resp->reply_service != (AB_EIP_CMD_CIP_READ | AB_EIP_CMD_CIP_OK) ) {
            pdebug(DEBUG_WARN, "CIP response reply service unexpected: %d", cip_resp->reply_service);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (cip_resp->status != AB_CIP_STATUS_OK && cip_resp->status != AB_CIP_STATUS_FRAG) {
            pdebug(DEBUG_WARN, "CIP read failed with status: 0x%x %s", cip_resp->status, decode_cip_error_short((uint8_t *)&cip_resp->status));
            pdebug(DEBUG_INFO, decode_cip_error_long((uint8_t *)&cip_resp->status));

            rc = decode_cip_error_code((uint8_t *)&cip_resp->status);

            break;
        }

        /* check to see if this is a partial response. */
        partial_data = (cip_resp->status == AB_CIP_STATUS_FRAG);

        /*
         * check to see if there is any data to process.  If this is a packed
         * response, there might not be.
         */
        payload_size = (data_end - data);
        if(payload_size > 0) {
            /* the first byte of the response is a type byte. */
            pdebug(DEBUG_DETAIL, "type byte = %d (%x)", (int)*data, (int)*data);

            /* handle the data type part.  This can be long. */

            /* check for a simple/base type */
            if ((*data) >= AB_CIP_DATA_BIT && (*data) <= AB_CIP_DATA_STRINGI) {
                /* copy the type info for later. */
                if (tag->encoded_type_info_size == 0) {
                    tag->encoded_type_info_size = 2;
                    mem_copy(tag->encoded_type_info, data, tag->encoded_type_info_size);
                }

                /* skip the type byte and zero length byte */
                data += 2;
            } else if ((*data) == AB_CIP_DATA_ABREV_STRUCT || (*data) == AB_CIP_DATA_ABREV_ARRAY ||
                       (*data) == AB_CIP_DATA_FULL_STRUCT || (*data) == AB_CIP_DATA_FULL_ARRAY) {
                /* this is an aggregate type of some sort, the type info is variable length */
                int type_length = *(data + 1) + 2;  /*
                                                       * MAGIC
                                                       * add 2 to get the total length including
                                                       * the type byte and the length byte.
                                                       */

                /* check for extra long types */
                if (type_length > MAX_TAG_TYPE_INFO) {
                    pdebug(DEBUG_WARN, "Read data type info is too long (%d)!", type_length);
                    rc = PLCTAG_ERR_TOO_LARGE;
                    break;
                }

                /* copy the type info for later. */
                if (tag->encoded_type_info_size == 0) {
                    tag->encoded_type_info_size = type_length;
                    mem_copy(tag->encoded_type_info, data, tag->encoded_type_info_size);
                }

                data += type_length;
            } else {
                pdebug(DEBUG_WARN, "Unsupported data type returned, type byte=%d", *data);
                rc = PLCTAG_ERR_UNSUPPORTED;
                break;
            }

            /* check payload size now that we have bumped past the data type info. */
            payload_size = (data_end - data);

            /* copy the data into the tag and realloc if we need more space. */
            if(payload_size + tag->offset > tag->size) {
                tag->size = (int)payload_size + tag->offset;
                tag->elem_size = tag->size / tag->elem_count;

                pdebug(DEBUG_DETAIL, "Increasing tag buffer size to %d bytes.", tag->size);

                tag->data = (uint8_t*)mem_realloc(tag->data, tag->size);
                if(!tag->data) {
                    pdebug(DEBUG_WARN, "Unable to reallocate tag data memory!");
                    rc = PLCTAG_ERR_NO_MEM;
                    break;
                }
            }

            pdebug(DEBUG_INFO, "Got %d bytes of data", (int)payload_size);

            mem_copy(tag->data + tag->offset, data, (int)(payload_size));

            /* bump the byte offset */
            tag->offset += (int)(payload_size);
        } else {
            pdebug(DEBUG_DETAIL, "Response returned no data and no error.");
        }

        /* set the return code */
        rc = PLCTAG_STATUS_OK;
    } while(0);

    /* clean up the request */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    /* are we actually done? */
    if (rc == PLCTAG_STATUS_OK) {
        /* this particular read is done. */
        tag->read_in_progress = 0;

        /* call read start again to get the next piece */
        pdebug(DEBUG_DETAIL, "calling tag_read_start() to get the next chunk.");
        rc = tag_read_start(tag);
    }

    /* this is not an else clause because the above if could result in bad rc. */
    if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
        /* error ! */
        pdebug(DEBUG_WARN, "Error received!");

        /* clean up everything. */
        ab_tag_abort(tag);
    }

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}



static int check_read_status_unconnected(ab_tag_p tag)
{
    int rc = PLCTAG_STATUS_OK;
    eip_cip_uc_resp* cip_resp;
    uint8_t* data;
    uint8_t* data_end;
    int partial_data = 0;
    ab_request_p request = NULL;

    pdebug(DEBUG_SPEW, "Starting.");

    if(!tag) {
        pdebug(DEBUG_ERROR,"Null tag pointer passed!");
        return PLCTAG_ERR_NULL_PTR;
    }

    /* guard against the request being deleted out from underneath us. */
    request = (ab_request_p)rc_inc(tag->req);
    rc = check_read_request_status(tag, request);
    if(rc != PLCTAG_STATUS_OK)  {
        pdebug(DEBUG_DETAIL, "Read request status is not OK.");
        rc_dec(request);
        return rc;
    }

    /* the request reference is still valid. */

    /* point to the data */
    cip_resp = (eip_cip_uc_resp*)(request->data);

    /* point to the start of the data */
    data = (request->data) + sizeof(eip_cip_uc_resp);

    /* point the end of the data */
    data_end = (request->data + le2h16(cip_resp->encap_length) + sizeof(eip_encap));

    /* check the status */
    do {
        ptrdiff_t payload_size = (data_end - data);

        if (le2h16(cip_resp->encap_command) != AB_EIP_UNCONNECTED_SEND) {
            pdebug(DEBUG_WARN, "Unexpected EIP packet type received: %d!", cip_resp->encap_command);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (le2h32(cip_resp->encap_status) != AB_EIP_OK) {
            pdebug(DEBUG_WARN, "EIP command failed, response code: %d", le2h32(cip_resp->encap_status));
            rc = PLCTAG_ERR_REMOTE_ERR;
            break;
        }

        /*
         * TODO
         *
         * It probably should not be necessary to check for both as setting the type to anything other
         * than fragmented is error-prone.
         */

        if (cip_resp->reply_service != (AB_EIP_CMD_CIP_READ_FRAG | AB_EIP_CMD_CIP_OK)
            && cip_resp->reply_service != (AB_EIP_CMD_CIP_READ | AB_EIP_CMD_CIP_OK) ) {
            pdebug(DEBUG_WARN, "CIP response reply service unexpected: %d", cip_resp->reply_service);
            rc = PLCTAG_ERR_BAD_DATA;
            break;
        }

        if (cip_resp->status != AB_CIP_STATUS_OK && cip_resp->status != AB_CIP_STATUS_FRAG) {
            pdebug(DEBUG_WARN, "CIP read failed with status: 0x%x %s", cip_resp->status, decode_cip_error_short((uint8_t *)&cip_resp->status));
            pdebug(DEBUG_INFO, decode_cip_error_long((uint8_t *)&cip_resp->status));

            rc = decode_cip_error_code((uint8_t *)&cip_resp->status);

            break;
        }

        /* check to see if this is a partial response. */
        partial_data = (cip_resp->status == AB_CIP_STATUS_FRAG);

        /* the first byte of the response is a type byte. */
        pdebug(DEBUG_DETAIL, "type byte = %d (%x)", (int)*data, (int)*data);

        /* copy the type data. */

        /* check for a simple/base type */

        if ((*data) >= AB_CIP_DATA_BIT && (*data) <= AB_CIP_DATA_STRINGI) {
            /* copy the type info for later. */
            if (tag->encoded_type_info_size == 0) {
                tag->encoded_type_info_size = 2;
                mem_copy(tag->encoded_type_info, data, tag->encoded_type_info_size);
            }

            /* skip the type byte and zero length byte */
            data += 2;
        } else if ((*data) == AB_CIP_DATA_ABREV_STRUCT || (*data) == AB_CIP_DATA_ABREV_ARRAY ||
                   (*data) == AB_CIP_DATA_FULL_STRUCT || (*data) == AB_CIP_DATA_FULL_ARRAY) {
            /* this is an aggregate type of some sort, the type info is variable length */
            int type_length =
                *(data + 1) + 2;  /*
                                   * MAGIC
                                   * add 2 to get the total length including
                                   * the type byte and the length byte.
                                   */

            /* check for extra long types */
            if (type_length > MAX_TAG_TYPE_INFO) {
                pdebug(DEBUG_WARN, "Read data type info is too long (%d)!", type_length);
                rc = PLCTAG_ERR_TOO_LARGE;
                break;
            }

            /* copy the type info for later. */
            if (tag->encoded_type_info_size == 0) {
                tag->encoded_type_info_size = type_length;
                mem_copy(tag->encoded_type_info, data, tag->encoded_type_info_size);
            }

            data += type_length;
        } else {
            pdebug(DEBUG_WARN, "Unsupported data type returned, type byte=%d", *data);
            rc = PLCTAG_ERR_UNSUPPORTED;
            break;
        }

        /* check payload size now that we have bumped past the data type info. */
        payload_size = (data_end - data);

        /* copy the data into the tag and realloc if we need more space. */
        if(payload_size + tag->offset > tag->size) {
            tag->size = (int)payload_size + tag->offset;
            tag->elem_size = tag->size / tag->elem_count;

            pdebug(DEBUG_DETAIL, "Increasing tag buffer size to %d bytes.", tag->size);

            tag->data = (uint8_t*)mem_realloc(tag->data, tag->size);
            if(!tag->data) {
                pdebug(DEBUG_WARN, "Unable to reallocate tag data memory!");
                rc = PLCTAG_ERR_NO_MEM;
                break;
            }
        }

        pdebug(DEBUG_INFO, "Got %d bytes of data", (int)payload_size);


        mem_copy(tag->data + tag->offset, data, (int)payload_size);

        /* bump the byte offset */
        tag->offset += (int)payload_size;

        /* set the return code */
        rc = PLCTAG_STATUS_OK;
    } while(0);


    /* clean up the request */
    request->abort_request = 1;
    tag->req = (ab_request_p)rc_dec(request);

    /*
     * huh?  Yes, we do it a second time because we already had
     * a reference and got another at the top of this function.
     * So we need to remove it twice.   Once for the capture above,
     * and once for the original reference.
     */

    rc_dec(request);

    /* are we actually done? */
    if (rc == PLCTAG_STATUS_OK) {
        /* this read is done. */
        tag->read_in_progress = 0;

        if (partial_data && tag->offset < tag->size) {
            /* call read start again to get the next piece */
            pdebug(DEBUG_DETAIL, "calling tag_read_start() to get the next chunk.");
            rc = tag_read_start(tag);
        } else {
            tag->offset = 0;
        }
    }

    /* this is not an else clause because the above if could result in bad rc. */
    if(rc != PLCTAG_STATUS_OK && rc != PLCTAG_STATUS_PENDING) {
        /* error ! */
        pdebug(DEBUG_WARN, "Error received!");

        /* clean up everything. */
        ab_tag_abort(tag);
    }

    /* release the referene to the request. */
    rc_dec(request);

    pdebug(DEBUG_SPEW, "Done.");

    return rc;
}



#ifdef __cplusplus
}
#endif