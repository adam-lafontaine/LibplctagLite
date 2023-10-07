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

#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifdef LIBPLCTAGDLL_EXPORTS
#undef LIBPLCTAGDLL_EXPORTS
#endif


#if  defined(_WIN32) || defined(WIN32) || defined(WIN64) || defined(_WIN64)
    #ifdef __cplusplus
        #define C_FUNC extern "C"
    #else
        #define C_FUNC
    #endif

    #ifdef LIBPLCTAGDLL_EXPORTS
        #define LIB_EXPORT __declspec(dllexport)
    #else
        //#define LIB_EXPORT __declspec(dllimport)
        #define LIB_EXPORT extern
    #endif
#else
    #ifdef LIBPLCTAGDLL_EXPORTS
        #define LIB_EXPORT __attribute__ ((visibility ("default")))
    #else
        #define LIB_EXPORT extern
    #endif
#endif



/* library internal status. */
#define PLCTAG_STATUS_PENDING       (1)
#define PLCTAG_STATUS_OK            (0)

#define PLCTAG_ERR_ABORT            (-1)
#define PLCTAG_ERR_BAD_CONFIG       (-2)
#define PLCTAG_ERR_BAD_CONNECTION   (-3)
#define PLCTAG_ERR_BAD_DATA         (-4)
#define PLCTAG_ERR_BAD_DEVICE       (-5)
#define PLCTAG_ERR_BAD_GATEWAY      (-6)
#define PLCTAG_ERR_BAD_PARAM        (-7)
#define PLCTAG_ERR_BAD_REPLY        (-8)
#define PLCTAG_ERR_BAD_STATUS       (-9)
#define PLCTAG_ERR_CLOSE            (-10)
#define PLCTAG_ERR_CREATE           (-11)
#define PLCTAG_ERR_DUPLICATE        (-12)
#define PLCTAG_ERR_ENCODE           (-13)
#define PLCTAG_ERR_MUTEX_DESTROY    (-14)
#define PLCTAG_ERR_MUTEX_INIT       (-15)
#define PLCTAG_ERR_MUTEX_LOCK       (-16)
#define PLCTAG_ERR_MUTEX_UNLOCK     (-17)
#define PLCTAG_ERR_NOT_ALLOWED      (-18)
#define PLCTAG_ERR_NOT_FOUND        (-19)
#define PLCTAG_ERR_NOT_IMPLEMENTED  (-20)
#define PLCTAG_ERR_NO_DATA          (-21)
#define PLCTAG_ERR_NO_MATCH         (-22)
#define PLCTAG_ERR_NO_MEM           (-23)
#define PLCTAG_ERR_NO_RESOURCES     (-24)
#define PLCTAG_ERR_NULL_PTR         (-25)
#define PLCTAG_ERR_OPEN             (-26)
#define PLCTAG_ERR_OUT_OF_BOUNDS    (-27)
#define PLCTAG_ERR_READ             (-28)
#define PLCTAG_ERR_REMOTE_ERR       (-29)
#define PLCTAG_ERR_THREAD_CREATE    (-30)
#define PLCTAG_ERR_THREAD_JOIN      (-31)
#define PLCTAG_ERR_TIMEOUT          (-32)
#define PLCTAG_ERR_TOO_LARGE        (-33)
#define PLCTAG_ERR_TOO_SMALL        (-34)
#define PLCTAG_ERR_UNSUPPORTED      (-35)
#define PLCTAG_ERR_WINSOCK          (-36)
#define PLCTAG_ERR_WRITE            (-37)
#define PLCTAG_ERR_PARTIAL          (-38)
#define PLCTAG_ERR_BUSY             (-39)




/*
 * helper function for errors.
 *
 * This takes one of the above errors and turns it into a const char * suitable
 * for printing.
 */

LIB_EXPORT const char *plc_tag_decode_error(int err);


/*
 * Set the debug level.
 *
 * This function takes values from the defined debug levels below.  It sets
 * the debug level to the passed value.  Higher numbers output increasing amounts
 * of information.   Input values not defined below will be ignored.
 */

#define PLCTAG_DEBUG_NONE      (0)
#define PLCTAG_DEBUG_ERROR     (1)
#define PLCTAG_DEBUG_WARN      (2)
#define PLCTAG_DEBUG_INFO      (3)
#define PLCTAG_DEBUG_DETAIL    (4)
#define PLCTAG_DEBUG_SPEW      (5)

LIB_EXPORT void plc_tag_set_debug_level(int debug_level);



/*
 * Check that the library supports the required API version.
 *
 * The version is passed as integers.   The three arguments are:
 *
 * ver_major - the major version of the library.  This must be an exact match.
 * ver_minor - the minor version of the library.   The library must have a minor
 *             version greater than or equal to the requested version.
 * ver_patch - the patch version of the library.   The library must have a patch
 *             version greater than or equal to the requested version if the minor
 *             version is the same as that requested.   If the library minor version
 *             is greater than that requested, any patch version will be accepted.
 *
 * PLCTAG_STATUS_OK is returned if the version is compatible.  If it does not,
 * PLCTAG_ERR_UNSUPPORTED is returned.
 *
 * Examples:
 *
 * To match version 2.1.4, call plc_tag_check_lib_version(2, 1, 4).
 *
 */

LIB_EXPORT int plc_tag_check_lib_version(int req_major, int req_minor, int req_patch);




/*
 * tag functions
 *
 * The following is the public API for tag operations.
 *
 * These are implemented in a protocol-specific manner.
 */

/*
 * plc_tag_create
 *
 * Create a new tag based on the passed attributed string.  The attributes
 * are protocol-specific.  The only required part of the string is the key-
 * value pair "protocol=XXX" where XXX is one of the supported protocol
 * types.
 *
 * Wait for timeout milliseconds for the tag to finish the creation process.
 * If this is zero, return immediately.  The application program will need to
 * poll the tag status with plc_tag_status() while the status is PLCTAG_STATUS_PENDING
 * until the status changes to PLCTAG_STATUS_OK if the creation was successful or
 * another PLCTAG_ERR_xyz if it was not.
 *
 * An opaque handle is returned. If the value is greater than zero, then
 * the operation was a success.  If the value is less than zero then the
 * tag was not created and the failure error is one of the PLCTAG_ERR_xyz
 * errors.
 */

LIB_EXPORT int32_t plc_tag_create(const char *attrib_str, int timeout);



/*
 * plc_tag_shutdown
 *
 * Some systems may not call the atexit() handlers.  In those cases, wrappers should
 * call this function before unloading the library or terminating.   Most OSes will cleanly
 * recover all system resources when a process is terminated and this will not be necessary.
 *
 * THIS IS NOT THREAD SAFE!   Do not call this if you have multiple threads running against
 * the library.  You have been warned.   Close all tags first with plc_tag_destroy() and make
 * sure that nothing can call any library functions until this function returns.
 *
 * Normally you do not need to call this function.   This is only for certain wrappers or
 * operating environments that use libraries in ways that prevent the normal exit handlers
 * from working.
 */

LIB_EXPORT void plc_tag_shutdown(void);




/*
 * plc_tag_destroy
 *
 * This frees all resources associated with the tag.  Internally, it may result in closed
 * connections etc.   This calls through to a protocol-specific function.
 *
 * This is a function provided by the underlying protocol implementation.
 */
LIB_EXPORT int plc_tag_destroy(int32_t tag);






/*
 * plc_tag_read
 *
 * Start a read.  If the timeout value is zero, then wait until the read
 * returns or the timeout occurs, whichever is first.  Return the status.
 * If the timeout value is zero, then plc_tag_read will normally return
 * PLCTAG_STATUS_PENDING.
 *
 * This is a function provided by the underlying protocol implementation.
 */
LIB_EXPORT int plc_tag_read(int32_t tag, int timeout);




/*
 * plc_tag_status
 *
 * Return the current status of the tag.  This will be PLCTAG_STATUS_PENDING if there is
 * an uncompleted IO operation.  It will be PLCTAG_STATUS_OK if everything is fine.  Other
 * errors will be returned as appropriate.
 *
 * This is a function provided by the underlying protocol implementation.
 */
LIB_EXPORT int plc_tag_status(int32_t tag);




/*
 * Tag data accessors.
 */


LIB_EXPORT int plc_tag_get_int_attribute(int32_t tag, const char *attrib_name, int default_value);
LIB_EXPORT int plc_tag_set_int_attribute(int32_t tag, const char *attrib_name, int new_value);

LIB_EXPORT int plc_tag_get_size(int32_t tag);

LIB_EXPORT int plc_tag_get_bit(int32_t tag, int offset_bit);

LIB_EXPORT uint64_t plc_tag_get_uint64(int32_t tag, int offset);

LIB_EXPORT int64_t plc_tag_get_int64(int32_t tag, int offset);


LIB_EXPORT uint32_t plc_tag_get_uint32(int32_t tag, int offset);

LIB_EXPORT int32_t plc_tag_get_int32(int32_t tag, int offset);


LIB_EXPORT uint16_t plc_tag_get_uint16(int32_t tag, int offset);

LIB_EXPORT int16_t plc_tag_get_int16(int32_t tag, int offset);


LIB_EXPORT uint8_t plc_tag_get_uint8(int32_t tag, int offset);

LIB_EXPORT int8_t plc_tag_get_int8(int32_t tag, int offset);


LIB_EXPORT double plc_tag_get_float64(int32_t tag, int offset);

LIB_EXPORT float plc_tag_get_float32(int32_t tag, int offset);

/* raw byte bulk access */
LIB_EXPORT int plc_tag_get_raw_bytes(int32_t id, int offset, uint8_t *buffer, int buffer_length);

/* string accessors */

LIB_EXPORT int plc_tag_get_string(int32_t tag_id, int string_start_offset, char *buffer, int buffer_length);
LIB_EXPORT int plc_tag_get_string_length(int32_t tag_id, int string_start_offset);
LIB_EXPORT int plc_tag_get_string_capacity(int32_t tag_id, int string_start_offset);
LIB_EXPORT int plc_tag_get_string_total_length(int32_t tag_id, int string_start_offset);

#ifdef __cplusplus
}
#endif
