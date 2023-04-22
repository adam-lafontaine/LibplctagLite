#include "plctag.hpp"
//#include "libplctag.h"

#include "libplctag.c"


namespace plctag
{
    constexpr auto ERR_NO_ERROR = "No error. Everything OK";
    constexpr auto ERR_TAG_SIZE = "Tag size error";
    constexpr auto ERR_ELEM_SIZE = "Tag element/count size error";



    template <class RESULT>
    static void decode_result(RESULT& result, int rc)
    {
        switch(rc)
        {
        case (int)PLCTAG_STATUS_OK:
            result.status = STATUS::TAG_OK;
            result.error = ERR_NO_ERROR;
            break;
        case (int)PLCTAG_STATUS_PENDING:
            result.status = STATUS::TAG_PENDING;
            result.error = ERR_NO_ERROR;
            break;
        default:
            result.status = STATUS::TAG_ERROR;
            result.error = plc_tag_decode_error(rc);
        }
    }


    /*
    * Set the debug level.
    *
    * This function takes values from the defined debug levels below.  It sets
    * the debug level to the passed value.  Higher numbers output increasing amounts
    * of information.   Input values not defined below will be ignored.
    */
   /*
    #define PLCTAG_DEBUG_NONE      (0)
    #define PLCTAG_DEBUG_ERROR     (1)
    #define PLCTAG_DEBUG_WARN      (2)
    #define PLCTAG_DEBUG_INFO      (3)
    #define PLCTAG_DEBUG_DETAIL    (4)
    #define PLCTAG_DEBUG_SPEW      (5)
    */

    void set_debug_level(DEBUG debug_level)
    {
        plc_tag_set_debug_level(static_cast<int>(debug_level));
    }
    


    ConnectResult connect(const char* attrib_str, int timeout)
    {
        ConnectResult result {};

        auto rc = plc_tag_create(attrib_str, timeout);

        decode_result(result, rc);

        if  (!result.is_ok())
        {
            return result;
        }

        result.data.tag_handle = rc;

        auto size = plc_tag_get_size(rc);
        if (size <= 0)
        {
            result.status = STATUS::TAG_ERROR;
            result.error = ERR_TAG_SIZE;
            result.data.tag_handle = -1;
            plc_tag_destroy(rc);
        }

        auto elem_size = plc_tag_get_int_attribute(rc, "elem_size", 0);
        auto elem_count = plc_tag_get_int_attribute(rc, "elem_count", 0);

        if (elem_size <= 0 || elem_count <= 0 || size != elem_size * elem_count)
        {
            result.status = STATUS::TAG_ERROR;
            result.error = ERR_ELEM_SIZE;
            result.data.tag_handle = -1;
            plc_tag_destroy(rc);
        }

        result.data.tag_size = (u64)size;
        result.data.elem_size = (u64)elem_size;
        result.data.elem_count = (u64)elem_count;

        return result;
    }




    /*
    * plc_tag_destroy
    *
    * This frees all resources associated with the tag.  Internally, it may result in closed
    * connections etc.   This calls through to a protocol-specific function.
    *
    * This is a function provided by the underlying protocol implementation.
    */
    void destroy(i32 tag)
    {
        plc_tag_destroy(tag);
    }


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

    void shutdown(void)
    {
        plc_tag_shutdown();
    }



    /*
    * plc_tag_lock
    *
    * Lock the tag against use by other threads.  Because operations on a tag are
    * very much asynchronous, actions like getting and extracting the data from
    * a tag take more than one API call.  If more than one thread is using the same tag,
    * then the internal state of the tag will get broken and you will probably experience
    * a crash.
    *
    * This should be used to initially lock a tag when starting operations with it
    * followed by a call to plc_tag_unlock when you have everything you need from the tag.
    */


    bool lock(i32 tag)
    {
        return plc_tag_lock(tag) == PLCTAG_STATUS_OK;
    }



    /*
    * plc_tag_unlock
    *
    * The opposite action of plc_tag_unlock.  This allows other threads to access the
    * tag.
    */

    bool unlock(i32 tag)
    {
        return plc_tag_unlock(tag) == PLCTAG_STATUS_OK;
    }





    /*
    * plc_tag_abort
    *
    * Abort any outstanding IO to the PLC.  If there is something in flight, then
    * it is marked invalid.  Note that this does not abort anything that might
    * be still processing in the report PLC.
    *
    * The status will be PLCTAG_STATUS_OK unless there is an error such as
    * a null pointer.
    *
    * This is a function provided by the underlying protocol implementation.
    */
    bool abort(i32 tag)
    {
        return plc_tag_abort(tag) == (int)PLCTAG_STATUS_OK;
    }






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
    Result<int> receive(i32 tag, int timeout)
    {
        Result<int> result{};

        auto rc = plc_tag_read(tag, timeout);
        decode_result(result, rc);

        return result;        
    }


    Result<int> get_bit(i32 tag, int offset_bit)
    {
        Result<int> result{};

        auto rc = plc_tag_get_bit(tag, offset_bit);
        decode_result(result, rc);

        return result;
    }


    template <typename T, typename API_GET>
    static Result<T> plctag_api_get(API_GET get, i32 tag, int offset)
    {
        Result<T> result{};

        auto rc = get(tag, offset_bit);
        decode_result(result, rc);

        return result;
    }


    Result<u64> get_u64(i32 tag, int offset)
    {
        return plctag_api_get<u64>(plc_tag_get_uint64, tag, offset);
    }


    Result<i64> get_i64(i32 tag, int offset)
    {
        return plctag_api_get<i64>(plc_tag_get_int64, tag, offset);
    }


    Result<u32> get_u32(i32 tag, int offset)
    {
        return plctag_api_get<u32>(plc_tag_get_uint32, tag, offset);
    }


    Result<i32> get_i32(i32 tag, int offset)
    {
        return plctag_api_get<i32>(plc_tag_get_int32, tag, offset);
    }


    Result<u16> get_u16(i32 tag, int offset)
    {
        return plctag_api_get<u16>(plc_tag_get_uint16, tag, offset);
    }


    Result<i16> get_i16(i32 tag, int offset)
    {
        return plctag_api_get<i16>(plc_tag_get_int16, tag, offset);
    }


    Result<u8> get_u8(i32 tag, int offset)
    {
        return plctag_api_get<u8>(plc_tag_get_uint8, tag, offset);
    }


    Result<i8> get_i8(i32 tag, int offset)
    {
        return plctag_api_get<i8>(plc_tag_get_int8, tag, offset);
    }


    Result<f32> get_f32(i32 tag, int offset)
    {
        return plctag_api_get<f32>(plc_tag_get_float32, tag, offset);
    }


    Result<f64> get_f64(i32 tag, int offset)
    {
        return plctag_api_get<f64>(plc_tag_get_float64, tag, offset);
    }


    Result<int> get_bytes(i32 id, int offset, u8 *buffer, int buffer_length)
    {
        Result<int> result{};

        auto rc = plc_tag_get_raw_bytes(id, offset, buffer, buffer_length);
        decode_result(result, rc);

        return result;
    }

    
    /*
    int get_string(i32 tag_id, int string_start_offset, char *buffer, int buffer_length);
    
    int get_string_length(i32 tag_id, int string_start_offset);
    int get_string_capacity(i32 tag_id, int string_start_offset);
    int get_string_total_length(i32 tag_id, int string_start_offset);

    */
}


#ifndef PLCTAG_NO_WRITE

namespace plctag
{
    



    /*
    * plc_tag_write
    *
    * Start a write.  If the timeout value is zero, then wait until the write_t
    * returns or the timeout occurs, whichever is first.  Return the status.
    * If the timeout value is zero, then plc_tag_write will usually return
    * PLCTAG_STATUS_PENDING.  The write is considered done
    * when it has been written to the socket.
    *
    * This is a function provided by the underlying protocol implementation.
    */
    Result<int> send(i32 tag, int timeout)
    {
        Result<int> result{};

        auto rc = plc_tag_write(tag, timeout);
        decode_result(result, rc);

        return result;
    }


    int plc_tag_set_bit(i32 tag, int offset_bit, int val);
    int plc_tag_set_uint64(i32 tag, int offset, u64  val);
    int plc_tag_set_int64(i32, int offset, i64  val);
    int plc_tag_set_uint32(i32 tag, int offset, u32 val);
    int plc_tag_set_int32(i32, int offset, i32 val);
    int plc_tag_set_uint16(i32 tag, int offset, u16 val);
    int plc_tag_set_int16(i32, int offset, i16 val);
    int plc_tag_set_uint8(i32 tag, int offset, u8 val);
    int plc_tag_set_int8(i32, int offset, i8 val);
    int plc_tag_set_float64(i32 tag, int offset, f64 val);
    int plc_tag_set_float32(i32 tag, int offset, f32 val);
    int plc_tag_set_raw_bytes(i32 id, int offset, u8 *buffer, int buffer_length);
    int plc_tag_set_string(i32 tag_id, int string_start_offset, const char* string_val);
}

#endif
