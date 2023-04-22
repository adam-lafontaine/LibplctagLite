#pragma once

#define PLCTAG_NO_WRITE


#include <cstdint>

using u8 = uint8_t;
using i8 = int8_t;
using u16 = uint16_t;
using i16 = int16_t;
using u32 = uint32_t;
using i32 = int32_t;
using u64 = uint64_t;
using i64 = int64_t;
using f32 = float;
using f64 = double;


namespace plctag
{
    /* library internal status. */
    /*
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
    */


   constexpr int TIMEOUT_DEFAULT_MS = 100;

    enum class STATUS : int
    {
        TAG_OK      = 0,
        TAG_PENDING = 1,
        TAG_ERROR   = 2,
        TAG_NOT_SET = 3
    };


    template <typename T>
    class Result
    {
    public:
        STATUS status = STATUS::TAG_NOT_SET;
        const char* error = nullptr;

        T data;

        bool is_ok() { return status == STATUS::TAG_OK; }
        bool is_pending() { return status == STATUS::TAG_PENDING; }
        bool is_error() { return status == STATUS::TAG_ERROR; }
    };


    class TagDesc
    {
    public:
        i32 tag_handle = -1;
        u64 tag_size = 0;
        u64 elem_size = 0;
        u64 elem_count = 0;
    };


    using ConnectResult = Result<TagDesc>;

}


namespace plctag
{ 
    /*
    * Set the debug level.
    *
    * This function takes values from the defined debug levels below.  It sets
    * the debug level to the passed value.  Higher numbers output increasing amounts
    * of information.   Input values not defined below will be ignored.
    */

    enum class DEBUG : int
    {
        NONE   = 0,
        ERROR  = 1,
        WARN   = 2,
        INFO   = 3,
        DETAIL = 4,
        SPEW   = 5
    };

    void set_debug_level(DEBUG debug_level);



    ConnectResult connect(const char* attrib_str, int timeout);

    inline ConnectResult connect(const char* attrib_str)
    {
        return connect(attrib_str, TIMEOUT_DEFAULT_MS);
    }



    /*
    * plc_tag_destroy
    *
    * This frees all resources associated with the tag.  Internally, it may result in closed
    * connections etc.   This calls through to a protocol-specific function.
    *
    * This is a function provided by the underlying protocol implementation.
    */
    void destroy(i32 tag);


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
    void shutdown(void);


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


    bool lock(i32 tag);



    /*
    * plc_tag_unlock
    *
    * The opposite action of plc_tag_unlock.  This allows other threads to access the
    * tag.
    */

    bool unlock(i32 tag);





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
    bool abort(i32 tag);






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
    Result<int> receive(i32 tag, int timeout);

    inline Result<int> receive(i32 tag) { return receive(tag, TIMEOUT_DEFAULT_MS); }
    



    Result<int> get_bit(i32 tag, int offset_bit);

    Result<u64> get_u64(i32 tag, int offset);

    Result<i64> get_i64(i32 tag, int offset);

    Result<u32> get_u32(i32 tag, int offset);

    Result<i32> get_i32(i32 tag, int offset);

    Result<u16> get_u16(i32 tag, int offset);

    Result<i16> get_i16(i32 tag, int offset);

    Result<u8> get_u8(i32 tag, int offset);

    Result<i8> get_i8(i32 tag, int offset);

    Result<f32> get_f32(i32 tag, int offset);

    Result<f64> get_f64(i32 tag, int offset);

    Result<int> get_bytes(i32 id, int offset, u8 *buffer, int buffer_length);
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
    Result<int> send(i32 tag, int timeout);

    inline Result<int> send(i32 tag) { return send(tag, TIMEOUT_DEFAULT_MS); }
}

#endif