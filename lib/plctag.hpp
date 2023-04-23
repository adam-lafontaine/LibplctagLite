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
   constexpr int TIMEOUT_DEFAULT_MS = 100;

    enum class STATUS : int
    {
        PLCTAG_STATUS_NOT_SET       = (99),

        PLCTAG_STATUS_PENDING       = (1),
        PLCTAG_STATUS_OK            = (0),

        PLCTAG_ERR_ABORT            = (-1),
        PLCTAG_ERR_BAD_CONFIG       = (-2),
        PLCTAG_ERR_BAD_CONNECTION   = (-3),
        PLCTAG_ERR_BAD_DATA         = (-4),
        PLCTAG_ERR_BAD_DEVICE       = (-5),
        PLCTAG_ERR_BAD_GATEWAY      = (-6),
        PLCTAG_ERR_BAD_PARAM        = (-7),
        PLCTAG_ERR_BAD_REPLY        = (-8),
        PLCTAG_ERR_BAD_STATUS       = (-9),
        PLCTAG_ERR_CLOSE            = (-10),
        PLCTAG_ERR_CREATE           = (-11),
        PLCTAG_ERR_DUPLICATE        = (-12),
        PLCTAG_ERR_ENCODE           = (-13),
        PLCTAG_ERR_MUTEX_DESTROY    = (-14),
        PLCTAG_ERR_MUTEX_INIT       = (-15),
        PLCTAG_ERR_MUTEX_LOCK       = (-16),
        PLCTAG_ERR_MUTEX_UNLOCK     = (-17),
        PLCTAG_ERR_NOT_ALLOWED      = (-18),
        PLCTAG_ERR_NOT_FOUND        = (-19),
        PLCTAG_ERR_NOT_IMPLEMENTED  = (-20),
        PLCTAG_ERR_NO_DATA          = (-21),
        PLCTAG_ERR_NO_MATCH         = (-22),
        PLCTAG_ERR_NO_MEM           = (-23),
        PLCTAG_ERR_NO_RESOURCES     = (-24),
        PLCTAG_ERR_NULL_PTR         = (-25),
        PLCTAG_ERR_OPEN             = (-26),
        PLCTAG_ERR_OUT_OF_BOUNDS    = (-27),
        PLCTAG_ERR_READ             = (-28),
        PLCTAG_ERR_REMOTE_ERR       = (-29),
        PLCTAG_ERR_THREAD_CREATE    = (-30),
        PLCTAG_ERR_THREAD_JOIN      = (-31),
        PLCTAG_ERR_TIMEOUT          = (-32),
        PLCTAG_ERR_TOO_LARGE        = (-33),
        PLCTAG_ERR_TOO_SMALL        = (-34),
        PLCTAG_ERR_UNSUPPORTED      = (-35),
        PLCTAG_ERR_WINSOCK          = (-36),
        PLCTAG_ERR_WRITE            = (-37),
        PLCTAG_ERR_PARTIAL          = (-38),
        PLCTAG_ERR_BUSY             = (-39),

        PLCTAG_ERR_BAD_SIZE         = (-99),
    };


    template <typename T>
    class Result
    {
    public:
        STATUS status = STATUS::PLCTAG_STATUS_NOT_SET;
        const char* error = nullptr;

        T data;

        bool is_ok() { return status == STATUS::PLCTAG_STATUS_OK; }
        bool is_pending() { return status == STATUS::PLCTAG_STATUS_PENDING; }
        bool is_error() { return static_cast<int>(status) < 0; }
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


    enum class DEBUG : int
    {
        NONE = 0,
        ERROR = 1,
        WARN = 2,
        INFO = 3,
        DETAIL = 4,
        SPEW = 5
    };
}


namespace plctag
{ 
    void set_debug_level(DEBUG debug_level);


    ConnectResult connect(const char* attrib_str, int timeout);

    inline ConnectResult connect(const char* attrib_str) { return connect(attrib_str, TIMEOUT_DEFAULT_MS); }

    
    void destroy(i32 tag);
    
    void shutdown(void);

    bool thread_lock(i32 tag);

    bool thread_unlock(i32 tag);
    
    bool abort(i32 tag);

    
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


    Result<u64> get_string_length(i32 tag_id, int string_start_offset);

    Result<u64> get_string_capacity(i32 tag_id, int string_start_offset);

    Result<u64> get_string_total_length(i32 tag_id, int string_start_offset);

    Result<int> get_string(i32 tag_id, int string_start_offset, char* buffer, int buffer_length);
}


#ifndef PLCTAG_NO_WRITE

namespace plctag
{
    
    Result<int> send(i32 tag, int timeout);

    inline Result<int> send(i32 tag) { return send(tag, TIMEOUT_DEFAULT_MS); }
}

#endif