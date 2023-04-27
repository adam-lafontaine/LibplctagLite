#pragma once

#define PLCTAG_NO_WRITE


#include <cstdint>
#include <vector>
#include <string>

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
using cstr = const char*;

template <typename T>
using List = std::vector<T>;

using String = std::string;


/* enums */

namespace plctag
{
   constexpr int TIMEOUT_DEFAULT_MS = 1000;

    enum class Status : int
    {
        NOT_SET              = 99,

        PENDING              = 1,
        OK                   = 0,

        ERR_ABORT            = -1,
        ERR_BAD_CONFIG       = -2,
        ERR_BAD_CONNECTION   = -3,
        ERR_BAD_DATA         = -4,
        ERR_BAD_DEVICE       = -5,
        ERR_BAD_GATEWAY      = -6,
        ERR_BAD_PARAM        = -7,
        ERR_BAD_REPLY        = -8,
        ERR_BAD_STATUS       = -9,
        ERR_CLOSE            = -10,
        ERR_CREATE           = -11,
        ERR_DUPLICATE        = -12,
        ERR_ENCODE           = -13,
        ERR_MUTEX_DESTROY    = -14,
        ERR_MUTEX_INIT       = -15,
        ERR_MUTEX_LOCK       = -16,
        ERR_MUTEX_UNLOCK     = -17,
        ERR_NOT_ALLOWED      = -18,
        ERR_NOT_FOUND        = -19,
        ERR_NOT_IMPLEMENTED  = -20,
        ERR_NO_DATA          = -21,
        ERR_NO_MATCH         = -22,
        ERR_NO_MEM           = -23,
        ERR_NO_RESOURCES     = -24,
        ERR_NULL_PTR         = -25,
        ERR_OPEN             = -26,
        ERR_OUT_OF_BOUNDS    = -27,
        ERR_READ             = -28,
        ERR_REMOTE_ERR       = -29,
        ERR_THREAD_CREATE    = -30,
        ERR_THREAD_JOIN      = -31,
        ERR_TIMEOUT          = -32,
        ERR_TOO_LARGE        = -33,
        ERR_TOO_SMALL        = -34,
        ERR_UNSUPPORTED      = -35,
        ERR_WINSOCK          = -36,
        ERR_WRITE            = -37,
        ERR_PARTIAL          = -38,
        ERR_BUSY             = -39,

        ERR_BAD_SIZE         = -1000,
        ERR_BAD_ATTRS        = -1001
    };


    enum class DebugLevel : int
    {
        NONE   = 0,
        ERROR  = 1,
        WARN   = 2,
        INFO   = 3,
        DETAIL = 4,
        SPEW   = 5
    };


    enum class Controller : int
    {
        ControlLogix,
        PLC5,
        SLC500,
        LogixPccc,
        Micro800,
        MicroLogix,
        OmronNJNX,
        Modbus
    };


    /*
    
    case 0xC1: type = "BOOL: Boolean value"; break;
            case 0xC2: type = "SINT: Signed 8-bit integer value"; break;
            case 0xC3: type = "INT: Signed 16-bit integer value"; break;
            case 0xC4: type = "DINT: Signed 32-bit integer value"; break;
            case 0xC5: type = "LINT: Signed 64-bit integer value"; break;
            case 0xC6: type = "USINT: Unsigned 8-bit integer value"; break;
            case 0xC7: type = "UINT: Unsigned 16-bit integer value"; break;
            case 0xC8: type = "UDINT: Unsigned 32-bit integer value"; break;
            case 0xC9: type = "ULINT: Unsigned 64-bit integer value"; break;
            case 0xCA: type = "REAL: 32-bit floating point value, IEEE format"; break;
            case 0xCB: type = "LREAL: 64-bit floating point value, IEEE format"; break;
            case 0xCC: type = "Synchronous time value"; break;
            case 0xCD: type = "Date value"; break;
            case 0xCE: type = "Time of day value"; break;
            case 0xCF: type = "Date and time of day value"; break;
            case 0xD0: type = "Character string, 1 byte per character"; break;
            case 0xD1: type = "8-bit bit string"; break;
            case 0xD2: type = "16-bit bit string"; break;
            case 0xD3: type = "32-bit bit string"; break;
            case 0xD4: type = "64-bit bit string"; break;
            case 0xD5: type = "Wide char character string, 2 bytes per character"; break;
            case 0xD6: type = "High resolution duration value"; break;
            case 0xD7: type = "Medium resolution duration value"; break;
            case 0xD8: type = "Low resolution duration value"; break;
            case 0xD9: type = "N-byte per char character string"; break;
            case 0xDA: type = "Counted character sting with 1 byte per character and 1 byte length indicator"; break;
            case 0xDB: type = "Duration in milliseconds"; break;
            case 0xDC: type = "CIP path segment(s)"; break;
            case 0xDD: type = "Engineering units"; break;
            case 0xDE: type = "International character string (encoding?)"; break;
    
    */


    enum class DataType : u16
    {
        BOOL                 = 0xC1,
        SINT                 = 0xC2,
        INT                  = 0xC3,
        DINT                 = 0xC4,
        LINT                 = 0xC5,
        USINT                = 0xC6,
        UINT                 = 0xC7,
        UDINT                = 0xC8,
        ULINT                = 0xC9,
        REAL                 = 0xCA,
        LREAL                = 0xCB,
        SYNCHRONOUS_TIME     = 0xCC,
        DATE                 = 0xCD,
        TIME                 = 0xCE,
        DATETIME             = 0xCF,
        CHAR_STRING          = 0xD0,
        STRING_8             = 0xD1,
        STRING_16            = 0xD2,
        STRING_32            = 0xD3,
        STRING_64            = 0xD4,
        WIDE_STRING          = 0xD5,
        HIGH_RES_DURATION    = 0xD6,
        MED_RES_DURATION     = 0xD7,
        LOW_RES_DURATION     = 0xD8,
        N_BYTE_STRING        = 0xD9,
        COUNTED_CHAR_STRING  = 0xDA,
        DURATION_MS          = 0xDB,
        CIP_PATH             = 0xDC,
        ENGINEERING_UNITS    = 0xDD,
        INTERNATIONAL_STRING = 0xDE
    };

}


/* class/struct definitions */

namespace plctag
{
    template <typename T>
    class Result
    {
    public:
        Status status = Status::NOT_SET;
        cstr error = nullptr;

        T data;

        bool is_ok() { return status == Status::OK; }
        bool is_error() { return static_cast<int>(status) < 0; }
    };


    class Tag_Desc
    {
    public:
        i32 tag_handle = -1;
        u64 tag_size = 0;
        u64 elem_size = 0;
        u64 elem_count = 0;
    };


    using ConnectResult = Result<Tag_Desc>;


    class Tag_Attr
    {
    public:
        Controller controller = Controller::ControlLogix;
        cstr gateway = nullptr;
        cstr path = "1,0";
        cstr tag_name = nullptr;
        bool has_dhp = false;
    };


    class Tag_Entry
    {
    public:
        u32 instance_id = 0;
        u16 type = 0;
        u16 elem_size = 0;
        u16 elem_count = 0;
        u16 num_dimensions = 0;
        u16 dimensions[3] = { 0 };
        String name;
    };


    class UDT_Field_Entry
    {
    public:
        cstr name;
        u16 type;
        u16 metadata;
        u32 size;
        u32 offset;
    };


    class UDT_Entry
    {
    public:
        cstr name;
        u16 id;
        u16 num_fields;
        u16 struct_handle;
        u32 instance_size;
        List<UDT_Field_Entry> fields;
    };


    class PLC_Desc
    {
    public:
        Controller controller = Controller::ControlLogix;
        cstr gateway = nullptr;
        cstr path = "1,0";
        bool has_dhp = false;

        List<cstr> program_names;
        List<Tag_Entry> tags;
        List<UDT_Entry> udts;
    };
}


/* api wrapper */
namespace plctag
{ 
    void set_debug_level(DebugLevel debug_level);


    ConnectResult connect(Tag_Attr attr, int timeout);

    inline ConnectResult connect(Tag_Attr attr) { return connect(attr, TIMEOUT_DEFAULT_MS); }

    
    void destroy(i32 tag);
    
    void shutdown(void);
    
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


/* extra */

namespace plctag
{
    ConnectResult enumerate_tags(PLC_Desc& data, int timeout);

    inline ConnectResult enumerate_tags(PLC_Desc& data) { return enumerate_tags(data, TIMEOUT_DEFAULT_MS); }


    cstr decode_controller(Controller c);

    cstr decode_controller(int c);
}


#ifndef PLCTAG_NO_WRITE

namespace plctag
{
    
    Result<int> send(i32 tag, int timeout);

    inline Result<int> send(i32 tag) { return send(tag, TIMEOUT_DEFAULT_MS); }
}

#endif


/* debugging */

namespace plctag 
{ 
namespace dbg
{
    bool build_attr_string(Tag_Attr attr, char* dst, size_t max_len);
}
}