#include "plctag.hpp"
#include "libplctag.h"

#include <cstdio>
#include <cstring>
#include <cassert>
#include <algorithm>
#include <ranges>
#include <stdarg.h>

namespace rng = std::ranges;
namespace rnv = std::views;


/* validation */

namespace plctag
{
    constexpr auto ERR_NO_ERROR = "No error. Everything OK";
    constexpr auto ERR_TAG_SIZE = "Tag size error";
    constexpr auto ERR_ELEM_SIZE = "Tag element/count size error";


    template <class T>
    static void decode_result(Result<T>& result, int rc)
    {
        if (rc < 0)
        {
            result.status = static_cast<Status>(rc);
            result.error = plc_tag_decode_error(rc);
        }
        else
        {
            result.status = Status::OK;
            result.error = ERR_NO_ERROR;
        }
    }


    template <class A, class B>
    static void copy_result_status(Result<A> const& src, Result<B>& dst)
    {
        dst.status = src.status;
        dst.error = src.error;
    }


    template <class T>
    static void make_ok_result(Result<T>& result)
    {
        result.status = Status::OK;
        result.error = ERR_NO_ERROR;
    }


    static bool validate_tag_attributes(Tag_Attr const& attr)
    {
        if (!attr.gateway || !attr.tag_name)
        {
            false;
        }

        auto has_path = strlen(attr.path) > 0;
        auto dhp_path_ok = (attr.has_dhp ? has_path : true);

        bool result = true;

        switch (attr.controller)
        {
        case Controller::ControlLogix:
            result &= has_path;
            break;
        case Controller::PLC5:
            result &= dhp_path_ok;
            break;
        case Controller::SLC500:
            result &= dhp_path_ok;
            break;
        case Controller::MicroLogix:
            result &= dhp_path_ok;
            break;
        default:            
            break;
        }

        return true;
    }
}


/* connection string */

namespace plctag
{
    // attribute max lengths
    constexpr size_t GATEWAY_SZ = sizeof("gateway=192.168.101.101");
    constexpr size_t PATH_SZ    = sizeof("path=1,0");
    constexpr size_t NAME_SZ = 256;

    constexpr size_t CONNECTION_STRING_SZ = GATEWAY_SZ + PATH_SZ + NAME_SZ + 50;


    namespace
    {
        template <size_t N>
        class CharString
        {
        public:
            static constexpr u32 capacity = N;
            char data[N] = { 0 };
        };
    }


    using GatewayStr = CharString<GATEWAY_SZ>;
    using PathStr = CharString<PATH_SZ>;
    using NameStr = CharString<NAME_SZ>;
    using ConnectionStr = CharString<CONNECTION_STRING_SZ>;


    template <size_t N>
    static void reset_string_data(CharString<N>& str)
    {
        memset(str.data, 0, str.capacity);
    }


    template <size_t N>
    static void write_string_data(CharString<N>& str, const char* format, ...)
    {
        va_list args;
        va_start(args, format);

        snprintf(str.data, str.capacity, format, args);

        va_end(args);
    }


    template <size_t N>
    static bool build_kv_string(cstr key, cstr value, CharString<N>& dst)
    {
        auto base = "%s=%s";

        auto len = strlen(key) + strlen(value) + 2;
        if (dst.capacity < len)
        {
            return false;
        }

        write_string_data(dst, base, key, value);
        return true;
    }


    static bool build_connection_string(cstr kv_1, cstr kv_2, cstr kv_3, cstr kv_4, cstr kv_5, ConnectionStr& dst)
    {
        auto base = "%s&%s&%s&%s&%s";

        auto len = strlen(kv_1) + strlen(kv_2) + strlen(kv_3) + strlen(kv_4) + strlen(kv_5) + 5;
        if (dst.capacity < len)
        {
            return false;
        }

        write_string_data(dst, base, kv_1, kv_2, kv_3, kv_4, kv_5);
        return true;
    }


    static bool build_connection_string(cstr kv_1, cstr kv_2, cstr kv_3, cstr kv_4, ConnectionStr& dst)
    {
        auto base = "%s&%s&%s&%s";

        auto len = strlen(kv_1) + strlen(kv_2) + strlen(kv_3) + strlen(kv_4) + 4;
        if (dst.capacity < len)
        {
            return false;
        }

        write_string_data(dst, base, kv_1, kv_2, kv_3, kv_4);
        return true;
    }


    static bool build_connection_string(Tag_Attr const& attr, ConnectionStr& dst)
    {
        cstr protocol = 0;
        cstr plc = 0;

        GatewayStr gateway{};
        PathStr path{};
        NameStr name{};

        auto has_path = strlen(attr.path) > 0;

        bool result = true;

        result &= build_kv_string("gateway", attr.gateway, gateway);
        result &= build_kv_string("name", attr.tag_name, name);

        if (has_path)
        {
            result &= build_kv_string("path", attr.path, path);
        }

        if (!result)
        {
            return false;
        }        

        switch (attr.controller)
        {
        case Controller::ControlLogix:
            protocol = "protocol=ab-eip";
            plc = "plc=controllogix";            
            result &= build_connection_string(protocol, plc, gateway.data, path.data, name.data, dst);
            break;

        case Controller::Modbus:
            protocol = "protocol=mb-tcp";
            result &= build_connection_string(protocol, gateway.data, path.data, name.data, dst);
            break;

        case Controller::PLC5:
            protocol = "protocol=ab-eip";
            plc = "plc=plc5"; 

            if (attr.has_dhp)
            {
                result &= has_path && build_connection_string(protocol, plc, gateway.data, path.data, name.data, dst);
            }
            else
            {
                result &= build_connection_string(protocol, plc, gateway.data, name.data, dst);
            }
            break;

        case Controller::SLC500:
            protocol = "protocol=ab-eip";
            plc = "plc=slc500";

            if (attr.has_dhp)
            {
                result &= has_path && build_connection_string(protocol, plc, gateway.data, path.data, name.data, dst);
            }
            else
            {
                result &= build_connection_string(protocol, plc, gateway.data, name.data, dst);
            }
            break;

        case Controller::MicroLogix:
            protocol = "protocol=ab-eip";
            plc = "plc=micrologix";

            if (attr.has_dhp)
            {
                result &= has_path && build_connection_string(protocol, plc, gateway.data, path.data, name.data, dst);
            }
            else
            {
                result &= build_connection_string(protocol, plc, gateway.data, name.data, dst);
            }
            break;

        case Controller::LogixPccc:
            protocol = "protocol=ab-eip";
            plc = "plc=lgxpccc";
            result &= build_connection_string(protocol, plc, gateway.data, name.data, dst);
            break;

        case Controller::Micro800:
            protocol = "protocol=ab-eip";
            plc = "plc=micro800";
            result &= build_connection_string(protocol, plc, gateway.data, name.data, dst);
            break;
        
        case Controller::OmronNJNX:
            protocol = "protocol=ab-eip";
            plc = "plc=omron-njnx";
            result &= build_connection_string(protocol, plc, gateway.data, name.data, dst);
            break;
        
        default:
            break;
        }

        return result;
    }



}


/* api wrapper */

namespace plctag
{
    static ConnectResult attempt_connection(Tag_Attr attr, int timeout)
    {
        ConnectResult result{};

        if (!validate_tag_attributes(attr))
        {
            result.status = Status::ERR_BAD_ATTRS;
            result.error = "Invalid tag attributes";
            return result;
        }

        ConnectionStr str{};

        if (!build_connection_string(attr, str))
        {
            result.status = Status::ERR_BAD_ATTRS;
            result.error = "Invalid tag attributes";
            return result;
        }

        auto rc = plc_tag_create(str.data, timeout);
        decode_result(result, rc);
        if (!result.is_ok())
        {
            return result;
        }

        i32 tag_id = rc;

        rc = plc_tag_read(tag_id, timeout);
        decode_result(result, rc);
        if (!result.is_ok())
        {
            return result;
        }

        auto size = std::max(plc_tag_get_size(rc), 0);
        auto elem_size = std::max(plc_tag_get_int_attribute(rc, "elem_size", 0), 0);
        auto elem_count = std::max(plc_tag_get_int_attribute(rc, "elem_count", 0), 0);

        result.data.tag_handle = tag_id;
        result.data.tag_size = (u32)size;
        result.data.elem_size = (u32)elem_size;
        result.data.elem_count = (u32)elem_count;

        return result;
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

    void set_debug_level(DebugLevel debug_level)
    {
        plc_tag_set_debug_level(static_cast<int>(debug_level));
    }


    ConnectResult connect(Tag_Attr attr, int timeout)
    {
        ConnectResult result = attempt_connection(attr, timeout);

        if (!result.is_ok())
        {
            return result;
        }

        // kill the connection if sizes don't make sense;
        auto data = result.data;

        if (data.tag_size == 0 || data.tag_size != data.elem_count * data.elem_size)
        {
            result.status = Status::ERR_BAD_SIZE;
            result.error = ERR_ELEM_SIZE;
            plc_tag_destroy(data.tag_handle);
            result.data.tag_handle = -1;            
            return result;
        }

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

    void shutdown()
    {
        plc_tag_shutdown();
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
        result.data = rc;

        return result;        
    }


    /*
    
    The following functions support getting and setting integer and floating point values from or in a tag.

    The getter functions return the value of the size in the function name from the tag at the byte offset in the tag's data.
    The setter functions do the opposite and put the passed value (using the appropriate number of bytes) at the passed byte offset in the tag's data.

    Unsigned getters return the appropriate UINT_MAX value for the type size on error. I.e. plc_tag_get_uint16() returns 65535 (UINT16_MAX) if there is an error. 
    You can check for this value and then call plc_tag_status to determine what went wrong. Signed getters return the appropriate INT_MIN value on error.

    Setters return one of the status codes above. If there is no error then PLCTAG_STATUS_OK will be returned.

    NOTE the implementation of the floating point getter and setter may not be totally portable. Please test before use on big-endian machines.

    All getters and setters convert their data into the correct endian type. A setter will convert the host endian data into the target endian data. 
    A getter will convert the target endian data it retrieves into the host endian format.    
    
    */


    Result<int> get_bit(i32 tag, int offset_bit)
    {
        Result<int> result{};

        auto value = plc_tag_get_bit(tag, offset_bit);
        auto rc = plc_tag_status(tag);
        decode_result(result, rc);

        if (result.is_ok())
        {
            result.data = value;
        }

        return result;
    }


    template <typename T, typename API_GET>
    static Result<T> plctag_api_get(API_GET get, i32 tag, int offset)
    {
        Result<T> result{};

        auto value = get(tag, offset);
        auto rc = plc_tag_status(tag);
        decode_result(result, rc);

        if (result.is_ok())
        {
            result.data = value;
        }

        return result;
    }


    template <typename T, typename API_GET>
    static Result<T> plctag_api_get_cast(API_GET get, i32 tag, int offset)
    {
        Result<T> result{};

        auto value = get(tag, offset);
        auto rc = plc_tag_status(tag);
        decode_result(result, rc);

        if (result.is_ok())
        {
            result.data = (T)value;
        }

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


    Result<u64> get_string_length(i32 tag_id, int string_start_offset)
    {
        return plctag_api_get_cast<u64>(plc_tag_get_string_length, tag_id, string_start_offset);
    }


    Result<u64> get_string_capacity(i32 tag_id, int string_start_offset)
    {
        return plctag_api_get_cast<u64>(plc_tag_get_string_capacity, tag_id, string_start_offset);
    }


    Result<u64> get_string_total_length(i32 tag_id, int string_start_offset)
    {
        return plctag_api_get_cast<u64>(plc_tag_get_string_total_length, tag_id, string_start_offset);
    }


    Result<int> get_string(i32 tag_id, int string_start_offset, char* buffer, int buffer_length)
    {
        Result<int> result{};

        auto rc = plc_tag_get_string(tag_id, string_start_offset, buffer, buffer_length);
        decode_result(result, rc);

        return result;
    }


    
}


/* decode enums */

namespace plctag
{
    constexpr auto TYPE_IS_STRUCT = (u16)0x8000;
    constexpr auto TYPE_IS_SYSTEM = (u16)0x1000;
    constexpr auto TYPE_DIM_MASK = (u16)0x6000;
    constexpr auto TYPE_UDT_ID_MASK = (u16)0x0FFF;
    constexpr auto TAG_DIM_MASK = (u16)0x6000;


    static TagType get_tag_type(uint16_t element_type)
    {
        if (element_type & TYPE_IS_SYSTEM)
        {
            return TagType::SYSTEM;
        }
        else if (element_type & TYPE_IS_STRUCT)
        {
            return TagType::UDT;
        }

        uint16_t atomic_type = element_type & 0x00FF; /* MAGIC */
        const char* type = NULL;

        switch (atomic_type)
        {
        case 0xC1: return TagType::BOOL;
        case 0xC2: return TagType::SINT;
        case 0xC3: return TagType::INT;
        case 0xC4: return TagType::DINT;
        case 0xC5: return TagType::LINT;
        case 0xC6: return TagType::USINT;
        case 0xC7: return TagType::UINT;
        case 0xC8: return TagType::UDINT;
        case 0xC9: return TagType::ULINT;
        case 0xCA: return TagType::REAL;
        case 0xCB: return TagType::LREAL;
        case 0xCC: return TagType::SYNCHRONOUS_TIME;
        case 0xCD: return TagType::DATE;
        case 0xCE: return TagType::TIME;
        case 0xCF: return TagType::DATE;
        case 0xD0: return TagType::CHAR_STRING;
        case 0xD1: return TagType::STRING_8;
        case 0xD2: return TagType::STRING_16;
        case 0xD3: return TagType::STRING_32;
        case 0xD4: return TagType::STRING_64;
        case 0xD5: return TagType::WIDE_STRING;
        case 0xD6: return TagType::HIGH_RES_DURATION;
        case 0xD7: return TagType::MED_RES_DURATION;
        case 0xD8: return TagType::LOW_RES_DURATION;
        case 0xD9: return TagType::N_BYTE_STRING;
        case 0xDA: return TagType::COUNTED_CHAR_STRING;
        case 0xDB: return TagType::DURATION_MS;
        case 0xDC: return TagType::CIP_PATH;
        case 0xDD: return TagType::ENGINEERING_UNITS;
        case 0xDE: return TagType::INTERNATIONAL_STRING;
        }

        return TagType::UNKNOWN;
    }


    cstr decode_status(Status s)
    {
        switch (s)
        {
        case Status::NOT_SET:       return "No status set";
        case Status::ERR_BAD_SIZE:  return "Unexpected or negative tag size";
        case Status::ERR_BAD_ATTRS: return "Invalid tag attributes/connection string";
        default:                    return plc_tag_decode_error(static_cast<int>(s));
        }
    }


    cstr decode_controller(Controller c)
    {
        switch (c)
        {
        case Controller::ControlLogix: return "Control Logix";
        case Controller::PLC5:         return "PLC/5";
        case Controller::SLC500:       return "SLC 500";
        case Controller::LogixPccc:    return "Control Logix PLC/5";
        case Controller::Micro800:     return "Micro800";
        case Controller::MicroLogix:   return "Micrologix";
        case Controller::OmronNJNX:    return "Omron NJ/NX";
        case Controller::Modbus:       return "Modbus";
        default:                       assert(false); return "unknown";
        }
    }


    cstr decode_controller(int c)
    {
        auto controller = static_cast<Controller>(c);
        switch (controller)
        {
        case Controller::ControlLogix:
        case Controller::PLC5:
        case Controller::SLC500:
        case Controller::LogixPccc:
        case Controller::Micro800:
        case Controller::MicroLogix:
        case Controller::OmronNJNX:
        case Controller::Modbus:
            return decode_controller(controller);
        default:
            // lets us know if the int passed is invalid
            return nullptr;
        }
    }


    cstr decode_tag_type(TagType t)
    {
        switch (t)
        {
        case TagType::SYSTEM:               return "System";
        case TagType::UDT:                  return "UDT: User defined type";
        case TagType::BOOL:                 return "BOOL: Boolean value";
        case TagType::SINT:                 return "SINT: Signed 8-bit integer value";
        case TagType::INT:                  return "INT: Signed 16-bit integer value";
        case TagType::DINT:                 return "DINT: Signed 32-bit integer value";
        case TagType::LINT:                 return "LINT: Signed 64-bit integer value";
        case TagType::USINT:                return "USINT: Unsigned 8-bit integer value";
        case TagType::UINT:                 return "UINT: Unsigned 16-bit integer value";
        case TagType::UDINT:                return "UDINT: Unsigned 32-bit integer value";
        case TagType::ULINT:                return "ULINT: Unsigned 64-bit integer value";
        case TagType::REAL:                 return "REAL: 32-bit floating point value, IEEE format";
        case TagType::LREAL:                return "LREAL: 64-bit floating point value, IEEE format";
        case TagType::SYNCHRONOUS_TIME:     return "Synchronous time value";
        case TagType::DATE:                 return "Date value";
        case TagType::TIME:                 return "Time of day value";
        case TagType::DATETIME:             return "Date and time of day value";
        case TagType::CHAR_STRING:          return "Character string, 1 byte per character";
        case TagType::STRING_8:             return "8-bit bit string";
        case TagType::STRING_16:            return "16-bit bit string";
        case TagType::STRING_32:            return "32-bit bit string";
        case TagType::STRING_64:            return "64-bit bit string";
        case TagType::WIDE_STRING:          return "Wide char character string, 2 bytes per character";
        case TagType::HIGH_RES_DURATION:    return "High resolution duration value";
        case TagType::MED_RES_DURATION:     return "Medium resolution duration value";
        case TagType::LOW_RES_DURATION:     return "Low resolution duration value";
        case TagType::N_BYTE_STRING:        return "N-byte per char character string";
        case TagType::COUNTED_CHAR_STRING:  return "Counted character sting with 1 byte per character and 1 byte length indicator";
        case TagType::DURATION_MS:          return "Duration in milliseconds";
        case TagType::CIP_PATH:             return "CIP path segment(s)";
        case TagType::ENGINEERING_UNITS:    return "Engineering units";
        case TagType::INTERNATIONAL_STRING: return "International character string (encoding?)";
        default:                            assert(false); return "unknown";
        }
    }
}


/* extra */

namespace plctag
{
    constexpr auto TAG_LIST_KEY = "@tags";
    constexpr auto UDT_KEY = "@udt";

    template <size_t N>
    static bool fill_string_buffer(CharString<N>& buffer, i32 handle, int offset)
    {
        reset_string_data(buffer);

        auto string_len = plc_tag_get_string_length(handle, offset);
        if (string_len <= 0 || string_len >= buffer.capacity)
        {
            write_string_data(buffer, "ERROR plc_tag_get_string_length");
            return false;
        }

        auto rc = plc_tag_get_string(handle, offset, buffer.data, string_len + 1);
        if (rc != PLCTAG_STATUS_OK)
        {
            write_string_data(buffer, "ERROR plc_tag_get_string: %s", plc_tag_decode_error(rc));
            return false;
        }

        return true;
    }


    template <class ENTRY>
    static void get_entry_name(ENTRY& entry, i32 handle, int offset)
    {
        NameStr name;

        fill_string_buffer(name, handle, offset);

        entry.name = name.data;
    }


    int build_tag_entry(Tag_Entry& entry, i32 handle, int offset)
    {
        /* each entry looks like this:
        uint32_t instance_id    monotonically increasing but not contiguous
        uint16_t symbol_type    type of the symbol.
        uint16_t element_length length of one array element in bytes.
        uint32_t array_dims[3]  array dimensions.
        uint8_t string_data[]   string bytes (string_len of them)
        */

        constexpr int sz32 = 4;
        constexpr int sz16 = 2;

        u16 symbol_type = 0;

        entry.instance_id = plc_tag_get_uint32(handle, offset);
        offset += sz32;

        symbol_type = plc_tag_get_uint16(handle, offset);
        entry.type_code = symbol_type;
        entry.tag_type = get_tag_type(symbol_type);
        offset += sz16;

        entry.elem_size = plc_tag_get_uint16(handle, offset);
        offset += sz16;

        entry.num_dimensions = (u16)((symbol_type & TAG_DIM_MASK) >> 13);
        entry.elem_count = 1;
        for (u32 i = 0; i < 3; ++i)
        {
            entry.dimensions[i] = (u16)plc_tag_get_uint32(handle, offset);
            entry.elem_count *= std::max((u16)1, entry.dimensions[i]);
            offset += sz32;
        }

        get_entry_name(entry, handle, offset);

        return offset;
    }


    int build_udt_entry(UDT_Entry& entry, i32 handle, u16 udt_id)
    {
        /*

        The format in the tag buffer is:

        A new header of 14 bytes:

        Bytes   Meaning
        0-1     16-bit UDT ID
        2-5     32-bit UDT member description size, in 32-bit words.
        6-9     32-bit UDT instance size, in bytes.
        10-11   16-bit UDT number of members (fields).
        12-13   16-bit UDT handle/type.

        Then the raw field info.

        N x field info entries
            uint16_t field_metadata - array element count or bit field number
            uint16_t field_type
            uint32_t field_offset

        int8_t string - zero-terminated string, UDT name, but name stops at first semicolon!

        N x field names
            int8_t string - zero-terminated.

        */

        // get header info
        entry.id = plc_tag_get_uint16(handle, 0);
        //member_desc_size = plc_tag_get_uint32(udt_info_tag, 2);
        entry.instance_size = plc_tag_get_uint32(handle, 6);
        entry.num_fields = plc_tag_get_uint16(handle, 10);
        entry.struct_handle = plc_tag_get_uint16(handle, 12);

        constexpr int sz32 = 4;
        constexpr int sz16 = 2;

        u16 symbol_type = 0;
        int offset = 14; // skip past the header

        if (entry.id != udt_id)
        {
            // should be the same
        }

        entry.fields = List<UDT_Field_Entry>(entry.num_fields);

        for (auto& field : entry.fields)
        {
            field.metadata = plc_tag_get_uint16(handle, offset);
            offset += sz16;

            symbol_type = plc_tag_get_uint16(handle, offset);
            field.type_code = symbol_type;
            field.tag_type = get_tag_type(symbol_type);
            offset += sz16;

            field.offset = plc_tag_get_uint32(handle, offset);
            offset += sz32;

            // child udts
            if (field.tag_type == TagType::UDT)
            {
                auto child_id = field.type_code & TYPE_UDT_ID_MASK;
            }
        }

        /*

        Get the template/UDT name.   This is weird.
        Scan until we see a 0x3B, semicolon, byte.   That is the end of the
        template name.   Actually we should look for ";n" but the semicolon
        seems to be enough for now.

        */

        NameStr name;

        if (!fill_string_buffer(name, handle, offset))
        {
            entry.name = name.data;
        }
        else
        {
            int idx = 0;
            for (; name.data[idx] != ';' && idx < 256; ++idx) {}
            if (name.data[idx] == ';')
            {
                name.data[idx] = 0;
                entry.name = name.data;
            }
            else
            {
                entry.name = "ERROR parsing UDT name string";
            }
        }

        offset += plc_tag_get_string_total_length(handle, offset);

        // field names
        for (auto& field : entry.fields)
        {
            get_entry_name(field, handle, offset);

            offset += plc_tag_get_string_total_length(handle, offset);
        }

        return offset;
    }


    static bool append_tag_list(Tag_Desc const& tag_info, List<Tag_Entry>& tag_list)
    {
        auto handle = tag_info.tag_handle;

        auto payload_size = plc_tag_get_size(tag_info.tag_handle);
        if (payload_size < 4)
        {
            return false;
        }
        
        int offset = 0;
        while (offset < payload_size)
        {
            Tag_Entry entry{};
            offset = build_tag_entry(entry, handle, offset);

            tag_list.push_back(std::move(entry));

            offset += plc_tag_get_string_total_length(handle, offset);
        }

        return true;
    }


    static bool append_udt_list(Tag_Desc const& tag_info, List<UDT_Entry>& udt_list, u16 udt_id)
    {
        auto handle = tag_info.tag_handle;

        auto payload_size = plc_tag_get_size(tag_info.tag_handle);
        if (payload_size < 4)
        {
            return false;
        }

        UDT_Entry entry{};

        build_udt_entry(entry, handle, udt_id);

        udt_list.push_back(std::move(entry));
        return true;
    }


    Result<int> enumerate_tags(PLC_Desc& data, int timeout)
    {
        Tag_Attr tag_info_attr{};
        tag_info_attr.controller = data.controller;
        tag_info_attr.gateway = data.gateway;
        tag_info_attr.path = data.path;
        tag_info_attr.has_dhp = data.has_dhp;
        tag_info_attr.tag_name = 0;

        Result<int> result{};

        ConnectResult controller_result{};
        ConnectResult program_result{};
        ConnectResult udt_result{};

        auto const close_connection = [](ConnectResult& res) { destroy(res.data.tag_handle); };

        // constroller tags
        tag_info_attr.tag_name = TAG_LIST_KEY;        
        controller_result = attempt_connection(tag_info_attr, timeout);
        if (!controller_result.is_ok())
        {
            copy_result_status(controller_result, result);
            return result;
        }

        append_tag_list(controller_result.data, data.controller_tags);
        close_connection(controller_result);

        NameStr name_str{};

        auto const is_program_tag = [](Tag_Entry const& tag) { return tag.name.starts_with("Program:"); };
        auto const is_udt_tag = [](Tag_Entry const& tag) { return tag.tag_type == TagType::UDT; };

        // program tags        
        auto program_headers = rnv::filter(data.controller_tags, is_program_tag);
        for(auto& header : program_headers)
        {
            reset_string_data(name_str);
            write_string_data(name_str, "%s.%s", header.name.c_str(), TAG_LIST_KEY);

            tag_info_attr.tag_name = name_str.data;
            program_result = attempt_connection(tag_info_attr, timeout);
            if (!program_result.is_ok())
            {
                header.name += " < " + std::string(decode_status(program_result.status)) + " > ";
                continue;
            }

            append_tag_list(program_result.data, data.program_tags);
            close_connection(program_result);
        }

        // UDT tags        
        auto udt_headers = rnv::filter(data.controller_tags, is_udt_tag);
        for (auto& header : udt_headers)
        {
            auto udt_id = header.type_code & TYPE_UDT_ID_MASK;

            reset_string_data(name_str);
            write_string_data(name_str, "%s/%u", UDT_KEY, (u32)udt_id);

            tag_info_attr.tag_name = name_str.data;
            udt_result = attempt_connection(tag_info_attr, timeout);
            if (!udt_result.is_ok())
            {
                header.name += " < " + std::string(decode_status(program_result.status)) + " >";
                continue;
            }

            append_udt_list(udt_result.data, data.udt_tags, udt_id);
            close_connection(udt_result);
        }

        make_ok_result(result);
        return result;
    }


    


    
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



/* debugging */

namespace plctag
{
namespace dbg
{
    bool build_attr_string(Tag_Attr attr, char* dst, size_t max_len)
    {
        ConnectionStr str{};

        if (strlen(str.data) > max_len)
        {
            return false;
        }

        if (!build_connection_string(attr, str))
        {
            return false;
        }

        strcpy_s(dst, max_len, str.data);

        return true;
    }
}
}