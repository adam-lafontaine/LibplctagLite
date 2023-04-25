#include "plctag.hpp"
#include "libplctag.h"

#include <cstdio>
#include <cstring>
#include <cassert>


namespace plctag
{
    constexpr auto ERR_NO_ERROR = "No error. Everything OK";
    constexpr auto ERR_TAG_SIZE = "Tag size error";
    constexpr auto ERR_ELEM_SIZE = "Tag element/count size error";

    // attribute max lengths
    constexpr size_t GATEWAY_SZ = sizeof("gateway=192.168.101.101");
    constexpr size_t PATH_SZ    = sizeof("path=1,0");
    constexpr size_t NAME_SZ    = sizeof("name=SomeTagNameWhichIsTheLongestNameThatWeWantToSupport");

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
    static bool build_kv_string(cstr key, cstr value, CharString<N>& dst)
    {
        auto base = "%s=%s";

        auto len = strlen(key) + strlen(value) + 2;
        if (dst.capacity < len)
        {
            return false;
        }

        snprintf(dst.data, dst.capacity, base, key, value);
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

        snprintf(dst.data, dst.capacity, base, kv_1, kv_2, kv_3, kv_4, kv_5);
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

        snprintf(dst.data, dst.capacity, base, kv_1, kv_2, kv_3, kv_4);
        return true;
    }


    static bool validate_tag_attributes(TagAttr const& attr)
    {
        if (!attr.gateway || !attr.tag_name)
        {
            false;
        }

        if (attr.controller != Controller::Modbus && !attr.path)
        {
            return false;
        }

        return true;
    }


    static bool build_connection_string(TagAttr const& attr, ConnectionStr& dst)
    {
        cstr protocol = 0;
        cstr plc = 0;

        GatewayStr gateway{};
        PathStr path{};
        NameStr name{};

        bool result = true;

        switch (attr.controller)
        {
        case Controller::ControlLogix:
            protocol = "protocol=ab-eip";
            plc = "plc=controllogix";

            result &= build_kv_string("gateway", attr.gateway, gateway);
            result &= build_kv_string("path", attr.path, path);
            result &= build_kv_string("name", attr.tag_name, name);

            if (result)
            {
                result &= build_connection_string(protocol, plc, gateway.data, path.data, name.data, dst);
            }

            break;
        case Controller::PLC5:
            protocol = "protocol=ab-eip";
            plc = "plc5";

            result &= build_kv_string("gateway", attr.gateway, gateway);
            result &= build_kv_string("path", attr.path, path);
            result &= build_kv_string("name", attr.tag_name, name);

            if (result)
            {
                result &= build_connection_string(protocol, plc, gateway.data, path.data, name.data, dst);
            }

            break;
        case Controller::SLC500:
            protocol = "protocol=ab-eip";
            plc = "slc500";

            result &= build_kv_string("gateway", attr.gateway, gateway);
            result &= build_kv_string("path", attr.path, path);
            result &= build_kv_string("name", attr.tag_name, name);

            if (result)
            {
                result &= build_connection_string(protocol, plc, gateway.data, path.data, name.data, dst);
            }

            break;
        case Controller::LogixPccc:
            protocol = "protocol=ab-eip";
            plc = "lgxpccc";

            result &= build_kv_string("gateway", attr.gateway, gateway);
            result &= build_kv_string("path", attr.path, path);
            result &= build_kv_string("name", attr.tag_name, name);

            if (result)
            {
                result &= build_connection_string(protocol, plc, gateway.data, path.data, name.data, dst);
            }

            break;
        case Controller::Micro800:
            protocol = "protocol=ab-eip";
            plc = "micro800";

            result &= build_kv_string("gateway", attr.gateway, gateway);
            result &= build_kv_string("name", attr.tag_name, name);

            if (result)
            {
                result &= build_connection_string(protocol, plc, gateway.data, name.data, dst);
            }

            break;
        case Controller::MicroLogix:
            protocol = "protocol=ab-eip";
            plc = "micrologix";

            result &= build_kv_string("gateway", attr.gateway, gateway);
            result &= build_kv_string("path", attr.path, path);
            result &= build_kv_string("name", attr.tag_name, name);

            if (result)
            {
                result &= build_connection_string(protocol, plc, gateway.data, path.data, name.data, dst);
            }

            break;
        case Controller::OmronNJNX:
            protocol = "protocol=ab-eip";
            plc = "omron-njnx";

            result &= build_kv_string("gateway", attr.gateway, gateway);
            result &= build_kv_string("path", attr.path, path);
            result &= build_kv_string("name", attr.tag_name, name);

            if (result)
            {
                result &= build_connection_string(protocol, plc, gateway.data, path.data, name.data, dst);
            }

            break;
        case Controller::Modbus:
            protocol = "protocol=mb-tcp";

            result &= build_kv_string("gateway", attr.gateway, gateway);
            result &= build_kv_string("path", attr.path, path);
            result &= build_kv_string("name", attr.tag_name, name);

            if (result)
            {
                result &= build_connection_string(protocol, gateway.data, path.data, name.data, dst);
            }

            break;
        default:
            break;
        }

        return result;
    }
}


namespace plctag
{
    


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

    void set_debug_level(DebugLevel debug_level)
    {
        plc_tag_set_debug_level(static_cast<int>(debug_level));
    }


    ConnectResult connect(TagAttr attr, int timeout)
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

        auto size = plc_tag_get_size(rc);
        if (size <= 0)
        {
            rc = plc_tag_status(size);
            decode_result(result, rc);
            plc_tag_destroy(tag_id);
            return result;
        }

        auto elem_size = plc_tag_get_int_attribute(rc, "elem_size", 0);
        auto elem_count = plc_tag_get_int_attribute(rc, "elem_count", 0);

        if (elem_size <= 0 || elem_count <= 0 || size != elem_size * elem_count)
        {
            result.status = Status::ERR_BAD_SIZE;
            result.error = ERR_ELEM_SIZE;
            result.data.tag_handle = -1;
            plc_tag_destroy(tag_id);
            return result;
        }

        result.data.tag_handle = tag_id;
        result.data.tag_size = (u64)size;
        result.data.elem_size = (u64)elem_size;
        result.data.elem_count = (u64)elem_count;

        return result;
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

        i32 tag_id = rc;

        auto size = plc_tag_get_size(rc);
        if (size <= 0)
        {
            rc = plc_tag_status(size);
            decode_result(result, rc);
            plc_tag_destroy(tag_id);
            return result;
        }

        auto elem_size = plc_tag_get_int_attribute(rc, "elem_size", 0);
        auto elem_count = plc_tag_get_int_attribute(rc, "elem_count", 0);

        if (elem_size <= 0 || elem_count <= 0 || size != elem_size * elem_count)
        {
            result.status = Status::ERR_BAD_SIZE;
            result.error = ERR_ELEM_SIZE;
            result.data.tag_handle = -1;
            plc_tag_destroy(tag_id);
            return result;
        }

        result.data.tag_handle = tag_id;
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


    cstr decode_controller(Controller c)
    {
        switch (c)
        {
        case Controller::ControlLogix:
            return "Control Logix";
            break;
        case Controller::PLC5:
            return "PLC/5";
            break;
        case Controller::SLC500:
            return "SLC 500";
            break;
        case Controller::LogixPccc:
            return "Control Logix PLC/5";
            break;
        case Controller::Micro800:
            return "Micro800";
            break;
        case Controller::MicroLogix:
            return "Micrologix";
            break;
        case Controller::OmronNJNX:
            return "Omron NJ/NX";
            break;
        case Controller::Modbus:
            return "Modbus";
            break;
        default:
            assert(false);
            return "unknown";
            break;
        }
    }


    cstr decode_controller(int c)
    {
        switch (static_cast<Controller>(c))
        {
        case Controller::ControlLogix:
            return "Control Logix";
            break;
        case Controller::PLC5:
            return "PLC/5";
            break;
        case Controller::SLC500:
            return "SLC 500";
            break;
        case Controller::LogixPccc:
            return "Control Logix PLC/5";
            break;
        case Controller::Micro800:
            return "Micro800";
            break;
        case Controller::MicroLogix:
            return "Micrologix";
            break;
        case Controller::OmronNJNX:
            return "Omron NJ/NX";
            break;
        case Controller::Modbus:
            return "Modbus";
            break;
        default:
            // lets us know if the int passed is invalid
            return nullptr;
            break;
        }
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


namespace plctag
{
namespace dbg
{
    bool build_attr_string(TagAttr attr, char* dst, size_t max_len)
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