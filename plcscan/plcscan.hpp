#pragma once

#include "../util/types.hpp"
#include "../util/memory_buffer.hpp"

#include <vector>
#include <functional>


namespace mb = memory_buffer;


using StringView = MemoryView<char>;
using ByteView = MemoryView<u8>;

template <typename T>
using List = std::vector<T>;


/* types */

namespace plcscan
{
    using DataTypeId32 = u32;


    class Tag
    {
    public:
        DataTypeId32 type_id = 0;
        u32 array_count = 0;

        StringView tag_name;

        ByteView bytes;

        // TODO: tag/connection status

        cstr name() const { return tag_name.begin; }
        u8* data() const { return bytes.begin; }
        u32 size() const { return (u32)bytes.length; }
    };


    class DataType
    {
    public:
        DataTypeId32 type_id = 0;

        StringView data_type_name;
        StringView data_type_description;

        u32 size = 0;

        cstr name() const { return data_type_name.begin; }
        cstr description() const { return data_type_description.begin; }
    };


    class UdtFieldType
    {
    public:
        DataTypeId32 type_id = 0;
        u32 offset;

        u32 array_count = 0;
        u32 bit_number = 0;

        StringView field_name;

        cstr name() const { return field_name.begin; }
    };


    class UdtType
    {
    public:
        DataTypeId32 type_id = 0;

        StringView udt_name;
        StringView udt_description;

        std::vector<UdtFieldType> fields;

        u32 size = 0;

        cstr name() const { return udt_name.begin; }
        cstr description() const { return udt_description.begin; }
    };


    class PlcTagData
    {
    public:
        List<DataType> data_types;
        List<UdtType> udt_types;
        List<Tag> tags;

        bool is_init = false;
        bool is_connected = false;
    };
}


/* enum */

namespace plcscan
{
    enum class TagType : int
    {
        BOOL,
        SINT,
        INT,
        DINT,
        LINT,
        USINT,
        UINT,
        UDINT,
        ULINT,
        REAL,
        LREAL,

        STRING,

        UDT,

        OTHER
    };
}


/* api */

namespace plcscan
{
    using data_f = std::function<void(PlcTagData&)>;
    using bool_f = std::function<bool()>;


    void disconnect();

    PlcTagData init();
    
    bool connect(cstr gateway, cstr path, PlcTagData& data);

    void scan(data_f const& scan_cb, bool_f const& scan_condition, PlcTagData& data);

    TagType get_tag_type(DataTypeId32 type_id);

    cstr get_fast_type_name(DataTypeId32 type_id);
}