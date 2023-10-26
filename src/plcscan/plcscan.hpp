#pragma once
/* LICENSE: See end of file for license information. */

#include "../util/mh_types.hpp"

#include <vector>
#include <functional>


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
        StringView data_type_name;

        ByteView bytes;

        // TODO: tag/connection status

        cstr name() const { return tag_name.data; }
        cstr type() const { return data_type_name.data; }
        u8* data() const { return bytes.data; }
        u32 size() const { return (u32)bytes.length; }
        bool is_array() const { return array_count > 1; }
    };


    class DataType
    {
    public:
        DataTypeId32 type_id = 0;

        StringView data_type_name;
        StringView data_type_description;

        u32 size = 0;

        cstr name() const { return data_type_name.data; }
        cstr description() const { return data_type_description.data; }
    };


    class UdtFieldType
    {
    public:
        DataTypeId32 type_id = 0;
        u32 offset;

        u32 array_count = 0;
        i32 bit_number = -1;

        StringView field_name;
        StringView data_type_name;

        cstr name() const { return field_name.data; }
        cstr type() const { return data_type_name.data; }

        bool is_array() const { return array_count > 1; }

        bool is_bit() const { return bit_number >= 0; }
    };


    class UdtType
    {
    public:
        DataTypeId32 type_id = 0;

        StringView udt_name;
        StringView udt_description;

        std::vector<UdtFieldType> fields;

        u32 size = 0;

        cstr name() const { return udt_name.data; }
        cstr description() const { return udt_description.data; }
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


/* api */

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

        MISC
    };


    using data_f = std::function<void(PlcTagData&)>;
    using bool_f = std::function<bool()>;


    void shutdown();

    PlcTagData init();
    
    bool connect(cstr gateway, cstr path, PlcTagData& data);

    TagType get_tag_type(DataTypeId32 type_id);

    void scan(data_f const& scan_cb, bool_f const& scan_condition, PlcTagData& data);    
}


/*
MIT License

Copyright (c) 2023 Adam Lafontaine

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/