#include "../util/types.hpp"
#include "../util/memory_buffer.hpp"

#include <vector>


namespace mb = memory_buffer;


using StringView = MemoryView<char>;
using ByteView = MemoryView<u8>;


/* types */

namespace plcscan
{
    using DataTypeId32 = u32;


    class Tag
    {
    public:
        DataTypeId32 type_id = 0;
        u32 array_count = 0;
        // how to parse scan data
        // fixed type size, udt fields

        ByteView data;

        StringView name;

        bool connection_ok = false;
        bool scan_ok = false;    

        u32 size() const { return data.length; }
    };


    class DataType
    {
    public:
        DataTypeId32 type_id = 0;

        StringView data_type_name;
        StringView data_type_description;

        u32 size = 0;

        // how to parse scan data
    };


    class UdtFieldType
    {
    public:
        DataTypeId32 type_id;
        u32 offset;

        u32 array_count = 0;
        u32 bit_number = 0;

        StringView field_name;
    };


    class UdtType
    {
    public:
        DataTypeId32 type_id = 0;

        StringView udt_name;
        StringView udt_description;

        std::vector<UdtFieldType> fields;

        u32 size = 0;
    };
}


/* api */

namespace plcscan
{
    
}