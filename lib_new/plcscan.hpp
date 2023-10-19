#include "../util/types.hpp"
#include "../util/memory_buffer.hpp"

#include <vector>
#include <functional>


namespace mb = memory_buffer;


using StringView = MemoryView<char>;
using ByteView = MemoryView<u8>;

template <typename T>
using List = std::vector<T>;

using void_f = std::function<void()>;
using bool_f = std::function<bool()>;


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

        StringView name;

        ByteView data;

        // TODO: tag/connection status

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
        DataTypeId32 type_id = 0;
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


    class PlcTagData
    {
    public:
        List<DataType> data_types;
        List<UdtType> udt_types;
        List<Tag> tags;

        cstr gateway = "192.168.123.123";
        cstr path = "1,0";

        bool is_connected = false;
    };
}


/* enum */

namespace plcscan
{
    enum class DataTypeCategory : int
    {
        Numeric,
        String,
        Udt,
        Other
    };
}


/* api */

namespace plcscan
{
    void disconnect();
    
    PlcTagData connect(cstr gateway, cstr path);

    void scan(void_f const& scan_cb, bool_f const& scan_condition);

    DataTypeCategory get_type_category(DataTypeId32 type_id);
}