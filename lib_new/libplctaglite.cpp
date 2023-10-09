#include "../util/types.hpp"
#include "../util/memory_buffer.hpp"

namespace mb = memory_buffer;

using Bytes = MemoryView<u8>;
using String = MemoryView<char>;


/* tag listing */

namespace
{
    class TagEntry
    {
    public:
        u16 type_code = 0;
        u32 elem_count = 0;
        u32 elem_size = 0;
        String name;
    };
}


/* tag types */

namespace
{
    class TypeTable
    {
    public:

    };
}


/* tag table */

namespace
{

    class Tag
    {
    public:
        int tag_id = -1;

        Bytes value;
        String name;
    };


    class TagQty
    {
    public:
        u32 n_tags = 0;
        u32 value_size = 0;
        u32 name_size = 0;
    };


    class TagTable
    {
    public:
        MemoryBuffer<Tag> tags;

        MemoryBuffer<u8> value_data;
        MemoryBuffer<char> name_data;
    };


    static void destroy_tag_table(TagTable& table)
    {
        mb::destroy_buffer(table.value_data);
        mb::destroy_buffer(table.name_data);

        mb::destroy_buffer(table.tags);
    }


    static DataResult<TagTable> create_tag_table(TagQty const& qty)
    {
        DataResult<TagTable> result{};
        auto& table = result.data;

        if (!mb::create_buffer(table.tags, qty.n_tags))
        {
            destroy_tag_table(table);
            return result;
        }

        if (!mb::create_buffer(table.value_data, qty.value_size))
        {
            destroy_tag_table(table);
            return result;
        }

        if (!mb::create_buffer(table.name_data, qty.name_size))
        {
            destroy_tag_table(table);
            return result;
        }

        mb::zero_buffer(table.name_data);
        mb::zero_buffer(table.name_data);

        result.success = true;
        return result;
    }


    static void add_tag(TagTable& table, Tag const& tag)
    {

    }
}


