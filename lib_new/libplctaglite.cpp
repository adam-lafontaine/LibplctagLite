#include "../util/types.hpp"
#include "../util/memory_buffer.hpp"

namespace mb = memory_buffer;


/* tag listing */

namespace
{
    class TagEntry
    {
    public:
        u16 type_code = 0;
        u32 elem_count = 0;

    };
}


/* tag table */
namespace
{
    class Tag
    {
    public:
        int tag_id = -1;

        u8* data = nullptr;
        u32 data_size = 0;
    };


    class TagTable
    {
    public:
        MemoryBuffer<Tag> tags;

        MemoryBuffer<u8> tag_data;
    };


    static DataResult<TagTable> create_tag_table(u32 n_tags)
    {
        DataResult<TagTable> result{};
        auto& table = result.data;

        mb::create_buffer(table.tags, n_tags);

        result.success = true;
        return result;
    }


    static void destroy_tag_table(TagTable& table)
    {
        mb::destroy_buffer(table.tag_data);

        mb::destroy_buffer(table.tags);
    }
}


