#include "devplctag.hpp"
#include "../util/memory_helper.hpp"

#include <vector>

template <typename T>
using List = std::vector<T>;

namespace mh = memory_helper;


namespace dev
{
    class TagEntry
    {
    public:
        u32 instance_id = 0;       // 4
        u16 symbol_type = 0;       // 2
        u16 element_length = 0;    // 2
        u32 array_dims[3] = { 0 }; // 3 * 4
        u16 string_len = 0;        // 2

        cstr tag_name = 0;

        u32 entry_size()
        {
            return (u32)(
                sizeof(instance_id) + 
                sizeof(symbol_type) + 
                sizeof(element_length) + 
                sizeof(array_dims) + 
                sizeof(string_len) +
                string_len
            );
        }
    };


    class TagValue
    {
    public:
        ByteView value_bytes;
    };
}


namespace
{
    class TagDatabase
    {
    public:

        List<dev::TagEntry> tag_entries;
        List<dev::TagValue> tag_values;

        ByteBuffer tag_value_bytes;
    };
}


static TagDatabase g_tag_db{};


/* api */

namespace dev
{
    int plc_tag_create(const char* attr, int timeout)
    {
        if (attr[0] == '@')
        {
            if (attr[1] == 't')
            {
                // generate tag listing
            }
            else if (attr[1] == 'u')
            {
                // generate udt entry
            }

            return -1;
        }

        // create a tag
        // name matches one in the listing

        return -1; // PLCTAG_STATUS_OK;
    }


    int plc_tag_read(int handle, int timeout)
    {
        // generate a new value for the tag
        return -1; // PLCTAG_STATUS_OK;
    }


    int plc_tag_get_size(int handle)
    {
        auto& tags = g_tag_db.tag_values;

        if (handle < 0 || (u64)handle >= tags.size())
        {
            return -1;
        }

        auto size = (int)tags[handle].value_bytes.length;

        return size ? size : -1;
    }


    int plc_tag_get_raw_bytes(int handle, int offset, unsigned char* dst, int length)
    {
        auto& tags = g_tag_db.tag_values;

        if (handle < 0 || (u64)handle >= tags.size())
        {
            return -1;
        }

        auto& value = tags[handle].value_bytes;

        auto len = (u32)length < value.length ? (u32)length : value.length;

        auto src = value.data;

        mh::copy_bytes(src, dst, len);

        return PLCTAG_STATUS_OK;
    }


    void plc_tag_shutdown()
    {
        mb::destroy_buffer(g_tag_db.tag_value_bytes);
    }
}