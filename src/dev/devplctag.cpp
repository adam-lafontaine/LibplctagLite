#include "devplctag.hpp"
#include "../util/memory_helper.hpp"

#include <vector>
#include <array>
#include <random>

template <typename T>
using List = std::vector<T>;

namespace mh = memory_helper;


namespace dev
{
    static constexpr auto BOOL  = (u16)0xC1;
    static constexpr auto SINT  = (u16)0xC2;
    static constexpr auto INT   = (u16)0xC3;
    static constexpr auto DINT  = (u16)0xC4;
    static constexpr auto LINT  = (u16)0xC5;
    static constexpr auto USINT = (u16)0xC6;
    static constexpr auto UINT  = (u16)0xC7;
    static constexpr auto UDINT = (u16)0xC8;
    static constexpr auto ULINT = (u16)0xC9;
    static constexpr auto REAL  = (u16)0xCA;
    static constexpr auto LREAL = (u16)0xCB;
}


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
    };


    class TagValue
    {
    public:
        u32 tag_id = 0;
        ByteView value_bytes;
    };


    static u32 entry_size(TagEntry const& entry)
    {
        return (u32)(
                sizeof(entry.instance_id) + 
                sizeof(entry.symbol_type) + 
                sizeof(entry.element_length) + 
                sizeof(entry.array_dims) + 
                sizeof(entry.string_len) +
                entry.string_len
            );
    }


    u16 get_symbol_size(u16 symbol_type)
    {
        switch (symbol_type)
        {
        case BOOL:
        case SINT: return 1;

        case INT:
        case UINT: return 2;

        case DINT:
        case UDINT: 
        case REAL: return 4;

        case LINT:
        case ULINT: 
        case LREAL: return 8;
        
        default: return 0;
        }
    }


    static TagEntry to_tag_entry(u16 symbol_type, u32 array_count, cstr name)
    {
        static u32 tag_id = 0;

        TagEntry entry{};
        entry.instance_id = tag_id++;
        entry.symbol_type = symbol_type;
        entry.element_length = get_symbol_size(symbol_type);
        entry.array_dims[0] = array_count;
        entry.string_len = (u16)strlen(name);
        entry.tag_name = name;

        return entry;
    }
}


namespace dev
{
    class TagDatabase
    {
    private:
        std::random_device rd;
        std::mt19937 gen;
        std::uniform_int_distribution<int> tag_value_byte_dist;
        std::uniform_int_distribution<int> new_tag_value_dist;

    public:

        List<TagEntry> tag_entries;
        List<TagValue> tag_values;

        ByteBuffer tag_entry_bytes;
        ByteBuffer tag_value_bytes;

        TagDatabase()
        {
            gen = std::mt19937(rd());
            tag_value_byte_dist = std::uniform_int_distribution<int>(0, 250);
            new_tag_value_dist = std::uniform_int_distribution<int>(1, 4);
        }

        u8 generate_byte() { return (u8)tag_value_byte_dist(gen); }

        bool new_tag_value() { return new_tag_value_dist(gen) > 1; }
    };


    static List<TagEntry> create_tag_entries()
    {
        return {
            to_tag_entry(BOOL, 1, "BOOL_tag_A"),
            to_tag_entry(BOOL, 1, "BOOL_tag_B"),
            to_tag_entry(BOOL, 1, "BOOL_tag_C"),
            to_tag_entry(SINT, 1, "SINT_tag_A"),
            to_tag_entry(SINT, 1, "SINT_tag_B"),
            to_tag_entry(SINT, 1, "SINT_tag_C"),
            to_tag_entry(INT, 1, "INT_tag_A"),
            to_tag_entry(INT, 1, "INT_tag_B"),
            to_tag_entry(INT, 1, "INT_tag_C"),
            to_tag_entry(DINT, 1, "DINT_tag_A"),
            to_tag_entry(DINT, 1, "DINT_tag_B"),
            to_tag_entry(DINT, 1, "DINT_tag_C"),
            to_tag_entry(LINT, 1, "LINT_tag_A"),
            to_tag_entry(LINT, 1, "LINT_tag_B"),
            to_tag_entry(LINT, 1, "LINT_tag_C"),
            to_tag_entry(USINT, 1, "USINT_tag_A"),
            to_tag_entry(USINT, 1, "USINT_tag_B"),
            to_tag_entry(USINT, 1, "USINT_tag_C"),
            to_tag_entry(UINT, 1, "UINT_tag_A"),
            to_tag_entry(UINT, 1, "UINT_tag_B"),
            to_tag_entry(UINT, 1, "UINT_tag_C"),
            to_tag_entry(ULINT, 1, "ULINT_tag_A"),
            to_tag_entry(ULINT, 1, "ULINT_tag_B"),
            to_tag_entry(ULINT, 1, "ULINT_tag_C"),
            to_tag_entry(REAL, 1, "REAL_tag_A"),
            to_tag_entry(REAL, 1, "REAL_tag_B"),
            to_tag_entry(REAL, 1, "REAL_tag_C"),
            to_tag_entry(LREAL, 1, "LREAL_tag_A"),
            to_tag_entry(LREAL, 1, "LREAL_tag_B"),
            to_tag_entry(LREAL, 1, "LREAL_tag_C"),

            to_tag_entry(BOOL, 5, "BOOL_array_tag_A"),
            to_tag_entry(BOOL, 5, "BOOL_array_tag_B"),
            to_tag_entry(BOOL, 5, "BOOL_array_tag_C"),
            to_tag_entry(SINT, 5, "SINT_array_tag_A"),
            to_tag_entry(SINT, 5, "SINT_array_tag_B"),
            to_tag_entry(SINT, 5, "SINT_array_tag_C"),
            to_tag_entry(INT, 5, "INT_array_tag_A"),
            to_tag_entry(INT, 5, "INT_array_tag_B"),
            to_tag_entry(INT, 5, "INT_array_tag_C"),
            to_tag_entry(DINT, 5, "DINT_array_tag_A"),
            to_tag_entry(DINT, 5, "DINT_array_tag_B"),
            to_tag_entry(DINT, 5, "DINT_array_tag_C"),
            to_tag_entry(LINT, 5, "LINT_array_tag_A"),
            to_tag_entry(LINT, 5, "LINT_array_tag_B"),
            to_tag_entry(LINT, 5, "LINT_array_tag_C"),
            to_tag_entry(USINT, 5, "USINT_array_tag_A"),
            to_tag_entry(USINT, 5, "USINT_array_tag_B"),
            to_tag_entry(USINT, 5, "USINT_array_tag_C"),
            to_tag_entry(UINT, 5, "UINT_array_tag_A"),
            to_tag_entry(UINT, 5, "UINT_array_tag_B"),
            to_tag_entry(UINT, 5, "UINT_array_tag_C"),
            to_tag_entry(ULINT, 5, "ULINT_array_tag_A"),
            to_tag_entry(ULINT, 5, "ULINT_array_tag_B"),
            to_tag_entry(ULINT, 5, "ULINT_array_tag_C"),
            to_tag_entry(REAL, 5, "REAL_array_tag_A"),
            to_tag_entry(REAL, 5, "REAL_array_tag_B"),
            to_tag_entry(REAL, 5, "REAL_array_tag_C"),
            to_tag_entry(LREAL, 5, "LREAL_array_tag_A"),
            to_tag_entry(LREAL, 5, "LREAL_array_tag_B"),
            to_tag_entry(LREAL, 5, "LREAL_array_tag_C"),
        };
    }
}


static dev::TagDatabase g_tag_db{};


/* api */

namespace dev
{
    int plc_tag_create(const char* attr, int timeout)
    {
        auto& tagdb = g_tag_db;

        u32 pos = 0;

        if (mh::string_contains(attr, '@', pos))
        {
            if (attr[pos + 1] == 't')
            {
                tagdb.tag_entries = create_tag_entries();
            }
            else if (attr[pos + 1] == 'u')
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
        auto& tagdb = g_tag_db;
        auto& tags = tagdb.tag_values;

        if (handle < 0 || (u64)handle >= tags.size())
        {
            return -1;
        }
        
        if (!tagdb.new_tag_value())
        {
            return PLCTAG_STATUS_OK;
        }

        auto& value = tags[handle].value_bytes;

        for (u32 i = 0; i < value.length; ++i)
        {
            value.data[i]++;
        }

        return PLCTAG_STATUS_OK;
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
        mb::destroy_buffer(g_tag_db.tag_entry_bytes);
        mb::destroy_buffer(g_tag_db.tag_value_bytes);
    }
}