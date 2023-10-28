#include "devplctag.hpp"
#include "../util/memory_helper.hpp"

#include <vector>
#include <array>
#include <random>
#include <string>

template <typename T>
using List = std::vector<T>;

namespace mh = memory_helper;

/* type codes */

namespace dev
{
    static constexpr auto TYPE_CODE_BOOL  = (u8)0xC1;
    static constexpr auto TYPE_CODE_SINT  = (u8)0xC2;
    static constexpr auto TYPE_CODE_INT   = (u8)0xC3;
    static constexpr auto TYPE_CODE_DINT  = (u8)0xC4;
    static constexpr auto TYPE_CODE_LINT  = (u8)0xC5;
    static constexpr auto TYPE_CODE_USINT = (u8)0xC6;
    static constexpr auto TYPE_CODE_UINT  = (u8)0xC7;
    static constexpr auto TYPE_CODE_UDINT = (u8)0xC8;
    static constexpr auto TYPE_CODE_ULINT = (u8)0xC9;
    static constexpr auto TYPE_CODE_REAL  = (u8)0xCA;
    static constexpr auto TYPE_CODE_LREAL = (u8)0xCB;

    static constexpr auto TYPE_CODE_CHAR_STRING = (u8)0x00D0;

    constexpr auto TYPE_MASK = (u16)0x00FF;


    union SymbolType
    {
        u16 symbol = 0;

        struct
        {
            u8 type_code;

            u8 top8;
        };
    };
}


namespace dev
{
    class TagEntry
    {
    public:
        u32 instance_id = 0;       // 4
        SymbolType symbol_type;    // 2
        u16 element_length = 0;    // 2
        u32 array_dims[3] = { 0 }; // 3 * 4
        u16 string_len = 0;        // 2

        cstr tag_name = 0;
    };


    class TagValue
    {
    public:
        u32 tag_id = 0;
        SymbolType symbol_type;
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


    static u32 value_size(TagEntry const& entry)
    {
        return entry.element_length * entry.array_dims[0];
    }


    u16 get_symbol_size(SymbolType symbol_type)
    {
        switch (symbol_type.type_code)
        {
        case TYPE_CODE_BOOL:
        case TYPE_CODE_SINT:
        case TYPE_CODE_USINT: return 1;

        case TYPE_CODE_INT:
        case TYPE_CODE_UINT: return 2;

        case TYPE_CODE_DINT:
        case TYPE_CODE_UDINT:
        case TYPE_CODE_REAL: return 4;

        case TYPE_CODE_LINT:
        case TYPE_CODE_ULINT:
        case TYPE_CODE_LREAL: return 8;

        case TYPE_CODE_CHAR_STRING: return 16;
        
        default: return 0;
        }
    }


    static TagEntry to_tag_entry(u8 type_code, u32 array_count, cstr name)
    {
        static u32 tag_id = 0;

        SymbolType sb{};
        sb.type_code = type_code;
        sb.top8 = (u8)(1 << 5);
        

        TagEntry entry{};
        entry.instance_id = tag_id++;
        entry.symbol_type = sb;
        entry.element_length = get_symbol_size(sb);
        entry.array_dims[0] = array_count;
        entry.string_len = (u16)strlen(name);
        entry.tag_name = name;

        return entry;
    }
}


/* sample tags */

namespace dev
{
    static List<TagEntry> create_tag_entries()
    {
        return {
            to_tag_entry(TYPE_CODE_BOOL, 1, "BOOL_tag_A"),
            to_tag_entry(TYPE_CODE_BOOL, 1, "BOOL_tag_B"),
            to_tag_entry(TYPE_CODE_BOOL, 1, "BOOL_tag_C"),

            to_tag_entry(TYPE_CODE_BOOL, 5, "BOOL_array_tag_A"),
            to_tag_entry(TYPE_CODE_BOOL, 5, "BOOL_array_tag_B"),
            to_tag_entry(TYPE_CODE_BOOL, 5, "BOOL_array_tag_C"),

            to_tag_entry(TYPE_CODE_SINT, 1, "SINT_tag_A"),
            to_tag_entry(TYPE_CODE_SINT, 1, "SINT_tag_B"),
            to_tag_entry(TYPE_CODE_SINT, 1, "SINT_tag_C"),

            to_tag_entry(TYPE_CODE_SINT, 5, "SINT_array_tag_A"),
            to_tag_entry(TYPE_CODE_SINT, 5, "SINT_array_tag_B"),
            to_tag_entry(TYPE_CODE_SINT, 5, "SINT_array_tag_C"),

            to_tag_entry(TYPE_CODE_INT, 1, "INT_tag_A"),
            to_tag_entry(TYPE_CODE_INT, 1, "INT_tag_B"),
            to_tag_entry(TYPE_CODE_INT, 1, "INT_tag_C"),

            to_tag_entry(TYPE_CODE_INT, 5, "INT_array_tag_A"),
            to_tag_entry(TYPE_CODE_INT, 5, "INT_array_tag_B"),
            to_tag_entry(TYPE_CODE_INT, 5, "INT_array_tag_C"),

            to_tag_entry(TYPE_CODE_DINT, 1, "DINT_tag_A"),
            to_tag_entry(TYPE_CODE_DINT, 1, "DINT_tag_B"),
            to_tag_entry(TYPE_CODE_DINT, 1, "DINT_tag_C"),

            to_tag_entry(TYPE_CODE_DINT, 5, "DINT_array_tag_A"),
            to_tag_entry(TYPE_CODE_DINT, 5, "DINT_array_tag_B"),
            to_tag_entry(TYPE_CODE_DINT, 5, "DINT_array_tag_C"),

            to_tag_entry(TYPE_CODE_LINT, 1, "LINT_tag_A"),
            to_tag_entry(TYPE_CODE_LINT, 1, "LINT_tag_B"),
            to_tag_entry(TYPE_CODE_LINT, 1, "LINT_tag_C"),

            to_tag_entry(TYPE_CODE_LINT, 5, "LINT_array_tag_A"),
            to_tag_entry(TYPE_CODE_LINT, 5, "LINT_array_tag_B"),
            to_tag_entry(TYPE_CODE_LINT, 5, "LINT_array_tag_C"),

            to_tag_entry(TYPE_CODE_USINT, 1, "USINT_tag_A"),
            to_tag_entry(TYPE_CODE_USINT, 1, "USINT_tag_B"),
            to_tag_entry(TYPE_CODE_USINT, 1, "USINT_tag_C"),

            to_tag_entry(TYPE_CODE_USINT, 5, "USINT_array_tag_A"),
            to_tag_entry(TYPE_CODE_USINT, 5, "USINT_array_tag_B"),
            to_tag_entry(TYPE_CODE_USINT, 5, "USINT_array_tag_C"),

            to_tag_entry(TYPE_CODE_UINT, 1, "UINT_tag_A"),
            to_tag_entry(TYPE_CODE_UINT, 1, "UINT_tag_B"),
            to_tag_entry(TYPE_CODE_UINT, 1, "UINT_tag_C"),

            to_tag_entry(TYPE_CODE_UINT, 5, "UINT_array_tag_A"),
            to_tag_entry(TYPE_CODE_UINT, 5, "UINT_array_tag_B"),
            to_tag_entry(TYPE_CODE_UINT, 5, "UINT_array_tag_C"),

            to_tag_entry(TYPE_CODE_ULINT, 1, "ULINT_tag_A"),
            to_tag_entry(TYPE_CODE_ULINT, 1, "ULINT_tag_B"),
            to_tag_entry(TYPE_CODE_ULINT, 1, "ULINT_tag_C"),

            to_tag_entry(TYPE_CODE_ULINT, 5, "ULINT_array_tag_A"),
            to_tag_entry(TYPE_CODE_ULINT, 5, "ULINT_array_tag_B"),
            to_tag_entry(TYPE_CODE_ULINT, 5, "ULINT_array_tag_C"),

            to_tag_entry(TYPE_CODE_REAL, 1, "REAL_tag_A"),
            to_tag_entry(TYPE_CODE_REAL, 1, "REAL_tag_B"),
            to_tag_entry(TYPE_CODE_REAL, 1, "REAL_tag_C"),

            to_tag_entry(TYPE_CODE_REAL, 5, "REAL_array_tag_A"),
            to_tag_entry(TYPE_CODE_REAL, 5, "REAL_array_tag_B"),
            to_tag_entry(TYPE_CODE_REAL, 5, "REAL_array_tag_C"),

            to_tag_entry(TYPE_CODE_LREAL, 1, "LREAL_tag_A"),
            to_tag_entry(TYPE_CODE_LREAL, 1, "LREAL_tag_B"),
            to_tag_entry(TYPE_CODE_LREAL, 1, "LREAL_tag_C"),

            to_tag_entry(TYPE_CODE_LREAL, 5, "LREAL_array_tag_A"),
            to_tag_entry(TYPE_CODE_LREAL, 5, "LREAL_array_tag_B"),
            to_tag_entry(TYPE_CODE_LREAL, 5, "LREAL_array_tag_C"),

            to_tag_entry(TYPE_CODE_CHAR_STRING, 1, "STRING_tag_A"),
            to_tag_entry(TYPE_CODE_CHAR_STRING, 1, "STRING_tag_B"),
            to_tag_entry(TYPE_CODE_CHAR_STRING, 1, "STRING_tag_C"),

            to_tag_entry(TYPE_CODE_CHAR_STRING, 5, "STRING_array_tag_A"),
            to_tag_entry(TYPE_CODE_CHAR_STRING, 5, "STRING_array_tag_B"),
            to_tag_entry(TYPE_CODE_CHAR_STRING, 5, "STRING_array_tag_C"),
        };
    }
}


namespace dev
{
    class TagDatabase
    {
    private:
        std::random_device rd;
        std::mt19937 gen;
        std::uniform_int_distribution<int> new_tag_value_dist;

        std::uniform_int_distribution<int> bool_byte_dist;
        std::uniform_int_distribution<int> numeric_byte_dist;
        std::uniform_int_distribution<int> string_byte_dist;
        

    public:

        List<TagEntry> tag_entries;
        List<TagValue> tag_values;

        ByteBuffer tag_value_data;

        TagDatabase()
        {
            gen = std::mt19937(rd());
            new_tag_value_dist = std::uniform_int_distribution<int>(1, 100);

            bool_byte_dist = std::uniform_int_distribution<int>(0, 1);
            numeric_byte_dist = std::uniform_int_distribution<int>(0, 255);
            string_byte_dist = std::uniform_int_distribution<int>(32, 126);
            
        }

        u8 generate_byte(SymbolType symbol)
        { 
            switch (symbol.type_code)
            {
            case TYPE_CODE_BOOL: return bool_byte_dist(gen);
            case TYPE_CODE_CHAR_STRING: return string_byte_dist(gen);

            default: return numeric_byte_dist(gen);
            }
        }

        bool new_tag_value() { return new_tag_value_dist(gen) == 1; }
    };    


    static int push_tag_entry(TagEntry const& entry, ByteView const& bytes, int offset)
    {        
        auto dst = bytes.data + offset;
        auto src = (u8*)(&entry.instance_id);
        auto len = sizeof(entry.instance_id); 
        mh::copy_bytes(src, dst, (u32)len);

        offset += (int)len;
        dst += len;
        src = (u8*)(&entry.symbol_type);
        len = sizeof(entry.symbol_type);
        mh::copy_bytes(src, dst, (u32)len);

        offset += (int)len;
        dst += len;
        src = (u8*)(&entry.element_length);
        len = sizeof(entry.element_length);
        mh::copy_bytes(src, dst, (u32)len);

        offset += (int)len;
        dst += len;
        src = (u8*)(&entry.array_dims);
        len = sizeof(entry.array_dims);
        mh::copy_bytes(src, dst, (u32)len);

        offset += (int)len;
        dst += len;
        src = (u8*)(&entry.string_len);
        len = sizeof(entry.string_len);
        mh::copy_bytes(src, dst, (u32)len);

        offset += (int)len;
        dst += len;
        src = (u8*)entry.tag_name;
        len = strlen(entry.tag_name);
        mh::copy_bytes(src, dst, (u32)len);

        offset += (int)len;

        return offset;
    }
    
    
    static int generate_listing_tag(TagDatabase& tagdb)
    {
        u32 listing_bytes = 0;
        u32 value_bytes = 0;

        auto& value_data = tagdb.tag_value_data;

        for (auto const& entry : tagdb.tag_entries)
        {
            listing_bytes += entry_size(entry);

            value_bytes += listing_bytes;
            value_bytes += value_size(entry);
        }

        if (!mb::create_buffer(value_data, value_bytes))
        {
            return false;
        }

        tagdb.tag_values.reserve(tagdb.tag_entries.size());

        TagValue listing_tag{};
        listing_tag.tag_id = (u32)tagdb.tag_values.size();
        listing_tag.value_bytes = mb::push_view(value_data, listing_bytes);
        tagdb.tag_values.push_back(listing_tag);

        int offset = 0;
        for (auto const& entry : tagdb.tag_entries)
        {
            offset = push_tag_entry(entry, listing_tag.value_bytes, offset);
        }

        return (int)tagdb.tag_values.size() - 1;
    }


    static int generate_tag(TagDatabase& tagdb, TagEntry const& entry)
    {
        TagValue tag{};

        tag.tag_id = (u32)tagdb.tag_values.size();
        tag.symbol_type = entry.symbol_type;
        tag.value_bytes = mb::push_view(tagdb.tag_value_data, value_size(entry));

        // initial value
        for (u32 i = 0; i < tag.value_bytes.length; ++i)
        {
            tag.value_bytes.data[i] = tagdb.generate_byte(tag.symbol_type);
        }

        tagdb.tag_values.push_back(tag);

        return (int)tagdb.tag_values.size() - 1;
    }
}


static dev::TagDatabase g_tag_db{};


/* api */

namespace dev
{
    int plc_tag_create(const char* attr, int timeout)
    {
        auto& tagdb = g_tag_db;

        if (tagdb.tag_entries.empty())
        {
            tagdb.tag_entries = create_tag_entries();
        }

        std::string str(attr);

        auto not_found = std::string::npos;

        if (str.find("@tags") != not_found)
        {
            int handle = generate_listing_tag(tagdb);
            if (handle < 0)
            {
                return -1;
            }

            return handle;
        }

        if (str.find("@udt") != not_found)
        {
            return -1;
        }

        auto pos = str.find("name=");
        if (pos == not_found)
        {
            return -1;
        }

        pos += strlen("name=");

        auto pos2 = str.find('&', pos);
        if (pos2 == not_found)
        {
            return -1;
        }

        auto name = str.substr(pos, (pos2 - pos));

        for (auto const& entry : tagdb.tag_entries)
        {
            if (name == entry.tag_name)
            {
                int handle = generate_tag(tagdb, entry);
                if (handle < 0)
                {
                    return -1;
                }

                return handle;
            }
        }

        return -1;
    }


    int plc_tag_read(int handle, int timeout)
    {
        auto& tagdb = g_tag_db;
        auto& tags = tagdb.tag_values;

        if (handle < 0 || (u64)handle >= tags.size())
        {
            return -1;
        }
        
        if (handle == 0 || !tagdb.new_tag_value())
        {
            return PLCTAG_STATUS_OK;
        }

        auto& tag = tags[handle];
        auto& bytes = tag.value_bytes;

        for (u32 i = 0; i < bytes.length; ++i)
        {
            bytes.data[i] = tagdb.generate_byte(tag.symbol_type);
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
        mb::destroy_buffer(g_tag_db.tag_value_data);
    }
}