#include "devplctag.hpp"
#include "../util/memory_helper.hpp"

#include <vector>
#include <array>
#include <random>
#include <string>
#include <algorithm>

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

    using u12 = u16;


    union SymbolType
    {
        u16 symbol = 0;

        u12 udt_id : 12;

        struct
        {
            u8 type_code;

            u8 top8;
        };

        struct
        {
            u8 bottom8;

            u8 bit_8 : 1;
            u8 bit_9 : 1;
            u8 bit_10 : 1;
            u8 bit_11 : 1;

            u8 is_system : 1;
            u8 field_is_array : 1;
            u8 bit_14 : 1;
            u8 is_struct : 1;
        };


        void set_array_dims(u8 dims) { top8 |= (u8)(dims << 5); }
    };


    static SymbolType to_type_symbol(u8 type_code)
    {
        SymbolType sb{};
        sb.type_code = type_code;
        sb.set_array_dims(1); // 1D arrays only

        return sb;
    }


    static SymbolType to_udt_symbol(u12 udt_id)
    {
        SymbolType sb{};
        sb.udt_id = udt_id;
        sb.is_struct = 1;
        sb.set_array_dims(1); // 1D arrays only

        return sb;
    }
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
        static_assert(sizeof(SymbolType) == sizeof(u16));

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


    static u16 get_type_size(u8 type_code)
    {
        switch (type_code)
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


    static TagEntry to_tag_entry(u8 type_code, u8 array_count, cstr name)
    {
        static u32 tag_id = 0;

        SymbolType sb = to_type_symbol(type_code);

        TagEntry entry{};
        entry.instance_id = tag_id++;
        entry.symbol_type = sb;
        entry.element_length = get_type_size(sb.type_code);
        entry.array_dims[0] = array_count;
        entry.string_len = (u16)strlen(name);
        entry.tag_name = name;

        return entry;
    }
}


namespace dev
{
    class UdtField
    {
    public:        
        cstr field_name = 0;

        u8 type_code = 0;
        //u12 udt_id; // no udt fields

        u16 array_count = 0;
        u16 bit_number = 0;
    };


    class UdtType
    {
    public:
        u12 udt_id = 0;
        cstr udt_name = 0;
        List<UdtField> fields;
    };


    static u16 get_udt_size(UdtType const& udt)
    {
        u16 size = 0;

        for (auto const& f : udt.fields)
        {
            if (!f.array_count)
            {
                // add 1 every 8 bits
                size += (f.bit_number % 8 == 0) ? 1 : 0;
            }
            else
            {
                size += get_type_size(f.type_code) * f.array_count;
            }            
        }

        return size;
    }


    static TagEntry to_udt_entry(UdtType const& udt, u32 array_count, cstr tag_name)
    {
        assert(udt.udt_id < TYPE_CODE_BOOL); // just to be safe

        SymbolType sb = to_udt_symbol(udt.udt_id);

        TagEntry entry{};
        entry.symbol_type = sb;
        entry.element_length = get_udt_size(udt);
        entry.array_dims[0] = array_count;
        entry.string_len = (u16)strlen(tag_name);
        entry.tag_name = tag_name;

        return entry;
    }
}


/* sample tags */

namespace dev
{
    static List<UdtType> create_udt_types()
    {
        UdtType udt_a{};
        udt_a.udt_id = 101;
        udt_a.udt_name = "UDTA";
        udt_a.fields =
        {
            { "INT field", TYPE_CODE_INT, 1, 0 },
            { "SINT field", TYPE_CODE_SINT, 1, 0 }
        };

        UdtType udt_b{};
        udt_b.udt_id = 102;
        udt_b.udt_name = "UDTB";
        udt_b.fields =
        {
            { "DINT field", TYPE_CODE_DINT, 1, 0 },
            { "REAL field", TYPE_CODE_REAL, 1, 0 }
        };

        UdtType udt_c{};
        udt_c.udt_id = 103;
        udt_c.udt_name = "UDTC";
        udt_c.fields =
        {
            { "LREAL field", TYPE_CODE_LREAL, 1, 0 },
            { "ULINT field", TYPE_CODE_ULINT, 1, 0 }
        };

        return { udt_a, udt_b, udt_c };
    }


    static void append_udt_entries(List<UdtType> const& udt_types, List<TagEntry>& entries)
    {
        auto& udt_a = udt_types[0]; // 101
        auto& udt_b = udt_types[1]; // 102
        auto& udt_c = udt_types[2]; // 103 

        entries.push_back(to_udt_entry(udt_a, 1, "UDTA_tag_A"));
        entries.push_back(to_udt_entry(udt_a, 1, "UDTA_tag_B"));
        entries.push_back(to_udt_entry(udt_a, 1, "UDTA_tag_C"));

        entries.push_back(to_udt_entry(udt_a, 5, "UDTA_array_tag_A"));
        entries.push_back(to_udt_entry(udt_a, 5, "UDTA_array_tag_B"));
        entries.push_back(to_udt_entry(udt_a, 5, "UDTA_array_tag_C"));

        entries.push_back(to_udt_entry(udt_b, 1, "UDTB_tag_A"));
        entries.push_back(to_udt_entry(udt_b, 1, "UDTB_tag_B"));
        entries.push_back(to_udt_entry(udt_b, 1, "UDTB_tag_C"));

        entries.push_back(to_udt_entry(udt_b, 5, "UDTB_array_tag_A"));
        entries.push_back(to_udt_entry(udt_b, 5, "UDTB_array_tag_B"));
        entries.push_back(to_udt_entry(udt_b, 5, "UDTB_array_tag_C"));

        entries.push_back(to_udt_entry(udt_c, 1, "UDTC_tag_A"));
        entries.push_back(to_udt_entry(udt_c, 1, "UDTC_tag_B"));
        entries.push_back(to_udt_entry(udt_c, 1, "UDTC_tag_C"));

        entries.push_back(to_udt_entry(udt_c, 5, "UDTC_array_tag_A"));
        entries.push_back(to_udt_entry(udt_c, 5, "UDTC_array_tag_B"));
        entries.push_back(to_udt_entry(udt_c, 5, "UDTC_array_tag_C"));


    }


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
    class TagValueGenerator
    {
    private:
        std::random_device rd;
        std::mt19937 gen;
        std::uniform_int_distribution<int> new_tag_value_dist;

        std::uniform_int_distribution<int> bool_byte_dist;
        std::uniform_int_distribution<int> numeric_byte_dist;
        std::uniform_int_distribution<int> string_byte_dist;

    public:

        TagValueGenerator()
        {
            gen = std::mt19937(rd());
            new_tag_value_dist = std::uniform_int_distribution<int>(1, 100);

            bool_byte_dist = std::uniform_int_distribution<int>(0, 1);
            numeric_byte_dist = std::uniform_int_distribution<int>(0, 255);
            string_byte_dist = std::uniform_int_distribution<int>(32, 126);

        }

        u8 generate_byte(SymbolType symbol)
        {
            if (symbol.is_struct || symbol.is_system)
            {
                return string_byte_dist(gen); // TODO: just random chars for now
            }

            switch (symbol.type_code)
            {
            case TYPE_CODE_BOOL: return bool_byte_dist(gen);
            case TYPE_CODE_CHAR_STRING: return string_byte_dist(gen);

            default: return numeric_byte_dist(gen);
            }
        }

        bool new_tag_value() { return new_tag_value_dist(gen) == 1; }
    };


    class TagDatabase
    {
    public:

        List<UdtType> udt_types;
        List<TagEntry> tag_entries;
        List<TagValue> tag_values;

        ByteBuffer tag_value_data;

        TagValueGenerator gen;
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
    
    
    static int generate_entry_listing_tag_buffer(TagDatabase& tagdb)
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


    static int generate_udt_listing_tag_buffer(TagDatabase& tagdb, std::string const& entry_name)
    {
        auto not_found = std::string::npos;
        auto pos = entry_name.find('/');
        if (pos == not_found)
        {
            return -1;
        }

        pos++;

        auto udt_id = (u12)std::stoi(entry_name.substr(pos));
        auto begin = tagdb.udt_types.begin();
        auto end = tagdb.udt_types.end();
        auto it = std::find_if(begin, end, [&](auto& udt) { return udt.udt_id == udt_id; });
        if (it == end)
        {
            return -1;
        }

        auto& udt = *it;

        // TODO: random data for now

        TagValue tag{};

        tag.tag_id = (u32)tagdb.tag_values.size();
        tag.symbol_type = to_udt_symbol(udt_id);
        tag.value_bytes = mb::push_view(tagdb.tag_value_data, (u32)get_udt_size(udt));

        // initial value
        for (u32 i = 0; i < tag.value_bytes.length; ++i)
        {
            tag.value_bytes.data[i] = tagdb.gen.generate_byte(tag.symbol_type);
        }

        tagdb.tag_values.push_back(tag);

        return (int)tagdb.tag_values.size() - 1;
    }


    static int generate_tag_value_buffer(TagDatabase& tagdb, TagEntry const& entry)
    {
        TagValue tag{};

        tag.tag_id = (u32)tagdb.tag_values.size();
        tag.symbol_type = entry.symbol_type;
        tag.value_bytes = mb::push_view(tagdb.tag_value_data, value_size(entry));

        // initial value
        for (u32 i = 0; i < tag.value_bytes.length; ++i)
        {
            tag.value_bytes.data[i] = tagdb.gen.generate_byte(tag.symbol_type);
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

        int handle = -1;

        if (tagdb.tag_entries.empty())
        {
            tagdb.udt_types = create_udt_types();
            tagdb.tag_entries = create_tag_entries();
            append_udt_entries(tagdb.udt_types, tagdb.tag_entries);
        }

        std::string str(attr);

        auto not_found = std::string::npos;

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

        if (name == "@tags")
        {
            handle = generate_entry_listing_tag_buffer(tagdb);
            if (handle < 0)
            {
                return -1;
            }

            return handle;
        }

        if (name.find("@udt") != not_found)
        {
            handle = generate_udt_listing_tag_buffer(tagdb, name);
            if (handle < 0)
            {
                return -1;
            }
        }

        auto begin = tagdb.tag_entries.begin();
        auto end = tagdb.tag_entries.end();

        auto it = std::find_if(begin, end, [&name](auto& e) { return name == e.tag_name; });
        if (it == end)
        {
            return -1;
        }

        auto& entry = *it;

        if (handle < 0)
        {
            return -1;
        }

        return handle;
    }


    int plc_tag_read(int handle, int timeout)
    {
        auto& tags = g_tag_db.tag_values;
        auto& gen = g_tag_db.gen;

        if (handle < 0 || (u64)handle >= tags.size())
        {
            return -1;
        }
        
        if (handle == 0 || !gen.new_tag_value())
        {
            return PLCTAG_STATUS_OK;
        }

        auto& tag = tags[handle];
        auto& bytes = tag.value_bytes;

        for (u32 i = 0; i < bytes.length; ++i)
        {
            bytes.data[i] = gen.generate_byte(tag.symbol_type);
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