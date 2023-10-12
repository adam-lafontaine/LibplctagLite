#include "../util/types.hpp"
#include "../util/memory_buffer.hpp"
#include "../util/qsprintf.hpp"

#include <vector>
#include <array>
#include <unordered_map>
#include <cassert>

namespace mb = memory_buffer;


using String = MemoryView<char>;
using Bytes = MemoryView<u8>;
using ByteOffset = MemoryOffset<u8>;


static void copy_bytes(u8* src, u8* dst, u32 len)
{
    constexpr auto size64 = sizeof(u64);

    auto len64 = len / size64;
    auto src64 = (u64*)src;
    auto dst64 = (u64*)dst;

    auto len8 = len - len64 * size64;
    auto src8 = (u8*)(src64 + len64);
    auto dst8 = (u8*)(dst64 + len64);

    for (size_t i = 0; i < len64; ++i)
    {
        dst64[i] = src64[i];
    }

    for (size_t i = 0; i < len8; ++i)
    {
        dst8[i] = src8[i];
    }
}


template <typename T>
static void copy_atomic_bytes(u8* src, u8* dst)
{
    *(T*)src = *(T*)dst;
}


static void copy(cstr src, String const& dst)
{
    for (u32 i = 0; i < dst.length; ++i)
    {
        dst.begin[i] = src[i];
    }
}


static void copy(String const& src, String const& dst)
{
    auto len = src.length < dst.length ? src.length : dst.length;

    for (u32 i = 0; i < len; ++i)
    {
        dst.begin[i] = src.begin[i];
    }
}


template <typename T>
static void destroy_vector(std::vector<T>& vec)
{
    std::vector<T> temp;
    std::swap(vec, temp);
}


template <typename T>
static bool vector_contains(std::vector<T> const& vec, T value)
{
    for (auto v : vec)
    {
        if (v == value)
        {
            return true;
        }
    }

    return false;    
}


template <typename K, typename V>
static void destroy_map(std::unordered_map<K, V>& map)
{
    std::unordered_map<K, V> temp;
    std::swap(map, temp);
}


template <typename K, typename V>
static bool map_contains(std::unordered_map<K, V> const& map, K key)
{
    auto not_found = map.end();
    auto it = map.find(key);

    return it == not_found;
}


using DataTypeId32 = u32;


/* 16 bit ids */

namespace id16
{
    constexpr auto TYPE_IS_STRUCT = (u16)0x8000;     // 0b1000'0000'0000'0000
    constexpr auto TYPE_IS_SYSTEM = (u16)0x1000;     // 0b0001'0000'0000'0000
    constexpr auto UDT_FIELD_IS_ARRAY = (u16)0x2000; // 0b0010'0000'0000'0000

    constexpr auto TAG_DIM_MASK = (u16)0x6000;       // 0b0110'0000'0000'0000    

    constexpr auto ATOMIC_TYPE_ID_MASK = (u16)0x00FF; // 0b0000'0000'1111'1111
    constexpr auto ATOMIC_TYPE_ID_MIN = (u16)0xC1;    // 0b0000'0000'1100'0001
    constexpr auto ATOMIC_TYPE_ID_MAX = (u16)0xDE;    // 0b0000'0000'1101'1110

    constexpr auto UDT_TYPE_ID_MASK = (u16)0x0FFF; // 0b0000'1111'1111'1111


    static inline u16 get_tag_dimensions(u16 type_code)
    {
        return (u16)((type_code & id16::TAG_DIM_MASK) >> 13);
    }


    static inline bool is_bit_field(u16 type_code)
    {
        return (AtomicType)(type_code & id16::ATOMIC_TYPE_ID_MASK) == AtomicType::BOOL;
    }


    static inline bool is_array_field(u16 type_code)
    {
        return type_code & id16::UDT_FIELD_IS_ARRAY;
    }


    static inline u16 get_udt_id(u16 type_code)
    {
        if (type_code & TYPE_IS_STRUCT)
        {
            return type_code & UDT_TYPE_ID_MASK;
        }

        return 0;        
    }
}


namespace id32
{
    // 0b0000'0000'0000'0000'0000'0000'0000'0000
    //  |----other-----|------udt-----|-atomic--|

    constexpr auto OTHER_TYPE_ID_MASK  = (DataTypeId32)0b1111'1111'1111'0000'0000'0000'0000'0000;
    constexpr auto UDT_TYPE_ID_MASK    = (DataTypeId32)0b0000'0000'0000'1111'1111'1111'0000'0000;
    constexpr auto ATOMIC_TYPE_ID_MASK = (DataTypeId32)0b0000'0000'0000'0000'0000'0000'1111'1111;

    constexpr auto SYSTEM_TYPE_ID = (DataTypeId32)0b0000'0000'0001'0000'0000'0000'0000'0000;
    constexpr auto UNKNOWN_TYPE_ID = (DataTypeId32)0b0000'0000'0010'0000'0000'0000'0000'0000;


    static inline DataTypeId32 get_udt_type_id(u16 type_code)
    {
        if (type_code & id16::TYPE_IS_STRUCT)
        {
            // shift left 8 to prevent conflicts with atomic types
            return (DataTypeId32)(id16::get_udt_id(type_code)) << 8;
        }

        return 0;        
    }


    static DataTypeId32 get_data_type_id(u16 type_code)
    {
        if (type_code & id16::TYPE_IS_SYSTEM)
        {
            return id32::SYSTEM_TYPE_ID;
        }
        else if (type_code & id16::TYPE_IS_STRUCT)
        {            
            return get_udt_type_id(type_code);
        }

        u16 atomic_type = type_code & id16::ATOMIC_TYPE_ID_MASK;

        if (atomic_type >= id16::ATOMIC_TYPE_ID_MIN && atomic_type <= id16::ATOMIC_TYPE_ID_MAX)
        {
            return (DataTypeId32)atomic_type;
        }

        return id32::UNKNOWN_TYPE_ID;
    }


    static bool is_udt_type(DataTypeId32 id)
    {
        return (id & UDT_TYPE_ID_MASK) && !(id & OTHER_TYPE_ID_MASK) && !(id & ATOMIC_TYPE_ID_MASK);
    }



}



/* tag types */

namespace
{   

    enum class AtomicType : DataTypeId32
    {
        UNKNOWN = id32::UNKNOWN_TYPE_ID,
        SYSTEM  = id32::SYSTEM_TYPE_ID,

        BOOL  = (DataTypeId32)0xC1,
        SINT  = (DataTypeId32)0xC2,
        INT   = (DataTypeId32)0xC3,
        DINT  = (DataTypeId32)0xC4,
        LINT  = (DataTypeId32)0xC5,
        USINT = (DataTypeId32)0xC6,
        UINT  = (DataTypeId32)0xC7,
        UDINT = (DataTypeId32)0xC8,
        ULINT = (DataTypeId32)0xC9,
        REAL  = (DataTypeId32)0xCA,
        LREAL = (DataTypeId32)0xCB,

        SYNCHRONOUS_TIME = (DataTypeId32)0xCC,
        DATE             = (DataTypeId32)0xCD,
        TIME             = (DataTypeId32)0xCE,
        DATETIME         = (DataTypeId32)0xCF,

        CHAR_STRING = (DataTypeId32)0xD0,
        STRING_8    = (DataTypeId32)0xD1,
        STRING_16   = (DataTypeId32)0xD2,
        STRING_32   = (DataTypeId32)0xD3,
        STRING_64   = (DataTypeId32)0xD4,
        WIDE_STRING = (DataTypeId32)0xD5,
        
        HIGH_RES_DURATION = (DataTypeId32)0xD6,
        MED_RES_DURATION  = (DataTypeId32)0xD7,
        LOW_RES_DURATION  = (DataTypeId32)0xD8,

        N_BYTE_STRING        = (DataTypeId32)0xD9,
        COUNTED_CHAR_STRING  = (DataTypeId32)0xDA,
        DURATION_MS          = (DataTypeId32)0xDB,
        CIP_PATH             = (DataTypeId32)0xDC,
        ENGINEERING_UNITS    = (DataTypeId32)0xDD,
        INTERNATIONAL_STRING = (DataTypeId32)0xDE,
        
    };


    constexpr std::array<AtomicType, 14> SUPPORTED_TYPES = 
    {
        AtomicType::BOOL,
        AtomicType::SINT,
        AtomicType::INT,
        AtomicType::DINT,
        AtomicType::LINT,
        AtomicType::USINT,
        AtomicType::UINT,
        AtomicType::UDINT,
        AtomicType::ULINT,
        AtomicType::REAL,
        AtomicType::LREAL,
        AtomicType::CHAR_STRING,

        AtomicType::SYSTEM,
        AtomicType::UNKNOWN,
    };    


    cstr tag_type_str(AtomicType t)
    {
        switch (t)
        {
        case AtomicType::SYSTEM:               return "SYS"; // ?
        case AtomicType::BOOL:                 return "BOOL";
        case AtomicType::SINT:                 return "SINT";
        case AtomicType::INT:                  return "INT";
        case AtomicType::DINT:                 return "DINT";
        case AtomicType::LINT:                 return "LINT";
        case AtomicType::USINT:                return "USINT";
        case AtomicType::UINT:                 return "UINT";
        case AtomicType::UDINT:                return "UDINT";
        case AtomicType::ULINT:                return "ULINT";
        case AtomicType::REAL:                 return "REAL";
        case AtomicType::LREAL:                return "LREAL";
        case AtomicType::SYNCHRONOUS_TIME:     return "SYNC_TIME"; // ?
        case AtomicType::DATE:                 return "DATE";
        case AtomicType::TIME:                 return "TIME";
        case AtomicType::DATETIME:             return "DATE_AND_TIME";
        case AtomicType::CHAR_STRING:          return "STRING";
        case AtomicType::STRING_8:             return "STRING_8";
        case AtomicType::STRING_16:            return "STRING_16";
        case AtomicType::STRING_32:            return "STRING_32";
        case AtomicType::STRING_64:            return "STRING_64";
        case AtomicType::WIDE_STRING:          return "WIDE_STRING";        
        case AtomicType::HIGH_RES_DURATION:    return "High resolution duration value";
        case AtomicType::MED_RES_DURATION:     return "Medium resolution duration value";
        case AtomicType::LOW_RES_DURATION:     return "Low resolution duration value";
        case AtomicType::N_BYTE_STRING:        return "N-byte per char character string";
        case AtomicType::COUNTED_CHAR_STRING:  return "Counted character sting with 1 byte per character and 1 byte length indicator";
        case AtomicType::DURATION_MS:          return "Duration in milliseconds";
        case AtomicType::CIP_PATH:             return "CIP path segment(s)";
        case AtomicType::ENGINEERING_UNITS:    return "Engineering units";
        case AtomicType::INTERNATIONAL_STRING: return "International character string (encoding?)";
        
        default:                               return "UNKNOWN";
        }
    }


    cstr tag_description_str(AtomicType t)
    {
        switch (t)
        {
        case AtomicType::SYSTEM:               return "System tag";
        case AtomicType::BOOL:                 return "Boolean value";
        case AtomicType::SINT:                 return "Signed 8-bit integer value";
        case AtomicType::INT:                  return "Signed 16-bit integer value";
        case AtomicType::DINT:                 return "Signed 32-bit integer value";
        case AtomicType::LINT:                 return "Signed 64-bit integer value";
        case AtomicType::USINT:                return "Unsigned 8-bit integer value";
        case AtomicType::UINT:                 return "Unsigned 16-bit integer value";
        case AtomicType::UDINT:                return "Unsigned 32-bit integer value";
        case AtomicType::ULINT:                return "Unsigned 64-bit integer value";
        case AtomicType::REAL:                 return "32-bit floating point value, IEEE format";
        case AtomicType::LREAL:                return "64-bit floating point value, IEEE format";
        case AtomicType::SYNCHRONOUS_TIME:     return "Synchronous time value";
        case AtomicType::DATE:                 return "Date value";
        case AtomicType::TIME:                 return "Time of day value";
        case AtomicType::DATETIME:             return "Date and time of day value";
        case AtomicType::CHAR_STRING:          return "Character string, 1 byte per character";
        case AtomicType::STRING_8:             return "8-bit bit string";
        case AtomicType::STRING_16:            return "16-bit bit string";
        case AtomicType::STRING_32:            return "32-bit bit string";
        case AtomicType::STRING_64:            return "64-bit bit string";
        case AtomicType::WIDE_STRING:          return "Wide char character string, 2 bytes per character";        
        case AtomicType::HIGH_RES_DURATION:    return "High resolution duration value";
        case AtomicType::MED_RES_DURATION:     return "Medium resolution duration value";
        case AtomicType::LOW_RES_DURATION:     return "Low resolution duration value";
        case AtomicType::N_BYTE_STRING:        return "N-byte per char character string";
        case AtomicType::COUNTED_CHAR_STRING:  return "Counted character sting with 1 byte per character and 1 byte length indicator";
        case AtomicType::DURATION_MS:          return "Duration in milliseconds";
        case AtomicType::CIP_PATH:             return "CIP path segment(s)";
        case AtomicType::ENGINEERING_UNITS:    return "Engineering units";
        case AtomicType::INTERNATIONAL_STRING: return "International character string (encoding?)";
        
        default:                               return "Unknown tag type";
        }
    }
    
}


/* tag listing */

namespace
{
    class TagEntry
    {
    public:
        u16 type_code = 0;
        u32 elem_size = 0;
        u32 elem_count = 0;
        String name;
    };


    using TagEntryList = std::vector<TagEntry>;


    static int append_tag_entry(TagEntryList& entries, u8* entry_data)
    {
        /* each entry looks like this:
        uint32_t instance_id    monotonically increasing but not contiguous
        uint16_t symbol_type    type of the symbol.
        uint16_t element_length length of one array element in bytes.
        uint32_t array_dims[3]  array dimensions.
        uint16_t string_len
        uint8_t string_data[]   string bytes (string_len of them)
        */

        class H
        {
        public:
            u32 instance_id;    // 4
            u16 symbol_type;    // 2
            u16 element_length; // 2
            u32 array_dims[3];  // 3 * 4
            u16 string_len;     // 2
        };

        constexpr auto H_size = 4 + 2 + 2 + 3 * 4 + 2;

        auto& h = *(H*)(entry_data);
        
        TagEntry entry{};

        entry.type_code = h.symbol_type;

        entry.elem_count = 1;

        auto n_dims = id16::get_tag_dimensions(entry.type_code);

        for (u32 i = 0; i < n_dims; ++i)
        {
            if (h.array_dims[i])
            {
                entry.elem_count *= h.array_dims[i];
            }
        }

        int offset = H_size;

        entry.name = mb::make_view((char*)(entry_data + offset),  h.string_len);

        entries.push_back(entry);

        offset += h.string_len;

        return offset;
    }


    static TagEntryList parse_tag_entries(u8* entry_data, u32 data_size)
    {
        TagEntryList list;

        int offset = 0;

        while (offset < data_size)
        {
            offset += append_tag_entry(list, entry_data + offset);
        }

        return list;
    }
}


/* tag table */

namespace
{

    class Tag
    {
    public:
        int tag_id = -1;

        DataTypeId32 type_id = id32::UNKNOWN_TYPE_ID;

        ByteOffset value;
        String name;
    };


    class TagTable
    {
    public:
        int plc = 0; // TODO

        std::vector<Tag> tags;

        ParallelBuffer<u8> value_data;
        MemoryBuffer<char> name_data;
    };


    static void destroy_tag_table(TagTable& table)
    {
        destroy_vector(table.tags);

        mb::destroy_buffer(table.value_data);
        mb::destroy_buffer(table.name_data);
    }    


    u32 elem_size(TagEntry const& e) { return e.elem_count * e.elem_size; }


    u32 cstr_size(TagEntry const& e) { return e.name.length + 1; /* zero terminated */}


    static void add_tag(TagTable& table, TagEntry const& entry)
    {
        auto value_len = elem_size(entry);
        auto name_alloc_len = cstr_size(entry);
        auto name_copy_len = entry.name.length;

        assert(name_alloc_len > name_copy_len); /* zero terminated */

        Tag tag{};

        tag.type_id = id32::get_data_type_id(entry.type_code);

        tag.value = mb::push_offset(table.value_data, value_len);
        tag.name = mb::push_cstr_view(table.name_data, name_alloc_len);

        copy(entry.name, tag.name);

        table.tags.push_back(tag);
    }


    static bool create_tag_table(TagEntryList const& entries, TagTable& table)
    {
        u32 value_bytes = 0;
        u32 str_bytes = 0;
        for (auto const& e : entries)
        {
            value_bytes += elem_size(e);
            str_bytes += cstr_size(e);
        }

        if (!mb::create_buffer(table.value_data, value_bytes))
        {
            destroy_tag_table(table);
            return false;
        }

        if (!mb::create_buffer(table.name_data, str_bytes))
        {
            destroy_tag_table(table);
            return false;
        }

        mb::zero_buffer(table.value_data);
        mb::zero_buffer(table.name_data);

        table.tags.reserve(entries.size());

        for (auto const& e : entries)
        {
            add_tag(table, e);
        }

        return true;
    }
}


/* udt entries */

namespace
{
    class FieldEntry
    {
    public:
        u16 elem_count = 0;
        u16 bit_number = 0;

        u16 type_code = 0;
        u16 offset = 0;

        String field_name;
    };


    class UdtEntry
    {
    public:
        u16 udt_id = 0;
        u32 udt_size = 0;
        u32 n_fields = 0;

        String udt_name;

        std::vector<FieldEntry> fields;

        // how to parse scan data
    };


    static UdtEntry parse_udt_entry(u8* udt_data)
    {
        /*

        The format in the tag buffer is:

        A new header of 14 bytes:

        Bytes   Meaning
        0-1     16-bit UDT ID
        2-5     32-bit UDT member description size, in 32-bit words.
        6-9     32-bit UDT instance size, in bytes.
        10-11   16-bit UDT number of members (fields).
        12-13   16-bit UDT handle/type.

        Then the raw field info.

        N x field info entries
            uint16_t field_metadata - array element count or bit field number
            uint16_t field_type
            uint32_t field_offset

        int8_t string - zero-terminated string, UDT name, but name stops at first semicolon!

        N x field names
            int8_t string - zero-terminated.

        */

        class H
        {
        public:
            u16 udt_id = 0;   // 2
            u32 desc = 0;     // 4
            u32 udt_size = 0; // 4
            u16 n_fields = 0; // 2
            u16 handle = 0;   // 2
        };

        constexpr auto H_size = 2 + 4 + 4 + 2 + 2;

        class F
        {
        public:
            u16 metadata = 0;  // 2
            u16 type_code = 0; // 2
            u32 offset = 0;    // 4
        };

        constexpr auto F_size = 2 + 2 + 4;

        auto& h = *(H*)(udt_data);

        UdtEntry entry{};
        entry.udt_id = h.udt_id;
        entry.udt_size = h.udt_size;

        entry.fields.reserve(h.n_fields);        

        int offset = H_size;

        for (u16 i = 0; i < h.n_fields; ++i)
        {
            auto& f = *(F*)(udt_data + offset);

            FieldEntry field{};
            field.type_code = f.type_code;
            field.offset = f.offset;

            field.elem_count = 1;
            if (id16::is_array_field(f.type_code))
            {
                field.elem_count = f.metadata;
            }
            else if (id16::is_bit_field(f.type_code))
            {
                field.bit_number = f.metadata;
            }

            entry.fields.push_back(field);

            offset += F_size;
        }

        auto string_data = (char*)(udt_data + offset);
        auto string_len = strlen(string_data);
        int str_offset = 0;

        u32 name_len = 0;
        char end = ';';
        while (string_data[name_len] != end && name_len < string_len)
        {
            ++name_len;
        }

        entry.udt_name = mb::make_view(string_data, name_len);

        str_offset = string_len + 1;
        for (auto& field : entry.fields)
        {
            auto str = string_data + str_offset;
            field.field_name = mb::make_view(str, strlen(str));
            str_offset++;
        }

        return entry; 
    }


    template <class ENTRY>
    static void append_udt_ids(std::vector<ENTRY> const& entries, std::vector<u16>& udt_ids)
    {
        for (auto const& e : entries)
        {
            auto id = id16::get_udt_id(e.type_code);
            if (id && !vector_contains(udt_ids, id))
            {
                udt_ids.push_back(id);
            }
        }
    }




}


/* data type table */

namespace
{
    class DataType
    {
    public:
        DataTypeId32 type_id = id32::UNKNOWN_TYPE_ID;
        String data_type_name;
        String data_type_description;

        // how to parse scan data
    };


    using DataTypeMap = std::unordered_map<DataTypeId32, DataType>;


    class DataTypeTable
    {
    public:

        DataTypeMap type_map;

        MemoryBuffer<char> atomic_str_data;
        std::vector<MemoryBuffer<char>> udt_str_data;
    };


    static void destroy_data_type_table(DataTypeTable& table)
    {
        destroy_map(table.type_map);

        mb::destroy_buffer(table.atomic_str_data);

        for (auto& buffer : table.udt_str_data)
        {
            mb::destroy_buffer(buffer);
        }

        destroy_vector(table.udt_str_data);
    }


    static void add_data_type(DataTypeTable& table, AtomicType type)
    {        
        auto type_id = (DataTypeId32)type;

        if (map_contains(table.type_map, type_id))
        {
            return;
        }

        // how to parse scan data - AtomicType

        auto name_str = tag_type_str(type);
        auto desc_str = tag_description_str(type);

        auto name_len = strlen(name_str);
        auto desc_len = strlen(desc_str);

        DataType dt{};
        dt.type_id = type_id;

        dt.data_type_name = mb::push_cstr_view(table.atomic_str_data, name_len + 1);
        dt.data_type_description = mb::push_cstr_view(table.atomic_str_data, desc_len + 1);

        copy(name_str, dt.data_type_name);
        copy(desc_str, dt.data_type_description);

        table.type_map[type_id] = dt;
    }


    static void update_data_type_table(DataTypeTable& table, UdtEntry const& entry)
    {
        auto type_id = id32::get_udt_type_id(entry.udt_id);
        
        if (!type_id || map_contains(table.type_map, type_id))
        {
            return;
        }

        // how to parse scan data - UdtEntry
        
        MemoryBuffer<char> buffer{};
        
        auto desc_str = "User defined type";

        auto name_len = entry.udt_name.length;
        auto desc_len = strlen(desc_str);

        if (!mb::create_buffer(buffer, desc_len + name_len + 2)) /* zero terminated */
        {            
            return;
        }

        mb::zero_buffer(buffer);        

        DataType dt{};

        dt.type_id = type_id;

        dt.data_type_name = mb::push_cstr_view(buffer, name_len + 1);
        dt.data_type_description = mb::push_cstr_view(buffer, desc_len + 1);
        
        copy(entry.udt_name, dt.data_type_name);
        copy(desc_str, dt.data_type_description);

        table.type_map[type_id] = dt;

        table.udt_str_data.push_back(buffer);
    }


    static void update_data_type_table(DataTypeTable& table, TagEntry const& entry)
    {
        
    }


    static bool create_data_type_table(DataTypeTable& table)
    {
        u32 str_bytes = 0;

        for (auto t : SUPPORTED_TYPES)
        {
            str_bytes += strlen(tag_type_str(t));
            str_bytes += strlen(tag_description_str(t));
            str_bytes += 2; /* zero terminated */
        }

        if (!mb::create_buffer(table.atomic_str_data, str_bytes))
        {
            return false;
        }

        mb::zero_buffer(table.atomic_str_data);

        for (auto t : SUPPORTED_TYPES)
        {
            add_data_type(table, t);
        }

        return true;
    }
}


/* scan cycle */

namespace
{
    static bool scan_tag(String const& tag_name, Bytes& dst)
    {
        // TODO
        dst.begin = 0;
        dst.length = 0;

        return false;
    }


    static bool scan_tag(cstr tag_name, Bytes& dst)
    {
        // TODO
        dst.begin = 0;
        dst.length = 0;

        return false;
    }


    static void init()
    {
        DataTypeTable data_types{};
        TagTable tags{};

        auto const cleanup = [&]()
        {
            destroy_data_type_table(data_types);
            destroy_tag_table(tags);
        };

        // TODO
        Bytes entry_data{};
        if (!scan_tag("@tags", entry_data))
        {
            return;
        }

        auto tag_entries = parse_tag_entries(entry_data.begin, entry_data.length);

        if (!create_tag_table(tag_entries, tags))
        {
            return;
        }

        if (!create_data_type_table(data_types))
        {
            return;
        }

        std::vector<u16> udt_ids;
        append_udt_ids(tag_entries, udt_ids);

        destroy_vector(tag_entries);

        char udt[20];

        for (u32 i = 0; i < udt_ids.size(); ++i)
        {
            auto id = (int)udt_ids[i];
            qsnprintf(udt, 20, "@udt/%d", id);

            if (!scan_tag(udt, entry_data)) // TODO
            {
                continue;
            }

            auto entry = parse_udt_entry(entry_data.begin);

            update_data_type_table(data_types, entry);

            

            append_udt_ids(entry.fields, udt_ids);
        }

        
    }
}