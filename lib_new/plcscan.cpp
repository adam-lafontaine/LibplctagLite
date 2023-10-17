#include "../util/types.hpp"
#include "../util/memory_buffer.hpp"
#include "../util/qsprintf.hpp"
#include "../util/stopwatch.hpp"

#include "../lib_old/libplctag/libplctag.h"

#include <vector>
#include <array>
#include <unordered_map>
#include <cassert>
#include <thread>

namespace mb = memory_buffer;


using StringView = MemoryView<char>;
using ByteView = MemoryView<u8>;
using ByteOffset = MemoryOffset<u8>;
using DataTypeId32 = u32;


static void copy_8(u8* src, u8* dst, u32 len8)
{
    switch (len8)
    {
    case 1:
        *dst = *src;
        return;

    case 2:
        *(u16*)dst = *(u16*)src;
        return;

    case 3:
        *(u16*)dst = *(u16*)src;
        dst[2] = src[2];
        return;

    case 4:
        *(u32*)dst = *(u32*)src;
        return;
    
    case 5:
        *(u32*)dst = *(u32*)src;
        dst[4] = src[4];
        return;
    
    case 6:
        *(u32*)dst = *(u32*)src;
        *(u16*)(dst + 4) = *(u16*)(src + 4);
        return;
    
    case 7:
        *(u32*)dst = *(u32*)src;
        *(u16*)(dst + 4) = *(u16*)(src + 4);
        dst[6] = src[6];
        return;

    case 8:
        *(u64*)dst = *(u64*)src;
        return;

    default:
        break;
    }
}


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

    copy_8(src8, dst8, len8);
}


static bool bytes_equal(u8* lhs, u8* rhs, u32 len)
{
    switch (len)
    {
    case 1: return *lhs == *rhs;

    case 2: return *((u16*)lhs) == *((u16*)rhs);

    case 4: return *((u32*)lhs) == *((u32*)rhs);

    case 8: return *((u64*)lhs) == *((u64*)rhs);

    default:
        break;
    }

    constexpr auto size64 = sizeof(u64);

    auto len64 = len / size64;
    auto lhs64 = (u64*)lhs;
    auto rhs64 = (u64*)rhs;

    for (u32 i = 0; i < len64; ++i)
    {
        if (lhs64[i] != rhs64[i])
        {
            return false;
        }
    }

    for (u32 i = len64 * 8; i < len; ++i)
    {
        if (lhs[i] != rhs[i])
        {
            return false;
        }
    }

    return true;
}


static void copy_unsafe(cstr src, StringView const& dst)
{
    auto len = (u32)strlen(src);
    auto len = len < dst.length ? len : dst.length;

    for (u32 i = 0; i < len; ++i)
    {
        dst.begin[i] = src[i];
    }
}


static void copy_unsafe(StringView const& src, char* dst)
{
    auto len = (u32)strlen(dst);
    auto len = len < src.length ? len : src.length;

    for (u32 i = 0; i < len; ++i)
    {
        dst[i] = src.begin[i];
    }
}


static void copy(StringView const& src, StringView const& dst)
{
    auto len = src.length < dst.length ? src.length : dst.length;

    copy_bytes((u8*)src.begin, (u8*)dst.begin, len);
}


static void copy(ByteView const& src, ByteView const& dst)
{
    assert(src.length <= dst.length);

    copy_bytes(src.begin, dst.begin, src.length);
}


static void zero_string(StringView const& str)
{
    memset(str.begin, 0, str.length);
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


namespace time_helper
{
    namespace chr = std::chrono;

	inline void delay_current_thread(Stopwatch& sw, double min_delay_ms = 20.0)
	{
		auto ms = sw.get_time_milli();
		if (ms < min_delay_ms)
		{
			auto delay_ms = (unsigned long long)(min_delay_ms - ms);
			std::this_thread::sleep_for(chr::milliseconds(delay_ms));
		}
	}


	inline long long get_timestamp()
	{
		return chr::duration_cast<chr::milliseconds>(chr::system_clock::now().time_since_epoch()).count();
	}
}


namespace tmh = time_helper;


/* 16 bin ids */

namespace id16
{
    constexpr auto TYPE_IS_STRUCT = (u16)0x8000;     // 0b1000'0000'0000'0000
    constexpr auto TYPE_IS_SYSTEM = (u16)0x1000;     // 0b0001'0000'0000'0000
    constexpr auto UDT_FIELD_IS_ARRAY = (u16)0x2000; // 0b0010'0000'0000'0000

    constexpr auto TAG_DIM_MASK = (u16)0x6000;       // 0b0110'0000'0000'0000    

    constexpr auto FIXED_TYPE_ID_MASK = (u16)0x00FF; // 0b0000'0000'1111'1111
    constexpr auto FIXED_TYPE_ID_MIN = (u16)0xC1;    // 0b0000'0000'1100'0001
    constexpr auto FIXED_TYPE_ID_MAX = (u16)0xDE;    // 0b0000'0000'1101'1110

    constexpr auto UDT_TYPE_ID_MASK = (u16)0x0FFF; // 0b0000'1111'1111'1111
}


/* 32 bit ids */

namespace id32
{
    // 0b0000'0000'0000'0000'0000'0000'0000'0000
    //  |----other-----|------udt-----|--fixed--|

    constexpr auto OTHER_TYPE_ID_MASK  = (DataTypeId32)0b1111'1111'1111'0000'0000'0000'0000'0000;
    constexpr auto UDT_TYPE_ID_MASK    = (DataTypeId32)0b0000'0000'0000'1111'1111'1111'0000'0000;
    constexpr auto FIXED_TYPE_ID_MASK = (DataTypeId32)0b0000'0000'0000'0000'0000'0000'1111'1111;

    constexpr auto UNKNOWN_TYPE_ID = (DataTypeId32)0b0000'0000'0001'0000'0000'0000'0000'0000;
    constexpr auto SYSTEM_TYPE_ID = (DataTypeId32)0b0000'0000'0010'0000'0000'0000'0000'0000;
}


/* tag types */

namespace
{ 
    enum class FixedType : DataTypeId32
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


    constexpr std::array<FixedType, 14> NUMERIC_FIXED_TYPES = 
    {
        FixedType::BOOL,
        FixedType::SINT,
        FixedType::INT,
        FixedType::DINT,
        FixedType::LINT,
        FixedType::USINT,
        FixedType::UINT,
        FixedType::UDINT,
        FixedType::ULINT,
        FixedType::REAL,
        FixedType::LREAL,
        FixedType::CHAR_STRING,

        FixedType::SYSTEM,
        FixedType::UNKNOWN,
    };


    constexpr std::array<FixedType, 11> STRING_FIXED_TYPES = 
    {
        FixedType::CHAR_STRING,
        FixedType::STRING_8,
        FixedType::STRING_16,
        FixedType::STRING_32,
        FixedType::STRING_64,
        FixedType::WIDE_STRING,
        FixedType::N_BYTE_STRING,
        FixedType::COUNTED_CHAR_STRING,
        FixedType::INTERNATIONAL_STRING,

        FixedType::SYSTEM,
        FixedType::UNKNOWN,
    };


    cstr tag_type_str(FixedType t)
    {
        switch (t)
        {
        case FixedType::SYSTEM:               return "SYS"; // ?
        case FixedType::BOOL:                 return "BOOL";
        case FixedType::SINT:                 return "SINT";
        case FixedType::INT:                  return "INT";
        case FixedType::DINT:                 return "DINT";
        case FixedType::LINT:                 return "LINT";
        case FixedType::USINT:                return "USINT";
        case FixedType::UINT:                 return "UINT";
        case FixedType::UDINT:                return "UDINT";
        case FixedType::ULINT:                return "ULINT";
        case FixedType::REAL:                 return "REAL";
        case FixedType::LREAL:                return "LREAL";
        case FixedType::SYNCHRONOUS_TIME:     return "SYNC_TIME"; // ?
        case FixedType::DATE:                 return "DATE";
        case FixedType::TIME:                 return "TIME";
        case FixedType::DATETIME:             return "DATE_AND_TIME";
        case FixedType::CHAR_STRING:          return "STRING";
        case FixedType::STRING_8:             return "STRING_8";
        case FixedType::STRING_16:            return "STRING_16";
        case FixedType::STRING_32:            return "STRING_32";
        case FixedType::STRING_64:            return "STRING_64";
        case FixedType::WIDE_STRING:          return "WIDE_STRING";        
        case FixedType::HIGH_RES_DURATION:    return "High resolution duration value";
        case FixedType::MED_RES_DURATION:     return "Medium resolution duration value";
        case FixedType::LOW_RES_DURATION:     return "Low resolution duration value";
        case FixedType::N_BYTE_STRING:        return "N-byte per char character string";
        case FixedType::COUNTED_CHAR_STRING:  return "Counted character sting with 1 byte per character and 1 byte length indicator";
        case FixedType::DURATION_MS:          return "Duration in milliseconds";
        case FixedType::CIP_PATH:             return "CIP path segment(s)";
        case FixedType::ENGINEERING_UNITS:    return "Engineering units";
        case FixedType::INTERNATIONAL_STRING: return "International character string (encoding?)";
        
        default:                               return "UNKNOWN";
        }
    }


    cstr tag_description_str(FixedType t)
    {
        switch (t)
        {
        case FixedType::SYSTEM:               return "System tag";
        case FixedType::BOOL:                 return "Boolean value";
        case FixedType::SINT:                 return "Signed 8-bit integer value";
        case FixedType::INT:                  return "Signed 16-bit integer value";
        case FixedType::DINT:                 return "Signed 32-bit integer value";
        case FixedType::LINT:                 return "Signed 64-bit integer value";
        case FixedType::USINT:                return "Unsigned 8-bit integer value";
        case FixedType::UINT:                 return "Unsigned 16-bit integer value";
        case FixedType::UDINT:                return "Unsigned 32-bit integer value";
        case FixedType::ULINT:                return "Unsigned 64-bit integer value";
        case FixedType::REAL:                 return "32-bit floating point value, IEEE format";
        case FixedType::LREAL:                return "64-bit floating point value, IEEE format";
        case FixedType::SYNCHRONOUS_TIME:     return "Synchronous time value";
        case FixedType::DATE:                 return "Date value";
        case FixedType::TIME:                 return "Time of day value";
        case FixedType::DATETIME:             return "Date and time of day value";
        case FixedType::CHAR_STRING:          return "Character string, 1 byte per character";
        case FixedType::STRING_8:             return "8-bit bit string";
        case FixedType::STRING_16:            return "16-bit bit string";
        case FixedType::STRING_32:            return "32-bit bit string";
        case FixedType::STRING_64:            return "64-bit bit string";
        case FixedType::WIDE_STRING:          return "Wide char character string, 2 bytes per character";        
        case FixedType::HIGH_RES_DURATION:    return "High resolution duration value";
        case FixedType::MED_RES_DURATION:     return "Medium resolution duration value";
        case FixedType::LOW_RES_DURATION:     return "Low resolution duration value";
        case FixedType::N_BYTE_STRING:        return "N-byte per char character string";
        case FixedType::COUNTED_CHAR_STRING:  return "Counted character sting with 1 byte per character and 1 byte length indicator";
        case FixedType::DURATION_MS:          return "Duration in milliseconds";
        case FixedType::CIP_PATH:             return "CIP path segment(s)";
        case FixedType::ENGINEERING_UNITS:    return "Engineering units";
        case FixedType::INTERNATIONAL_STRING: return "International character string (encoding?)";
        
        default:                               return "Unknown tag type";
        }
    }
    
}


/* 16 bit id helpers */

namespace id16
{
    static inline u16 get_tag_dimensions(u16 type_code)
    {
        return (u16)((type_code & id16::TAG_DIM_MASK) >> 13);
    }


    static inline bool is_bit_field(u16 type_code)
    {
        return (FixedType)(type_code & id16::FIXED_TYPE_ID_MASK) == FixedType::BOOL;
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


/* 32 bit id helpers */

namespace id32
{
    static inline DataTypeId32 get_udt_type_id(u16 type_code)
    {
        if (type_code & id16::TYPE_IS_STRUCT)
        {
            // shift left 8 to prevent conflicts with numeric types
            return (DataTypeId32)(id16::get_udt_id(type_code)) << 8;
        }

        return 0;        
    }


    static DataTypeId32 get_data_type_id(u16 type_code)
    {
        if (type_code & id16::TYPE_IS_SYSTEM)
        {
            return UNKNOWN_TYPE_ID;
        }
        else if (type_code & id16::TYPE_IS_STRUCT)
        {            
            return get_udt_type_id(type_code);
        }

        u16 numeric_type = type_code & id16::FIXED_TYPE_ID_MASK;

        if (numeric_type >= id16::FIXED_TYPE_ID_MIN && numeric_type <= id16::FIXED_TYPE_ID_MAX)
        {
            return (DataTypeId32)numeric_type;
        }

        return UNKNOWN_TYPE_ID;
    }


    static bool is_udt_type(DataTypeId32 id)
    {
        return (id & UDT_TYPE_ID_MASK) && !(id & OTHER_TYPE_ID_MASK) && !(id & FIXED_TYPE_ID_MASK);
    }
}



/* tag listing */

namespace
{
    constexpr size_t MAX_TAG_NAME_LENGTH = 32;


    class TagEntry
    {
    public:
        u16 type_code = 0;
        u32 elem_size = 0;
        u32 elem_count = 0;
        StringView name;
    };


    using TagEntryList = std::vector<TagEntry>;    


    static bool is_valid_tag_name(cstr tag_name)
    {
        auto len = strlen(tag_name);
        if (len == 0 || len > MAX_TAG_NAME_LENGTH)
        {
            return false;
        }

        const char* invalid_begin_chars = "0123456789_";
        if (std::strchr(invalid_begin_chars, tag_name[0]) != nullptr)
        {
            return false;
        }

        const char* valid_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
        if (tag_name[0] == '@')
        {
            valid_chars = "@/abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
        }

        for (u32 i = 0; i < len; ++i)
        {
            auto c = tag_name[i];
            if (std::strchr(valid_chars, c) == nullptr)
            {
                return false;
            }
        }

        return true;
    }


    static bool is_valid_tag_name(StringView tag_name)
    {
        char buffer[MAX_TAG_NAME_LENGTH + 1] = { 0 };
        copy_unsafe(tag_name, buffer);

        return is_valid_tag_name(buffer);
    }


    static int append_tag_entry(TagEntryList& entries, u8* entry_data)
    {
        /* each entry looks like this:
        uint32_t instance_id    monotonically increasing but not contiguous
        uint16_t symbol_type    type of the symbol.
        uint16_t element_length length of one array element in bytes.
        uint32_t array_dims[3]  array dimensions.
        uint16_t string_len
        uint8_t string_data[string_len (or 82?)]   string bytes
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

        entry.elem_size = (u32)h.element_length;

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

        if (is_valid_tag_name(entry.name))
        {
            entries.push_back(entry);
        }

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
        int connection_handle = -1;

        DataTypeId32 type_id = id32::UNKNOWN_TYPE_ID;
        u32 array_count = 0;
        // how to parse scan data
        // fixed type size, udt fields

        ByteOffset scan_offset;
        StringView name;

        bool is_connected() const { return connection_handle > 0; }
    };


    class TagTable
    {
    public:
        std::vector<Tag> tags;

        ParallelBuffer<u8> scan_data;
        MemoryBuffer<u8> value_data;
        MemoryBuffer<char> name_data;
    };


    static void destroy_tag_table(TagTable& table)
    {
        destroy_vector(table.tags);

        mb::destroy_buffer(table.scan_data );
        mb::destroy_buffer(table.value_data );
        mb::destroy_buffer(table.name_data);
    }    


    u32 elem_size(TagEntry const& e) { return e.elem_count * e.elem_size; }


    u32 cstr_size(TagEntry const& e) { return e.name.length + 1; /* zero terminated */ }


    static void add_tag(TagTable& table, TagEntry const& entry)
    {
        auto value_len = elem_size(entry);
        auto name_alloc_len = cstr_size(entry);
        auto name_copy_len = entry.name.length;

        assert(name_alloc_len > name_copy_len); /* zero terminated */

        Tag tag{};

        tag.type_id = id32::get_data_type_id(entry.type_code);
        tag.array_count = entry.elem_count;

        tag.scan_offset = mb::push_offset(table.scan_data , value_len);
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

        if (!mb::create_buffer(table.scan_data , value_bytes))
        {
            destroy_tag_table(table);
            return false;
        }

        if (!mb::create_buffer(table.value_data , value_bytes))
        {
            destroy_tag_table(table);
            return false;
        }

        if (!mb::create_buffer(table.name_data, str_bytes))
        {
            destroy_tag_table(table);
            return false;
        }

        mb::zero_buffer(table.scan_data );
        mb::zero_buffer(table.value_data );
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

        StringView field_name;
    };


    class UdtEntry
    {
    public:
        u16 udt_id = 0;
        u32 udt_size = 0;
        u32 n_fields = 0;

        StringView udt_name;

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
        StringView data_type_name;
        StringView data_type_description;

        // how to parse scan data
    };


    using DataTypeMap = std::unordered_map<DataTypeId32, DataType>;


    class DataTypeTable
    {
    public:

        DataTypeMap numeric_type_map;
        DataTypeMap string_type_map;
        DataTypeMap udt_type_map;

        MemoryBuffer<char> numeric_string_name_data;
        std::vector<MemoryBuffer<char>> udt_name_data;
    };


    static void destroy_data_type_table(DataTypeTable& table)
    {
        destroy_map(table.numeric_type_map);
        destroy_map(table.string_type_map);
        destroy_map(table.udt_type_map);

        mb::destroy_buffer(table.numeric_string_name_data);

        for (auto& buffer : table.udt_name_data)
        {
            mb::destroy_buffer(buffer);
        }

        destroy_vector(table.udt_name_data);
    }


    static void add_data_type(DataTypeMap& type_map, MemoryBuffer<char>& name_data, FixedType type)
    {        
        auto type_id = (DataTypeId32)type;

        if (map_contains(type_map, type_id))
        {
            return;
        }

        // how to parse scan data - FixedType

        auto name_str = tag_type_str(type);
        auto desc_str = tag_description_str(type);

        auto name_len = strlen(name_str);
        auto desc_len = strlen(desc_str);

        DataType dt{};
        dt.type_id = type_id;

        dt.data_type_name = mb::push_cstr_view(name_data, name_len + 1);
        dt.data_type_description = mb::push_cstr_view(name_data, desc_len + 1);

        copy_unsafe(name_str, dt.data_type_name);
        copy_unsafe(desc_str, dt.data_type_description);

        type_map[type_id] = dt;
    }


    static void update_data_type_table(DataTypeTable& table, UdtEntry const& entry)
    {
        auto type_id = id32::get_udt_type_id(entry.udt_id);
        
        if (!type_id || map_contains(table.udt_type_map, type_id))
        {
            return;
        }
        
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
        copy_unsafe(desc_str, dt.data_type_description);

        // TODO: entry.fields - offsets

        table.udt_type_map[type_id] = dt;

        table.udt_name_data.push_back(buffer);
    }


    static bool create_data_type_table(DataTypeTable& table)
    {
        u32 name_bytes = 0;

        for (auto t : NUMERIC_FIXED_TYPES)
        {
            name_bytes += strlen(tag_type_str(t));
            name_bytes += strlen(tag_description_str(t));
            name_bytes += 2; /* zero terminated */
        }

        for (auto t : STRING_FIXED_TYPES)
        {
            name_bytes += strlen(tag_type_str(t));
            name_bytes += strlen(tag_description_str(t));
            name_bytes += 2; /* zero terminated */
        }

        if (!mb::create_buffer(table.numeric_string_name_data, name_bytes))
        {
            return false;
        }

        mb::zero_buffer(table.numeric_string_name_data);

        for (auto t : NUMERIC_FIXED_TYPES)
        {
            add_data_type(table.numeric_type_map, table.numeric_string_name_data, t);
        }

        for (auto t : STRING_FIXED_TYPES)
        {
            add_data_type(table.string_type_map, table.numeric_string_name_data, t);
        }

        return true;
    }


    DataType lookup_type(DataTypeMap const& type_map, DataTypeId32 type_id)
    {
        auto not_found = type_map.end();
        auto it = type_map.find(type_id);

        return it == not_found ? type_map.at(id32::UNKNOWN_TYPE_ID) : (*it).second;
    }
}


/* tag scan */

namespace
{
    class ControllerAttr
    {
    public:
        // TODO
        cstr gateway = "192.168.123.123";
        cstr path = "1,0";

        StringView connection_string;
        StringView tag_name;

        MemoryBuffer<char> string_data;
    };


    static void destroy_controller(ControllerAttr& attr)
    {
        mb::destroy_buffer(attr.string_data);
    }


    static bool init_controller(ControllerAttr& attr)
    {
        cstr base = "protocol=ab-eip&plc=controllogix"; // 32

        cstr gateway_key = "&gateway="; // 9
        cstr path_key = "&path="; // 6
        cstr name_key = "&name="; // 6

        auto base_len = 
            strlen(base) + 
            strlen(gateway_key) +
            strlen(attr.gateway) + 
            strlen(path_key) + 
            strlen(attr.path) +
            strlen(name_key);

        auto total_size = base_len + MAX_TAG_NAME_LENGTH + 1; /* zero terminated */

        if (!mb::create_buffer(attr.string_data, total_size))
        {
            return false;
        }

        mb::zero_buffer(attr.string_data);

        attr.connection_string = mb::push_view(attr.string_data, base_len);
        attr.tag_name = mb::push_cstr_view(attr.string_data, MAX_TAG_NAME_LENGTH + 1);

        auto dst = attr.connection_string.begin;
        cstr fmt = "%s%s%s%s%s%s";

        qsnprintf(dst, base_len, fmt, base, gateway_key, attr.gateway, path_key, attr.path, name_key);

        return true;
    }


    static bool set_tag_name(ControllerAttr const& attr, Tag& tag)
    {
        zero_string(attr.tag_name);

        copy(tag.name, attr.tag_name);

        return true;
    }


    static bool set_tag_name(ControllerAttr const& attr, cstr name)
    {
        zero_string(attr.tag_name);

        copy_unsafe(name, attr.tag_name);

        return true;
    }


    static bool connect_tag(ControllerAttr const& attr, Tag& tag)
    {
        if (!set_tag_name(attr, tag))
        {
            return false;
        }

        auto timeout = 100;

        auto rc = plc_tag_create(attr.connection_string.begin, timeout);
        if (rc < 0)
        {
            return false;
        }

        tag.connection_handle = rc;

        return true;
    }
    
    
    static bool scan_to_view(int tag_handle, ByteView const& view)
    {
        auto timeout = 100;

        auto rc = plc_tag_read(tag_handle, timeout);
        if (rc != PLCTAG_STATUS_OK)
        {
            return false;
        }

        rc = plc_tag_get_raw_bytes(tag_handle, 0, view.begin, view.length);
        if (rc != PLCTAG_STATUS_OK)
        {
            return false;
        }

        return true;
    }


    static bool scan_tag(Tag const& tag, ParallelBuffer<u8> const& buffer)
    {
        auto view = mb::make_write_view(buffer, tag.scan_offset);

        return scan_to_view(tag.connection_handle, view);
    }


    static bool scan_to_buffer(int tag_handle, MemoryBuffer<u8>& dst)
    {
        auto timeout = 100;

        auto rc = plc_tag_read(tag_handle, timeout);
        if (rc != PLCTAG_STATUS_OK)
        {
            return false;
        }

        auto size = plc_tag_get_size(tag_handle);
        if (size < 4)
        {
            return false;
        }

        if (!mb::create_buffer(dst, (u32)size))
        {
            return false;
        }

        auto view = mb::push_view(dst, (u32)size);

        rc = plc_tag_get_raw_bytes(tag_handle, 0, view.begin, view.length);
        if (rc != PLCTAG_STATUS_OK)
        {
            return false;
        }

        return true;
    }


    static bool scan_to_buffer(ControllerAttr const& attr, cstr tag_name, MemoryBuffer<u8>& dst)
    {
        if (!set_tag_name(attr, tag_name))
        {
            return false;
        }

        auto timeout = 100;

        auto rc = plc_tag_create(attr.connection_string.begin, timeout);
        if (rc < 0)
        {
            return false;
        }

        auto handle = rc;

        return scan_to_buffer(handle, dst);
    }


    static bool scan_tag_entry_listing(ControllerAttr const& attr, MemoryBuffer<u8>& dst)
    {
        return scan_to_buffer(attr, "@tags", dst);
    }


    static bool scan_udt_entry(ControllerAttr const& attr, int udt_id, MemoryBuffer<u8>& dst)
    {
        char udt[20];
        qsnprintf(udt, 20, "@udt/%d", udt_id);

        return scan_to_buffer(attr, udt, dst);
    }

}


/* scan cycle */

namespace
{
    void enumerate_tags(ControllerAttr const& attr, TagTable& tags, DataTypeTable& data_types)
    {
        MemoryBuffer<u8> entry_buffer;
        
        if (!scan_tag_entry_listing(attr, entry_buffer))
        {
            return;
        }

        auto entry_data = mb::make_view(entry_buffer);

        auto tag_entries = parse_tag_entries(entry_data.begin, entry_data.length);

        if (!create_tag_table(tag_entries, tags))
        {
            mb::destroy_buffer(entry_buffer);
            return;
        }

        std::vector<u16> udt_ids;
        append_udt_ids(tag_entries, udt_ids);

        destroy_vector(tag_entries);
        mb::destroy_buffer(entry_buffer);

        for (u32 i = 0; i < udt_ids.size(); ++i)
        {
            auto id = (int)udt_ids[i];
            
            MemoryBuffer<u8> udt_buffer;

            if (!scan_udt_entry(attr, id, udt_buffer))
            {
                continue;
            }

            auto entry = parse_udt_entry(udt_buffer.data_);

            update_data_type_table(data_types, entry);
            append_udt_ids(entry.fields, udt_ids);

            mb::destroy_buffer(udt_buffer);
        }
    }


    static void connect_tags(ControllerAttr const& attr, TagTable& tags)
    {
        auto timeout = 100;

        for (auto& tag : tags.tags)
        {
            connect_tag(attr, tag);
        }
    }


    static void scan_tags(TagTable const& tags)
    {
        auto timeout = 100;

        for (auto const& tag : tags.tags)
        {
            if (!tag.is_connected())
            {
                continue;
            }

            auto rc = plc_tag_read(tag.connection_handle, timeout);
            if (rc != PLCTAG_STATUS_OK)
            {
                
            }
        }

        for (auto const& tag : tags.tags)
        {
            if (!tag.is_connected())
            {
                continue;
            }

            auto view = mb::make_write_view(tags.scan_data , tag.scan_offset);
            auto rc = plc_tag_get_raw_bytes(tag.connection_handle, 0, view.begin, view.length);
            if (rc != PLCTAG_STATUS_OK)
            {
                
            }
        }
    }


    static void process_tags(TagTable const& tags)
    {
        auto src = mb::make_read_view(tags.scan_data);
        auto dst = mb::make_view(tags.value_data);

        copy(src, dst);


    }


    static void sample_main()
    {
        DataTypeTable data_types{};
        ControllerAttr attr{};
        TagTable tags{};

        if (!create_data_type_table(data_types))
        {
            return;
        }        

        if (!init_controller(attr))
        {
            return;
        }

        enumerate_tags(attr, tags, data_types);

        connect_tags(attr, tags);

        Stopwatch sw;
        f64 scan_ms = 0.0;
        f64 proc_ms = 0.0;
        f64 total_ms = 0.0;

        auto const scan = [&](){ scan_tags(tags); scan_ms = sw.get_time_milli(); };
        auto const process = [&](){ process_tags(tags); proc_ms = sw.get_time_milli(); };

        int running = 500;

        while (running > 0)
        {
            sw.start();

            std::thread scan_th(scan);
            std::thread process_th(process);

            scan_th.join();
            process_th.join();

            mb::flip_read_write(tags.scan_data );

            running--;
            total_ms = sw.get_time_milli();

            tmh::delay_current_thread(sw, 100);
        }

        destroy_tag_table(tags);
        destroy_controller(attr);
        destroy_data_type_table(data_types);
    }
}