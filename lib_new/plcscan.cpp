#include "../util/qsprintf.hpp"
#include "../util/stopwatch.hpp"
#include "plcscan.hpp"

#include "../lib_old/libplctag/libplctag.h"

#include <array>
#include <cassert>
#include <thread>


using ByteOffset = MemoryOffset<u8>;

using DataTypeId32 = plcscan::DataTypeId32;
using Tag = plcscan::Tag;
using DataType = plcscan::DataType;
using UdtFieldType = plcscan::UdtFieldType;
using UdtType = plcscan::UdtType;
using PlcTagData = plcscan::PlcTagData;


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
    constexpr auto size64 = sizeof(u64);

    auto len = str.length;

    auto len64 = len / size64;
    auto dst64 = (u64*)str.begin;

    auto len8 = len - len64 * size64;
    auto dst8 = (u8*)(dst64 + len64);

    for (size_t i = 0; i < len64; ++i)
    {
        dst64[i] = 0;
    }

    for (size_t i = 0; i < len8; ++i)
    {
        dst8[i] = 0;
    }
}


static bool string_contains(cstr str, char c)
{
    auto len = strlen(str);

    for (u32 i = 0; i < len; ++i)
    {
        if (str[i] == c)
        {
            return true;
        }
    }

    return false;
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

namespace /* private */
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


    constexpr std::array<FixedType, 11> NUMERIC_FIXED_TYPES = 
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
    };


    constexpr std::array<FixedType, 9> STRING_FIXED_TYPES = 
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
    };


    constexpr std::array<FixedType, 12> OTHER_FIXED_TYPES = 
    {        
        FixedType::SYSTEM,
        FixedType::UNKNOWN,

        FixedType::SYNCHRONOUS_TIME,
        FixedType::DATE,
        FixedType::TIME,
        FixedType::DATETIME,

        FixedType::HIGH_RES_DURATION,
        FixedType::MED_RES_DURATION,
        FixedType::LOW_RES_DURATION,

        FixedType::DURATION_MS,
        FixedType::CIP_PATH,
        FixedType::ENGINEERING_UNITS,
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
        
        default:                              return "Unknown tag type";
        }
    }


    constexpr u32 MAX_TYPE_BYTES = 16;


    u32 data_type_size(FixedType t)
    {
        switch (t)
        {
        case FixedType::SYSTEM:               return 0;
        case FixedType::BOOL:                 return 1;
        case FixedType::SINT:                 return 1;
        case FixedType::INT:                  return 2;
        case FixedType::DINT:                 return 4;
        case FixedType::LINT:                 return 8;
        case FixedType::USINT:                return 1;
        case FixedType::UINT:                 return 2;
        case FixedType::UDINT:                return 4;
        case FixedType::ULINT:                return 8;
        case FixedType::REAL:                 return 4;
        case FixedType::LREAL:                return 8;
        case FixedType::SYNCHRONOUS_TIME:     return MAX_TYPE_BYTES;
        case FixedType::DATE:                 return MAX_TYPE_BYTES;
        case FixedType::TIME:                 return MAX_TYPE_BYTES;
        case FixedType::DATETIME:             return MAX_TYPE_BYTES;
        case FixedType::CHAR_STRING:          return MAX_TYPE_BYTES;
        case FixedType::STRING_8:             return MAX_TYPE_BYTES;
        case FixedType::STRING_16:            return MAX_TYPE_BYTES;
        case FixedType::STRING_32:            return MAX_TYPE_BYTES;
        case FixedType::STRING_64:            return MAX_TYPE_BYTES;
        case FixedType::WIDE_STRING:          return MAX_TYPE_BYTES;    
        case FixedType::HIGH_RES_DURATION:    return MAX_TYPE_BYTES;
        case FixedType::MED_RES_DURATION:     return MAX_TYPE_BYTES;
        case FixedType::LOW_RES_DURATION:     return MAX_TYPE_BYTES;
        case FixedType::N_BYTE_STRING:        return MAX_TYPE_BYTES;
        case FixedType::COUNTED_CHAR_STRING:  return MAX_TYPE_BYTES;
        case FixedType::DURATION_MS:          return MAX_TYPE_BYTES;
        case FixedType::CIP_PATH:             return MAX_TYPE_BYTES;
        case FixedType::ENGINEERING_UNITS:    return MAX_TYPE_BYTES;
        case FixedType::INTERNATIONAL_STRING: return MAX_TYPE_BYTES;
        
        default:                              return MAX_TYPE_BYTES;
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

        return UNKNOWN_TYPE_ID;     
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

namespace /* private */
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

        auto invalid_begin_chars = "0123456789_";
        if (string_contains(invalid_begin_chars, tag_name[0]))
        {
            return false;
        }

        auto valid_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";

        u32 begin = 0;
        
        if (tag_name[0] == '@')
        {
            begin++; // start checking from 2nd character
        }

        for (u32 i = begin; i < len; ++i)
        {
            auto c = tag_name[i];
            if (!string_contains(valid_chars, c))
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


/* tag memory */

namespace /* private */
{
    class TagConnection
    {
    public:
        int connection_handle = -1;        

        ByteOffset scan_offset;

        bool scan_ok = false;

        bool is_connected() const { return connection_handle > 0; }
    };


    class TagMemory
    {
    public:
        std::vector<TagConnection> connections;
        // TODO tag_status
        //std::vector<Tag> tags;

        u32 n_tags = 0;

        ParallelBuffer<u8> scan_data;
        MemoryBuffer<u8> public_tag_data;
        MemoryBuffer<char> name_data;
    };


    static void destroy_tag_memory(TagMemory& mem)
    {
        destroy_vector(mem.connections);

        mb::destroy_buffer(mem.scan_data);
        mb::destroy_buffer(mem.public_tag_data);
        mb::destroy_buffer(mem.name_data);
    }    


    u32 elem_size(TagEntry const& e) { return e.elem_count * e.elem_size; }


    u32 cstr_size(TagEntry const& e) { return e.name.length + 1; /* zero terminated */ }


    static void add_tag(TagEntry const& entry, TagMemory& mem, List<Tag>& tags)
    {
        auto value_len = elem_size(entry);
        auto name_alloc_len = cstr_size(entry);
        auto name_copy_len = entry.name.length;

        assert(name_alloc_len > name_copy_len); /* zero terminated */

        TagConnection conn{};
        conn.scan_offset = mb::push_offset(mem.scan_data , value_len);

        Tag tag{};
        tag.type_id = id32::get_data_type_id(entry.type_code);
        tag.array_count = entry.elem_count;
        tag.name = mb::push_cstr_view(mem.name_data, name_alloc_len);
        tag.data = mb::make_view(mem.public_tag_data, conn.scan_offset);

        copy(entry.name, tag.name);

        mem.connections.push_back(conn);
        tags.push_back(tag);

        mem.n_tags++;
    }


    static bool create_tags(TagEntryList const& entries, TagMemory& mem, List<Tag>& tags)
    {
        u32 value_bytes = 0;
        u32 str_bytes = 0;

        for (auto const& e : entries)
        {
            value_bytes += elem_size(e);
            str_bytes += cstr_size(e);
        }

        if (!mb::create_buffer(mem.scan_data , value_bytes))
        {
            destroy_tag_memory(mem);
            return false;
        }

        if (!mb::create_buffer(mem.public_tag_data , value_bytes))
        {
            destroy_tag_memory(mem);
            return false;
        }

        if (!mb::create_buffer(mem.name_data, str_bytes))
        {
            destroy_tag_memory(mem);
            return false;
        }

        mb::zero_buffer(mem.scan_data);
        mb::zero_buffer(mem.public_tag_data);
        mb::zero_buffer(mem.name_data);

        mem.connections.reserve(entries.size());
        mem.n_tags = 0;

        tags.reserve(entries.size());

        for (auto const& e : entries)
        {
            add_tag(e, mem, tags);
        }

        return true;
    }
}


/* udt entries */

namespace /* private */
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


/* data type memory */

namespace /* private */
{
    template <typename T>
    static bool list_contains(std::vector<T> const& vec, DataTypeId32 type_id)
    {
        for (auto const& type : vec)
        {
            if (type.type_id == type_id)
            {
                return true;
            }
        }

        return false;
    }


    template <typename T>
    T lookup_type(std::vector<T> const& type_map, DataTypeId32 type_id)
    {
        assert(type_map.size());

        for (auto const& type : type_map)
        {
            if (type.type_id == type_id)
            {
                return type;
            }
        }

        return type_map.back();
    }


    class DataTypeMemory
    {
    public:
        MemoryBuffer<char> type_name_data;
        std::vector<MemoryBuffer<char>> udt_name_data;
    };


    static void destroy_data_type_memory(DataTypeMemory& mem)
    {
        mb::destroy_buffer(mem.type_name_data);

        for (auto& buffer : mem.udt_name_data)
        {
            mb::destroy_buffer(buffer);
        }

        destroy_vector(mem.udt_name_data);
    }


    static void add_data_type(List<DataType>& types, DataTypeMemory& mem, FixedType type)
    {        
        auto type_id = (DataTypeId32)type;

        if (list_contains(types, type_id))
        {
            return;
        }

        auto& name_data = mem.type_name_data;

        auto name_str = tag_type_str(type);
        auto desc_str = tag_description_str(type);

        auto name_len = strlen(name_str);
        auto desc_len = strlen(desc_str);

        DataType dt{};
        dt.type_id = type_id;
        dt.size = data_type_size(type);

        dt.data_type_name = mb::push_cstr_view(name_data, name_len + 1);
        dt.data_type_description = mb::push_cstr_view(name_data, desc_len + 1);

        copy_unsafe(name_str, dt.data_type_name);
        copy_unsafe(desc_str, dt.data_type_description);

        types.push_back(dt);
    }


    static void add_udt_type(List<UdtType>& udt_types, DataTypeMemory& mem, UdtEntry const& entry)
    {
        auto type_id = id32::get_udt_type_id(entry.udt_id);
        
        if (!type_id || list_contains(udt_types, type_id))
        {
            return;
        }
        
        MemoryBuffer<char> buffer{};
        
        auto desc_str = "User defined type";

        auto name_len = entry.udt_name.length;
        auto desc_len = strlen(desc_str);

        auto buffer_len = desc_len + name_len + 2; /* zero terminated */
        for (auto const& f : entry.fields)
        {
            buffer_len += f.field_name.length + 1; /* zero terminated */
        }

        if (!mb::create_buffer(buffer, buffer_len))
        {            
            return;
        }

        mb::zero_buffer(buffer);

        UdtType ut{};

        ut.type_id = type_id;
        ut.size = entry.udt_size;

        ut.udt_name = mb::push_cstr_view(buffer, name_len + 1);
        ut.udt_description = mb::push_cstr_view(buffer, desc_len + 1);
        
        copy(entry.udt_name, ut.udt_name);
        copy_unsafe(desc_str, ut.udt_description);

        ut.fields.reserve(entry.fields.size());
        for (auto const& f : entry.fields)
        {
            UdtFieldType ft{};
            ft.type_id = id32::get_data_type_id(f.type_code);
            ft.offset = f.offset;
            ft.array_count = f.elem_count;
            ft.bit_number = f.bit_number;

            ft.field_name = mb::push_cstr_view(buffer, f.field_name.length + 1);
            copy(f.field_name, ft.field_name);

            ut.fields.push_back(ft);
        }

        udt_types.push_back(ut);
        mem.udt_name_data.push_back(buffer);
    }


    static bool create_data_type_memory(DataTypeMemory& mem)
    {
        u32 str_bytes = 0;

        for (auto t : NUMERIC_FIXED_TYPES)
        {
            str_bytes += strlen(tag_type_str(t));
            str_bytes += strlen(tag_description_str(t));
            str_bytes += 2; /* zero terminated */
        }

        for (auto t : STRING_FIXED_TYPES)
        {
            str_bytes += strlen(tag_type_str(t));
            str_bytes += strlen(tag_description_str(t));
            str_bytes += 2; /* zero terminated */
        }

        for (auto t : OTHER_FIXED_TYPES)
        {
            str_bytes += strlen(tag_type_str(t));
            str_bytes += strlen(tag_description_str(t));
            str_bytes += 2; /* zero terminated */
        }

        if (!mb::create_buffer(mem.type_name_data, str_bytes))
        {
            return false;
        }

        mb::zero_buffer(mem.type_name_data);

        

        return true;
    }


    static void add_data_types(DataTypeMemory& mem, List<DataType>& data_types)
    {
        for (auto t : NUMERIC_FIXED_TYPES)
        {
            add_data_type(data_types, mem, t);
        }

        for (auto t : STRING_FIXED_TYPES)
        {
            add_data_type(data_types, mem, t);
        }

        for (auto t : OTHER_FIXED_TYPES)
        {
            add_data_type(data_types, mem, t);
        }
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


    static bool set_tag_name(ControllerAttr const& attr, Tag const& tag)
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


    static bool connect_tag(ControllerAttr const& attr, Tag const& tag, TagConnection& conn)
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

        conn.connection_handle = rc;

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


    static bool scan_tag(TagConnection const& tag, ParallelBuffer<u8> const& buffer)
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
    static bool enumerate_tags(ControllerAttr const& attr, TagMemory& tag_mem, DataTypeMemory& dt_mem, PlcTagData data)
    {
        MemoryBuffer<u8> entry_buffer;
        
        if (!scan_tag_entry_listing(attr, entry_buffer))
        {
            return false;
        }

        auto entry_data = mb::make_view(entry_buffer);

        auto tag_entries = parse_tag_entries(entry_data.begin, entry_data.length);

        if (!create_tags(tag_entries, tag_mem, data.tags))
        {
            mb::destroy_buffer(entry_buffer);
            return false;
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

            add_udt_type(data.udt_types, dt_mem, entry);

            // add new udts as we find them
            append_udt_ids(entry.fields, udt_ids);

            mb::destroy_buffer(udt_buffer);
        }

        return true;
    }


    static void connect_tags(ControllerAttr const& attr, TagMemory& mem, List<Tag>& tags)
    {
        for (u32 i = 0; i < mem.n_tags; ++i)
        {
            auto& conn = mem.connections[i];
            auto& tag = tags[i];

            connect_tag(attr, tag, conn);

            //tag.connection_ok = connect_tag(attr, tag, conn);
        }
    }


    static void scan_tags(TagMemory& mem)
    {
        auto timeout = 100;

        for (auto& conn : mem.connections)
        {
            if (!conn.is_connected())
            {
                continue;
            }

            auto rc = plc_tag_read(conn.connection_handle, timeout);
            conn.scan_ok = rc == PLCTAG_STATUS_OK;
        }

        for (auto& conn : mem.connections)
        {
            if (!conn.is_connected() || !conn.scan_ok)
            {
                continue;
            }

            auto view = mb::make_write_view(mem.scan_data , conn.scan_offset);
            auto rc = plc_tag_get_raw_bytes(conn.connection_handle, 0, view.begin, view.length);
            conn.scan_ok = rc == PLCTAG_STATUS_OK;
        }
    }


    static void copy_tags(TagMemory& mem)
    {
        auto src = mb::make_read_view(mem.scan_data);
        auto dst = mb::make_view(mem.public_tag_data);

        copy(src, dst);
    }
  
}


/* api */

namespace plcscan
{
    static DataTypeMemory g_dt_mem;
    static TagMemory g_tag_mem;
    static ControllerAttr g_attr;


    void disconnect()
    {
        destroy_data_type_memory(g_dt_mem);
        destroy_tag_memory(g_tag_mem);
        destroy_controller(g_attr);
        plc_tag_shutdown();
    }


    PlcTagData init()
    {
        PlcTagData data{};

        if (!create_data_type_memory(g_dt_mem))
        {
            disconnect();
            return data;
        }

        add_data_types(g_dt_mem, data.data_types);

        data.is_init = true;
        return data;
    }


    bool connect(cstr gateway, cstr path, PlcTagData& data)
    {     
        if (!data.is_init)
        {
            disconnect();
            return false;
        }   

        g_attr.gateway = gateway;
        g_attr.path = path;

        if (!init_controller(g_attr))
        {
            disconnect();
            return false;
        }

        if (!enumerate_tags(g_attr, g_tag_mem, g_dt_mem, data))
        {
            disconnect();
            return false;
        }

        connect_tags(g_attr, g_tag_mem, data.tags);        

        data.is_connected = true;
        return false;
    }


    void scan(void_f const& scan_cb, bool_f const& scan_condition)
    {
        constexpr int target_scan_ms = 100;

        Stopwatch sw;
        f64 scan_ms = 0.0;
        f64 proc_ms = 0.0;
        f64 total_ms = 0.0;

        auto const scan = [&]()
        { 
            scan_tags(g_tag_mem); 
            scan_ms = sw.get_time_milli(); 
        };

        auto const process = [&]()
        { 
            copy_tags(g_tag_mem); 
            scan_cb(); 
            proc_ms = sw.get_time_milli(); 
        };

        while(scan_condition())
        {
            sw.start();

            std::thread scan_th(scan);
            std::thread process_th(process);

            scan_th.join();
            process_th.join();

            mb::flip_read_write(g_tag_mem.scan_data);
            
            total_ms = sw.get_time_milli();

            tmh::delay_current_thread(sw, target_scan_ms);
        }
    }


    DataTypeCategory get_type_category(DataTypeId32 type_id)
    {
        if (type_id >= (DataTypeId32)FixedType::BOOL && type_id <= (DataTypeId32)FixedType::LREAL)
        {
            return DataTypeCategory::Numeric;
        }

        if (id32::is_udt_type(type_id))
        {
            return DataTypeCategory::Udt;
        }

        for (auto t : STRING_FIXED_TYPES)
        {
            if (type_id == (DataTypeId32)t)
            {
                return DataTypeCategory::String;
            }
        }

        return DataTypeCategory::Other;
    }
}