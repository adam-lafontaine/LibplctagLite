/* LICENSE: See end of file for license information. */

#define DEVPLCTAG

#ifdef DEVPLCTAG

#include "../dev/devplctag.hpp"
using namespace dev;

#else

#include "../libplctag/libplctag.h"

#endif



#include "../util/qsprintf.hpp"
#include "../util/time_helper.hpp"
#include "../util/memory_helper.hpp"
#include "plcscan.hpp"

#include <array>
#include <cassert>

namespace tmh = time_helper;
namespace mh = memory_helper;


using ByteOffset = MemoryOffset<u8>;

using DataTypeId32 = plcscan::DataTypeId32;
using Tag = plcscan::Tag;
using DataType = plcscan::DataType;
using UdtFieldType = plcscan::UdtFieldType;
using UdtType = plcscan::UdtType;
using PlcTagData = plcscan::PlcTagData;



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


/* 16 bit ids */

// TODO: union tricks

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

// TODO: union tricks

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
        case FixedType::SYSTEM:               return "SYSTEM"; // ?
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
        case FixedType::HIGH_RES_DURATION:    return "HIGH_RES_DURATION";
        case FixedType::MED_RES_DURATION:     return "MED_RES_DURATION";
        case FixedType::LOW_RES_DURATION:     return "LOW_RES_DURATION";
        case FixedType::N_BYTE_STRING:        return "N_BYTE_STRING";
        case FixedType::COUNTED_CHAR_STRING:  return "COUNTED_CHAR_STRING";
        case FixedType::DURATION_MS:          return "DURATION_MS";
        case FixedType::CIP_PATH:             return "CIP_PATH";
        case FixedType::ENGINEERING_UNITS:    return "ENGINEERING_UNITS";
        case FixedType::INTERNATIONAL_STRING: return "INTERNATIONAL_STRING";
        
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
        case FixedType::SYSTEM:               return MAX_TYPE_BYTES;
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

        u16 fixed_type = type_code & id16::FIXED_TYPE_ID_MASK;

        if (fixed_type >= id16::FIXED_TYPE_ID_MIN && fixed_type <= id16::FIXED_TYPE_ID_MAX)
        {
            return (DataTypeId32)fixed_type;
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
        
        char* name_ptr = nullptr;
        u32 name_length = 0;
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
        if (mh::string_contains(invalid_begin_chars, tag_name[0]))
        {
            return false;
        }

        auto valid_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
        
        if (tag_name[0] == '@') // e.g. @udt/
        {
            return true;
        }

        for (u32 i = 0; i < len; ++i)
        {
            auto c = tag_name[i];
            if (!mh::string_contains(valid_chars, c))
            {
                return false;
            }
        }

        return true;
    }


    static bool is_valid_tag_name(StringView tag_name)
    {
        return is_valid_tag_name(tag_name.data());
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

        if (entry.elem_count > 1)
        {
            int x = 0;
        }

        int offset = H_size;

        entry.name_ptr = (char*)(entry_data + offset);
        entry.name_length = (u32)h.string_len;

        char buffer[MAX_TAG_NAME_LENGTH + 1] = { 0 };
        mh::copy_unsafe(entry.name_ptr, buffer, entry.name_length);

        if (is_valid_tag_name(buffer))
        {
            entries.push_back(std::move(entry));
        }

        offset += h.string_len;

        return offset;
    }


    static TagEntryList parse_tag_entries(u8* entry_data, u32 data_size)
    {
        TagEntryList list;

        int offset = 0;

        while ((u32)offset < data_size)
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
        ByteBuffer public_tag_data;
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


    u32 cstr_size(TagEntry const& e) { return e.name_length + 1; /* zero terminated */ }


    static void add_tag(TagEntry const& entry, TagMemory& mem, List<Tag>& tags)
    {
        auto value_len = elem_size(entry);
        auto name_alloc_len = cstr_size(entry);
        auto name_copy_len = entry.name_length;

        assert(value_len);

        if (!value_len)
        {
            return;
        }

        assert(name_alloc_len > name_copy_len); /* zero terminated */

        TagConnection conn{};
        conn.scan_offset = mb::push_offset(mem.scan_data , value_len);

        Tag tag{};
        tag.type_id = id32::get_data_type_id(entry.type_code);
        tag.array_count = entry.elem_count;
        tag.tag_name = mh::push_cstr_view(mem.name_data, name_alloc_len);        
        tag.bytes = mb::sub_view(mem.public_tag_data, conn.scan_offset);

        mh::copy_unsafe(entry.name_ptr, tag.tag_name, name_copy_len);

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


    static StringView get_data_type_name(DataTypeId32 type_id, List<UdtType> const& udts)
    {
        static constexpr auto udt_type = "UDT";

        if (!id32::is_udt_type(type_id))
        {
            auto str = tag_type_str((FixedType)type_id);
            auto len = (u32)strlen(str);

            return mh::to_string_view_unsafe((char*)str, len);
        }
        
        for (auto const& udt : udts)
        {
            if (type_id == udt.type_id)
            {
                return udt.udt_name;
            }
        }

        return mh::to_string_view_unsafe((char*)udt_type, (u32)strlen(udt_type));
    }
    
    
    static void set_tag_data_type_names(List<Tag>& tags, List<UdtType> const& udts)
    {
        for (auto& tag : tags)
        {
            tag.data_type_name = get_data_type_name(tag.type_id, udts);
        }
    }


    static void set_udt_field_data_type_names(List<UdtType>& udts)
    {
        for (auto& udt : udts)
        {
            for (auto& field : udt.fields)
            {
                field.data_type_name = get_data_type_name(field.type_id, udts);
            }
        }
    }
}


/* udt entries */

namespace /* private */
{
    class FieldEntry
    {
    public:
        u16 elem_count = 0;
        i32 bit_number = -1;

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

        char name_buffer[MAX_TAG_NAME_LENGTH + 1] = { 0 };

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
                field.bit_number = (i32)f.metadata;
            }

            entry.fields.push_back(field);

            offset += F_size;
        }

        auto string_data = (char*)(udt_data + offset);
        auto string_len = strlen(string_data);
        size_t str_offset = 0;

        u32 name_len = 0;
        char end = ';';
        while (string_data[name_len] != end && name_len < string_len)
        {
            ++name_len;
        }

        entry.udt_name = mh::to_string_view_unsafe(entry.name_buffer, name_len);
        mh::copy_unsafe(string_data, entry.udt_name, name_len);

        str_offset = string_len + 1;
        for (auto& field : entry.fields)
        {
            auto str = string_data + str_offset;
            auto len = strlen(str);
            field.field_name = mh::to_string_view_unsafe(str, (u32)len);
            str_offset += len + 1;
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

        auto name_len = (u32)strlen(name_str);
        auto desc_len = (u32)strlen(desc_str);

        DataType dt{};
        dt.type_id = type_id;
        dt.size = data_type_size(type);

        dt.data_type_name = mh::push_cstr_view(name_data, name_len + 1);
        dt.data_type_description = mh::push_cstr_view(name_data, desc_len + 1);

        mh::copy_unsafe((char*)name_str, dt.data_type_name, name_len);
        mh::copy_unsafe((char*)desc_str, dt.data_type_description, desc_len);

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
        auto desc_len = (u32)strlen(desc_str);

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

        ut.udt_name = mh::push_cstr_view(buffer, name_len + 1);
        ut.udt_description = mh::push_cstr_view(buffer, desc_len + 1);
        
        mh::copy(entry.udt_name, ut.udt_name);
        mh::copy_unsafe((char*)desc_str, ut.udt_description, desc_len);

        ut.fields.reserve(entry.fields.size());
        for (auto const& f : entry.fields)
        {
            UdtFieldType ft{};
            ft.type_id = id32::get_data_type_id(f.type_code);
            ft.offset = f.offset;
            ft.array_count = f.elem_count;
            ft.bit_number = f.bit_number;

            ft.field_name = mh::push_cstr_view(buffer, f.field_name.length + 1);
            mh::copy(f.field_name, ft.field_name);

            ut.fields.push_back(ft);
        }

        udt_types.push_back(ut);
        mem.udt_name_data.push_back(buffer);
    }


    static bool create_data_type_memory(DataTypeMemory& mem)
    {
        u64 str_bytes = 0;

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

        char string_data[100 + MAX_TAG_NAME_LENGTH] = { 0 }; // should be enough
    };


    static void init_controller(ControllerAttr& attr)
    {
        attr.connection_string = mh::to_string_view_unsafe(attr.string_data, (u32)sizeof(attr.string_data));

        mh::zero_string(attr.connection_string);
    }


    static void set_connection_string(ControllerAttr const& attr, cstr tag_name, int elem_size, int elem_count)
    {
        constexpr auto fmt =
            "protocol=ab-eip"
            "&plc=controllogix"
            "&gateway=%s"
            "&path=%s"
            "&name=%s"
            "&elem_size=%d"
            "&elem_count=%d";

        mh::zero_string(attr.connection_string);

        auto dst = attr.connection_string.char_data;
        auto max_len = (int)attr.connection_string.length;

        qsnprintf(dst, max_len, fmt, attr.gateway, attr.path, tag_name, elem_size, elem_count);
    }


    static void set_tag(ControllerAttr const& attr, Tag const& tag)
    {
        auto el_count = (int)tag.array_count;
        auto el_size = (int)(tag.size() / tag.array_count);

        set_connection_string(attr, tag.name(), el_size, el_count);
    }


    static void set_tag(ControllerAttr const& attr, cstr tag_name)
    {
        set_connection_string(attr, tag_name, 1, 1);
    }


    static bool connect_tag(ControllerAttr const& attr, Tag const& tag, TagConnection& conn)
    {
        set_tag(attr, tag);

        auto timeout = 100;

        auto rc = plc_tag_create(attr.connection_string.data(), timeout);
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

        rc = plc_tag_get_raw_bytes(tag_handle, 0, view.data, view.length);
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


    static bool scan_to_buffer(int tag_handle, ByteBuffer& dst)
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

        rc = plc_tag_get_raw_bytes(tag_handle, 0, view.data, view.length);
        if (rc != PLCTAG_STATUS_OK)
        {
            return false;
        }

        return true;
    }


    static bool scan_to_buffer(ControllerAttr const& attr, cstr tag_name, ByteBuffer& dst)
    {
        set_tag(attr, tag_name);

        auto timeout = 100;

        auto rc = plc_tag_create(attr.connection_string.data(), timeout);
        if (rc < 0)
        {
            return false;
        }

        auto handle = rc;

        return scan_to_buffer(handle, dst);
    }


    static bool scan_tag_entry_listing(ControllerAttr const& attr, ByteBuffer& dst)
    {
        return scan_to_buffer(attr, "@tags", dst);
    }


    static bool scan_udt_entry(ControllerAttr const& attr, int udt_id, ByteBuffer& dst)
    {
        char udt[20];
        qsnprintf(udt, 20, "@udt/%d", udt_id);

        return scan_to_buffer(attr, udt, dst);
    }

}


/* scan cycle */

namespace
{
    static bool enumerate_tags(ControllerAttr const& attr, TagMemory& tag_mem, DataTypeMemory& dt_mem, PlcTagData& data)
    {
        ByteBuffer entry_buffer;
        
        if (!scan_tag_entry_listing(attr, entry_buffer))
        {
            return false;
        }

        auto entry_data = mb::make_view(entry_buffer);

        auto tag_entries = parse_tag_entries(entry_data.data, entry_data.length);

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
            
            ByteBuffer udt_buffer;

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

        set_tag_data_type_names(data.tags, data.udt_types);
        set_udt_field_data_type_names(data.udt_types);

        return true;
    }


    static void connect_tags(ControllerAttr const& attr, TagMemory& mem, List<Tag>& tags)
    {
        assert(mem.n_tags == (u32)tags.size());

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
            auto rc = plc_tag_get_raw_bytes(conn.connection_handle, 0, view.data, view.length);
            conn.scan_ok = rc == PLCTAG_STATUS_OK;
        }
    }


    static void copy_tags(TagMemory& mem)
    {
        auto src = mb::make_read_view(mem.scan_data);
        auto dst = mb::make_view(mem.public_tag_data);

        mh::copy(src, dst);
    }
  
}


/* api */

namespace plcscan
{
    static DataTypeMemory g_dt_mem;
    static TagMemory g_tag_mem;
    static ControllerAttr g_attr;


    void shutdown()
    {
        destroy_data_type_memory(g_dt_mem);
        destroy_tag_memory(g_tag_mem);
        plc_tag_shutdown();
    }


    PlcTagData init()
    {
        PlcTagData data{};

        if (!create_data_type_memory(g_dt_mem))
        {
            shutdown();
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
            shutdown();
            return false;
        }   

        g_attr.gateway = gateway;
        g_attr.path = path;

        init_controller(g_attr);

        if (!enumerate_tags(g_attr, g_tag_mem, g_dt_mem, data))
        {
            return false;
        }

        connect_tags(g_attr, g_tag_mem, data.tags);        

        data.is_connected = true;
        return true;
    }


    TagType get_tag_type(DataTypeId32 type_id)
    {
        if (id32::is_udt_type(type_id))
        {
            return TagType::UDT;
        }

        if (type_id >= (DataTypeId32)FixedType::BOOL && type_id <= (DataTypeId32)FixedType::LREAL)
        {
            auto offset = (u32)type_id - (u32)FixedType::BOOL;

            return (TagType)((u32)TagType::BOOL + offset);
        }        

        for (auto t : STRING_FIXED_TYPES)
        {
            if (type_id == (DataTypeId32)t)
            {
                return TagType::STRING;
            }
        }

        return TagType::MISC;
    }
    
    
    void scan(data_f const& scan_cb, bool_f const& scan_condition, PlcTagData& data)
    {
        constexpr int target_scan_ms = 100;

        auto const scan = [&]()
        { 
            scan_tags(g_tag_mem);
        };

        Stopwatch sw;

        while(scan_condition())
        {
            sw.start();

            // TODO: better parallelism
            std::thread scan_th(scan);
            copy_tags(g_tag_mem); 
            scan_cb(data);

            scan_th.join();

            mb::flip_read_write(g_tag_mem.scan_data);

            tmh::delay_current_thread_ms(sw, target_scan_ms);
        }
    }    
}


#ifdef DEVPLCTAG

#include "../dev/devplctag.Cpp"

#endif

/*
MIT License

Copyright (c) 2023 Adam Lafontaine

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/