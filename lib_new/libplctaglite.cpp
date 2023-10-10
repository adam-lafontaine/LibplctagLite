#include "../util/types.hpp"
#include "../util/memory_buffer.hpp"

#include <vector>
#include <array>
#include <cassert>

namespace mb = memory_buffer;

using Bytes = MemoryView<u8>;
using String = MemoryView<char>;


template <typename T>
static void destroy_vector(std::vector<T>& vec)
{
    std::vector<T> temp;
    std::swap(vec, temp);
}



/* tag types */

namespace
{
    constexpr auto TYPE_IS_STRUCT = (u16)0x8000;   // 0b1000'0000'0000'0000
    constexpr auto TYPE_IS_SYSTEM = (u16)0x1000;   // 0b0001'0000'0000'0000

    constexpr auto TAG_DIM_MASK = (u16)0x6000;     // 0b0110'0000'0000'0000

    constexpr auto UDT_FIELD_IS_ARRAY = (u16)0x2000; // 0b0010'0000'0000'0000

    constexpr auto ATOMIC_TYPE_ID_MASK = (u16)0x00FF; // 0b0000'0000'1111'1111
    constexpr auto ATOMIC_TYPE_ID_MIN = (u16)0xC1;    // 0b0000'0000'1100'0001
    constexpr auto ATOMIC_TYPE_ID_MAX = (u16)0xDE;    // 0b0000'0000'1101'1110

    constexpr auto UDT_TYPE_ID_MASK = (u16)0x0FFF; // 0b0000'1111'1111'1111

    

    using DataTypeId32 = u32;

    // 0b0000'0000'0000'0000'0000'0000'0000'0000
    //  |----other-----|------udt-----|-atomic--|

    constexpr auto SYSTEM_TYPE_ID = (DataTypeId32)0b0000'0000'0001'0000'0000'0000'0000'0000;
    constexpr auto UNKOWN_TYPE_ID = (DataTypeId32)0b0000'0000'0010'0000'0000'0000'0000'0000;

    enum class AtomicType : DataTypeId32
    {
        UNKNOWN = UNKOWN_TYPE_ID,
        SYSTEM  = SYSTEM_TYPE_ID,

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


    constexpr std::array<AtomicType, 16> SUPPORTED_TYPES = 
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
        AtomicType::DATE,
        AtomicType::TIME,
        AtomicType::CHAR_STRING,

        AtomicType::SYSTEM,
        AtomicType::UNKNOWN,
    };



    static inline u16 get_tag_dimensions(u16 type_code)
    {
        return (u16)((type_code & TAG_DIM_MASK) >> 13);
    }


    static inline u16 get_udt_id(u16 type_code)
    {
        return type_code & UDT_TYPE_ID_MASK;
    }


    static inline bool is_bit_field(u16 type_code)
    {
        return (AtomicType)(type_code & ATOMIC_TYPE_ID_MASK) == AtomicType::BOOL;
    }


    static inline bool is_array_field(u16 type_code)
    {
        return type_code & UDT_FIELD_IS_ARRAY;
    }


    static DataTypeId32 get_data_type_id(u16 type_code)
    {
        if (type_code & TYPE_IS_SYSTEM)
        {
            return SYSTEM_TYPE_ID;
        }
        else if (type_code & TYPE_IS_STRUCT)
        {
            // shift left 8 to prevent conflicts with atomic types
            return (DataTypeId32)(type_code & UDT_TYPE_ID_MASK) << 8;
        }

        u16 atomic_type = type_code & ATOMIC_TYPE_ID_MASK;

        if (atomic_type >= ATOMIC_TYPE_ID_MIN && atomic_type <= ATOMIC_TYPE_ID_MAX)
        {
            return (DataTypeId32)atomic_type;
        }

        return UNKOWN_TYPE_ID;
    }


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


    
    

    class TagType
    {
    public:
        String data_type_name;
        String data_type_description;
        u32 size;
    };


    class DataTypeTable
    {
    public:
        MemoryBuffer<char> str_data;
    };
    

    
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

        class Header
        {
        public:
            uint32_t instance_id;
            uint16_t symbol_type;
            uint16_t element_length;
            uint32_t array_dims[3];
            uint16_t string_len;
        };

        auto& h = *(Header*)(entry_data);
        
        TagEntry entry{};

        entry.type_code = h.symbol_type;

        entry.elem_count = 1;

        auto n_dims = get_tag_dimensions(entry.type_code);

        for (u32 i = 0; i < n_dims; ++i)
        {
            if (h.array_dims[i])
            {
                entry.elem_count *= h.array_dims[i];
            }
        }

        int offset = 14;

        entry.name.begin = (char*)(entry_data + offset);
        entry.name.length = h.string_len;

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

        DataTypeId32 type_id = UNKOWN_TYPE_ID;

        Bytes value;
        String name;
    };


    class TagTable
    {
    public:
        int plc = 0; // TODO

        std::vector<Tag> tags;

        MemoryBuffer<u8> value_data;
        MemoryBuffer<char> name_data;
    };


    static void destroy_tag_table(TagTable& table)
    {
        mb::destroy_buffer(table.value_data);
        mb::destroy_buffer(table.name_data);

        destroy_vector(table.tags);
    }    


    u32 elem_size(TagEntry const& e) { return e.elem_count * e.elem_size; }


    u32 str_size(TagEntry const& e) { return e.name.length + 1; /* zero terminated */}


    static void add_tag(TagTable& table, TagEntry const& entry)
    {
        auto value_len = elem_size(entry);
        auto name_alloc_len = str_size(entry);
        auto name_copy_len = entry.name.length;

        assert(name_alloc_len > name_copy_len); /* zero terminated */

        auto value_data = mb::push_elements(table.value_data, value_len);
        if (!value_data)
        {
            return;
        }

        auto str_data = mb::push_elements(table.name_data, name_alloc_len);
        if (!str_data)
        {
            mb::pop_elements(table.value_data, value_len);
            return;
        }

        Tag tag{};

        tag.type_id = get_data_type_id(entry.type_code);
        tag.value.begin = value_data;
        tag.value.length = value_len;
        
        for (u32 i = 0; i < name_copy_len; ++i)
        {
            str_data[i] = tag.name.begin[i];
        }

        tag.name.begin = str_data;
        tag.name.length = name_copy_len;

        table.tags.push_back(tag);
    }


    static bool create_tag_table(TagEntryList const& entries, TagTable& table)
    {
        u32 value_bytes = 0;
        u32 str_bytes = 0;
        for (auto const& e : entries)
        {
            value_bytes += elem_size(e);
            str_bytes += str_size(e);
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

        mb::zero_buffer(table.name_data);
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
    class UdtEntry
    {
    public:
        DataTypeId32 type_id = UNKOWN_TYPE_ID;

    };


    static void append_udt(u8* udt_data)
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

        class Header
        {
        public:
            u16 udt_id = 0;
            u32 desc = 0;
            u32 udt_size = 0;
            u16 n_fields = 0;
        };

        auto& h = *(Header*)(udt_data);

        UdtEntry entry{};
        entry.type_id = get_data_type_id(h.udt_id);

        //entry

        int offset = 14;


    }
}