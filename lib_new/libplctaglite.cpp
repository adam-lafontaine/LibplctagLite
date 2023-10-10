#include "../util/types.hpp"
#include "../util/memory_buffer.hpp"

#include <vector>

namespace mb = memory_buffer;

using Bytes = MemoryView<u8>;
using String = MemoryView<char>;


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


    static int append_tag_entry(TagEntryList& entries, u8* entry_data, int offset)
    {
        /* each entry looks like this:
        uint32_t instance_id    monotonically increasing but not contiguous
        uint16_t symbol_type    type of the symbol.
        uint16_t element_length length of one array element in bytes.
        uint32_t array_dims[3]  array dimensions.
        uint16_t string_len
        uint8_t string_data[]   string bytes (string_len of them)
        */

        class C
        {
        public:
            uint32_t instance_id;
            uint16_t symbol_type;
            uint16_t element_length;
            uint32_t array_dims[3];
            uint16_t string_len;
        };

        auto& c = *(C*)(entry_data + offset);
        
        TagEntry entry{};

        entry.type_code = c.symbol_type;
        entry.elem_count = c.array_dims[0];

        if (c.array_dims[1])
        {
            entry.elem_count *= c.array_dims[1];
        }

        if (c.array_dims[2])
        {
            entry.elem_count *= c.array_dims[2];
        }

        offset += sizeof(C);

        entry.name.begin = (char*)(entry_data + offset);
        entry.name.length = c.string_len;

        entries.push_back(entry);

        offset += c.string_len;

        return offset;
    }


    static TagEntryList parse_tag_entries(u8* entry_data, u32 data_size)
    {
        TagEntryList list;

        int offset = 0;

        while (offset < data_size)
        {
            offset = append_tag_entry(list, entry_data, offset);
        }

        return list;
    }
}


/* tag types */

namespace
{
    constexpr auto TYPE_IS_STRUCT = (u16)0x8000;
    constexpr auto TYPE_IS_SYSTEM = (u16)0x1000;
    constexpr auto ATOMIC_TYPE_MASK = (u16)0x00FF;

    constexpr auto TYPE_UDT_ID_MASK = (u16)0x0FFF;
    constexpr auto TAG_DIM_MASK = (u16)0x6000;

    constexpr auto UDT_ARRAY_FIELD_MASK = (u16)0x2000;


    enum class TagType : int
    {
        SYSTEM = 0,
        UDT,
        BOOL,
        SINT,
        INT,
        DINT,
        LINT,
        USINT,
        UINT,
        UDINT,
        ULINT,
        REAL,
        LREAL,
        SYNCHRONOUS_TIME,
        DATE,
        TIME,
        DATETIME,
        CHAR_STRING,
        STRING_8,
        STRING_16,
        STRING_32,
        STRING_64,
        WIDE_STRING,
        HIGH_RES_DURATION,
        MED_RES_DURATION,
        LOW_RES_DURATION,
        N_BYTE_STRING,
        COUNTED_CHAR_STRING,
        DURATION_MS,
        CIP_PATH,
        ENGINEERING_UNITS,
        INTERNATIONAL_STRING,
        UNKNOWN
    };


    cstr decode_tag_type(TagType t)
    {
        switch (t)
        {
        case TagType::SYSTEM:               return "System";
        case TagType::UDT:                  return "UDT: User defined type";
        case TagType::BOOL:                 return "BOOL: Boolean value";
        case TagType::SINT:                 return "SINT: Signed 8-bit integer value";
        case TagType::INT:                  return "INT: Signed 16-bit integer value";
        case TagType::DINT:                 return "DINT: Signed 32-bit integer value";
        case TagType::LINT:                 return "LINT: Signed 64-bit integer value";
        case TagType::USINT:                return "USINT: Unsigned 8-bit integer value";
        case TagType::UINT:                 return "UINT: Unsigned 16-bit integer value";
        case TagType::UDINT:                return "UDINT: Unsigned 32-bit integer value";
        case TagType::ULINT:                return "ULINT: Unsigned 64-bit integer value";
        case TagType::REAL:                 return "REAL: 32-bit floating point value, IEEE format";
        case TagType::LREAL:                return "LREAL: 64-bit floating point value, IEEE format";
        case TagType::SYNCHRONOUS_TIME:     return "Synchronous time value";
        case TagType::DATE:                 return "Date value";
        case TagType::TIME:                 return "Time of day value";
        case TagType::DATETIME:             return "Date and time of day value";
        case TagType::CHAR_STRING:          return "Character string, 1 byte per character";
        case TagType::STRING_8:             return "8-bit bit string";
        case TagType::STRING_16:            return "16-bit bit string";
        case TagType::STRING_32:            return "32-bit bit string";
        case TagType::STRING_64:            return "64-bit bit string";
        case TagType::WIDE_STRING:          return "Wide char character string, 2 bytes per character";
        case TagType::HIGH_RES_DURATION:    return "High resolution duration value";
        case TagType::MED_RES_DURATION:     return "Medium resolution duration value";
        case TagType::LOW_RES_DURATION:     return "Low resolution duration value";
        case TagType::N_BYTE_STRING:        return "N-byte per char character string";
        case TagType::COUNTED_CHAR_STRING:  return "Counted character sting with 1 byte per character and 1 byte length indicator";
        case TagType::DURATION_MS:          return "Duration in milliseconds";
        case TagType::CIP_PATH:             return "CIP path segment(s)";
        case TagType::ENGINEERING_UNITS:    return "Engineering units";
        case TagType::INTERNATIONAL_STRING: return "International character string (encoding?)";
        default:                            return "unknown";
        }
    }


    cstr decode_tag_type_short(TagType t)
    {
        switch (t)
        {
        case TagType::UDT:   return "UDT";
        case TagType::BOOL:  return "BOOL";
        case TagType::SINT:  return "SINT";
        case TagType::INT:   return "INT";
        case TagType::DINT:  return "DINT";
        case TagType::LINT:  return "LINT";
        case TagType::USINT: return "USINT";
        case TagType::UINT:  return "UINT";
        case TagType::UDINT: return "UDINT";
        case TagType::ULINT: return "ULINT";
        case TagType::REAL:  return "REAL";
        case TagType::LREAL: return "LREAL";
        default:             return decode_tag_type(t);
        }
    }


    static TagType get_tag_type(uint16_t type_code)
    {
        if (type_code & TYPE_IS_SYSTEM)
        {
            return TagType::SYSTEM;
        }
        else if (type_code & TYPE_IS_STRUCT)
        {
            return TagType::UDT;
        }

        uint16_t atomic_type = type_code & ATOMIC_TYPE_MASK;
        const char* type = NULL;

        switch (atomic_type)
        {
        case 0xC1: return TagType::BOOL;
        case 0xC2: return TagType::SINT;
        case 0xC3: return TagType::INT;
        case 0xC4: return TagType::DINT;
        case 0xC5: return TagType::LINT;
        case 0xC6: return TagType::USINT;
        case 0xC7: return TagType::UINT;
        case 0xC8: return TagType::UDINT;
        case 0xC9: return TagType::ULINT;
        case 0xCA: return TagType::REAL;
        case 0xCB: return TagType::LREAL;
        case 0xCC: return TagType::SYNCHRONOUS_TIME;
        case 0xCD: return TagType::DATE;
        case 0xCE: return TagType::TIME;
        case 0xCF: return TagType::DATE;
        case 0xD0: return TagType::CHAR_STRING;
        case 0xD1: return TagType::STRING_8;
        case 0xD2: return TagType::STRING_16;
        case 0xD3: return TagType::STRING_32;
        case 0xD4: return TagType::STRING_64;
        case 0xD5: return TagType::WIDE_STRING;
        case 0xD6: return TagType::HIGH_RES_DURATION;
        case 0xD7: return TagType::MED_RES_DURATION;
        case 0xD8: return TagType::LOW_RES_DURATION;
        case 0xD9: return TagType::N_BYTE_STRING;
        case 0xDA: return TagType::COUNTED_CHAR_STRING;
        case 0xDB: return TagType::DURATION_MS;
        case 0xDC: return TagType::CIP_PATH;
        case 0xDD: return TagType::ENGINEERING_UNITS;
        case 0xDE: return TagType::INTERNATIONAL_STRING;
        }

        return TagType::UNKNOWN;
    }
    

    static inline u16 get_tag_dimensions(u16 type_code)
    {
        return (u16)((type_code & TAG_DIM_MASK) >> 13);
    }


    static inline u16 get_udt_id(u16 type_code)
    {
        return type_code & TYPE_UDT_ID_MASK;
    }


    static inline bool is_bit_field(u16 type_code)
    {
        return get_tag_type(type_code) == TagType::BOOL;
    }


    static inline bool is_array_field(u16 type_code)
    {
        return type_code & UDT_ARRAY_FIELD_MASK;
    }



    class TypeTable
    {
    public:

    };
}


/* tag table */

namespace
{

    class Tag
    {
    public:
        int tag_id = -1;

        Bytes value;
        String name;
    };


    class TagQty
    {
    public:
        u32 n_tags = 0;
        u32 value_size = 0;
        u32 name_size = 0;
    };


    class TagTable
    {
    public:
        MemoryBuffer<Tag> tags;

        MemoryBuffer<u8> value_data;
        MemoryBuffer<char> name_data;
    };


    static void destroy_tag_table(TagTable& table)
    {
        mb::destroy_buffer(table.value_data);
        mb::destroy_buffer(table.name_data);

        mb::destroy_buffer(table.tags);
    }


    static DataResult<TagTable> create_tag_table(TagQty const& qty)
    {
        DataResult<TagTable> result{};
        auto& table = result.data;

        if (!mb::create_buffer(table.tags, qty.n_tags))
        {
            destroy_tag_table(table);
            return result;
        }

        if (!mb::create_buffer(table.value_data, qty.value_size))
        {
            destroy_tag_table(table);
            return result;
        }

        if (!mb::create_buffer(table.name_data, qty.name_size))
        {
            destroy_tag_table(table);
            return result;
        }

        mb::zero_buffer(table.name_data);
        mb::zero_buffer(table.name_data);

        result.success = true;
        return result;
    }


    static void add_tag(TagTable& table, Tag const& tag)
    {

    }
}


