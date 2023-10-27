#include "devplctag.hpp"

#include "../util/mh_types.hpp"


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
}


/* api */

namespace dev
{
    int plc_tag_create(const char* attr, int timeout)
    {
        return -1;
    }


    int plc_tag_read(int handle, int timeout)
    {
        return -1;
    }


    int plc_tag_get_size(int handle)
    {
        return -1;
    }


    int plc_tag_get_raw_bytes(int handle, int offset, unsigned char* dst, int length)
    {
        return -1;
    }


    void plc_tag_shutdown()
    {
        
    }
}