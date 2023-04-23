#include "../../lib/plctag.hpp"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

namespace plc = plctag;


#define TAG_STRING_SIZE (200)
#define TAG_STRING_TEMPLATE "protocol=ab-eip&gateway=%s&path=%s&plc=ControlLogix&name="
#define TIMEOUT_MS 5000


#define TYPE_IS_STRUCT ((uint16_t)0x8000)
#define TYPE_IS_SYSTEM ((uint16_t)0x1000)
#define TYPE_DIM_MASK ((uint16_t)0x6000)
#define TYPE_UDT_ID_MASK ((uint16_t)0x0FFF)
#define TAG_DIM_MASK ((uint16_t)0x6000)


#define MAX_UDTS (1 << 12)





struct program_entry_s {
    struct program_entry_s* next;
    char* program_name;
};

struct tag_entry_s {
    struct tag_entry_s* next;
    char* name;
    struct tag_entry_s* parent;
    uint16_t type;
    uint16_t elem_size;
    uint16_t elem_count;
    uint16_t num_dimensions;
    uint16_t dimensions[3];
};


struct udt_field_entry_s {
    char* name;
    uint16_t type;
    uint16_t metadata;
    uint32_t size;
    uint32_t offset;
};


struct udt_entry_s {
    char* name;
    uint16_t id;
    uint16_t num_fields;
    uint16_t struct_handle;
    uint32_t instance_size;
    struct udt_field_entry_s fields[];
};


static void usage(void);
static char* setup_tag_string(int argc, char** argv);
static plc::ConnectResult open_tag(const char* base, const char* tag_name);
static bool get_tag_list(plc::TagDesc const& tag, struct tag_entry_s** tag_list, struct tag_entry_s* parent);
static void print_element_type(uint16_t element_type);
static bool process_tag_entry(int32_t tag, int* offset, uint16_t* last_tag_id, struct tag_entry_s** tag_list, struct tag_entry_s* parent);
static bool get_udt_definition(const char* base, uint16_t udt_id);


/* a local cache of all found UDT definitions. */
static struct udt_entry_s* udts[MAX_UDTS] = { NULL };
static uint16_t udts_to_process[MAX_UDTS] = { 0 };
static int last_udt = 0;
static int current_udt = 0;


int main(int argc, char** argv)
{
    
    char* host = NULL;
    char* path = NULL;
    char* tag_string_base = NULL;
    int32_t controller_listing_tag = 0;
    int32_t program_listing_tag = 0;
    struct tag_entry_s* tag_list = NULL;

    /* clear the UDTs. */
    for (int index = 0; index < MAX_UDTS; index++) {
        udts[index] = NULL;
    }


    tag_string_base = setup_tag_string(argc, argv);
    if (tag_string_base == NULL) {
        fprintf(stderr, "Unable to set up the tag string base!\n");
        usage();
    }

    /* this means that the args are right. */
    host = argv[1];
    path = argv[2];

    /* set up the tag for the listing first. */
    auto controller_result = open_tag(tag_string_base, "@tags");
    if (!controller_result.is_ok())
    {
        fprintf(stderr, "Unable to create listing tag, error %s!\n", controller_result.error);
        usage();
    }

    controller_listing_tag = controller_result.data.tag_handle;

    /* get the list of controller tags. */
    auto rc = get_tag_list(controller_result.data, &tag_list, NULL);
    if (!rc) {
        fprintf(stderr, "Unable to get tag list or no tags visible in the target PLC!\n");
        usage();
    }

    fprintf(stderr, "Controller tag listing tag ID: %d\n", controller_listing_tag);

    /*
     * now loop through the tags and get the list for the program tags.
     *
     * This is safe because we push the new tags on the front of the list and
     * so do not change any existing tag in the list.
     */
    for (struct tag_entry_s* entry = tag_list; entry; entry = entry->next) {
        if (strncmp(entry->name, "Program:", strlen("Program:")) == 0) {
            char buf[256] = { 0 };

            /* this is a program tag, check for its tags. */
            fprintf(stderr, "Getting tags for program \"%s\".\n", entry->name);

            snprintf(buf, sizeof(buf), "%s.@tags", entry->name);

            auto program_result = open_tag(tag_string_base, buf);
            if (program_result.is_error())
            {
                fprintf(stderr, "Unable to create listing tag, error %s!\n", program_result.error);
                usage();
            }

            program_listing_tag = program_result.data.tag_handle;

            fprintf(stderr, "Program tag listing tag ID: %d\n", program_listing_tag);

            rc = get_tag_list(program_result.data, &tag_list, entry);
            if (!rc) {
                fprintf(stderr, "Unable to get program tag list or no tags visible in the target PLC!\n");
                usage();
            }

            plc::destroy(program_listing_tag);
            fprintf(stderr, "Destroying program tag listing tag ID: %d\n", program_listing_tag);
        }
    }

    /* loop through the tags and get the UDT information. */
    for (struct tag_entry_s* entry = tag_list; entry; entry = entry->next) {
        /* check the type of the tag's element type. */
        uint16_t element_type = entry->type;

        /* if this is a structure, make sure we have the definition. */
        if ((element_type & TYPE_IS_STRUCT) && !(element_type & TYPE_IS_SYSTEM)) {
            uint16_t udt_id = element_type & TYPE_UDT_ID_MASK;

            udts_to_process[last_udt] = udt_id;
            last_udt++;

            if (last_udt >= MAX_UDTS) {
                plc::destroy(controller_listing_tag);
                fprintf(stderr, "More than %d UDTs are requested!\n", MAX_UDTS);
                usage();
            }
        }
    }

    /* get all the UDTs that we have touched. Note that this can add UDTs to the stack to process! */
    while (current_udt < last_udt) {
        uint16_t udt_id = udts_to_process[current_udt];

        /* see if we already have it. */
        if (udts[udt_id] == NULL) {
            rc = get_udt_definition(tag_string_base, udt_id);
            if (!rc) {
                plc::destroy(controller_listing_tag);
                fprintf(stderr, "Unable to get UDT template ID %u!\n", (unsigned int)(udt_id));
                usage();
            }
        }
        else {
            fprintf(stderr, "Already have UDT (%04x) %s.\n", (unsigned int)udt_id, udts[udt_id]->name);
        }

        current_udt++;
    }


    /* output all the tags. */
    for (struct tag_entry_s* tag = tag_list; tag; tag = tag->next) {
        if (!tag->parent) {
            printf("Tag \"%s", tag->name);
        }
        else {
            printf("Tag \"%s.%s", tag->parent->name, tag->name);
        }

        switch (tag->num_dimensions) {
        case 1:
            printf("[%d]", tag->dimensions[0]);
            break;

        case 2:
            printf("[%d,%d]", tag->dimensions[0], tag->dimensions[1]);
            break;

        case 3:
            printf("[%d,%d,%d]", tag->dimensions[0], tag->dimensions[1], tag->dimensions[2]);
            break;

        default:
            break;
        }

        /* handle the type. */
        printf("\" ");
        print_element_type(tag->type);
        printf(".  ");

        /* print the tag string */
        if (!tag->parent) {
            printf("tag string = \"protocol=ab-eip&gateway=%s&path=%s&plc=ControlLogix&elem_size=%u&elem_count=%u&name=%s\"\n", host, path, tag->elem_size, tag->elem_count, tag->name);
        }
        else {
            printf("tag string = \"protocol=ab-eip&gateway=%s&path=%s&plc=ControlLogix&elem_size=%u&elem_count=%u&name=%s.%s\"\n", host, path, tag->elem_size, tag->elem_count, tag->parent->name, tag->name);
        }
    }

    printf("UDTs:\n");

    /* print out all the UDTs */
    for (int index = 0; index < MAX_UDTS; index++) {
        struct udt_entry_s* udt = udts[index];

        if (udt) {
            if (udt->name) {
                printf(" UDT %s (ID %x, %d bytes, struct handle %x):\n", udt->name, (unsigned int)(udt->id), (int)(unsigned int)udt->instance_size, (int)(unsigned int)udt->struct_handle);
            }
            else {
                printf(" UDT *UNNAMED* (ID %x, %d bytes, struct handle %x):\n", (unsigned int)(udt->id), (int)(unsigned int)udt->instance_size, (int)(unsigned int)udt->struct_handle);
            }
            for (int field_index = 0; field_index < udt->num_fields; field_index++) {
                if (udt->fields[field_index].name) {
                    printf("    Field %d: %s, offset %d", field_index, udt->fields[field_index].name, udt->fields[field_index].offset);
                }
                else {
                    printf("    Field %d: *UNNAMED*, offset %d", field_index, udt->fields[field_index].offset);
                }

                /* is it a bit? */
                if (udt->fields[field_index].type == 0xC1) {
                    /* bit type, the metadata is the bit number. */
                    printf(":%d", (int)(unsigned int)(udt->fields[field_index].metadata));
                }

                /* is it an array? */
                if (udt->fields[field_index].type & 0x2000) { /* MAGIC */
                    printf(", array [%d] of type ", (int)(unsigned int)(udt->fields[field_index].metadata));
                }
                else {
                    printf(", type ");
                }

                print_element_type(udt->fields[field_index].type);

                printf(".\n");
            }
        }
    }

    /* clean up memory */
    while (tag_list) {
        struct tag_entry_s* tag = tag_list;

        /* unlink */
        tag_list = tag_list->next;

        if (tag->name) {
            free(tag->name);
            tag->name = NULL;
        }

        free(tag);
    }

    for (int index = 0; index < MAX_UDTS; index++) {
        struct udt_entry_s* udt = udts[index];

        if (udt) {
            if (udt->name) {
                free(udt->name);
            }

            for (int field_index = 0; field_index < udt->num_fields; field_index++) {
                struct udt_field_entry_s* field = &(udt->fields[field_index]);

                if (field->name) {
                    free(field->name);
                }
            }

            free(udt);

            udts[index] = NULL;
        }
    }

    free(tag_string_base);

    /* Destroy this at the end to keep the session open. */
    plc::destroy(controller_listing_tag);

    printf("SUCCESS!\n");

    return 0;
}


void usage()
{
    fprintf(stderr, "Usage: list_tags <PLC IP> <PLC path>\nExample: list_tags 10.1.2.3 1,0\n");
    exit(1);
}

char* setup_tag_string(int argc, char** argv)
{
    char tag_string[TAG_STRING_SIZE + 1] = { 0 };
    const char* gateway = NULL;
    const char* path = NULL;

    if (argc < 3) {
        usage();
    }

    if (!argv[1] || strlen(argv[1]) == 0) {
        fprintf(stderr, "Hostname or IP address must not be zero length!\n");
        usage();
    }

    gateway = argv[1];

    if (!argv[2] || strlen(argv[2]) == 0) {
        fprintf(stderr, "PLC path must not be zero length!\n");
        usage();
    }

    path = argv[2];

    /* build the tag string. */
    snprintf(tag_string, TAG_STRING_SIZE, TAG_STRING_TEMPLATE, gateway, path);

    /* FIXME - check size! */
    fprintf(stderr, "Using tag string \"%s\".\n", tag_string);

    return _strdup(tag_string);
}



plc::ConnectResult open_tag(const char* base, const char* tag_name)
{
    
    int32_t tag = 0;
    char tag_string[TAG_STRING_SIZE + 1] = { 0, };

    /* build the tag string. */
    strncpy_s(tag_string, base, TAG_STRING_SIZE);

    strncat_s(tag_string, tag_name, TAG_STRING_SIZE);

    fprintf(stderr, "Using tag string \"%s\".\n", tag_string);

    auto result = plc::connect(tag_string, TIMEOUT_MS);

    if (result.is_error())
    {
        fprintf(stderr, "Unable to open tag!  Return code %s\n", result.error);
        usage();
    }

    return result;
}


bool get_tag_list(plc::TagDesc const& tag, struct tag_entry_s** tag_list, struct tag_entry_s* parent)
{
    uint16_t last_tag_entry_id = 0;
    int payload_size = 0;
    int offset = 0;

    /* go get it. */
    auto result = plc::receive(tag.tag_handle, TIMEOUT_MS);
    if (result.is_error())
    {
        fprintf(stderr, "Error %s trying to send CIP request!\n", result.error);
        usage();
    }

    fprintf(stderr, "Listing tag read, result of size %d.\n", payload_size);

    /* check the payload size. */
    if (payload_size < 4) {
        fprintf(stderr, "Unexpectedly small payload size %d!\n", payload_size);
        usage();
    }

    /* process each entry */
    bool rc = false;
    do {
        rc = process_tag_entry(tag.tag_handle, &offset, &last_tag_entry_id, tag_list, parent);
    } while (rc && offset < payload_size);

    return true;
}




bool process_tag_entry(int32_t tag, int* offset, uint16_t* last_tag_id, struct tag_entry_s** tag_list, struct tag_entry_s* parent)
{
    uint16_t tag_type = 0;
    uint16_t element_length = 0;
    uint16_t array_dims[3] = { 0, };
    int tag_name_len = 0;
    char* tag_name = NULL;
    struct tag_entry_s* tag_entry = NULL;

    /* each entry looks like this:
        uint32_t instance_id    monotonically increasing but not contiguous
        uint16_t symbol_type    type of the symbol.
        uint16_t element_length length of one array element in bytes.
        uint32_t array_dims[3]  array dimensions.
        uint16_t string_len     string length count.
        uint8_t string_data[]   string bytes (string_len of them)
    */

    * last_tag_id = (uint16_t)plc::get_u32(tag, *offset).data;
    *offset += 4;

    tag_type = plc::get_u16(tag, *offset).data;
    *offset += 2;

    element_length = plc::get_u16(tag, *offset).data;
    *offset += 2;

    array_dims[0] = (uint16_t)plc::get_u32(tag, *offset).data;
    *offset += 4;
    array_dims[1] = (uint16_t)plc::get_u32(tag, *offset).data;
    *offset += 4;
    array_dims[2] = (uint16_t)plc::get_u32(tag, *offset).data;
    *offset += 4;

    /* get the tag name length. */
    tag_name_len = plc::get_string_length(tag, *offset).data;
    // *offset += 2;

    /* allocate space for the the tag name.  Add one for the zero termination. */
    tag_name = (char*)calloc((size_t)(unsigned int)(tag_name_len + 1), 1);
    if (!tag_name) {
        fprintf(stderr, "Unable to allocate memory for the tag name!\n");
        return false;
    }

    auto str_result =  plc::get_string(tag, *offset, tag_name, tag_name_len + 1);
    if (!str_result.is_ok()) {
        fprintf(stderr, "Unable to get tag name string, error %s!\n", str_result.error);
        free(tag_name);
        return false;
    }

    /* skip past the string. */
    (*offset) += plc::get_string_total_length(tag, *offset).data;

    fprintf(stderr, "Tag %s, string length: %d.\n", tag_name, (int)(unsigned int)strlen(tag_name));

    /* allocate the new tag entry. */
    tag_entry = (tag_entry_s*)calloc(1, sizeof(*tag_entry));

    if (!tag_entry) {
        fprintf(stderr, "Unable to allocate memory for tag entry!\n");
        free(tag_name);
        return false;
    }

    fprintf(stderr, "Found tag name=%s, tag instance ID=%x, tag type=%x, element length (in bytes) = %d, array dimensions = (%d, %d, %d)\n", tag_name, *last_tag_id, tag_type, (int)element_length, (int)array_dims[0], (int)array_dims[1], (int)array_dims[2]);

    /* fill in the fields. */
    tag_entry->name = tag_name;
    tag_entry->parent = parent;
    tag_entry->type = tag_type;
    tag_entry->elem_size = element_length;
    tag_entry->num_dimensions = (uint16_t)((tag_type & TAG_DIM_MASK) >> 13);
    tag_entry->dimensions[0] = array_dims[0];
    tag_entry->dimensions[1] = array_dims[1];
    tag_entry->dimensions[2] = array_dims[2];

    /* calculate the element count. */
    tag_entry->elem_count = 1;
    for (uint16_t i = 0; i < tag_entry->num_dimensions; i++) {
        tag_entry->elem_count = (uint16_t)((uint16_t)tag_entry->elem_count * (uint16_t)(tag_entry->dimensions[i]));
    }

    /* link it up to the list */
    tag_entry->next = *tag_list;
    *tag_list = tag_entry;

    return true;
}



void print_element_type(uint16_t element_type)
{
    if (element_type & TYPE_IS_SYSTEM) {
        printf("element type SYSTEM %04x", (unsigned int)(element_type));
    }
    else if (element_type & TYPE_IS_STRUCT) {
        printf("element type UDT (0x%04x) %s", (unsigned int)(element_type), udts[(size_t)(unsigned int)(element_type & TYPE_UDT_ID_MASK)]->name);
    }
    else {
        uint16_t atomic_type = element_type & 0xFF; /* MAGIC */
        const char* type = NULL;

        switch (atomic_type) {
        case 0xC1: type = "BOOL: Boolean value"; break;
        case 0xC2: type = "SINT: Signed 8-bit integer value"; break;
        case 0xC3: type = "INT: Signed 16-bit integer value"; break;
        case 0xC4: type = "DINT: Signed 32-bit integer value"; break;
        case 0xC5: type = "LINT: Signed 64-bit integer value"; break;
        case 0xC6: type = "USINT: Unsigned 8-bit integer value"; break;
        case 0xC7: type = "UINT: Unsigned 16-bit integer value"; break;
        case 0xC8: type = "UDINT: Unsigned 32-bit integer value"; break;
        case 0xC9: type = "ULINT: Unsigned 64-bit integer value"; break;
        case 0xCA: type = "REAL: 32-bit floating point value, IEEE format"; break;
        case 0xCB: type = "LREAL: 64-bit floating point value, IEEE format"; break;
        case 0xCC: type = "Synchronous time value"; break;
        case 0xCD: type = "Date value"; break;
        case 0xCE: type = "Time of day value"; break;
        case 0xCF: type = "Date and time of day value"; break;
        case 0xD0: type = "Character string, 1 byte per character"; break;
        case 0xD1: type = "8-bit bit string"; break;
        case 0xD2: type = "16-bit bit string"; break;
        case 0xD3: type = "32-bit bit string"; break;
        case 0xD4: type = "64-bit bit string"; break;
        case 0xD5: type = "Wide char character string, 2 bytes per character"; break;
        case 0xD6: type = "High resolution duration value"; break;
        case 0xD7: type = "Medium resolution duration value"; break;
        case 0xD8: type = "Low resolution duration value"; break;
        case 0xD9: type = "N-byte per char character string"; break;
        case 0xDA: type = "Counted character sting with 1 byte per character and 1 byte length indicator"; break;
        case 0xDB: type = "Duration in milliseconds"; break;
        case 0xDC: type = "CIP path segment(s)"; break;
        case 0xDD: type = "Engineering units"; break;
        case 0xDE: type = "International character string (encoding?)"; break;
        }

        if (type) {
            printf("(%04x) %s", (unsigned int)element_type, type);
        }
        else {
            printf("UNKNOWN TYPE %04x", (unsigned int)element_type);
        }
    }
}


bool encode_request_prefix(const char* name, uint8_t* buffer, int* encoded_size)
{
    int symbol_length_index = 0;
    /* start with the symbolic type identifier */
    *encoded_size = 0;
    buffer[*encoded_size] = (uint8_t)(unsigned int)0x91; (*encoded_size)++;

    /* dummy value for the encoded length */
    symbol_length_index = *encoded_size;
    buffer[*encoded_size] = (uint8_t)(unsigned int)0; (*encoded_size)++;

    /* copy the string. */
    for (int index = 0; index < (int)(unsigned int)strlen(name); index++) {
        buffer[*encoded_size] = (uint8_t)name[index];
        (*encoded_size)++;
    }

    /* backfill the encoded size */
    buffer[symbol_length_index] = (uint8_t)(unsigned int)(strlen(name));

    /* make sure we have an even number of bytes. */
    if ((*encoded_size) & 0x01) {
        buffer[*encoded_size] = 0;
        (*encoded_size)++;
    }

    return true;
}


bool get_udt_definition(const char* tag_string_base, uint16_t udt_id)
{
    int32_t udt_info_tag = 0;
    int tag_size = 0;
    char buf[32] = { 0 };
    int offset = 0;
    uint16_t template_id = 0;
    uint16_t num_members = 0;
    uint16_t struct_handle = 0;
    uint32_t udt_instance_size = 0;
    //uint32_t member_desc_size = 0;
    int name_len = 0;
    char* name_str = NULL;
    int name_index = 0;
    int field_index = 0;

    /* memoize, check to see if we have this type already. */
    if (udts[udt_id]) {
        return true;
    }

    snprintf(buf, sizeof(buf), "@udt/%u", (unsigned int)udt_id);

    auto tag_result = open_tag(tag_string_base, buf);
    if (tag_result.is_error())
    {
        if (tag_result.status == plc::STATUS::PLCTAG_ERR_UNSUPPORTED)
        {
            fprintf(stderr, "This PLC type does not support listing UDT definitions.\n");
            return false;
        }

        fprintf(stderr, "Unable to open UDT info tag, error %s!\n", tag_result.error);
        usage();

        return false;
    }

    udt_info_tag = tag_result.data.tag_handle;

    fprintf(stderr, "UDT info tag ID: %d\n", udt_info_tag);

    auto result = plc::receive(udt_info_tag, TIMEOUT_MS);
    if (result.status == plc::STATUS::PLCTAG_ERR_UNSUPPORTED) {
        plc::destroy(udt_info_tag);
        fprintf(stderr, "UDT tag introspection is not supported on this PLC.\n");
        return false;
    }
    else if (result.is_ok()) {
        fprintf(stderr, "Error %s while trying to read UDT info!\n", result.error);
        usage();
        return false;
    }

    tag_size = tag_result.data.tag_size;

    /* the format in the tag buffer is:
     *
     * A new header of 14 bytes:
     *
     * Bytes   Meaning
     * 0-1     16-bit UDT ID
     * 2-5     32-bit UDT member description size, in 32-bit words.
     * 6-9     32-bit UDT instance size, in bytes.
     * 10-11   16-bit UDT number of members (fields).
     * 12-13   16-bit UDT handle/type.
     *
     * Then the raw field info.
     *
     * N x field info entries
     *     uint16_t field_metadata - array element count or bit field number
     *     uint16_t field_type
     *     uint32_t field_offset
     *
     * int8_t string - zero-terminated string, UDT name, but name stops at first semicolon!
     *
     * N x field names
     *     int8_t string - zero-terminated.
     *
     */

     /* get the ID, number of members and the instance size. */
    template_id = plc::get_u16(udt_info_tag, 0).data;
    //member_desc_size = plc::get_u32(udt_info_tag, 2).data;
    udt_instance_size = plc::get_u32(udt_info_tag, 6).data;
    num_members = plc::get_u16(udt_info_tag, 10).data;
    struct_handle = plc::get_u16(udt_info_tag, 12).data;

    /* skip past this header. */
    offset = 14;

    /* just a sanity check */
    if (template_id != udt_id) {
        fprintf(stderr, "The ID, %x, of the UDT we are reading is not the same as the UDT ID we requested, %x!\n", (unsigned int)template_id, (unsigned int)udt_id);
        usage();
    }

    /* allocate a UDT struct with this info. */
    udts[(size_t)udt_id] = (udt_entry_s*)calloc(1, sizeof(struct udt_entry_s) + (sizeof(struct udt_field_entry_s) * num_members));
    if (!udts[(size_t)udt_id]) {
        fprintf(stderr, "Unable to allocate a new UDT definition structure!\n");
        usage();
        return false;
    }

    udts[(size_t)udt_id]->id = udt_id;
    udts[(size_t)udt_id]->num_fields = num_members;
    udts[(size_t)udt_id]->struct_handle = struct_handle;
    udts[(size_t)udt_id]->instance_size = udt_instance_size;

    /* first section is the field type and size info for all fields. */
    for (int field_index = 0; field_index < udts[udt_id]->num_fields; field_index++) {
        uint16_t field_metadata = 0;
        uint16_t field_element_type = 0;
        uint32_t field_offset = 0;

        field_metadata = plc::get_u16(udt_info_tag, offset).data;
        offset += 2;

        field_element_type = plc::get_u16(udt_info_tag, offset).data;
        offset += 2;

        field_offset = plc::get_u32(udt_info_tag, offset).data;
        offset += 4;

        udts[udt_id]->fields[field_index].metadata = field_metadata;
        udts[udt_id]->fields[field_index].type = field_element_type;
        udts[udt_id]->fields[field_index].offset = field_offset;

        /* make sure that we have or will get any UDT field types */
        if ((field_element_type & TYPE_IS_STRUCT) && !(field_element_type & TYPE_IS_SYSTEM)) {
            uint16_t child_udt = (field_element_type & TYPE_UDT_ID_MASK);

            if (!udts[child_udt]) {
                udts_to_process[last_udt] = child_udt;
                last_udt++;
            }
        }
    }

    fprintf(stderr, "Offset after reading field descriptors: %d.\n", offset);

    /*
     * then get the template/UDT name.   This is weird.
     * Scan until we see a 0x3B, semicolon, byte.   That is the end of the
     * template name.   Actually we should look for ";n" but the semicolon
     * seems to be enough for now.
     */

     /* first get the zero-terminated string length */
    auto len_result = plc::get_string_length(udt_info_tag, offset);
    name_len = (int)len_result.data;

    if (!len_result.is_ok() || name_len >= 256)
    {
        fprintf(stderr, "Unexpected raw UDT name length: %d! Error: %s\n", name_len, len_result.error);
        return false;
    }

    /* create a string for this. */
    name_str = (char*)calloc((size_t)(name_len + 1), (size_t)1);
    if (!name_str) {
        fprintf(stderr, "Unable to allocate UDT name string!\n");
        usage();
        return false;
    }

    /* copy the name */
    auto str_result = plc::get_string(udt_info_tag, offset, name_str, name_len + 1);
    if (!str_result.is_ok())
    {
        fprintf(stderr, "Error %s retrieving UDT name string from the tag!\n", str_result.error);        
        usage();
        free(name_str);
        return false;
    }

    /* zero terminate the name when we hit the first semicolon. */
    for (name_index = 0; name_index < name_len && name_str[name_index] != ';'; name_index++) {};

    if (name_str[name_index] == ';') {
        name_str[name_index] = 0;
    }

    /* check the name length again. */
    name_len = (int)(unsigned int)strlen(name_str);
    if (name_len == 0 || name_len >= 256) {
        fprintf(stderr, "Unexpected UDT name length: %d!\n", name_len);
        free(name_str);
        return false;
    }

    udts[udt_id]->name = name_str; // TODO

    fprintf(stderr, "Getting data from UDT \"%s\".\n", udts[udt_id]->name);

    /* skip past the UDT name. */
    offset += plc::get_string_total_length(udt_info_tag, offset).data;

    /*
     * This is the second section of the data, the field names.   They appear
     * to be zero terminated.
     */

    fprintf(stderr, "Getting %d field names for UDT %s.\n", udts[udt_id]->num_fields, udts[udt_id]->name);
    fprintf(stderr, "offset=%u, tag_size=%u.\n", offset, tag_size);


    /* loop over all fields and get name strings.  They are zero terminated. */
    for (field_index = 0; field_index < udts[udt_id]->num_fields && offset < tag_size; field_index++) {
        fprintf(stderr, "Getting name for field %u.\n", field_index);

        /* first get the zero-terminated string length */
        len_result = plc::get_string_length(udt_info_tag, offset);
        name_len = len_result.data;
        if (!len_result.is_ok() || name_len >= 256) {
            plc::destroy(udt_info_tag);
            fprintf(stderr, "Unexpected UDT field name length: %d! Error: %s\n", name_len, len_result.error);
            usage();
            // TODO
        }

        fprintf(stderr, "The name for field %u is %u characters long.\n", field_index, name_len);

        /* create a string for this. */
        if (name_len > 0) {
            name_str = (char*)calloc((size_t)(name_len + 1), (size_t)1);
            if (!name_str) {
                plc::destroy(udt_info_tag);
                fprintf(stderr, "Unable to allocate UDT field name string!\n");
                usage();
            }

            fprintf(stderr, "The string for field %u is at %p.\n", field_index, (void*)name_str);

            /* copy the name */
            str_result = plc::get_string(udt_info_tag, offset, name_str, name_len + 1);
            if (!str_result.is_ok()) {
                plc::destroy(udt_info_tag);
                fprintf(stderr, "Error %s retrieving UDT field name string from the tag!\n", str_result.error);
                free(name_str);
                usage();
            }

            udts[udt_id]->fields[field_index].name = name_str; // TODO

            fprintf(stderr, "UDT field %d is \"%s\".\n", field_index, udts[udt_id]->fields[field_index].name);

            offset += plc::get_string_total_length(udt_info_tag, offset).data;
        }
        else {
            /* field name was zero length. */
            udts[udt_id]->fields[field_index].name = NULL;

            fprintf(stderr, "UDT field %d is not named.\n", field_index);

            /*
             * The string is either zero length in which case we need to bump past the null
             * terminator or it is at the end of the tag and we need to step past the edge.
             */
            offset++;
        }
    }

    /* sanity check */
    if (offset != tag_size) {
        fprintf(stderr, "Processed %d bytes out of %d bytes.\n", offset, tag_size);
    }

    /* if we had a system tag, we might not have the full set of member/field names.  Fill in the gaps. */
    for (; field_index < udts[udt_id]->num_fields; field_index++) {
        /* field name was zero length. */
        udts[udt_id]->fields[field_index].name = NULL;

        fprintf(stderr, "UDT field %d is not named.\n", field_index);
    }

    free(name_str);

    fprintf(stderr, "Destroying UDT info tag: %d\n", udt_info_tag);
    plc::destroy(udt_info_tag);

    return true;
}