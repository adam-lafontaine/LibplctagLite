#ifndef _AB_BYTE_ORDER_H__
#define __AB_BYTE_ORDER_H__

#include "../../libplctaglite_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

/* default string types used for ControlLogix-class PLCs. */
/*
static tag_byte_order_t logix_tag_byte_order_def = {

    .str_is_counted = 1,
    .str_is_fixed_length = 1,
    .str_is_zero_terminated = 0,

    .str_count_word_bytes = 4,
    .str_max_capacity = 82,
    .str_total_length = 88,
    .str_pad_bytes = 2,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {0,1,2,3},
    .float64_order = {0,1,2,3,4,5,6,7},
};
*/


tag_byte_order_p logix_tag_byte_order();


/*

tag_byte_order_t listing_tag_logix_byte_order = {

    .str_is_counted = 1,
    .str_is_fixed_length = 0,
    .str_is_zero_terminated = 0,

    .str_count_word_bytes = 2,
    .str_max_capacity = 0,
    .str_total_length = 0,
    .str_pad_bytes = 0,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {0,1,2,3},
    .float64_order = {0,1,2,3,4,5,6,7},
};


// strings are zero terminated.

tag_byte_order_t udt_tag_logix_byte_order = {

    .str_is_counted = 0,
    .str_is_fixed_length = 0,
    .str_is_zero_terminated = 1,

    .str_count_word_bytes = 0,
    .str_max_capacity = 0,
    .str_total_length = 0,
    .str_pad_bytes = 0,

    .int16_order = {0,1},
    .int32_order = {0,1,2,3},
    .int64_order = {0,1,2,3,4,5,6,7},
    .float32_order = {0,1,2,3},
    .float64_order = {0,1,2,3,4,5,6,7},
};


*/


#ifdef __cplusplus
}
#endif





#endif // __AB_BYTE_ORDER_H__