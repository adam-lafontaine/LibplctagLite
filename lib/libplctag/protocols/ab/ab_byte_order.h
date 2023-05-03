#ifndef _AB_BYTE_ORDER_H__
#define __AB_BYTE_ORDER_H__

#include "../../libplctag_internal.h"

#ifdef __cplusplus
extern "C" {
#endif


tag_byte_order_p logix_tag_byte_order();

tag_byte_order_p omron_njnx_tag_byte_order();

tag_byte_order_p logix_tag_listing_byte_order();

tag_byte_order_p plc5_tag_byte_order();

tag_byte_order_p slc_tag_byte_order();


#ifdef __cplusplus
}
#endif

#endif // __AB_BYTE_ORDER_H__