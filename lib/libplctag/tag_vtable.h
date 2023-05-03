#ifndef __TAG_VTABLE_H__
#define __TAG_VTABLE_H__


#include "libplctag_internal.h"

#ifdef __cplusplus
extern "C" {
#endif


struct tag_vtable_t eip_cip_raw_tag_vtable;

struct tag_vtable_t eip_cip_vtable;

struct tag_vtable_t lgx_pccc_vtable;

/* PCCC with DH+ last hop */
struct tag_vtable_t eip_plc5_dhp_vtable;

struct tag_vtable_t plc5_vtable;

struct tag_vtable_t eip_slc_dhp_vtable;

struct tag_vtable_t slc_vtable;


#ifdef __cplusplus
}
#endif


#endif // __TAG_VTABLE_H__