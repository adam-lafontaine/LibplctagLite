#ifndef __AB_VTABLE_H__
#define __AB_VTABLE_H__


#include "../../libplctag_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

tag_vtable_p eip_cip_vtable();

tag_vtable_p lgx_pccc_vtable();

/* PCCC with DH+ last hop */
tag_vtable_p eip_plc5_dhp_vtable();

tag_vtable_p plc5_vtable();

tag_vtable_p eip_slc_dhp_vtable();

tag_vtable_p slc_vtable();


#ifdef __cplusplus
}
#endif


#endif // __AB_VTABLE_H__