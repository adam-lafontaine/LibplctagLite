// not yet used
// ...after pruning the library

#include "./protocols/ab/ab_common.c"
#include "./protocols/ab/cip.c"
#include "./protocols/ab/eip_cip.c"
#include "./protocols/ab/eip_cip_special.c"

#include "./protocols/ab/eip_lgx_pccc.c"
#include "./protocols/ab/eip_plc5_dhp.c"
#include "./protocols/ab/eip_plc5_pccc.c"
#include "./protocols/ab/eip_slc_dhp.c"
#include "./protocols/ab/eip_slc_pccc.c"

#include "./protocols/ab/error_codes.c"
#include "./protocols/ab/pccc.c"

#include "./protocols/ab/session.c"

#include "./protocols/modbus.c"
#include "./protocols/system.c"


#ifdef _WIN32

#include "./platform_windows.c"

#else

#include "./platform_posix.c"

#endif

#include "./libplctag.c"
