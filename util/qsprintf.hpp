#pragma once


namespace stb
{
#include "stb_sprintf.h"
}


#define qvsprintf stb::stbsp_vsprintf
#define qvsnprintf stb::stbsp_vsnprintf
#define qsprintf stb::stbsp_sprintf
#define qsnprintf stb::stbsp_snprintf
#define qvsprintfcb stb::stbsp_vsprintfcb
#define qset_separators stb::stbsp_set_separators


