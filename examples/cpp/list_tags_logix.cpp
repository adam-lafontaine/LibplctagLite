#include "../../lib/plctag.hpp"

namespace plc = plctag;

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <algorithm>


namespace plc = plctag;


constexpr auto DEFAULT_IP = "192.168.19.230";
constexpr auto DEFAULT_PATH = "1,0";


FILE* out_file;
FILE* err_file;


void tag_print(const char* format, ...)
{
    va_list args;
    va_start(args, format);

    if (out_file)
    {
        vfprintf(out_file, format, args);
    }

    vfprintf(stdout, format, args);

    va_end(args);
}


void tag_print_error(const char* format, ...)
{
    va_list args;
    va_start(args, format);

    if (err_file)
    {
        vfprintf(err_file, format, args);
    }

    vfprintf(stderr, format, args);

    va_end(args);
}


bool str_is_equal(cstr lhs, cstr rhs)
{
    auto len = std::max(strlen(lhs), strlen(rhs));

    return strncmp(lhs, rhs, len) == 0;
}


bool str_starts_with(cstr str, cstr prefix)
{
    auto len = strlen(prefix);
    return strlen(str) >= len && strncmp(str, prefix, len) == 0;
}


int main(int argc, char** argv)
{
    auto error = fopen_s(&out_file, "output.txt", "w");
    error = error = fopen_s(&err_file, "errors.txt", "w");

    auto const cleanup = [&]() 
    {
        if (out_file) { fclose(out_file); }
        if (err_file) { fclose(err_file); }
    };

    plc::PLC_Desc plc;
    plc.controller = plc::Controller::ControlLogix;
    plc.gateway = DEFAULT_IP;
    plc.path = DEFAULT_PATH;

    auto result = plc::enumerate_tags(plc);

    if (!result.is_ok())
    {
        tag_print_error("Error: enumerate_tags() - %s\n", result.error);
        cleanup();
        return 0;
    }

    tag_print("Controller tags:\n");
    for (auto const& tag : plc.controller_tags)
    {
        tag_print("%s: %s\n", tag.name.c_str(), plc::decode_tag_type(tag.tag_type));
    }

    tag_print("Program tags:\n");
    for (auto const& tag : plc.program_tags)
    {
        tag_print("%s: %s\n", tag.name.c_str(), plc::decode_tag_type(tag.tag_type));
    }

    tag_print("Done!\n");   

    cleanup();
    return 0;
}