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


FILE* out_files[4] = { 0 };
constexpr int OUT_CTL = 0;
constexpr int OUT_PGM = 1;
constexpr int OUT_UDT = 2;
constexpr int OUT_ERR = 3;


static void close_files()
{
    for (int i = 0; i < 4; ++i)
    {
        if (out_files[i])
        {
            fclose(out_files[i]);
        }
    }
}


static bool open_files()
{
#ifndef _WIN32

#define fopen_s(pFile,filename,mode) ((*(pFile))=fopen((filename),(mode)))==NULL

#endif

    auto err = fopen_s(&out_files[OUT_CTL], "controller_tags.txt", "w");
    err     += fopen_s(&out_files[OUT_PGM], "program_tags.txt", "w");
    err     += fopen_s(&out_files[OUT_UDT], "udt_tags.txt", "w");
    err     += fopen_s(&out_files[OUT_ERR], "errors.txt", "w");

    if (err)
    {
        close_files();
        return false;
    }

    return true;
}


static void tag_print(int file_id, const char* format, ...)
{
    va_list args;
    va_start(args, format);

    if (out_files[file_id])
    {
        vfprintf(out_files[file_id], format, args);
    }

    vfprintf(stdout, format, args);

    va_end(args);
}


static void tag_print_error(const char* format, ...)
{
    va_list args;
    va_start(args, format);

    if (out_files[OUT_ERR])
    {
        vfprintf(out_files[OUT_ERR], format, args);
    }

    vfprintf(stderr, format, args);

    va_end(args);
}


int main(int argc, char** argv)
{
    if (!open_files())
    {
        printf("could not open output files\n");
        return 0;
    }

    auto const cleanup = [&]() 
    {
        close_files();
        plc::shutdown();
    };

    plc::PLC_Desc plc;
    plc.controller = plc::Controller::ControlLogix;
    plc.gateway = DEFAULT_IP;
    plc.path = DEFAULT_PATH;

    printf("Scanning for tags... ");

    auto result = plc::enumerate_tags(plc);

    if (!result.is_ok())
    {
        tag_print_error("Error: enumerate_tags() - %s\n", result.error);
        cleanup();
        return 0;
    }

    printf("Done!\n");

    printf("Testing controller tags... ");

    plc::Tag_Attr attr{};
    attr.controller = plc.controller;
    attr.gateway = plc.gateway;
    attr.path = plc.path;

    tag_print(OUT_CTL, "Controller tags:\n");
    for (auto const& tag : plc.controller_tags)
    {
        u32 size = 0;
        attr.tag_name = tag.name.c_str();
        auto result = plc::connect(attr);
        if (result.is_ok())
        {
            size = result.data.tag_size;
        }
        tag_print(OUT_CTL, "%35s: %5u bytes - %s\n", tag.name.c_str(), size, plc::decode_tag_type(tag.tag_type));
        plc::disconnect(result.data.tag_handle);
    }

    printf("Done!\n");

    printf("Listing program tags... ");

    tag_print(OUT_PGM, "Program tags:\n");
    for (auto const& tag : plc.program_tags)
    {
        tag_print(OUT_PGM, "%35s: %s\n", tag.name.c_str(), plc::decode_tag_type(tag.tag_type));
    }

    printf("Done!\n");

    printf("Listing UDT tags... ");

    tag_print(OUT_UDT, "UDT tags:\n");
    for (auto const& tag : plc.udt_tags)
    {
        tag_print(OUT_UDT, "%35s:\n", tag.name.c_str());
        for (auto const& field : tag.fields)
        {
            tag_print(OUT_UDT, "  %-35s: %s\n", field.name.c_str(), plc::decode_tag_type(field.tag_type));
        }        
    }

    printf("Done!\n");   

    cleanup();
    return 0;
}