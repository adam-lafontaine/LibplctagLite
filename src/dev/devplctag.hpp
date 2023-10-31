#pragma once


namespace dev
{
    constexpr int PLCTAG_STATUS_OK = 0;

    int plc_tag_create(const char* attr, int timeout);

    int plc_tag_read(int handle, int timeout);

    int plc_tag_get_size(int handle);

    int plc_tag_get_raw_bytes(int handle, int offset, unsigned char* dst, int length);

    void plc_tag_shutdown();
}