#pragma once

#include "mh_types.hpp"

#include <cstring>


namespace memory_helper
{
    inline StringView push_cstr_view(MemoryBuffer<char>& buffer, unsigned total_bytes)
    {
        assert(total_bytes > 0);
        assert(buffer.data_);
        assert(buffer.capacity_);

        auto bytes_available = (buffer.capacity_ - buffer.size_) >= total_bytes;
        assert(bytes_available);

        StringView view{};

        view.char_data = mb::push_elements(buffer, total_bytes);

        view.length = total_bytes - 1; /* zero terminated */
        view.char_data[view.length] = 0;

        return view;
    }


    inline StringView push_cstr_view(MemoryBuffer<char>& buffer, size_t total_bytes)
    {
        return push_cstr_view(buffer, (unsigned)total_bytes);
    }
}


namespace memory_helper
{
    inline void copy_8(u8* src, u8* dst, u32 len8)
    {
        switch (len8)
        {
        case 1:
            *dst = *src;
            return;

        case 2:
            *(u16*)dst = *(u16*)src;
            return;

        case 3:
            *(u16*)dst = *(u16*)src;
            dst[2] = src[2];
            return;

        case 4:
            *(u32*)dst = *(u32*)src;
            return;
        
        case 5:
            *(u32*)dst = *(u32*)src;
            dst[4] = src[4];
            return;
        
        case 6:
            *(u32*)dst = *(u32*)src;
            *(u16*)(dst + 4) = *(u16*)(src + 4);
            return;
        
        case 7:
            *(u32*)dst = *(u32*)src;
            *(u16*)(dst + 4) = *(u16*)(src + 4);
            dst[6] = src[6];
            return;

        case 8:
            *(u64*)dst = *(u64*)src;
            return;

        default:
            break;
        }
    }


    inline void copy_bytes(u8* src, u8* dst, u32 len)
    {
        constexpr auto size64 = (u32)sizeof(u64);

        auto len64 = len / size64;
        auto src64 = (u64*)src;
        auto dst64 = (u64*)dst;

        auto len8 = len - len64 * size64;
        auto src8 = (u8*)(src64 + len64);
        auto dst8 = (u8*)(dst64 + len64);

        for (size_t i = 0; i < len64; ++i)
        {
            dst64[i] = src64[i];
        }

        copy_8(src8, dst8, len8);
    }


    inline bool bytes_equal(u8* lhs, u8* rhs, u32 len)
    {
        switch (len)
        {
        case 1: return *lhs == *rhs;

        case 2: return *((u16*)lhs) == *((u16*)rhs);

        case 4: return *((u32*)lhs) == *((u32*)rhs);

        case 8: return *((u64*)lhs) == *((u64*)rhs);

        default:
            break;
        }

        constexpr auto size64 = (u32)sizeof(u64);

        auto len64 = len / size64;
        auto lhs64 = (u64*)lhs;
        auto rhs64 = (u64*)rhs;

        for (u32 i = 0; i < len64; ++i)
        {
            if (lhs64[i] != rhs64[i])
            {
                return false;
            }
        }

        auto len8 = len - len64 * size64;
        auto lhs8 = (u8*)(lhs64 + len64);
        auto rhs8 = (u8*)(rhs64 + len64);

        for (u32 i = 0; i < len8; ++i)
        {
            if (lhs8[i] != rhs8[i])
            {
                return false;
            }
        }

        return true;
    }


    template <typename T>
    static T cast_bytes(u8* src, u32 size)
    {
        u8 b1 = 0;
        u16 b2 = 0;
        u32 b4 = 0;
        u64 b8 = 0;

        assert(size >= (u32)sizeof(T));

        switch (size) // sizeof(T) ?
        {
        case 1:
            b1 = *src;
            return *(T*)&b1;
        case 2:
            b2 = *(u16*)src;
            return *(T*)&b2;
        case 4:
            b4 = *(u32*)src;
            return *(T*)&b4;
        case 8:
            b8 = *(u64*)src;
            return *(T*)&b8;
        }

        assert(false);
        return (T)(*src);
    }


    inline void copy_unsafe(cstr src, StringView const& dst)
    {
        auto len = strlen(src);
        len = len < dst.length ? len : dst.length;

        u32 i = 0;

        for (; i < len; ++i)
        {
            dst.char_data[i] = src[i];
        }

        /*for (; i <= dst.length; ++i)
        {
            dst.char_data[i] = 0;
        }*/
    }


    inline void copy_unsafe(StringView const& src, char* dst)
    {
        auto len = strlen(dst);
        len = len < src.length ? len : src.length;

        for (u32 i = 0; i < len; ++i)
        {
            dst[i] = src.char_data[i];
        }
    }


    inline void copy(StringView const& src, StringView const& dst)
    {
        auto len = src.length < dst.length ? src.length : dst.length;

        copy_bytes((u8*)src.char_data, (u8*)dst.char_data, len);
    }


    inline void copy(ByteView const& src, ByteView const& dst)
    {
        assert(src.length <= dst.length);

        copy_bytes(src.data, dst.data, src.length);
    }


    inline void zero_string(StringView const& str)
    {
        for (u32 i = 0; i < str.length; ++i)
        {
            str.char_data[i] = 0;
        }
    }


    inline StringView to_string_view_unsafe(cstr str)
    {
        StringView view{};
        view.char_data = (char*)str;
        view.length = (u32)strlen(str);

        return view;
    }


    inline StringView to_string_view_unsafe(char* str, u32 len)
    {
        StringView view{};
        view.char_data = str;
        view.length = len;

        return view;
    }


    inline bool string_contains(cstr str, char c)
    {
        auto len = strlen(str);

        for (u32 i = 0; i < len; ++i)
        {
            if (str[i] == c)
            {
                return true;
            }
        }

        return false;
    }


    inline bool string_contains(cstr str, char c, u32& pos)
    {
        auto len = strlen(str);

        for (u32 i = 0; i < len; ++i)
        {
            if (str[i] == c)
            {
                pos = i;
                return true;
            }
        }

        return false;
    }
}


