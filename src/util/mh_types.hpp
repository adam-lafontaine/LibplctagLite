#pragma once

#include "types.hpp"
#include "memory_buffer.hpp"

namespace mb = memory_buffer;

using ByteView = MemoryView<u8>;


class StringView
{
public:
	char* char_data = nullptr;
	unsigned length = 0;

	cstr data() const { return char_data; }
};