#pragma once

#include "types.hpp"
#include "memory_buffer.hpp"

namespace mb = memory_buffer;

using StringView = MemoryView<char>;
using ByteView = MemoryView<u8>;