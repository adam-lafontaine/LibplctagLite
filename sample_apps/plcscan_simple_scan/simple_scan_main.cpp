#include "../../src/plcscan/plcscan.hpp"
#include "../../src/util/qsprintf.hpp"

#include <cstdio>

/*

1. Initialize the library
2. Connect to the PLC
3. Verify that tag infomation has been generated
4. Setup output formatting
5. Scan and process tag values until told to stop
6. Shutdown the library

*/


// PLC IP and path
constexpr auto PLC_IP = "192.168.123.123";
constexpr auto PLC_PATH = "1,0";


/* helpers */

unsigned name_len = 0; // output formatting

inline void print_tag_as_hex(plcscan::Tag const& tag)
{
	constexpr int out_len = 20;
	char buffer[out_len + 1] = { 0 };

	auto len = (int)tag.size();
	if (len < out_len / 2)
	{
		len = out_len / 2;
	}

	for (int i = 0; i < len; i++)
	{
		auto src = tag.data() + i;
		auto dst = buffer + i * 2;
		qsnprintf(dst, 3, "%02x", src);
	}
	buffer[len] = NULL;

	printf("%*s: %s\n", (int)name_len, tag.name(), buffer);
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


inline void print_tag_as_number(plcscan::Tag const& tag)
{
	constexpr int out_len = 20;
	char buffer[out_len + 1] = { 0 };

	auto size = tag.size();
	auto src = tag.data();

	using T = plcscan::TagType;

	switch (plcscan::get_tag_type(tag.type_id))
	{
	case T::BOOL:
	case T::USINT:	
		qsnprintf(buffer, out_len, "%hhu", cast_bytes<u8>(src, size));
		break;

	case T::SINT:
		qsnprintf(buffer, out_len, "%hhd", cast_bytes<i8>(src, size));
		break;

	case T::UINT:
		qsnprintf(buffer, out_len, "%hu", cast_bytes<u16>(src, size));
		break;

	case T::INT:
		qsnprintf(buffer, out_len, "%hd", cast_bytes<i16>(src, size));
		break;

	case T::UDINT:
		qsnprintf(buffer, out_len, "%u", cast_bytes<u32>(src, size));
		break;

	case T::DINT:
		qsnprintf(buffer, out_len, "%d", cast_bytes<i32>(src, size));
		break;

	case T::ULINT:
		qsnprintf(buffer, out_len, "%llu", cast_bytes<u64>(src, size));
		break;

	case T::LINT:
		qsnprintf(buffer, out_len, "%lld", cast_bytes<i64>(src, size));
		break;	

	case T::REAL:
		qsnprintf(buffer, out_len, "%f", cast_bytes<f32>(src, size));
		break;

	case T::LREAL:
		qsnprintf(buffer, out_len, "%lf", cast_bytes<f64>(src, size));
		break;

	default:
		qsnprintf(buffer, out_len, "error");
		break;
	}

	printf("%*s: %s\n", (int)name_len, tag.name(), buffer);
}


/* scan callback functions */

// Scanning will stop when this returns false
int n_scans = 50;
bool still_scanning()
{
	return n_scans > 0;
}


// print each tag value based on its data type
void print_tags(plcscan::PlcTagData& data)
{
	using T = plcscan::TagType;

	for (auto const& tag : data.tags)
	{
		switch (plcscan::get_tag_type(tag.type_id))
		{
		case T::STRING:
			printf("%*s: %s\n", (int)name_len, tag.name(), (cstr)tag.data());
			break;

		case T::UDT:
		case T::MISC:
			print_tag_as_hex(tag);
			break;

		default:
			print_tag_as_number(tag);
		}
	}

	n_scans--;
}


int main()
{
	// 1. Initialize the library
	auto plc_data = plcscan::init();

	if (!plc_data.is_init)
	{
		printf("Error. Unable to initialize library\n");
		return 1;
	}

	// 2. Connect to the PLC
	if (!plcscan::connect(PLC_IP, PLC_PATH, plc_data))
	{
		printf("Error. Could not connect to PLC\n");
		return 1;
	}

	auto& tags = plc_data.tags;

	// 3. Verify that tag infomation has been generated
	if (tags.empty())
	{
		printf("Error. No tags found\n");
		return 1;
	}

	// 4. Setup output formatting
	name_len = tags[0].tag_name.length;
	for (auto const& tag : tags)
	{
		if (tag.tag_name.length > name_len)
		{
			name_len = tag.tag_name.length;
		}
	}

	name_len++;

	// 5. Scan and process tag values until told to stop
	plcscan::scan(print_tags, still_scanning, plc_data);

	// 6. Shutdown the library
	plcscan::shutdown();
	return 0;
}