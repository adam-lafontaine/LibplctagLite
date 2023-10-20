#include "../../plcscan/plcscan.hpp"
#include "../../util/qsprintf.hpp"

#include <cstdio>


// PLC IP and path
constexpr auto PLC_IP = "192.168.123.123";
constexpr auto PLC_PATH = "1,0";


// run 50 scans
int n_scans = 50;

bool still_scanning()
{
	return n_scans > 0;
}


inline void print_tag_as_string(plcscan::Tag const& tag)
{
	printf("%s\n", (cstr)tag.data());
}


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

	printf("%s: %s\n", tag.name(), buffer);
}


void print_tags(plcscan::PlcTagData& data)
{
	using Cat = plcscan::DataTypeCategory;

	for (auto const& tag : data.tags)
	{
		switch (plcscan::get_type_category(tag.type_id))
		{
		case Cat::Numeric:

			break;

		case Cat::String:
			print_tag_as_string(tag);
			break;

		default:
			print_tag_as_hex(tag);
		}
	}

	n_scans--;
}


int main()
{
	auto plc_data = plcscan::init();

	if (!plc_data.is_init)
	{
		printf("Error. Unable to initialize library\n");
		return 1;
	}

	if (!plcscan::connect(PLC_IP, PLC_PATH, plc_data))
	{
		printf("Error. Could not connect to PLC\n");
		return 1;
	}

	if (plc_data.tags.empty())
	{
		printf("Error. No tags found\n");
		return 1;
	}

	plcscan::scan(print_tags, still_scanning, plc_data);

	plcscan::disconnect();
	return 0;
}