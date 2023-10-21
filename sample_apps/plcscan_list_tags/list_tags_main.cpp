#include "../../src/plcscan/plcscan.hpp"

#include <cstdio>


// PLC IP and path
constexpr auto PLC_IP = "192.168.123.123";
constexpr auto PLC_PATH = "1,0";


int main()
{
	auto plc_data = plcscan::init();

	if (!plc_data.is_init)
	{
		printf("Error. Unable to initialize library.\n");
		return 1;
	}

	if (!plcscan::connect(PLC_IP, PLC_PATH, plc_data))
	{
		printf("Error. Could not connect to PLC\n");
		return 1;
	}

	auto& tags = plc_data.tags;

	if (tags.empty())
	{
		printf("Error. No tags found\n");
		return 1;
	}

	auto max_len = tags[0].tag_name.length;
	for (auto const& tag : tags)
	{
		if (tag.tag_name.length > max_len)
		{
			max_len = tag.tag_name.length;
		}
	}

	max_len++;

	for (auto const& tag : tags)
	{
		if (tag.array_count > 1)
		{
			printf("%*s: %s[%d]\n", (int)max_len, tag.name(), plcscan::get_fast_type_name(tag.type_id), (int)tag.array_count);
		}
		else
		{
			printf("%*s: %s\n", (int)max_len, tag.name(), plcscan::get_fast_type_name(tag.type_id));
		}		
	}

	plcscan::disconnect();

	return 0;
}