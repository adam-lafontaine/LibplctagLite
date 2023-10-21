#include "../../src/plcscan/plcscan.hpp"

#include <cstdio>

/*

1. Initialize the library
2. Connect to the PLC
3. Verify that tag infomation has been generated
4. Setup output formatting
5. Print the name and data type of each tag to the console
6. Shutdown the library

*/


// PLC IP and path
constexpr auto PLC_IP = "192.168.123.123";
constexpr auto PLC_PATH = "1,0";


int main()
{
	// 1. Initialize the library
	auto plc_data = plcscan::init();

	if (!plc_data.is_init)
	{
		printf("Error. Unable to initialize library.\n");
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
	auto name_len = tags[0].tag_name.length;
	for (auto const& tag : tags)
	{
		if (tag.tag_name.length > name_len)
		{
			name_len = tag.tag_name.length;
		}
	}

	name_len++;

	// 5. Print the name and data type of each tag to the console
	for (auto const& tag : tags)
	{
		if (tag.array_count > 1)
		{
			printf("%*s: %s[%d]\n", (int)name_len, tag.name(), plcscan::get_fast_type_name(tag.type_id), (int)tag.array_count);
		}
		else
		{
			printf("%*s: %s\n", (int)name_len, tag.name(), plcscan::get_fast_type_name(tag.type_id));
		}		
	}

	// 6. Shutdown the library
	plcscan::shutdown();

	return 0;
}