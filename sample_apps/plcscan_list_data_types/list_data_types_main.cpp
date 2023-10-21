#include "../../src/plcscan/plcscan.hpp"

#include <cstdio>

/*

The basic data types are generated when the library is initialized

1. Initialize the library
2. Verify that data type data has been generated
3. Setup output formatting
4. Print the types and descriptions to the console
5. Shutdown the library

*/


int main()
{
	// 1. Initialize the library
	auto plc_data = plcscan::init();

	if (!plc_data.is_init)
	{
		printf("Error. Unable to initialize library\n");
		return 1;
	}

	auto& types = plc_data.data_types;

	// 2. Verify that data type data has been generated
	if (types.empty())
	{
		printf("Error. No PLC data types found\n");
		return 1;
	}

	// 3. Setup output formatting
	auto name_len = types[0].data_type_name.length;
	for (auto const& type : types)
	{
		if (type.data_type_name.length > name_len)
		{
			name_len = type.data_type_name.length;
		}
	}

	name_len++;

	// 4. Print the types and descriptions to the console
	for (auto const& type : types)
	{
		printf("%*s: %s\n", (int)name_len, type.name(), type.description());
	}

	// 5. Shutdown the library
	plcscan::shutdown();

	return 0;
}