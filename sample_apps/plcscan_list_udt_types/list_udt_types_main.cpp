#include "../../src/plcscan/plcscan.hpp"

#include <cstdio>


/*

1. Initialize the library
2. Connect to the PLC
3. Verify that the UDT definitions have been generated
4. Print the name of each UDT along with the field and type information to the console
5. Shutdown the library

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

	auto& udts = plc_data.udt_types;

	// 3. Verify that the UDT definitions have been generated
	if (udts.empty())
	{
		printf("Error. No UDTs found\n");
		return 1;
	}

	// 4. Print the name of each UDT along with the field and type information to the console
	for (auto const& udt : udts)
	{
		printf("%s\n", udt.name());
		for (auto const& field : udt.fields)
		{
			if (field.is_array())
			{
				printf("    %s: %s[%d]\n", field.name(), plcscan::get_fast_type_name(field.type_id), (int)field.array_count);
			}
			else
			{
				printf("    %s: %s\n", field.name(), plcscan::get_fast_type_name(field.type_id));
			}			
		}
		printf("\n");
	}

	// 5. Shutdown the library
	plcscan::shutdown();

	return 0;
}