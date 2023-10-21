#include "../../plcscan/plcscan.hpp"

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

	auto& udts = plc_data.udt_types;

	if (udts.empty())
	{
		printf("Error. No UDTs found\n");
		return 1;
	}

	for (auto const& udt : udts)
	{
		printf("%s\n", udt.name());
		for (auto const& field : udt.fields)
		{
			printf("    %s: %s\n", field.name(), plcscan::get_fast_type_name(field.type_id));
		}
		printf("\n");
	}

	plcscan::disconnect();

	return 0;
}