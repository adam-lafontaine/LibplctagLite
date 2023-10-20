#include "../../plcscan/plcscan.hpp"

#include <cstdio>


int main()
{
	auto data = plcscan::init();

	if (!data.is_init)
	{
		printf("Error. Unable to initialize library\n");
		return 1;
	}

	auto& types = data.data_types;

	if (types.empty())
	{
		printf("Error. No PLC data types found\n");
		return 1;
	}

	auto max_len = types[0].data_type_name.length;
	for (auto const& type : types)
	{
		if (type.data_type_name.length > max_len)
		{
			max_len = type.data_type_name.length;
		}
	}

	max_len++;

	for (auto const& type : types)
	{
		printf("%*s: %s\n", (int)max_len, type.data_type_name.begin, type.data_type_description.begin);
	}

	plcscan::disconnect();

	return 0;
}