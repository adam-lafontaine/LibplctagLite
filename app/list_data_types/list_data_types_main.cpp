#include "../../plcscan/plcscan.hpp"

#include <cstdio>




int main()
{
	auto data = plcscan::init();

	for (auto const& type : data.data_types)
	{
		printf("%s: %s\n", type.data_type_name.begin, type.data_type_description.begin);
	}

	plcscan::disconnect();
}