#include "../../lib/plctag.hpp"

#include <cstdio>

namespace plc = plctag;


int main()
{
	char buffer[256] = { 0 };

	auto ip_address = "192.168.10.222";
	auto path = "1,0";
	auto name = "SomeTag";

	plc::TagAttr attr{};
	attr.gateway = ip_address;
	attr.path = path;
	attr.tag_name = name;
	attr.has_dhp = false;

	for (int c = 0; plc::decode_controller(c); ++c)
	{
		attr.controller = static_cast<plc::Controller>(c);

		printf("%s:\n", plc::decode_controller(attr.controller));

		if (plc::dbg::build_attr_string(attr, buffer, 256))
		{
			printf("%s\n\n", buffer);
		}
		else
		{
			printf("Could not build connection string\n\n");
		}
	}

	
}