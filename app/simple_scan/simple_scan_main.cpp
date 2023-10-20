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


void print_all_tags_as_hex(plcscan::PlcTagData& data)
{
	constexpr int out_len = 20;
	char buffer[out_len + 1] = { 0 };

	for (auto const& tag : data.tags)
	{
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

		printf("%s: %s\n", tag.name.begin, buffer);
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

	plcscan::scan(print_all_tags_as_hex, still_scanning, plc_data);

	plcscan::disconnect();
	return 0;
}