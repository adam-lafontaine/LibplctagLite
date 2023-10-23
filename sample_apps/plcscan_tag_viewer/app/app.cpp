#include "app.hpp"
#include "../../../src/imgui/imgui.h"
#include "../../../src/plcscan/plcscan.hpp"


constexpr auto DEFAULT_PLC_IP = "192.168.123.123";
constexpr auto DEFAULT_CONNECT_TIMEOUT = "1000";


namespace
{
	class App_State
	{
	public:

		plcscan::PlcTagData plc;


	};


	class UI_Command
	{
	public:
		bool stop_app_running = false;

		bool start_scanning = false;
		bool stop_scanning = false;

		bool has_command()
		{
			return false ||
				stop_app_running ||
				start_scanning ||
				stop_scanning;
		}

		void reset()
		{
			stop_app_running = false;
			start_scanning = false;
			stop_scanning = false;
		}
	};
}





namespace app
{
	void init()
	{

	}


	void shutdown()
	{
		plcscan::shutdown();
	}


	bool render_ui()
	{
		return true;
	}
}