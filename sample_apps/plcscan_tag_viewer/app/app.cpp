#include "app.hpp"
#include "../../../src/imgui/imgui.h"
#include "../../../src/plcscan/plcscan.hpp"
#include "../../../src/util/time_helper.hpp"

namespace tmh = time_helper;



constexpr auto DEFAULT_PLC_IP = "192.168.123.123";
constexpr auto DEFAULT_PLC_PATH = "1,0";
constexpr auto DEFAULT_CONNECT_TIMEOUT = "1000";


namespace
{
	class PLC_State
	{
	public:
		plcscan::PlcTagData data;

		bool is_initializing = false;
		bool is_scanning = false;
		bool has_error = false;
	};


	class UI_Input
	{
	public:
		cstr plc_ip = DEFAULT_PLC_IP;
		cstr plc_path = DEFAULT_PLC_PATH;

	};


	class App_State
	{
	public:
		UI_Input user_input;
		PLC_State plc;

		bool app_running;
		
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
	};
}


static App_State g_app_state;


namespace scan
{
	static void tag_values_to_string(plcscan::PlcTagData& data)
	{

	}


	static void start()
	{
		auto& state = g_app_state;
		auto& plc = state.plc;
		auto& input = state.user_input;

		Stopwatch sw;

		plc.is_initializing = true;

		plc.data = plcscan::init();
		if (!plc.data.is_init)
		{
			plc.has_error = true;
			return;
		}

		plc.is_initializing = false;

		plc.is_scanning = false;

		while (!plc.is_scanning)
		{
			sw.start();

			if (!state.app_running)
			{
				return;
			}

			tmh::delay_current_thread_ms(sw, 20);
		}

		if (!plcscan::connect(input.plc_ip, input.plc_path, plc.data))
		{
			plc.has_error = true;
			return;
		}

		auto const is_scanning = [&]() { return plc.is_scanning && state.app_running; };

		plcscan::scan(tag_values_to_string, is_scanning, plc.data);

		// TODO: stop/start
	}
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


	std::thread start_worker()
	{
		auto worker = std::thread(scan::start);

		return worker;
	}


	bool render_ui()
	{
		UI_Command cmd{};


		if (!cmd.has_command())
		{
			return true;
		}

		if (cmd.stop_app_running)
		{
			return false;
		}

		return true;
	}
}