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
		
		PLC_State plc;

		bool app_running = false;
		
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
static UI_Input g_user_input;


namespace scan
{
	static void tag_values_to_string(plcscan::PlcTagData& data)
	{

	}


	static void start()
	{
		auto& state = g_app_state;
		auto& input = g_user_input;
		auto& plc = state.plc;

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


namespace render
{
	constexpr auto WHITE = ImVec4(1, 1, 1, 1);
	constexpr auto GREEN = ImVec4(0, 1, 0, 1);
	constexpr auto RED = ImVec4(1, 0, 0, 1);


	static void command_window(UI_Command& cmd, UI_Input& input)
	{
		ImGui::Begin("Commands");

		ImGui::End();
	}


	static void data_type_window(List<plcscan::DataType> const& data_types)
	{
		ImGui::Begin("Data Types");

		constexpr int n_columns = 3;
		auto table_flags = ImGuiTableFlags_ScrollY | ImGuiTableFlags_BordersV | ImGuiTableFlags_BordersOuterH | ImGuiTableFlags_RowBg | ImGuiTableFlags_NoBordersInBody;
		auto table_dims = ImGui::GetContentRegionAvail();

		auto text_color = WHITE;

		if (ImGui::BeginTable("DataTypeTable", n_columns, table_flags, table_dims))
		{
			ImGui::TableSetupScrollFreeze(0, 1);

			ImGui::TableSetupColumn("Type", ImGuiTableColumnFlags_WidthFixed);
			ImGui::TableSetupColumn("Description", ImGuiTableColumnFlags_WidthStretch);
			ImGui::TableSetupColumn("Size", ImGuiTableColumnFlags_WidthFixed);
			ImGui::TableHeadersRow();

			for (auto const& dt : data_types)
			{
				ImGui::TableNextRow();

				ImGui::TableSetColumnIndex(0);
				ImGui::TextColored(text_color, "%s", dt.name());

				ImGui::TableSetColumnIndex(1);
				ImGui::TextColored(text_color, "%s", dt.description());

				ImGui::TableSetColumnIndex(2);
				ImGui::TextColored(text_color, "%u", dt.size);
			}

			ImGui::EndTable();
		}

		ImGui::End();
	}


	static void udt_type_window()
	{
		ImGui::Begin("UDTs");

		ImGui::End();
	}


	static void tag_window()
	{
		ImGui::Begin("Tags");

		ImGui::End();
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
		auto& state = g_app_state;
		auto& input = g_user_input;

		UI_Command cmd{};

		ImGui::DockSpaceOverViewport(nullptr, ImGuiDockNodeFlags_None);

		render::command_window(cmd, input);

		if (cmd.has_command())
		{
			if (cmd.stop_app_running)
			{
				return false;
			}


		}		

		render::data_type_window(state.plc.data.data_types);

		render::udt_type_window();

		render::tag_window();

		return true;
	}
}