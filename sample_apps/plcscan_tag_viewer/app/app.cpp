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
	using UI_DataType = plcscan::DataType;


	class UI_UdtField
	{
	public:
		cstr name = 0;
		cstr type = 0;

		u32 offset = 0;
	};


	class UI_UdtType
	{
	public:
		cstr name = 0;
		u32 size = 0;

		List<UI_UdtField> fields;
	};


	class PLC_State
	{
	public:

		plcscan::PlcTagData data;

		List<UI_UdtType> udts;

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
	static List<UI_UdtType> transform_udts(List<plcscan::UdtType> const& udts)
	{
		auto const get_type_str = [&](plcscan::UdtFieldType const& field) 
		{
			if (field.is_bit())
			{
				return "bit";
			}

			auto type_str = "";

			auto type = plcscan::get_tag_type(field.type_id);
			if (type != plcscan::TagType::UDT)
			{
				type_str = plcscan::get_fast_type_name(field.type_id);
			}
			else
			{
				for (auto const& udt : udts)
				{
					if (field.type_id == udt.type_id)
					{
						type_str = udt.name();
					}
				}

				type_str = plcscan::get_fast_type_name(field.type_id);
			}


			
		};

		List<UI_UdtType> list;

		list.reserve(udts.size());
		for (auto const& udt : udts)
		{
			UI_UdtType ui{};
			ui.name = udt.name();
			ui.size = udt.size;

			ui.fields.reserve(udt.fields.size());
			for (auto const& field : udt.fields)
			{
				UI_UdtField f{};
				f.name = field.name();
				f.offset = field.offset;

				f.type = get_type_str(field);

				ui.fields.push_back(f);
			}

			list.push_back(ui);
		}

		return list;
	}


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

		plc.udts = transform_udts(plc.data.udt_types);

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


	static void data_type_window(List<UI_DataType> const& data_types)
	{
		ImGui::Begin("Data Types");

		constexpr int n_columns = 3;
		constexpr int col_name = 0;
		constexpr int col_desc = 1;
		constexpr int col_size = 2;

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

				ImGui::TableSetColumnIndex(col_name);
				ImGui::TextColored(text_color, "%s", dt.name());

				ImGui::TableSetColumnIndex(col_desc);
				ImGui::TextColored(text_color, "%s", dt.description());

				ImGui::TableSetColumnIndex(col_size);
				ImGui::TextColored(text_color, "%u", dt.size);
			}

			ImGui::EndTable();
		}

		ImGui::End();
	}


	static void udt_type_window(List<UI_UdtType> const& udt_types)
	{
		ImGui::Begin("UDTs");

		constexpr int n_columns = 4;
		constexpr int col_name = 0;
		constexpr int col_offset = 1;
		constexpr int col_size = 2;
		constexpr int col_type = 3;


		auto table_flags = ImGuiTableFlags_ScrollY | ImGuiTableFlags_BordersV | ImGuiTableFlags_BordersOuterH | ImGuiTableFlags_RowBg | ImGuiTableFlags_NoBordersInBody;
		auto table_dims = ImGui::GetContentRegionAvail();

		auto text_color = WHITE;

		if (ImGui::BeginTable("UDTEnteryTable", n_columns, table_flags, table_dims))
		{
			ImGui::TableSetupScrollFreeze(0, 1);
			ImGui::TableSetupColumn("UDT", ImGuiTableColumnFlags_WidthStretch);
			ImGui::TableSetupColumn("Offset", ImGuiTableColumnFlags_WidthFixed);
			ImGui::TableSetupColumn("Size", ImGuiTableColumnFlags_WidthFixed);
			ImGui::TableSetupColumn("Type", ImGuiTableColumnFlags_WidthFixed);
			ImGui::TableHeadersRow();

			for (auto const& udt : udt_types)
			{
				auto has_fields = !udt.fields.empty();

				ImGui::TableNextRow();

				ImGui::TableSetColumnIndex(col_offset);
				ImGui::TextDisabled("--");

				ImGui::TableSetColumnIndex(col_size);
				ImGui::TextColored(text_color, "%u", udt.size);

				ImGui::TableSetColumnIndex(col_type);
				ImGui::TextDisabled("--");

				ImGui::TableSetColumnIndex(0);
				if (has_fields)
				{
					if (ImGui::TreeNode(udt.name))
					{
						for (auto const& f : udt.fields)
						{
							ImGui::TableNextRow();

							ImGui::TableSetColumnIndex(0);
							ImGui::TextColored(text_color, f.name);

							ImGui::TableSetColumnIndex(col_offset);
							ImGui::TextColored(text_color, "%u", f.offset);

							ImGui::TableSetColumnIndex(col_size);
							ImGui::TextDisabled("--");

							ImGui::TableSetColumnIndex(col_type);
							ImGui::TextColored(text_color, f.type);
						}
					}					
				}
				else
				{
					ImGui::TextColored(text_color, udt.name);
				}
			}

			ImGui::EndTable();
		}

		ImGui::End();
	}


	static void tag_window()
	{
		ImGui::Begin("Tags");

		ImGui::End();
	}
}


namespace command
{
	static void process_command(UI_Command const& cmd)
	{

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

			command::process_command(cmd);
		}		

		render::data_type_window(state.plc.data.data_types);

		render::udt_type_window(state.plc.udts);

		render::tag_window();

		return true;
	}
}