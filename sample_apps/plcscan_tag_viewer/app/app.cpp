#include "app.hpp"
#include "../../../src/imgui/imgui.h"
#include "../../../src/plcscan/plcscan.hpp"
#include "../../../src/util/time_helper.hpp"
#include "../../../src/util/qsprintf.hpp"

namespace tmh = time_helper;


constexpr auto DEFAULT_PLC_IP = "192.168.123.123";
constexpr auto DEFAULT_PLC_PATH = "1,0";
constexpr auto DEFAULT_CONNECT_TIMEOUT = "1000";


namespace
{
	using UI_DataType = plcscan::DataType;
	using UI_UdtType = plcscan::UdtType;
	using UI_UdtFieldType = plcscan::UdtFieldType;
	using UI_Tag = plcscan::Tag;


	class UI_StringTag
	{
	public:
		u32 tag_id;

		cstr name = 0;
		cstr type = 0;
		u32 size = 0;
		cstr value = 0;
	};


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

		List<UI_StringTag> string_tags;

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


/* transform */

namespace
{
	static void map_value(plcscan::Tag const& src, UI_StringTag& dst, u32 tag_id)
	{
		assert(dst.tag_id == tag_id);

		if (dst.tag_id != tag_id)
		{
			return;
		}

		dst.value = (cstr)src.bytes.begin;
	}


	UI_StringTag to_ui_string_tag(plcscan::Tag const& tag, u32 tag_id)
	{
		assert(plcscan::get_tag_type(tag.type_id) == plcscan::TagType::STRING);

		UI_StringTag ui{};

		ui.tag_id = tag_id;
		ui.name = tag.name();
		ui.type = tag.type();
		ui.size = tag.size();
		
		map_value(tag, ui, tag_id);

		return ui;
	}


	static void create_ui_tags(List<plcscan::Tag> const& tags, App_State& state)
	{
		using T = plcscan::TagType;

		for (u32 tag_id = 0; tag_id < (u32)tags.size(); ++tag_id)
		{
			auto& tag = tags[tag_id];

			switch (plcscan::get_tag_type(tag.type_id))
			{
			case T::STRING:
				state.string_tags.push_back(to_ui_string_tag(tag, tag_id));
				break;
			}
		}
	}
}


namespace render
{
	constexpr auto WHITE = ImVec4(1, 1, 1, 1);
	constexpr auto GREEN = ImVec4(0, 1, 0, 1);
	constexpr auto RED = ImVec4(1, 0, 0, 1);

	using ImColor = ImVec4;


	template <typename T>
	static T cast_bytes(u8* src, u32 size)
	{
		u8 b1 = 0;
		u16 b2 = 0;
		u32 b4 = 0;
		u64 b8 = 0;

		assert(size >= (u32)sizeof(T));

		switch (size) // sizeof(T) ?
		{
		case 1:
			b1 = *src;
			return *(T*)&b1;
		case 2:
			b2 = *(u16*)src;
			return *(T*)&b2;
		case 4:
			b4 = *(u32*)src;
			return *(T*)&b4;
		case 8:
			b8 = *(u64*)src;
			return *(T*)&b8;
		}

		assert(false);
		return (T)(*src);
	}


	static void bytes_as_number(ByteView const& bytes, plcscan::TagType type, ImColor const& text_color)
	{
		using T = plcscan::TagType;

		auto size = bytes.length;
		auto src = bytes.begin;

		switch (type)
		{
		case T::BOOL:
		case T::USINT:
			ImGui::TextColored(text_color, "%hhu", cast_bytes<u8>(src, size));
			break;

		case T::SINT:
			ImGui::TextColored(text_color, "%hhd", cast_bytes<i8>(src, size));
			break;

		case T::UINT:
			ImGui::TextColored(text_color, "%hu", cast_bytes<u16>(src, size));
			break;

		case T::INT:
			ImGui::TextColored(text_color, "%hd", cast_bytes<i16>(src, size));
			break;

		case T::UDINT:
			ImGui::TextColored(text_color, "%u", cast_bytes<u32>(src, size));
			break;

		case T::DINT:
			ImGui::TextColored(text_color, "%d", cast_bytes<i32>(src, size));
			break;

		case T::ULINT:
			ImGui::TextColored(text_color, "%llu", cast_bytes<u64>(src, size));
			break;

		case T::LINT:
			ImGui::TextColored(text_color, "%lld", cast_bytes<i64>(src, size));
			break;

		case T::REAL:
			ImGui::TextColored(text_color, "%f", cast_bytes<f32>(src, size));
			break;

		case T::LREAL:
			ImGui::TextColored(text_color, "%lf", cast_bytes<f64>(src, size));
			break;

		default:
			ImGui::TextColored(text_color, "error");
			break;
		}
	}


	static void bytes_as_hex(ByteView const& bytes, ImColor const& text_color)
	{
		constexpr int out_len = 20;
		char buffer[out_len + 1] = { 0 };

		auto len = (int)bytes.length;
		if (len < out_len / 2)
		{
			len = out_len / 2;
		}

		for (int i = 0; i < len; i++)
		{
			auto src = bytes.begin + i;
			auto dst = buffer + i * 2;
			qsnprintf(dst, 3, "%02x", src);
		}
		buffer[len] = NULL;

		ImGui::TextColored(text_color, buffer);
	}


	static void bytes_as_string(ByteView const& bytes, ImColor const& text_color)
	{
		ImGui::TextColored(text_color, (cstr)bytes.begin);
	}


	static void bytes_as_value(ByteView const& bytes, plcscan::TagType type, ImColor const& text_color)
	{
		using T = plcscan::TagType;

		switch (type)
		{
		case T::STRING:
			bytes_as_string(bytes, text_color);
			break;

		case T::UDT:
			assert(type != T::UDT && "UDT tags not displayed as values");
			break;

		case T::OTHER:
			bytes_as_hex(bytes, text_color);
			break;

		default:
			bytes_as_number(bytes, type, text_color);
		}
	}


	static void bytes_as_udt(ByteView const& bytes, ImColor const& text_color);


	static void bytes_as_array(ByteView const& bytes, plcscan::TagType type, u32 array_count, ImColor const& text_color, int id_col, int val_col)
	{
		auto is_udt = type == plcscan::TagType::UDT;

		MemoryOffset<u8> offset{};
		offset.begin = 0;
		offset.length = bytes.length / array_count;

		for (u32 i = 0; i < array_count; ++i)
		{
			ImGui::TableNextRow();

			ImGui::TableSetColumnIndex(id_col);
			ImGui::TextColored(text_color, "%u", i);

			ImGui::TableSetColumnIndex(val_col);
			auto elem_bytes = mb::sub_view(bytes, offset);
			if (is_udt)
			{
				bytes_as_udt(elem_bytes, text_color);
			}
			else
			{
				bytes_as_value(elem_bytes, type, text_color);
			}

			offset.begin += offset.length;
		}


	}


	static void bytes_as_udt(ByteView const& bytes, ImColor const& text_color)
	{
		ImGui::TextColored(text_color, "TODO");
	}


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

		if (ImGui::BeginTable("UDTTable", n_columns, table_flags, table_dims))
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

				ImGui::TableSetColumnIndex(col_name);
				if (has_fields)
				{
					if (ImGui::TreeNode(udt.name()))
					{
						for (auto const& f : udt.fields)
						{
							ImGui::TableNextRow();

							ImGui::TableSetColumnIndex(col_name);
							ImGui::TextColored(text_color, f.name());

							ImGui::TableSetColumnIndex(col_offset);
							ImGui::TextColored(text_color, "%u", f.offset);

							ImGui::TableSetColumnIndex(col_size);
							ImGui::TextDisabled("--");

							ImGui::TableSetColumnIndex(col_type);
							if (f.array_count > 0)
							{
								ImGui::TextColored(text_color, f.type());
							}
							else
							{
								ImGui::TextColored(text_color, "%s[%u]", f.type(), f.array_count);
							}
						}

						ImGui::TreePop();
					}
				}
				else
				{
					ImGui::TextColored(text_color, udt.name());
				}
			}

			ImGui::EndTable();
		}

		ImGui::End();
	}


	static void tag_window(List<UI_Tag> const& tags)
	{
		ImGui::Begin("Tags");

		constexpr int n_columns = 5;
		constexpr int col_name = 0;
		constexpr int col_type = 1;
		constexpr int col_size = 2;
		constexpr int col_id = 3;
		constexpr int col_value = 4;


		auto table_flags = ImGuiTableFlags_ScrollY | ImGuiTableFlags_BordersV | ImGuiTableFlags_BordersOuterH | ImGuiTableFlags_RowBg | ImGuiTableFlags_NoBordersInBody;
		auto table_dims = ImGui::GetContentRegionAvail();

		auto text_color = WHITE;

		if (ImGui::BeginTable("TagTable", n_columns, table_flags, table_dims))
		{
			ImGui::TableSetupScrollFreeze(0, 1);
			ImGui::TableSetupColumn("Tag", ImGuiTableColumnFlags_WidthStretch);
			ImGui::TableSetupColumn("Type", ImGuiTableColumnFlags_WidthFixed);
			ImGui::TableSetupColumn("Size", ImGuiTableColumnFlags_WidthFixed);
			ImGui::TableSetupColumn("Index", ImGuiTableColumnFlags_WidthFixed);
			ImGui::TableSetupColumn("Value", ImGuiTableColumnFlags_WidthFixed);
			ImGui::TableHeadersRow();

			for (auto const& tag : tags)
			{
				auto type = plcscan::get_tag_type(tag.type_id);

				ImGui::TableNextRow();

				if (tag.array_count > 1)
				{
					ImGui::TableSetColumnIndex(col_type);
					ImGui::TextColored(text_color, "%s[%u]", tag.type(), tag.array_count);

					ImGui::TableSetColumnIndex(col_size);
					ImGui::TextColored(text_color, "%u", tag.size());

					ImGui::TableSetColumnIndex(col_name);
					if (ImGui::TreeNode(tag.name()))
					{
						bytes_as_array(tag.bytes, type, tag.array_count, text_color, col_id, col_value);

						ImGui::TreePop();
					}
				}
				else if (type == plcscan::TagType::UDT)
				{
					ImGui::TableSetColumnIndex(col_type);
					ImGui::TextColored(text_color, tag.type());

					ImGui::TableSetColumnIndex(col_size);
					ImGui::TextColored(text_color, "%u", tag.size());

					ImGui::TableSetColumnIndex(col_value);
					bytes_as_udt(tag.bytes, text_color);
				}
				else
				{
					ImGui::TableSetColumnIndex(col_type);
					ImGui::TextColored(text_color, tag.type());

					ImGui::TableSetColumnIndex(col_size);
					ImGui::TextColored(text_color, "%u", tag.size());

					ImGui::TableSetColumnIndex(col_value);
					bytes_as_value(tag.bytes, type, text_color);
				}
			}

			ImGui::EndTable();
		}

		ImGui::End();
	}


	static void string_tag_window(List<UI_StringTag> const& tags)
	{
		ImGui::Begin("Strings");
		
		constexpr int col_name = 0;
		constexpr int col_type = 1;
		constexpr int col_size = 2;
		constexpr int col_value = 3;
		constexpr int n_columns = 4;


		auto table_flags = ImGuiTableFlags_ScrollY | ImGuiTableFlags_BordersV | ImGuiTableFlags_BordersOuterH | ImGuiTableFlags_RowBg | ImGuiTableFlags_NoBordersInBody;
		auto table_dims = ImGui::GetContentRegionAvail();

		auto text_color = WHITE;

		if (ImGui::BeginTable("StringTagTable", n_columns, table_flags, table_dims))
		{
			ImGui::TableSetupScrollFreeze(0, 1);
			ImGui::TableSetupColumn("Tag", ImGuiTableColumnFlags_WidthStretch);
			ImGui::TableSetupColumn("Type", ImGuiTableColumnFlags_WidthFixed);
			ImGui::TableSetupColumn("Size", ImGuiTableColumnFlags_WidthFixed);
			ImGui::TableSetupColumn("Value", ImGuiTableColumnFlags_WidthFixed);
			ImGui::TableHeadersRow();

			for (auto const& tag : tags)
			{
				ImGui::TableNextRow();

				ImGui::TableSetColumnIndex(col_name);
				ImGui::TextColored(text_color, "%s", tag.name);

				ImGui::TableSetColumnIndex(col_type);
				ImGui::TextColored(text_color, "%s", tag.type);

				ImGui::TableSetColumnIndex(col_size);
				ImGui::TextColored(text_color, "%u", tag.size);

				ImGui::TableSetColumnIndex(col_value);
				ImGui::TextColored(text_color, "%s", tag.value);
			}

			ImGui::EndTable();
		}

		ImGui::End();
	}
}


static App_State g_app_state;
static UI_Input g_user_input;


namespace scan
{
	static void map_ui_tag_values(plcscan::PlcTagData& data)
	{
		using T = plcscan::TagType;

		auto& tags = data.tags;
		auto& state = g_app_state;

		u32 string_id = 0;

		for (u32 tag_id = 0; tag_id < (u32)tags.size(); ++tag_id)
		{
			auto& tag = tags[tag_id];

			switch (plcscan::get_tag_type(tag.type_id))
			{
			case T::STRING:
				map_value(tag, state.string_tags[string_id++], tag_id);
				break;
			}
		}
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

		create_ui_tags(plc.data.tags, state);

		auto const is_scanning = [&]() { return plc.is_scanning && state.app_running; };

		plcscan::scan(map_ui_tag_values, is_scanning, plc.data);

		// TODO: stop/start
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

		render::udt_type_window(state.plc.data.udt_types);

		render::string_tag_window(state.string_tags);

		return true;
	}
}