#include "app.hpp"
#include "../../../src/imgui/imgui.h"
#include "../../../src/plcscan/plcscan.hpp"
#include "../../../src/util/time_helper.hpp"
#include "../../../src/util/qsprintf.hpp"
#include "../../../src/util/memory_helper.hpp"

namespace tmh = time_helper;
namespace mh = memory_helper;


constexpr auto DEFAULT_PLC_IP = "192.168.123.123";
constexpr auto DEFAULT_PLC_PATH = "1,0";
constexpr auto DEFAULT_CONNECT_TIMEOUT = "1000";


/* ui tag types */

namespace
{
	using UI_DataType = plcscan::DataType;
	using UI_UdtType = plcscan::UdtType;
	using UI_UdtFieldType = plcscan::UdtFieldType;



	class UI_Tag
	{
	public:
		u32 tag_id;

		cstr name = 0;
		cstr type = 0;
		u32 size = 0;

		StringView value_str;

		MemoryBuffer<char> value_data;
	};


	class UI_ArrayTag
	{
	public:
		u32 tag_id;

		cstr name = 0;
		cstr type = 0;
		u32 size = 0;
		u32 element_size = 0;

		List<StringView> values;

		MemoryBuffer<char> value_data;
	};


	class UI_UdtTag
	{
	public:
		u32 tag_id;

		cstr name = 0;
		cstr type = 0;
		u32 size = 0;

		// TODO: fields
		
		StringView value_str;

		MemoryBuffer<char> value_data;
	};


	class UI_UdtArrayTag
	{
	public:
		u32 tag_id;

		cstr name = 0;
		cstr type = 0;
		u32 size = 0;

		// TODO: fields
		
		List<StringView> values;

		MemoryBuffer<char> value_data;
	};
}


/* ui generic */

namespace
{
	static UI_Tag create_ui_tag(plcscan::Tag const& tag, u32 tag_id, u32 bytes_per_value)
	{
		UI_Tag ui{};

		ui.tag_id = tag_id;
		ui.name = tag.name();
		ui.type = tag.type();
		ui.size = tag.size();

		if (mb::create_buffer(ui.value_data, bytes_per_value))
		{
			ui.value_str = mh::push_cstr_view(ui.value_data, bytes_per_value);
		}

		return ui;
	}


	static void destroy_ui_tag(UI_Tag& ui_tag)
	{
		mb::destroy_buffer(ui_tag.value_data);
	}


	static void create_ui_array_tag_values(plcscan::Tag const& tag, UI_ArrayTag& ui_tag, u32 bytes_per_value)
	{
		if (!mb::create_buffer(ui_tag.value_data, tag.array_count * bytes_per_value))
		{
			return;
		}

		mb::zero_buffer(ui_tag.value_data);

		ui_tag.values.reserve(tag.array_count);
		for (u32 i = 0; i < tag.array_count; ++i)
		{
			ui_tag.values.push_back(mh::push_cstr_view(ui_tag.value_data, bytes_per_value));
		}
	}


	UI_ArrayTag create_ui_array_tag(plcscan::Tag const& tag, u32 tag_id, u32 bytes_per_value)
	{
		UI_ArrayTag ui{};

		ui.tag_id = tag_id;
		ui.name = tag.name();
		ui.type = tag.type();
		ui.size = tag.size();
		ui.element_size = tag.size() / tag.array_count;

		create_ui_array_tag_values(tag, ui, bytes_per_value);

		return ui;
	}


	static void destroy_ui_array_tag(UI_ArrayTag& ui_tag)
	{
		ui_tag.values.clear();
		mb::destroy_buffer(ui_tag.value_data);
	}
}


/* ui misc */

namespace
{
	static void map_hex(ByteView const& src, StringView const& dst)
	{
		auto dst_len = dst.length - 2;
		auto src_len = src.length;

		if (src_len > dst_len / 2)
		{
			src_len = dst_len / 2;
		}

		auto dst_data = dst.char_data;

		qsnprintf(dst_data, 3, "0x");
		dst_data += 2;

		for (u32 i = 0; i < src_len; i++)
		{
			auto s = *(int*)(src.data + i);
			auto d = dst_data + i * 2;
			qsnprintf(d, 3, "%02x", s);
		}

		dst.char_data[2 * src_len] = NULL;
	}


	static void map_value_hex(plcscan::Tag const& src, UI_Tag& dst, u32 tag_id)
	{
		assert(dst.tag_id == tag_id);

		if (dst.tag_id != tag_id)
		{
			return;
		}

		map_hex(src.value_bytes, dst.value_str);
	}


	static void map_value_hex(plcscan::Tag const& src, UI_ArrayTag& dst, u32 tag_id)
	{
		assert(dst.tag_id == tag_id);
		assert(src.is_array());
		assert((u32)dst.values.size() == src.array_count);

		if (dst.tag_id != tag_id || (u32)dst.values.size() != src.array_count)
		{
			return;
		}

		MemoryOffset<u8> offset{};
		offset.begin = 0;
		offset.length = src.value_bytes.length / src.array_count;

		for (u32 i = 0; i < src.array_count; ++i)
		{
			auto elem_bytes = mb::sub_view(src.value_bytes, offset);

			map_hex(elem_bytes, dst.values[i]);

			offset.begin += offset.length;
		}
	}
}


/* ui string */

namespace
{
	static void map_string(ByteView const& src, StringView const& dst)
	{
		auto len = src.length < dst.length ? src.length : dst.length;

		mh::copy_bytes(src.data, (u8*)dst.char_data, len);
	}


	static void map_value_string(plcscan::Tag const& src, UI_Tag& dst, u32 tag_id)
	{
		assert(plcscan::get_tag_type(src.type_id) == plcscan::TagType::STRING);
		assert(dst.tag_id == tag_id);

		if (dst.tag_id != tag_id)
		{
			return;
		}

		mh::zero_string(dst.value_str);
		map_string(src.value_bytes, dst.value_str);
	}


	static void map_value_string(plcscan::Tag const& src, UI_ArrayTag& dst, u32 tag_id)
	{
		assert(plcscan::get_tag_type(src.type_id) == plcscan::TagType::STRING);
		assert(dst.tag_id == tag_id);
		assert(src.is_array());
		assert((u32)dst.values.size() == src.array_count);

		if (dst.tag_id != tag_id || (u32)dst.values.size() != src.array_count)
		{
			return;
		}

		mb::zero_buffer(dst.value_data);

		MemoryOffset<u8> offset{};
		offset.begin = 0;
		offset.length = src.value_bytes.length / src.array_count;

		for (u32 i = 0; i < src.array_count; ++i)
		{
			auto src_bytes = mb::sub_view(src.value_bytes, offset);

			map_string(src_bytes, dst.values[i]);

			offset.begin += offset.length;
		}
	}
}


/* ui number */

namespace
{
	static void map_number(ByteView const& src, StringView const& dst, plcscan::TagType type)
	{
		using T = plcscan::TagType;

		switch (type)
		{
		case T::BOOL:
		case T::USINT:
			qsnprintf(dst.char_data, dst.length, "%hhu", mh::cast_bytes<u8>(src.data, src.length));
			break;

		case T::SINT:
			qsnprintf(dst.char_data, dst.length, "%hhd", mh::cast_bytes<i8>(src.data, src.length));
			break;

		case T::UINT:
			qsnprintf(dst.char_data, dst.length, "%hu", mh::cast_bytes<u16>(src.data, src.length));
			break;

		case T::INT:
			qsnprintf(dst.char_data, dst.length, "%hd", mh::cast_bytes<i16>(src.data, src.length));
			break;

		case T::UDINT:
			qsnprintf(dst.char_data, dst.length, "%u", mh::cast_bytes<u32>(src.data, src.length));
			break;

		case T::DINT:
			qsnprintf(dst.char_data, dst.length, "%d", mh::cast_bytes<i32>(src.data, src.length));
			break;

		case T::ULINT:
			qsnprintf(dst.char_data, dst.length, "%llu", mh::cast_bytes<u64>(src.data, src.length));
			break;

		case T::LINT:
			qsnprintf(dst.char_data, dst.length, "%lld", mh::cast_bytes<i64>(src.data, src.length));
			break;

		case T::REAL:
			qsnprintf(dst.char_data, dst.length, "%f", mh::cast_bytes<f32>(src.data, src.length));
			break;

		case T::LREAL:
			qsnprintf(dst.char_data, dst.length, "%lf", mh::cast_bytes<f64>(src.data, src.length));
			break;

		default:
			qsnprintf(dst.char_data, dst.length, "error");
			break;
		}
	}

	
	static void map_value_number(plcscan::Tag const& src, UI_Tag& dst, u32 tag_id)
	{
		assert(dst.tag_id == tag_id);

		if (dst.tag_id != tag_id)
		{
			return;
		}

		mh::zero_string(dst.value_str);
		map_number(src.value_bytes, dst.value_str, plcscan::get_tag_type(src.type_id));
	}


	static void map_value_number(plcscan::Tag const& src, UI_ArrayTag& dst, u32 tag_id)
	{
		assert(dst.tag_id == tag_id);
		assert(src.is_array());
		assert((u32)dst.values.size() == src.array_count);

		if (dst.tag_id != tag_id || (u32)dst.values.size() != src.array_count)
		{
			return;
		}

		auto type = plcscan::get_tag_type(src.type_id);

		MemoryOffset<u8> offset{};
		offset.begin = 0;
		offset.length = src.value_bytes.length / src.array_count;

		for (u32 i = 0; i < src.array_count; ++i)
		{
			auto elem_bytes = mb::sub_view(src.value_bytes, offset);

			map_number(elem_bytes, dst.values[i], type);

			offset.begin += offset.length;
		}
	}
}


/* ui udt */

namespace
{
	static UI_UdtTag create_ui_tag_udt(plcscan::Tag const& tag, u32 tag_id, u32 bytes_per_value)
	{
		UI_UdtTag ui{};

		ui.tag_id = tag_id;
		ui.name = tag.name();
		ui.type = tag.type();
		ui.size = tag.size();

		if (mb::create_buffer(ui.value_data, bytes_per_value))
		{
			ui.value_str = mh::push_cstr_view(ui.value_data, bytes_per_value);
		}

		return ui;
	}


	static void destroy_ui_tag_udt(UI_UdtTag& ui_tag)
	{
		mb::destroy_buffer(ui_tag.value_data);
	}


	static void create_ui_array_tag_values_udt(plcscan::Tag const& tag, UI_UdtArrayTag& ui_tag, u32 bytes_per_value)
	{
		if (!mb::create_buffer(ui_tag.value_data, tag.array_count * bytes_per_value))
		{
			return;
		}

		mb::zero_buffer(ui_tag.value_data);

		ui_tag.values.reserve(tag.array_count);
		for (u32 i = 0; i < tag.array_count; ++i)
		{
			ui_tag.values.push_back(mh::push_cstr_view(ui_tag.value_data, bytes_per_value));
		}
	}


	UI_UdtArrayTag create_ui_array_tag_udt(plcscan::Tag const& tag, u32 tag_id, u32 bytes_per_value)
	{
		UI_UdtArrayTag ui{};

		ui.tag_id = tag_id;
		ui.name = tag.name();
		ui.type = tag.type();
		ui.size = tag.size();

		create_ui_array_tag_values_udt(tag, ui, bytes_per_value);

		return ui;
	}


	static void destroy_ui_array_tag_udt(UI_UdtArrayTag& ui_tag)
	{
		ui_tag.values.clear();
		mb::destroy_buffer(ui_tag.value_data);
	}


	static void map_value_udt(plcscan::Tag const& src, UI_UdtTag& dst, u32 tag_id)
	{
		assert(dst.tag_id == tag_id);

		if (dst.tag_id != tag_id)
		{
			return;
		}

		map_hex(src.value_bytes, dst.value_str);
	}


	static void map_value_udt(plcscan::Tag const& src, UI_UdtArrayTag& dst, u32 tag_id)
	{
		assert(dst.tag_id == tag_id);
		assert(src.is_array());
		assert((u32)dst.values.size() == src.array_count);

		if (dst.tag_id != tag_id || (u32)dst.values.size() != src.array_count)
		{
			return;
		}

		MemoryOffset<u8> offset{};
		offset.begin = 0;
		offset.length = src.value_bytes.length / src.array_count;

		for (u32 i = 0; i < src.array_count; ++i)
		{
			auto elem_bytes = mb::sub_view(src.value_bytes, offset);

			map_hex(elem_bytes, dst.values[i]);

			offset.begin += offset.length;
		}
	}
}


/* ui state */

namespace
{
	class PLC_State
	{
	public:

		plcscan::PlcTagData data;

		bool is_initializing = false;
		bool start_scanning = false;
		bool is_connected = false;
		bool is_scanning = false;
		bool has_error = false;
	};


	class UI_Input
	{
	public:
		StringView plc_ip;
		StringView plc_path;

		MemoryBuffer<char> string_data;
	};


	class App_State
	{
	public:
		
		PLC_State plc;

		List<UI_Tag> string_tags;
		List<UI_ArrayTag> string_array_tags;

		List<UI_Tag> misc_tags;
		List<UI_ArrayTag> misc_array_tags;

		List<UI_Tag> number_tags;
		List<UI_ArrayTag> number_array_tags;

		List<UI_UdtTag> udt_tags;
		List<UI_UdtArrayTag> udt_array_tags;

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


/* ui state create destroy */

namespace
{	
	constexpr u32 UI_PLC_IP_BYTES = (u32)sizeof(DEFAULT_PLC_IP) + 10;
	constexpr u32 UI_PLC_PATH_BYTES = (u32)sizeof(DEFAULT_PLC_PATH) + 10;

	constexpr u32 UI_MISC_BYTES_PER_VALUE = 20 + 1;
	constexpr u32 UI_STRING_BYTES_PER_VALUE = 20 + 1;
	constexpr u32 UI_NUMBER_BYTES_PER_VALUE = 20 + 1;
	constexpr u32 UI_UDT_BYTES_PER_VALUE = 20 + 1;


	static void create_ui_input(UI_Input& input)
	{
		auto str_bytes = UI_PLC_IP_BYTES + UI_PLC_PATH_BYTES;

		if (!mb::create_buffer(input.string_data, str_bytes))
		{
			return;
		}

		mb::zero_buffer(input.string_data);

		input.plc_ip = mh::push_cstr_view(input.string_data, UI_PLC_IP_BYTES);
		input.plc_path = mh::push_cstr_view(input.string_data, UI_PLC_PATH_BYTES);

		mh::copy_unsafe((char*)DEFAULT_PLC_IP, input.plc_ip, (u32)strlen(DEFAULT_PLC_IP));
		mh::copy_unsafe((char*)DEFAULT_PLC_PATH, input.plc_path, (u32)strlen(DEFAULT_PLC_PATH));
	}


	static void destroy_ui_input(UI_Input& input)
	{
		mb::destroy_buffer(input.string_data);
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
				if (tag.is_array())
				{
					state.string_array_tags.push_back(create_ui_array_tag(tag, tag_id, UI_STRING_BYTES_PER_VALUE));
				}
				else
				{
					state.string_tags.push_back(create_ui_tag(tag, tag_id, UI_STRING_BYTES_PER_VALUE));
				}				
				break;

			case T::UDT:
				if (tag.is_array())
				{
					state.udt_array_tags.push_back(create_ui_array_tag_udt(tag, tag_id, UI_UDT_BYTES_PER_VALUE));
				}
				else
				{
					state.udt_tags.push_back(create_ui_tag_udt(tag, tag_id, UI_UDT_BYTES_PER_VALUE));
				}
				break;

			case T::MISC:
				if (tag.is_array())
				{
					state.misc_array_tags.push_back(create_ui_array_tag(tag, tag_id, UI_MISC_BYTES_PER_VALUE));
				}
				else
				{
					state.misc_tags.push_back(create_ui_tag(tag, tag_id, UI_MISC_BYTES_PER_VALUE));
				}
				break;

			default:
				if (tag.is_array())
				{
					state.number_array_tags.push_back(create_ui_array_tag(tag, tag_id, UI_NUMBER_BYTES_PER_VALUE));
				}
				else
				{
					state.number_tags.push_back(create_ui_tag(tag, tag_id, UI_MISC_BYTES_PER_VALUE));
				}
				break;
			}
		}
	}


	static void destroy_ui_tags(App_State& state)
	{
		for (auto& tag : state.string_tags)
		{
			destroy_ui_tag(tag);
		}

		for (auto& tag : state.string_array_tags)
		{
			destroy_ui_array_tag(tag);
		}

		for (auto& tag : state.misc_tags)
		{
			destroy_ui_tag(tag);
		}

		for (auto& tag : state.misc_array_tags)
		{
			destroy_ui_array_tag(tag);
		}

		for (auto& tag : state.number_tags)
		{
			destroy_ui_tag(tag);
		}

		for (auto& tag : state.number_array_tags)
		{
			destroy_ui_array_tag(tag);
		}

		for (auto& tag : state.udt_tags)
		{
			destroy_ui_tag_udt(tag);
		}

		for (auto& tag : state.udt_array_tags)
		{
			destroy_ui_array_tag_udt(tag);
		}
	}
}


/* ui commands */

namespace command
{
	static void process_command(UI_Command const& cmd, App_State& state)
	{
		if (cmd.stop_app_running)
		{
			state.app_running = false;
		}
		else if (cmd.start_scanning)
		{
			state.plc.start_scanning = true;
		}
		else if (cmd.stop_scanning)
		{
			state.plc.is_scanning = false;
		}
	}
}


namespace render
{
	constexpr auto WHITE = ImVec4(1, 1, 1, 1);
	constexpr auto GREEN = ImVec4(0, 1, 0, 1);
	constexpr auto RED = ImVec4(1, 0, 0, 1);

	using ImColor = ImVec4;


	static int numeric_or_dot(ImGuiInputTextCallbackData* data)
	{
		auto c = data->EventChar;

		if ((c < '0' || c >'9') && c != '.' && c != '\0')
		{
			return 1;
		}

		return 0;
	}


	static int numeric_or_comma(ImGuiInputTextCallbackData* data)
	{
		auto c = data->EventChar;

		if ((c < '0' || c >'9') && c != ',' && c != '\0')
		{
			return 1;
		}

		return 0;
	}


	static void data_type_table(List<UI_DataType> const& data_types)
	{		
		constexpr int col_name = 0;
		constexpr int col_desc = 1;
		constexpr int col_size = 2;
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

				ImGui::TableSetColumnIndex(col_name);
				ImGui::TextColored(text_color, "%s", dt.name());

				ImGui::TableSetColumnIndex(col_desc);
				ImGui::TextColored(text_color, "%s", dt.description());

				ImGui::TableSetColumnIndex(col_size);
				ImGui::TextColored(text_color, "%u", dt.size);
			}

			ImGui::EndTable();
		}
	}


	static void udt_type_table(List<UI_UdtType> const& udt_types)
	{	
		constexpr int col_name = 0;
		constexpr int col_offset = 1;
		constexpr int col_size = 2;
		constexpr int col_type = 3;
		constexpr int n_columns = 4;

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
							if (f.is_array())
							{
								ImGui::TextColored(text_color, "%s[%u]", f.type(), f.array_count);
							}
							else
							{
								ImGui::TextColored(text_color, f.type());
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
	}
	

	static void ui_tag_table(List<UI_Tag> const& tags, cstr label)
	{
		constexpr int col_name = 0;
		constexpr int col_type = 1;
		constexpr int col_size = 2;
		constexpr int col_value = 3;
		constexpr int n_columns = 4;

		auto table_flags = ImGuiTableFlags_ScrollY | ImGuiTableFlags_BordersV | ImGuiTableFlags_BordersOuterH | ImGuiTableFlags_RowBg | ImGuiTableFlags_NoBordersInBody;
		auto table_dims = ImGui::GetContentRegionAvail();
		table_dims.y /= 2;

		auto text_color = WHITE;

		if (ImGui::BeginTable(label, n_columns, table_flags, table_dims))
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
				ImGui::TextColored(text_color, "%s", tag.value_str.data());
			}

			ImGui::EndTable();
		}
	}


	static void ui_tag_array_table(List<UI_ArrayTag> const& tags, cstr label)
	{
		constexpr int col_name = 0;
		constexpr int col_type = 1;
		constexpr int col_size = 2;
		constexpr int col_value = 3;
		constexpr int n_columns = 4;

		auto table_flags = ImGuiTableFlags_ScrollY | ImGuiTableFlags_BordersV | ImGuiTableFlags_BordersOuterH | ImGuiTableFlags_RowBg | ImGuiTableFlags_NoBordersInBody;
		auto table_dims = ImGui::GetContentRegionAvail();

		auto text_color = WHITE;

		if (ImGui::BeginTable(label, n_columns, table_flags, table_dims))
		{
			ImGui::TableSetupScrollFreeze(0, 1);
			ImGui::TableSetupColumn("Tag", ImGuiTableColumnFlags_WidthStretch);
			ImGui::TableSetupColumn("Type", ImGuiTableColumnFlags_WidthFixed);
			ImGui::TableSetupColumn("Size", ImGuiTableColumnFlags_WidthFixed);
			ImGui::TableSetupColumn("Value", ImGuiTableColumnFlags_WidthFixed);
			ImGui::TableHeadersRow();

			for (auto const& tag : tags)
			{
				auto array_count = (u32)tag.values.size();

				ImGui::TableNextRow();

				ImGui::TableSetColumnIndex(col_type);
				ImGui::TextColored(text_color, "%s", tag.type);

				ImGui::TableSetColumnIndex(col_size);
				ImGui::TextColored(text_color, "%u", tag.size);

				ImGui::TableSetColumnIndex(col_value);
				ImGui::TextDisabled("--");

				ImGui::TableSetColumnIndex(col_name);
				if (ImGui::TreeNode(tag.name))
				{
					for (u32 i = 0; i < array_count; ++i)
					{
						ImGui::TableNextRow();

						ImGui::TableSetColumnIndex(col_name);
						ImGui::TextColored(text_color, "    %s[%u]", tag.name, i);

						ImGui::TableSetColumnIndex(col_size);
						ImGui::TextColored(text_color, "%u", tag.element_size);

						ImGui::TableSetColumnIndex(col_value);
						ImGui::TextColored(text_color, "%s", tag.values[i].data());
					}

					ImGui::TreePop();
				}
			}

			ImGui::EndTable();
		}
	}


	static void ui_udt_tag_table(List<UI_UdtTag> const& tags, cstr label)
	{
		constexpr int col_name = 0;
		constexpr int col_type = 1;
		constexpr int col_size = 2;
		constexpr int col_value = 3;
		constexpr int n_columns = 4;


		auto table_flags = ImGuiTableFlags_ScrollY | ImGuiTableFlags_BordersV | ImGuiTableFlags_BordersOuterH | ImGuiTableFlags_RowBg | ImGuiTableFlags_NoBordersInBody;
		auto table_dims = ImGui::GetContentRegionAvail();
		table_dims.y /= 2;

		auto text_color = WHITE;

		if (ImGui::BeginTable(label, n_columns, table_flags, table_dims))
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

				ImGui::TableSetColumnIndex(col_type);
				ImGui::TextColored(text_color, "%s", tag.type);

				ImGui::TableSetColumnIndex(col_size);
				ImGui::TextColored(text_color, "%u", tag.size);

				// temp
				ImGui::TableSetColumnIndex(col_name);
				ImGui::TextColored(text_color, "%s", tag.name);				

				ImGui::TableSetColumnIndex(col_value);
				ImGui::TextColored(text_color, "%s", tag.value_str.data());

				// TODO
				//ImGui::TableSetColumnIndex(col_value);
				//ImGui::TextDisabled("--");

				//ImGui::TableSetColumnIndex(col_name);				
				//if (ImGui::TreeNode(tag.name))
				//{
				//	// fields

				//	ImGui::TreePop();
				//}
			}

			ImGui::EndTable();
		}
	}


	static void ui_udt_tag_array_table(List<UI_UdtArrayTag> const& tags, cstr label)
	{
		constexpr int col_name = 0;
		constexpr int col_type = 1;
		constexpr int col_size = 2;
		constexpr int col_index = 3;
		constexpr int col_value = 4;
		constexpr int n_columns = 5;

		auto table_flags = ImGuiTableFlags_ScrollY | ImGuiTableFlags_BordersV | ImGuiTableFlags_BordersOuterH | ImGuiTableFlags_RowBg | ImGuiTableFlags_NoBordersInBody;
		auto table_dims = ImGui::GetContentRegionAvail();

		auto text_color = WHITE;

		if (ImGui::BeginTable(label, n_columns, table_flags, table_dims))
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
				auto array_count = (u32)tag.values.size();

				ImGui::TableNextRow();

				ImGui::TableSetColumnIndex(col_type);
				ImGui::TextColored(text_color, "%s", tag.type);

				ImGui::TableSetColumnIndex(col_size);
				ImGui::TextColored(text_color, "%u", tag.size);

				ImGui::TableSetColumnIndex(col_index);
				ImGui::TextColored(text_color, "[%u]", array_count);

				ImGui::TableSetColumnIndex(col_value);
				ImGui::TextDisabled("--");

				ImGui::TableSetColumnIndex(col_name);
				if (ImGui::TreeNode(tag.name))
				{
					for (u32 i = 0; i < array_count; ++i)
					{
						ImGui::TableNextRow();						

						// temp
						ImGui::TableSetColumnIndex(col_index);
						ImGui::TextColored(text_color, "[%u]", i);
						
						ImGui::TableSetColumnIndex(col_value);
						ImGui::TextColored(text_color, "%s", tag.values[i].data());

						// TODO:
						//ImGui::TableSetColumnIndex(col_value);
						//ImGui::TextDisabled("--");

						//ImGui::TableSetColumnIndex(col_index);						
						//if (ImGui::TreeNode(tag.name /* does this work?*/, "[%u]", i))
						//{
						//	// fields

						//	ImGui::TreePop();
						//}
					}

					ImGui::TreePop();
				}
			}

			ImGui::EndTable();
		}
	}
	
	
	static void command_window(App_State const& state, UI_Command& cmd, UI_Input& input)
	{
		ImGui::Begin("Controller");

		ImGui::SetNextItemWidth(150);
		ImGui::InputText("IP", input.plc_ip.char_data, input.plc_ip.length, ImGuiInputTextFlags_CallbackCharFilter, numeric_or_dot);

		ImGui::SetNextItemWidth(150);
		ImGui::InputText("Path", input.plc_path.char_data, input.plc_path.length, ImGuiInputTextFlags_CallbackCharFilter, numeric_or_comma);

		ImGui::SameLine();
		ImGui::SetNextItemWidth(150);

		ImGui::SetCursorPos(ImVec2(250, 39));
		if (ImGui::Button("Go", ImVec2(50, 30)))
		{
			cmd.start_scanning = true;
		}

		ImGui::SetCursorPos(ImVec2(320, 57));

		if (state.plc.is_scanning)
		{
			ImGui::TextColored(GREEN, "OK");
		}

		if (state.plc.has_error)
		{
			ImGui::TextColored(RED, "ERROR");
		}

		ImGui::End();
	}


	static void data_types_window(App_State const& state)
	{
		ImGui::Begin("Data Types");

		ImGuiTabBarFlags tab_bar_flags = ImGuiTabBarFlags_None;
		if (ImGui::BeginTabBar("DataTypesTabBar", tab_bar_flags))
		{
			if (ImGui::BeginTabItem("Types"))
			{
				data_type_table(state.plc.data.data_types);

				ImGui::EndTabItem();
			}

			if (ImGui::BeginTabItem("UDT"))
			{
				udt_type_table(state.plc.data.udt_types);

				ImGui::EndTabItem();
			}

			ImGui::EndTabBar();
		}

		ImGui::End();
	}


	static void tag_window(App_State const& state)
	{
		ImGui::Begin("Tags");

		ImGuiTabBarFlags tab_bar_flags = ImGuiTabBarFlags_None;
		if (ImGui::BeginTabBar("TagsTabBar", tab_bar_flags))
		{
			if (ImGui::BeginTabItem("Numeric"))
			{
				if (ImGui::CollapsingHeader("Tags"))
				{
					ui_tag_table(state.number_tags, "NumericTagTable");
				}

				if (ImGui::CollapsingHeader("Arrays"))
				{
					ui_tag_array_table(state.number_array_tags, "NumericArrayTagTable");
				}

				ImGui::EndTabItem();
			}

			if (ImGui::BeginTabItem("String"))
			{
				if (ImGui::CollapsingHeader("Tags"))
				{
					ui_tag_table(state.string_tags, "StringTagTable");
				}

				if (ImGui::CollapsingHeader("Arrays"))
				{
					ui_tag_array_table(state.string_array_tags, "StringArrayTagTable");
				}

				ImGui::EndTabItem();
			}

			if (ImGui::BeginTabItem("UDT"))
			{
				if (ImGui::CollapsingHeader("Tags"))
				{
					ui_udt_tag_table(state.udt_tags, "UdtTagTable");
				}

				if (ImGui::CollapsingHeader("Arrays"))
				{
					ui_udt_tag_array_table(state.udt_array_tags, "UdtArrayTagTable");
				}

				ImGui::EndTabItem();
			}


			if (ImGui::BeginTabItem("Misc"))
			{
				if (ImGui::CollapsingHeader("Tags"))
				{
					ui_tag_table(state.misc_tags, "MiscTableTable");
				}

				if (ImGui::CollapsingHeader("Arrays"))
				{
					ui_tag_array_table(state.misc_array_tags, "MiscArrayTagTable");
				}

				ImGui::EndTabItem();
			}


			ImGui::EndTabBar();
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
		u32 string_array_id = 0;

		u32 misc_id = 0;
		u32 misc_array_id = 0;

		u32 udt_id = 0;
		u32 udt_array_id = 0;

		u32 number_id = 0;
		u32 number_array_id = 0;

		for (u32 tag_id = 0; tag_id < (u32)tags.size(); ++tag_id)
		{
			auto& tag = tags[tag_id];

			switch (plcscan::get_tag_type(tag.type_id))
			{
			case T::STRING:
				if (tag.is_array())
				{
					map_value_string(tag, state.string_array_tags[string_array_id++], tag_id);
				}
				else
				{
					map_value_string(tag, state.string_tags[string_id++], tag_id);
				}				
				break;

			case T::UDT:
				if (tag.is_array())
				{
					map_value_udt(tag, state.udt_array_tags[udt_array_id++], tag_id);
				}
				else
				{
					map_value_udt(tag, state.udt_tags[udt_id++], tag_id);
				}
				break;

			case T::MISC:
				if (tag.is_array())
				{
					map_value_hex(tag, state.misc_array_tags[misc_array_id++], tag_id);
				}
				else
				{
					map_value_hex(tag, state.misc_tags[udt_id++], tag_id);
				}
				break;

			default:
				if (tag.is_array())
				{
					map_value_number(tag, state.number_array_tags[number_array_id++], tag_id);
				}
				else
				{
					map_value_number(tag, state.number_tags[number_id++], tag_id);
				}
				break;
			}

		}
	}


	static void start()
	{
		auto& state = g_app_state;
		auto& input = g_user_input;
		auto& plc = state.plc;

		plc.is_initializing = true;

		plc.data = plcscan::init();
		if (!plc.data.is_init)
		{
			plc.has_error = true;
			return;
		}

		plc.is_initializing = false;

		plc.start_scanning = false;
		plc.is_scanning = false;

		while (!plc.start_scanning)
		{
			if (!state.app_running)
			{
				return;
			}

			tmh::delay_current_thread_ms(20);
		}

		plc.is_connected = false;

		while (!plc.is_connected)
		{
			if (!state.app_running)
			{
				return;
			}

			if (plc.start_scanning)
			{
				plc.start_scanning = false;

				if (plcscan::connect(input.plc_ip.data(), input.plc_path.data(), plc.data))
				{
					plc.is_connected = true;
					plc.has_error = false;
				}
				else
				{
					plc.has_error = true;
				}
			}

			tmh::delay_current_thread_ms(20);
		}

		create_ui_tags(plc.data.tags, state);

		auto const is_scanning = [&]() { return plc.is_scanning && state.app_running; };

		plc.is_scanning = true;

		plcscan::scan(map_ui_tag_values, is_scanning, plc.data);

		// TODO: stop/start
	}
}


namespace app
{
	void init()
	{
		create_ui_input(g_user_input);
	}


	void shutdown()
	{
		g_app_state.app_running = false;

		plcscan::shutdown();
		destroy_ui_tags(g_app_state);
		destroy_ui_input(g_user_input);
	}


	std::thread start_worker()
	{
		g_app_state.app_running = true;

		auto worker = std::thread(scan::start);

		return worker;
	}


	bool render_ui()
	{
		auto& state = g_app_state;
		auto& input = g_user_input;

		UI_Command cmd{};

		ImGui::DockSpaceOverViewport(nullptr, ImGuiDockNodeFlags_None);

		render::command_window(state, cmd, input);

		if (cmd.has_command())
		{
			if (cmd.stop_app_running)
			{
				return false;
			}

			command::process_command(cmd, state);
		}		

		render::data_types_window(state);

		render::tag_window(state);

		return true;
	}
}