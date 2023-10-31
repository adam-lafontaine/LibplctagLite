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
		cstr name = 0;
		cstr type = 0;
		u32 size = 0;

		plcscan::DataTypeId32 type_id = 0;

		ByteView value_bytes;
		StringView value_str;

		MemoryBuffer<char> value_data;
	};


	class UI_ArrayTagElement
	{
	public:
		ByteView value_bytes;
		StringView value_str;
	};


	class UI_ArrayTag
	{
	public:
		cstr name = 0;
		cstr type = 0;
		u32 size = 0;
		u32 element_size = 0;

		plcscan::DataTypeId32 type_id = 0;
		
		List<UI_ArrayTagElement> elements;

		MemoryBuffer<char> value_data;
	};


	class UI_UdtField
	{
	public:
		cstr name = 0;
		cstr type = 0;

		plcscan::DataTypeId32 type_id = 0;

		ByteView value_bytes;

		StringView value_str;

		u32 size() const { return value_bytes.length; }
	};


	class UI_UdtTag
	{
	public:

		cstr name = 0;
		cstr type = 0;
		u32 size = 0;

		List<UI_UdtField> fields;

		MemoryBuffer<char> value_data;
	};


	class UI_UdtArrayTagElement
	{
	public:
		List<UI_UdtField> fields;
	};


	class UI_UdtArrayTag
	{
	public:
		cstr name = 0;
		cstr type = 0;
		u32 size = 0;

		u32 element_size = 0;

		// TODO: fields
		
		List<UI_UdtArrayTagElement> elements;

		MemoryBuffer<char> value_data;
	};
}


/* create destroy ui tags */

namespace
{
	static UI_Tag create_ui_tag(plcscan::Tag const& tag, u32 bytes_per_value)
	{
		UI_Tag ui{};

		ui.name = tag.name();
		ui.type = tag.type();
		ui.size = tag.size();

		ui.type_id = tag.type_id;
		ui.value_bytes = tag.value_bytes;

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


	static void create_ui_array_tag_elements(plcscan::Tag const& tag, UI_ArrayTag& ui_tag, u32 bytes_per_value)
	{
		if (!mb::create_buffer(ui_tag.value_data, tag.array_count * bytes_per_value))
		{
			return;
		}

		mb::zero_buffer(ui_tag.value_data);

		MemoryOffset offset{};
		offset.begin = 0;
		offset.length = ui_tag.element_size;

		ui_tag.elements.reserve(tag.array_count);
		for (u32 i = 0; i < tag.array_count; ++i)
		{
			ui_tag.elements.push_back({
				mb::sub_view(tag.value_bytes, offset),
				mh::push_cstr_view(ui_tag.value_data, bytes_per_value)
			});

			offset.begin += offset.length;
		}
	}


	UI_ArrayTag create_ui_array_tag(plcscan::Tag const& tag, u32 bytes_per_value)
	{
		UI_ArrayTag ui{};

		ui.name = tag.name();
		ui.type = tag.type();
		ui.size = tag.size();
		ui.element_size = tag.size() / tag.array_count;

		ui.type_id = tag.type_id;

		create_ui_array_tag_elements(tag, ui, bytes_per_value);

		return ui;
	}


	static void destroy_ui_array_tag(UI_ArrayTag& ui_tag)
	{
		ui_tag.elements.clear();
		mb::destroy_buffer(ui_tag.value_data);
	}
}


/* create destroy ui udt tags */

namespace
{
	static UI_UdtTag create_ui_tag_udt(plcscan::Tag const& tag, plcscan::UdtType const& udt_def, u32 bytes_per_value)
	{
		UI_UdtTag ui{};

		ui.name = tag.name();
		ui.type = tag.type();
		ui.size = tag.size();

		auto n_out_bytes = bytes_per_value * udt_def.fields.size();
		
		if (!mb::create_buffer(ui.value_data, n_out_bytes))
		{
			return ui;
		}

		mb::zero_buffer(ui.value_data);

		auto end = udt_def.size;
		assert(end == tag.value_bytes.length);

		ui.fields.reserve(udt_def.fields.size());

		for (int i = (int)udt_def.fields.size() - 1; i >= 0; --i)
		{
			auto& f = udt_def.fields[i];

			UI_UdtField field{};
			field.name = f.name();
			field.type = f.type();
			field.type_id = f.type_id;

			field.value_str = mh::push_cstr_view(ui.value_data, bytes_per_value);

			MemoryOffset m_offset{};
			m_offset.begin = f.offset;
			m_offset.length = end - f.offset;

			end = f.offset;

			field.value_bytes = mb::sub_view(tag.value_bytes, m_offset);

			ui.fields.push_back(field);
		}		

		return ui;
	}


	static void destroy_ui_tag_udt(UI_UdtTag& ui_tag)
	{
		mb::destroy_buffer(ui_tag.value_data);
	}


	static void create_ui_array_tag_elements_udt(plcscan::Tag const& tag, plcscan::UdtType const& udt_def, UI_UdtArrayTag& ui_tag, u32 bytes_per_value)
	{
		auto const n_fields = (u32)udt_def.fields.size();
		if (!mb::create_buffer(ui_tag.value_data, tag.array_count * bytes_per_value * n_fields))
		{
			return;
		}

		mb::zero_buffer(ui_tag.value_data);

		MemoryOffset offset{};
		offset.begin = 0;
		offset.length = ui_tag.element_size;

		ui_tag.elements.reserve(tag.array_count);
		for (u32 i = 0; i < tag.array_count; ++i)
		{
			UI_UdtArrayTagElement e{};

			auto end = udt_def.size;
			assert(end = ui_tag.element_size);

			e.fields.reserve(n_fields);
			for (int j = (int)n_fields - 1; j >= 0; --j)
			{
				auto& f = udt_def.fields[j];

				UI_UdtField field{};
				field.name = f.name();
				field.type = f.type();
				field.type_id = f.type_id;

				field.value_str = mh::push_cstr_view(ui_tag.value_data, bytes_per_value);

				MemoryOffset m_offset{};
				m_offset.begin = f.offset;
				m_offset.length = end - f.offset;

				end = f.offset;

				field.value_bytes = mb::sub_view(tag.value_bytes, m_offset);

				e.fields.push_back(field);
			}

			ui_tag.elements.push_back(e);

			offset.begin += offset.length;
		}
	}


	UI_UdtArrayTag create_ui_array_tag_udt(plcscan::Tag const& tag, plcscan::UdtType const& udt_def, u32 bytes_per_value)
	{
		UI_UdtArrayTag ui{};

		ui.name = tag.name();
		ui.type = tag.type();
		ui.size = tag.size();
		ui.element_size = tag.size() / tag.array_count;

		create_ui_array_tag_elements_udt(tag, udt_def, ui, bytes_per_value);

		return ui;
	}


	static void destroy_ui_array_tag_udt(UI_UdtArrayTag& ui_tag)
	{
		ui_tag.elements.clear();
		mb::destroy_buffer(ui_tag.value_data);
	}
}


/* map bytes */

namespace
{
	static void map_string(ByteView const& src, StringView const& dst)
	{
		auto len = src.length < dst.length ? src.length : dst.length;

		mh::copy_bytes(src.data, (u8*)dst.char_data, len);
	}


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


	static void map_value(ByteView const& src, StringView const& dst, plcscan::TagType type)
	{
		using T = plcscan::TagType;

		switch (type)
		{
		case T::STRING:
			map_string(src, dst);
			break;

		case T::UDT:
			map_hex(src, dst);
			break;

		case T::MISC:
			map_hex(src, dst);
			break;

		case T::BOOL:
			// not supported
			break;

		default:
			map_number(src, dst, type);
			break;
		}
	}


	static void map_tag_value(UI_Tag const& ui)
	{
		mh::zero_string(ui.value_str);
		map_value(ui.value_bytes, ui.value_str, plcscan::get_tag_type(ui.type_id));
	}


	static void map_tag_value(UI_ArrayTag const& ui)
	{
		auto type = plcscan::get_tag_type(ui.type_id);

		mb::zero_buffer(ui.value_data);

		for (auto const& e : ui.elements)
		{
			map_value(e.value_bytes, e.value_str, type);
		}
	}


	static void map_tag_value(UI_UdtTag const& ui)
	{
		mb::zero_buffer(ui.value_data);

		for (auto const& f : ui.fields)
		{
			map_value(f.value_bytes, f.value_str, plcscan::get_tag_type(f.type_id));
		}
	}


	static void map_tag_value(UI_UdtArrayTag const& ui)
	{
		mb::zero_buffer(ui.value_data);

		for (auto const& e : ui.elements)
		{
			for (auto const& f : e.fields)
			{
				map_value(f.value_bytes, f.value_str, plcscan::get_tag_type(f.type_id));
			}
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

		auto udt_begin = state.plc.data.udt_types.begin();
		auto udt_end = state.plc.data.udt_types.end();


		for (u32 tag_id = 0; tag_id < (u32)tags.size(); ++tag_id)
		{
			auto& tag = tags[tag_id];

			switch (plcscan::get_tag_type(tag.type_id))
			{
			case T::STRING:
				if (tag.is_array())
				{
					state.string_array_tags.push_back(create_ui_array_tag(tag, UI_STRING_BYTES_PER_VALUE));
				}
				else
				{
					state.string_tags.push_back(create_ui_tag(tag, UI_STRING_BYTES_PER_VALUE));
				}				
				break;

			case T::UDT:
			{
				// get udt type
				auto const pred = [&](auto const& t) { return t.type_id == tag.type_id; };
				auto it = std::find_if(udt_begin, udt_end, pred);

				if (it != udt_end)
				{
					if (tag.is_array())
					{
						state.udt_array_tags.push_back(create_ui_array_tag_udt(tag, *it, UI_UDT_BYTES_PER_VALUE));
					}
					else
					{
						state.udt_tags.push_back(create_ui_tag_udt(tag, *it, UI_UDT_BYTES_PER_VALUE));
					}
				}
				else
				{
					// TODO
				}							
			}	break;

			case T::MISC:
				if (tag.is_array())
				{
					state.misc_array_tags.push_back(create_ui_array_tag(tag, UI_MISC_BYTES_PER_VALUE));
				}
				else
				{
					state.misc_tags.push_back(create_ui_tag(tag, UI_MISC_BYTES_PER_VALUE));
				}
				break;

			default:
				if (tag.is_array())
				{
					state.number_array_tags.push_back(create_ui_array_tag(tag, UI_NUMBER_BYTES_PER_VALUE));
				}
				else
				{
					state.number_tags.push_back(create_ui_tag(tag, UI_MISC_BYTES_PER_VALUE));
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
				ImGui::TableNextRow();

				ImGui::TableSetColumnIndex(col_offset);
				ImGui::TextDisabled("--");

				ImGui::TableSetColumnIndex(col_size);
				ImGui::TextColored(text_color, "%u", udt.size);

				ImGui::TableSetColumnIndex(col_type);
				ImGui::TextDisabled("--");

				ImGui::TableSetColumnIndex(col_name);
				if (udt.fields.empty())
				{
					ImGui::TextColored(text_color, udt.name());
				}
				else
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
				auto array_count = (u32)tag.elements.size();

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
						ImGui::TextColored(text_color, "  %s[%u]", tag.name, i);

						ImGui::TableSetColumnIndex(col_size);
						ImGui::TextColored(text_color, "%u", tag.element_size);

						ImGui::TableSetColumnIndex(col_value);
						ImGui::TextColored(text_color, "%s", tag.elements[i].value_str.data());
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

				ImGui::TableSetColumnIndex(col_value);
				ImGui::TextDisabled("--");
				
				if (tag.fields.empty())
				{
					ImGui::TableSetColumnIndex(col_name);
					ImGui::TextColored(text_color, "%s", tag.name);
				}
				else
				{
					ImGui::TableSetColumnIndex(col_name);
					if (ImGui::TreeNode(tag.name))
					{
						for (auto const& field : tag.fields)
						{
							ImGui::TableNextRow();

							ImGui::TableSetColumnIndex(col_name);
							ImGui::TextColored(text_color, "  %s", field.name);

							ImGui::TableSetColumnIndex(col_type);
							ImGui::TextColored(text_color, "%s", field.type);

							ImGui::TableSetColumnIndex(col_size);
							ImGui::TextColored(text_color, "%u", field.size());

							ImGui::TableSetColumnIndex(col_value);
							ImGui::TextColored(text_color, "%s", field.value_str.data());
						}

						ImGui::TreePop();
					}
				}
			}

			ImGui::EndTable();
		}
	}


	static void ui_udt_tag_array_table(List<UI_UdtArrayTag> const& tags, cstr label)
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
				auto array_count = (u32)tag.elements.size();

				ImGui::TableNextRow();

				ImGui::TableSetColumnIndex(col_type);
				ImGui::TextColored(text_color, "%s[%u]", tag.type, array_count);

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

						auto& udt = tag.elements[i];
						if (udt.fields.empty())
						{
							ImGui::TableSetColumnIndex(col_name);
							ImGui::TextColored(text_color, "  %s[%u]", tag.name, i);
						}
						else
						{
							ImGui::TableSetColumnIndex(col_name);
							char buffer[40] = { 0 };
							qsnprintf(buffer, 40, "  %s[%u]", tag.name, i);

							if (ImGui::TreeNode(buffer))
							{
								for (auto const& f : udt.fields)
								{
									ImGui::TableNextRow();

									ImGui::TableSetColumnIndex(col_name);
									ImGui::TextColored(text_color, "  %s", f.name);

									ImGui::TableSetColumnIndex(col_type);
									ImGui::TextColored(text_color, "%s", f.type);

									ImGui::TableSetColumnIndex(col_size);
									ImGui::TextColored(text_color, "%u", f.size());

									ImGui::TableSetColumnIndex(col_value);
									ImGui::TextColored(text_color, "%s", f.value_str.data());
								}

								ImGui::TreePop();
							}
						}

						

						
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
				if (ImGui::CollapsingHeader("Tags - Numeric"))
				{
					ui_tag_table(state.number_tags, "NumericTagTable");
				}

				if (ImGui::CollapsingHeader("Arrays - Numeric"))
				{
					ui_tag_array_table(state.number_array_tags, "NumericArrayTagTable");
				}

				ImGui::EndTabItem();
			}

			if (ImGui::BeginTabItem("String"))
			{
				if (ImGui::CollapsingHeader("Tags - String"))
				{
					ui_tag_table(state.string_tags, "StringTagTable");
				}

				if (ImGui::CollapsingHeader("Arrays - String"))
				{
					ui_tag_array_table(state.string_array_tags, "StringArrayTagTable");
				}

				ImGui::EndTabItem();
			}

			if (ImGui::BeginTabItem("UDT"))
			{
				if (ImGui::CollapsingHeader("Tags - UDT"))
				{
					ui_udt_tag_table(state.udt_tags, "UdtTagTable");
				}

				if (ImGui::CollapsingHeader("Arrays - UDT"))
				{
					ui_udt_tag_array_table(state.udt_array_tags, "UdtArrayTagTable");
				}

				ImGui::EndTabItem();
			}


			if (ImGui::BeginTabItem("Misc"))
			{
				if (ImGui::CollapsingHeader("Tags - Misc"))
				{
					ui_tag_table(state.misc_tags, "MiscTableTable");
				}

				if (ImGui::CollapsingHeader("Arrays - Misc"))
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
		auto& state = g_app_state;

		for (auto const& tag : state.string_tags)
		{
			map_tag_value(tag);

		}

		for (auto const& tag : state.string_array_tags)
		{
			map_tag_value(tag);
		}

		for (auto const& tag : state.number_tags)
		{
			map_tag_value(tag);

		}

		for (auto const& tag : state.number_array_tags)
		{
			map_tag_value(tag);
		}

		for (auto const& tag : state.misc_tags)
		{
			map_tag_value(tag);

		}

		for (auto const& tag : state.misc_array_tags)
		{
			map_tag_value(tag);
		}

		for (auto const& tag : state.udt_tags)
		{
			map_tag_value(tag);

		}

		for (auto const& tag : state.udt_array_tags)
		{
			map_tag_value(tag);
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