#include "app.hpp"
#include "../../../src/imgui/imgui.h"
#include "../../../src/plcscan/plcscan.hpp"
#include "../../../src/util/time_helper.hpp"
#include "../../../src/util/qsprintf.hpp"

namespace tmh = time_helper;


constexpr auto DEFAULT_PLC_IP = "192.168.123.123";
constexpr auto DEFAULT_PLC_PATH = "1,0";
constexpr auto DEFAULT_CONNECT_TIMEOUT = "1000";


/* ui types */

namespace
{
	using UI_DataType = plcscan::DataType;
	using UI_UdtType = plcscan::UdtType;
	using UI_UdtFieldType = plcscan::UdtFieldType;
	using UI_Tag = plcscan::Tag;


	class UI_MiscTag
	{
	public:
		u32 tag_id;

		cstr name = 0;
		cstr type = 0;
		u32 size = 0;

		StringView value;

		char display_buffer[21] = { 0 };
	};


	class UI_MiscArrayTag
	{
	public:
		u32 tag_id;

		cstr name = 0;
		cstr type = 0;
		u32 size = 0;

		List<StringView> values;

		MemoryBuffer<char> value_data;
	};


	class UI_StringTag
	{
	public:
		u32 tag_id;

		cstr name = 0;
		cstr type = 0;
		u32 size = 0;
		cstr value = 0;
	};


	class UI_StringArrayTag
	{
	public:
		u32 tag_id;

		cstr name = 0;
		cstr type = 0;
		u32 size = 0;

		List<cstr> values;
	};

	using UI_NumberTag = UI_MiscTag;


	class UI_NumberTag
	{
	public:
		u32 tag_id;

		cstr name = 0;
		cstr type = 0;
		u32 size = 0;

		StringView value;

		char display_buffer[21] = { 0 };
	};


	class UI_NumberArrayTag
	{
	public:
		u32 tag_id;

		cstr name = 0;
		cstr type = 0;
		u32 size = 0;

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
		
		StringView value;

		char display_buffer[21] = { 0 };
	};


	class UI_UdtArrayTag
	{
	public:
		u32 tag_id;

		cstr name = 0;
		cstr type = 0;
		u32 size = 0;
		
		List<StringView> values;

		static constexpr u32 bytes_per_value = 21;

		MemoryBuffer<char> value_data;
	};
}


/* ui generic */

namespace
{
	template <class UI_Tag>
	static void create_ui_tag_value(UI_Tag& ui_tag)
	{
		if (!ui_tag.value.data)
		{
			ui_tag.value.data = ui_tag.display_buffer;
			ui_tag.value.length = (u32)(sizeof(ui_tag.display_buffer) - 1);
		}
	}


	template <class UI_ArrayTag>
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
			ui_tag.values.push_back(mb::push_cstr_view(ui_tag.value_data, bytes_per_value));
		}
	}


	template <class UI_ArrayTag>
	static void destroy_ui_array_tag_values(UI_ArrayTag& ui_tag)
	{
		ui_tag.values.clear();
		mb::destroy_buffer(ui_tag.value_data);
	}
}


/* ui misc */

namespace
{
	constexpr u32 UI_MISC_BYTES_PER_VALUE = 21;

	static void map_hex(ByteView const& src, StringView const& dst)
	{
		auto out_len = dst.length;
		auto len = src.length;

		if (len < out_len / 2)
		{
			len = out_len / 2;
		}

		for (u32 i = 0; i < len; i++)
		{
			auto s = src.data + i;
			auto d = dst.data + i * 2;
			qsnprintf(d, 3, "%02x", s);
		}
		dst.data[len] = NULL;
	}


	static void create_value(UI_MiscTag& ui_tag)
	{
		create_ui_tag_value(ui_tag);
	}


	static void destroy_value(UI_MiscTag& ui_tag)
	{
		//...
	}


	static void map_value(plcscan::Tag const& src, UI_MiscTag& dst, u32 tag_id)
	{
		map_hex(src.bytes, dst.value);
	}


	static void create_array_values(plcscan::Tag const& tag, UI_MiscArrayTag& ui_tag)
	{
		create_ui_array_tag_values(tag, ui_tag, UI_MISC_BYTES_PER_VALUE);
	}


	static void destroy_array_values(UI_MiscArrayTag& ui_tag)
	{
		destroy_ui_array_tag_values(ui_tag);
	}


	static void map_value(plcscan::Tag const& src, UI_MiscArrayTag& dst, u32 tag_id)
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
		offset.length = src.bytes.length / src.array_count;

		for (u32 i = 0; i < src.array_count; ++i)
		{
			auto elem_bytes = mb::sub_view(src.bytes, offset);

			map_hex(elem_bytes, dst.values[i]);

			offset.begin += offset.length;
		}
	}
}


/* ui string */

namespace
{
	static void create_value(UI_StringTag& ui_tag)
	{
		ui_tag.value = 0;
	}


	static void destroy_value(UI_StringTag& ui_tag)
	{
		ui_tag.value = 0;
	}

	static void map_value(plcscan::Tag const& src, UI_StringTag& dst, u32 tag_id)
	{
		assert(plcscan::get_tag_type(src.type_id) == plcscan::TagType::STRING);
		assert(dst.tag_id == tag_id);

		if (dst.tag_id != tag_id)
		{
			return;
		}

		dst.value = (cstr)src.bytes.data;
	}


	static void create_array_values(plcscan::Tag const& tag, UI_StringArrayTag& ui_tag)
	{
		ui_tag.values.resize(tag.array_count);
	}


	static void destroy_array_values(UI_StringArrayTag& ui_tag)
	{
		ui_tag.values.clear();
	}


	static void map_value(plcscan::Tag const& src, UI_StringArrayTag& dst, u32 tag_id)
	{
		assert(plcscan::get_tag_type(src.type_id) == plcscan::TagType::STRING);
		assert(dst.tag_id == tag_id);
		assert(src.is_array());
		assert((u32)dst.values.size() == src.array_count);

		if (dst.tag_id != tag_id || (u32)dst.values.size() != src.array_count)
		{
			return;
		}

		MemoryOffset<u8> offset{};
		offset.begin = 0;
		offset.length = src.bytes.length / src.array_count;

		for (u32 i = 0; i < src.array_count; ++i)
		{
			auto elem_bytes = mb::sub_view(src.bytes, offset);

			dst.values[i] = (cstr)elem_bytes.data;

			offset.begin += offset.length;
		}
	}
}


/* ui number */

namespace
{
	constexpr u32 UI_NUMBER_BYTES_PER_VALUE = 21;

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


	static void map_number(ByteView const& src, StringView const& dst, plcscan::TagType type)
	{
		using T = plcscan::TagType;

		switch (type)
		{
		case T::BOOL:
		case T::USINT:
			qsnprintf(dst.data, dst.length, "%hhu", cast_bytes<u8>(src.data, src.length));
			break;

		case T::SINT:
			qsnprintf(dst.data, dst.length, "%hhd", cast_bytes<i8>(src.data, src.length));
			break;

		case T::UINT:
			qsnprintf(dst.data, dst.length, "%hu", cast_bytes<u16>(src.data, src.length));
			break;

		case T::INT:
			qsnprintf(dst.data, dst.length, "%hd", cast_bytes<i16>(src.data, src.length));
			break;

		case T::UDINT:
			qsnprintf(dst.data, dst.length, "%u", cast_bytes<u32>(src.data, src.length));
			break;

		case T::DINT:
			qsnprintf(dst.data, dst.length, "%d", cast_bytes<i32>(src.data, src.length));
			break;

		case T::ULINT:
			qsnprintf(dst.data, dst.length, "%llu", cast_bytes<u64>(src.data, src.length));
			break;

		case T::LINT:
			qsnprintf(dst.data, dst.length, "%lld", cast_bytes<i64>(src.data, src.length));
			break;

		case T::REAL:
			qsnprintf(dst.data, dst.length, "%f", cast_bytes<f32>(src.data, src.length));
			break;

		case T::LREAL:
			qsnprintf(dst.data, dst.length, "%lf", cast_bytes<f64>(src.data, src.length));
			break;

		default:
			qsnprintf(dst.data, dst.length, "error");
			break;
		}
	}


	static void create_value(UI_NumberTag& ui_tag)
	{
		create_ui_tag_value(ui_tag);
	}


	static void destroy_value(UI_NumberTag& ui_tag)
	{
		//...
	}


	static void map_value(plcscan::Tag const& src, UI_NumberTag& dst, u32 tag_id)
	{
		map_number(src.bytes, dst.value, plcscan::get_tag_type(src.type_id));
	}


	static void create_array_values(plcscan::Tag const& tag, UI_NumberArrayTag& ui_tag)
	{
		create_ui_array_tag_values(tag, ui_tag, UI_NUMBER_BYTES_PER_VALUE);
	}


	static void destroy_array_values(UI_NumberArrayTag& ui_tag)
	{
		destroy_ui_array_tag_values(ui_tag);
	}


	static void map_value(plcscan::Tag const& src, UI_NumberArrayTag& dst, u32 tag_id)
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
		offset.length = src.bytes.length / src.array_count;

		for (u32 i = 0; i < src.array_count; ++i)
		{
			auto elem_bytes = mb::sub_view(src.bytes, offset);

			map_number(elem_bytes, dst.values[i], type);

			offset.begin += offset.length;
		}
	}
}





/* ui udt */

namespace
{
	static void create_value(UI_UdtTag& ui_tag)
	{
		if (!ui_tag.value.data)
		{
			ui_tag.value.data = ui_tag.display_buffer;
			ui_tag.value.length = (u32)(sizeof(ui_tag.display_buffer) - 1);
		}
	}


	static void destroy_value(UI_UdtTag& ui_tag)
	{
		//...
	}


	static void map_value(plcscan::Tag const& src, UI_UdtTag& dst, u32 tag_id)
	{
		map_hex(src.bytes, dst.value);
	}


	static void create_array_values(plcscan::Tag const& tag, UI_UdtArrayTag& ui_tag)
	{
		if (!mb::create_buffer(ui_tag.value_data, tag.array_count * UI_UdtArrayTag::bytes_per_value))
		{
			return;
		}

		mb::zero_buffer(ui_tag.value_data);

		ui_tag.values.reserve(tag.array_count);
		for (u32 i = 0; i < tag.array_count; ++i)
		{
			ui_tag.values.push_back(mb::push_cstr_view(ui_tag.value_data, UI_UdtArrayTag::bytes_per_value));
		}
	}


	static void destroy_array_values(UI_UdtArrayTag& ui_tag)
	{
		ui_tag.values.clear();
		mb::destroy_buffer(ui_tag.value_data);
	}


	static void map_value(plcscan::Tag const& src, UI_UdtArrayTag& dst, u32 tag_id)
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
		offset.length = src.bytes.length / src.array_count;

		for (u32 i = 0; i < src.array_count; ++i)
		{
			auto elem_bytes = mb::sub_view(src.bytes, offset);

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
		List<UI_StringArrayTag> string_array_tags;

		List<UI_MiscTag> misc_tags;
		List<UI_MiscArrayTag> misc_array_tags;

		List<UI_NumberTag> number_tags;
		List<UI_NumberArrayTag> number_array_tags;

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


/* transform */

namespace
{
	template <class UI_Tag>
	UI_Tag create_ui_tag(plcscan::Tag const& tag, u32 tag_id)
	{
		UI_Tag ui{};

		ui.tag_id = tag_id;
		ui.name = tag.name();
		ui.type = tag.type();
		ui.size = tag.size();

		create_value(ui);

		map_value(tag, ui, tag_id);

		return ui;
	}


	template <class UI_Tag>
	UI_Tag create_ui_array_tag(plcscan::Tag const& tag, u32 tag_id)
	{
		UI_Tag ui{};

		ui.tag_id = tag_id;
		ui.name = tag.name();
		ui.type = tag.type();
		ui.size = tag.size();

		create_array_values(tag, ui);

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
				if (tag.is_array())
				{
					state.string_array_tags.push_back(create_ui_array_tag<UI_StringArrayTag>(tag, tag_id));
				}
				else
				{
					state.string_tags.push_back(create_ui_tag<UI_StringTag>(tag, tag_id));
				}				
				break;

			case T::UDT:
				if (tag.is_array())
				{
					state.udt_array_tags.push_back(create_ui_array_tag<UI_UdtArrayTag>(tag, tag_id));
				}
				else
				{
					state.udt_tags.push_back(create_ui_tag<UI_UdtTag>(tag, tag_id));
				}
				break;

			case T::OTHER:
				if (tag.is_array())
				{
					state.misc_array_tags.push_back(create_ui_array_tag<UI_MiscArrayTag>(tag, tag_id));
				}
				else
				{
					state.misc_tags.push_back(create_ui_tag<UI_MiscTag>(tag, tag_id));
				}
				break;

			default:
				if (tag.is_array())
				{
					state.number_array_tags.push_back(create_ui_array_tag<UI_NumberArrayTag>(tag, tag_id));
				}
				else
				{
					state.number_tags.push_back(create_ui_tag<UI_NumberTag>(tag, tag_id));
				}
				break;
			}
		}
	}


	static void destroy_ui_tags(App_State& state)
	{
		for (auto& tag : state.string_tags)
		{
			destroy_value(tag);
		}

		for (auto& tag : state.string_array_tags)
		{
			destroy_array_values(tag);
		}

		for (auto& tag : state.misc_tags)
		{
			destroy_value(tag);
		}

		for (auto& tag : state.misc_array_tags)
		{
			destroy_array_values(tag);
		}

		for (auto& tag : state.number_tags)
		{
			destroy_value(tag);
		}

		for (auto& tag : state.number_array_tags)
		{
			destroy_array_values(tag);
		}

		for (auto& tag : state.udt_tags)
		{
			destroy_value(tag);
		}

		for (auto& tag : state.udt_array_tags)
		{
			destroy_array_values(tag);
		}
	}
}


namespace render
{
	constexpr auto WHITE = ImVec4(1, 1, 1, 1);
	constexpr auto GREEN = ImVec4(0, 1, 0, 1);
	constexpr auto RED = ImVec4(1, 0, 0, 1);

	using ImColor = ImVec4;

	static void command_window(UI_Command& cmd, UI_Input& input)
	{
		ImGui::Begin("Controller");

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


	static void string_array_tag_window(List<UI_StringArrayTag> const& tags)
	{
		ImGui::Begin("String Arrays");

		constexpr int col_name = 0;
		constexpr int col_type = 1;
		constexpr int col_size = 2;
		constexpr int col_index = 3;
		constexpr int col_value = 4;
		constexpr int n_columns = 5;

		auto table_flags = ImGuiTableFlags_ScrollY | ImGuiTableFlags_BordersV | ImGuiTableFlags_BordersOuterH | ImGuiTableFlags_RowBg | ImGuiTableFlags_NoBordersInBody;
		auto table_dims = ImGui::GetContentRegionAvail();

		auto text_color = WHITE;

		if (ImGui::BeginTable("StringTagTable", n_columns, table_flags, table_dims))
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

						ImGui::TableSetColumnIndex(col_index);
						ImGui::TextColored(text_color, "[%u]", i);

						ImGui::TableSetColumnIndex(col_value);
						ImGui::TextColored(text_color, "%s", tag.values[i]);
					}

					ImGui::TreePop();
				}
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
					map_value(tag, state.string_array_tags[string_array_id++], tag_id);
				}
				else
				{
					map_value(tag, state.string_tags[string_id++], tag_id);
				}				
				break;

			case T::UDT:
				if (tag.is_array())
				{
					map_value(tag, state.udt_array_tags[udt_array_id++], tag_id);
				}
				else
				{
					map_value(tag, state.udt_tags[udt_id++], tag_id);
				}
				break;

			case T::OTHER:
				if (tag.is_array())
				{
					map_value(tag, state.misc_array_tags[misc_array_id++], tag_id);
				}
				else
				{
					map_value(tag, state.udt_tags[udt_id++], tag_id);
				}
				break;

			default:
				if (tag.is_array())
				{
					map_value(tag, state.number_array_tags[number_array_id++], tag_id);
				}
				else
				{
					map_value(tag, state.number_tags[number_id++], tag_id);
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
		destroy_ui_tags(g_app_state);
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
		render::string_array_tag_window(state.string_array_tags);

		return true;
	}
}