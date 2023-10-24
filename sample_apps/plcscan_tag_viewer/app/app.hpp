#pragma once

#include <thread>


namespace app
{
	constexpr auto APPLICATION_ID_NAME = L"ImGui Tag Viewer";

	constexpr auto MAIN_WINDOW_TITLE = L"Tag Viewer";

	constexpr int MAIN_WINDOW_WIDTH = 1200;
	constexpr int MAIN_WINDW_HEIGHT = 800;

	
	void init();

	void shutdown();

	std::thread start_worker();

	bool render_ui();
}


//#define PLC_IMGUI_SHOW_DEMO