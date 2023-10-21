# LibplctagLite

### Minimal C++ wrapper(s) for [libplctag](https://github.com/libplctag/libplctag)

## plcscan

Enables continuously scanning a ControlLogix PLC for updated tag values.

### API overview

The `PlcTagData` object contains the type, tag, and UDT information.  It is created when the library is initialized.  It will contain basic data type information is at first, but tag and UDT information are obtained after a connection is made with a PLC.

```cpp
#include "plcscan.hpp"

// ...

plcscan::PlcTagData plc_data = plcscan::init();

for (auto const& type : plc_data.data_types)
{
    // do something with type
}

// ...
```

Cleanup resources by shutting down the library when finished.

```cpp
plcscan::PlcTagData plc_data = plcscan::init();

// ...

plcscan::shutdown();
```

Successfully connecting to a PLC generates tag and UDT information.

```cpp
#include "plcscan.hpp"

// PLC IP and path
constexpr auto PLC_IP = "192.168.123.123";
constexpr auto PLC_PATH = "1,0";


auto plc_data = plcscan::init();

bool success = plcscan::connect(PLC_IP, PLC_PATH, plc_data);

if (!success)
{
    // error
}

// ...

for (auto const& tag : plc_data.tags)
{
    // do something with tag
}

for (auto const& udt : plc_data.udt_types)
{
    // do something with udt
}

// ...

plcscan::shutdown();
```

Tag information is stored in a `Tag` object.

A tag's data type information can be found using its `type_id` property.  The `TagType` enumeration can be retrieved with `get_tag_type()`.

```cpp
auto plc_data = plcscan::init();
if (!plcscan::connect(PLC_IP, PLC_PATH, plc_data))
{
    // error
}

// just get the first tag
plcscan::Tag tag = plc_data.tags[0];

plcscan::TagType type = plcscan::get_tag_type(tag.type_id);

// ...

plcscan::shutdown();
```

The possible `TagType` values are: `BOOL`,`SINT`, `INT`, `DINT`, `LINT`, `USINT`, `UINT`, `UDINT`, `ULINT`, `REAL`, `LREAL`, `STRING`, `UDT`, and `OTHER`.

If the tag is not a `UDT` type, use the `type_id` to seach the `data_types` for specific type information.  If it is a `UDT`, then search the `udt_types`.

```cpp
#include <algorithm> // std::find_if
#include <cstdio> // printf

// ...

auto plc_data = plcscan::init();
if (!plcscan::connect(PLC_IP, PLC_PATH, plc_data))
{
    // error
}

// just get the first tag
auto tag = plc_data.tags[0];

auto type = plcscan::get_tag_type(tag.type_id);

// type info to display
auto name = "";
auto description = "";

auto const search_predicate = [&](auto const& t){ return t.type_id == tag.type_id; };

if (type == plcscan::TagType::UDT)
{
    // search udts
    auto b = plc_data.udt_types.begin();
    auto e = plc_data.udt_types.end();
    auto it = std::find_if(b, e, search_predicate);
    if (it != e)
    {
        name = it->name();
        description = it->description();
    }
}
else
{
    // search data types
    auto b = plc_data.data_types.begin();
    auto e = plc_data.data_types.end();
    auto it = std::find_if(b, e, search_predicate);
    if (it != e)
    {
        name = it->name();
        description = it->description();
    }
}

printf("Type: %s / %s\n", name, description);

// ...

plcscan::shutdown();
```

Use `get_fast_type_name()` as an easier alternative to simply get the type name;

```cpp
auto plc_data = plcscan::init();
if (!plcscan::connect(PLC_IP, PLC_PATH, plc_data))
{
    // error
}

// just get the first tag
auto tag = plc_data.tags[0];

const char* type_name = plcscan::get_fast_type_name(tag.type_id);

printf("Type: %s\n", type_name);

// ...

plcscan::shutdown();
```

Note: At this time `get_fast_type_name()` will only return the string "UDT" for UDT types.

Have the library continuously scan the PLC for tag values by providing a callback function for processing tag data and a callback function to signal when scanning should stop.

```cpp
void process_plc_scan(plcscan::PlcTagData& data)
{
    for (auto const& tag : data.tags)
    {
        // do something with tag
    }
}


bool scan_flag = false;

bool is_scanning()
{
    return scan_flag;
}


int main()
{
    auto plc_data = plcscan::init();
    if (!plcscan::connect(PLC_IP, PLC_PATH, plc_data))
    {
        // error
    }

    scan_flag = true;

    // scan_flag never changes, so this will run forever
    plcscan::scan(process_plc_scan, is_scanning, plc_data);

    // ...

    plcscan::shutdown();
}
```

`plcscan::scan()` is blocking, so the scanning condition must updated while processing or it should be started in a separate thread.

```cpp
#include <thread>

// ...

auto plc_data = plcscan::init();
if (!plcscan::connect(PLC_IP, PLC_PATH, plc_data))
{
    // error
}

std::thread scan_th([&](){ plcscan::scan(process_plc_scan, is_scanning, plc_data); });

// ...

scan_th.join();

plcscan::shutdown();
```

And here is an example of how to scan tags in the background within another application.

```cpp
#include <thread>

auto plc_data = plcscan::init();
if (!plcscan::connect(PLC_IP, PLC_PATH, plc_data))
{
    // error
}

// ...

bool app_terminated = false;
bool scan_terminated = false;


auto const process_data = [](plcscan::PlcTagData& data){ /* process */ };

auto const is_scanning = [&](){ return !scan_terminated; };

// start scanning in a background thread
std::thread scan_th([&](){ plcscan::scan(process_data, is_scanning, plc_data); });

while (!app_terminated)
{
    // run application while scans run in the background
}

// stop scanning
scan_terminated = true;
scan_th.join();

// ...

plcscan::shutdown();
```

### Limitations

* Compatable with ControlLogix PLCs only
    * Tag and UDT listing is not supported by other PLC types
* Supports connecting to only one PLC at a time.
    * Problems have occured when multiple PLCs have tags with the same name
    * More investigation is necessary

### Example 1: List data types

Demonstrates how to access the basic Allen-Bradley datatype information after the library is initialized.

`/sample_apps/plcscan_list_data_types/list_data_types_main.cpp`

### Example 2: List UDT types

Demonstrates how to access UDT information when connected to a PLC.

`/sample_apps/plcscan_list_udt_data_types/list_udt_types_main.cpp`

* untested (2023-10-21)

### Example 3: List tags

Demonstrates how to access tag information when connected to a PLC.

`/sample_apps/plcscan_list_tags/list_tags_main.cpp`

* untested (2023-10-21)

### Example 4: Simple scan

Demonstrates how to setup an application to scan and process tags continuously.  Displays tag values based on their data type.

`/sample_apps/plcscan_simple_scan/simple_scan_main.cpp`

* untested (2023-10-21)

## libplctag

Source files are taken from the [libplctag](https://github.com/libplctag/libplctag) library (v2.5.0).  Files have been edited and merged together to allow for simply including the .c files in a project instead of building a library to link to.

The intent is to eventually have the entire library as a header file (libplctag.h) and a single source file.