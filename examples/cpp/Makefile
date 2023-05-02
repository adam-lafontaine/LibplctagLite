GPP := g++-11

build := ./build_files

# TODO: rename
src := ../../lib

libplctag := $(src)/libplctag
protocols := $(libplctag)/protocols
ab := $(protocols)/ab

exe := list_tags_logix


libplctag_h := $(libplctag)/libplctag.h

libplctag_internal_h := $(libplctag)/libplctag_internal.h
libplctag_internal_h += $(libplctag_h)

plctag_h := $(src)/plctag.hpp



# main
main_dep := $(plctag_h)

main_c := ./list_tags_logix.cpp
main_o := $(build)/main.o
object_files := $(main_o)


# plctag
plctag_dep := $(plctag_h)
plctag_dep += $(libplctag_h)

plctag_c := $(src)/plctag.cpp
plctag_o := $(build)/plctag.o
object_files += $(plctag_o)


# libplctag
libplctag_dep := $(libplctag_internal_h)

libplctag_c := $(libplctag)/libplctag_lib.c
libplctag_c += $(libplctag)/libplctag_platform_posix.c
libplctag_c += $(libplctag)/libplctag_util.c
libplctag_c += $(protocols)/modbus.c
libplctag_c += $(protocols)/system.c
libplctag_c += $(ab)/ab_common.c
libplctag_c += $(ab)/cip.c
libplctag_c += $(ab)/eip_cip.c
libplctag_c += $(ab)/eip_cip_special.c
libplctag_c += $(ab)/eip_lgx_pccc.c
libplctag_c += $(ab)/eip_slc_dhp.c
libplctag_c += $(ab)/eip_slc_pccc.c
libplctag_c += $(ab)/error_codes.c
libplctag_c += $(ab)/pccc.c
libplctag_c += $(ab)/session.c

libplctag_o := $(build)/libplctag.o
object_files += $(libplctag_o)



# flags
INCLUDES  := 

LIBRARIES := -pthread

CCFLAGS := -std=c++20

#CCFLAGS += -O3 -DNDEBUG

# build rules

$(main_o): $(main_c) $(main_dep)
	@echo "\n main"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(plctag_o): $(plctag_c) $(plctag_dep)
	@echo "\n plctag"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(libplctag_o): $(libplctag_c) $(libplctag_dep)
	@echo "\n libplctag"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(exe): $(object_files)
	@echo "\n exe"
	$(GPP) $(CCFLAGS) -o $@ $+ $(LIBRARIES)


build: $(exe)

run: build
	$(exe)

clean:
	rm -rfv $(build)/*

setup:
	mkdir -p $(build)