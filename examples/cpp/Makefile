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

tag_vtable_h := $(libplctag)/tag_vtable.h
tag_vtable_h += $(libplctag_internal_h)


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
libplctag_c          := $(libplctag)/libplctag.c
platform_c           := $(libplctag)/platform_posix.c
modbus_c             := $(protocols)/modbus.c
system_c             := $(protocols)/system.c
ab_common_c          := $(ab)/ab_common.c
ab_cip_c             := $(ab)/cip.c
ab_eip_cip_c         := $(ab)/eip_cip.c
ab_eip_cip_special_c := $(ab)/eip_cip_special.c
ab_eip_lgx_pccc_c    := $(ab)/eip_lgx_pccc.c
ab_eip_slc_dhp_c     := $(ab)/eip_slc_dhp.c
ab_eip_slc_pccc_c    := $(ab)/eip_slc_pccc.c
ab_error_codes_c     := $(ab)/error_codes.c
ab_pccc_c            := $(ab)/pccc.c
ab_session_c         := $(ab)/session.c

libplctag_o          := $(build)/libplctag.o
platform_o           := $(build)/platform.o
modbus_o             := $(build)/modbus.o
system_o             := $(build)/system.o
ab_common_o          := $(build)/ab_common.o
ab_cip_o             := $(build)/ab_cip.o
ab_eip_cip_o         := $(build)/ab_eip_cip.o
ab_eip_cip_special_o := $(build)/ab_eip_cip_special.o
ab_eip_lgx_pccc_o    := $(build)/ab_eip_lgx_pcccc.o
ab_eip_slc_dhp_o     := $(build)/ab_eip_slc_dhp.o
ab_eip_slc_pccc_o    := $(build)/ab_eip_slc_pccc.o
ab_error_codes_o     := $(build)/ab_error_codes.o
ab_pccc_o            := $(build)/ab_pccc.o
ab_session_o         := $(build)/ab_session.o

object_files += $(libplctag_o)
object_files += $(platform_o)
object_files += $(modbus_o)
object_files += $(system_o)
object_files += $(ab_common_o)
object_files += $(ab_cip_o)
object_files += $(ab_eip_cip_o)
object_files += $(ab_eip_cip_special_o)
object_files += $(ab_eip_lgx_pccc_o)
object_files += $(ab_eip_slc_dhp_o)
object_files += $(ab_eip_slc_pccc_o)
object_files += $(ab_error_codes_o)
object_files += $(ab_pccc_o)
object_files += $(ab_session_o)



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


$(libplctag_o): $(libplctag_c) $(tag_vtable_h)
	@echo "\n libplctag"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(platform_o): $(platform_c) $(libplctag_internal_h)
	@echo "\n platform"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(modbus_o): $(modbus_c) $(libplctag_internal_h)
	@echo "\n modbus"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(system_o): $(system_c) $(libplctag_internal_h)
	@echo "\n system"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(ab_common_o): $(ab_common_c) $(tag_vtable_h)
	@echo "\n ab_common"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(ab_cip_o): $(ab_cip_c) $(libplctag_internal_h)
	@echo "\n ab_cip"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(ab_eip_cip_o): $(ab_eip_cip_c) $(libplctag_internal_h)
	@echo "\n ab_eip_cip"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(ab_eip_cip_special_o): $(ab_eip_cip_special_c) $(tag_vtable_h)
	@echo "\n ab_eip_cip_special"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(ab_eip_lgx_pccc_o): $(ab_eip_lgx_pccc_c) $(libplctag_internal_h)
	@echo "\n ab_eip_lgx_pccc"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(ab_eip_slc_dhp_o): $(ab_eip_slc_dhp_c) $(libplctag_internal_h)
	@echo "\n ab_eip_slc_dhp"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(ab_eip_slc_pccc_o): $(ab_eip_slc_pccc_c) $(libplctag_internal_h)
	@echo "\n ab_eip_slc_pccc"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(ab_error_codes_o): $(ab_error_codes_c) $(libplctag_internal_h)
	@echo "\n ab_error_codes"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(ab_pccc_o): $(ab_pccc_c) $(libplctag_internal_h)
	@echo "\n ab_pccc"
	$(GPP) $(CCFLAGS) -o $@ -c $< $(LIBRARIES)


$(ab_session_o): $(ab_session_c) $(libplctag_internal_h)
	@echo "\n ab_session"
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