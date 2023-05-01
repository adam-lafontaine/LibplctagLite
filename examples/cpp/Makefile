GPP := g++-11

build := ./build_files

src := ../lib # TODO: rename
libplctag := $(src)/libplctag
lib := $(libplctag)/lib
platform := $(libplctag)/platform/posix
protocols := $(libplctag)/protocols

util := 


libplctag_h := $(src)/libplctag.h

libplctag_internal_h := $(src)/libplctag_internal.hpp
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
object_files := $(plctag_o)






 flags
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



build: $(exe)

run: build
	$(exe)

clean:
	rm -rfv $(build)/*

setup:
	mkdir -p $(build)