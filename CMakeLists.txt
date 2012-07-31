# CMake build script for RFA broadcast provider
# x64 Windows Server-only
# 2012/07/18 -- Steven.McCoy@thomsonreuters.com

cmake_minimum_required (VERSION 2.8.8)

project (Usagi)

# Thomson Reuters Robust Foundation API
set(RFA_ROOT D:/rfa7.2.1.L1.win-shared.rrg)
set(RFA_INCLUDE_DIRS
	${RFA_ROOT}/Include
	${RFA_ROOT}/Include/rwf
)
set(RFA_LIBRARY_DIR ${RFA_ROOT}/Libs/WIN_64_VS100/Release_MD)
set(RFA_LIBRARY_DIRS ${RFA_LIBRARY_DIR})
set(RFA_LIBRARIES
	RFA7_Common100_x64.lib
	RFA7_Config100_x64.lib
	RFA7_Logger100_x64.lib
	RFA7_Data100_x64.lib
	RFA7_Connections100_x64.lib
	RFA7_Connections_OMM100_x64.lib
	RFA7_SessionLayer100_x64.lib
	DACS7_lock100_x64.lib
)

# ZeroMQ SDK 2.2.0
set(ZEROMQ_ROOT "C:/Program Files/ZeroMQ 2.2.0")
set(ZEROMQ_INCLUDE_DIRS "${ZEROMQ_ROOT}/include")
set(ZEROMQ_LIBRARY_DIRS "${ZEROMQ_ROOT}/lib")
set(ZEROMQ_LIBRARIES
	optimized libzmq-v100-mt.lib
	debug libzmq-v100-mt-gd.lib
)

# Google Protocol BUffers
set(PROTOBUF_SRC_ROOT_FOLDER "D:/protobuf-2.4.1")
find_package (Protobuf REQUIRED)

# Boost headers plus built libraries
set(BOOST_ROOT D:/boost_1_50_0)
set(BOOST_LIBRARYDIR ${BOOST_ROOT}/stage/lib)
set(Boost_USE_STATIC_LIBS ON)
find_package (Boost 1.50 COMPONENTS chrono thread REQUIRED)

#-----------------------------------------------------------------------------
# force off-tree build

if(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
message(FATAL_ERROR "CMake generation is not allowed within the source directory!
Remove the CMakeCache.txt file and try again from another folder, e.g.:

   del CMakeCache.txt
   mkdir build
   cd build
   cmake ..
")
endif(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})

#-----------------------------------------------------------------------------
# default to Release build

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING
      "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel."
      FORCE)
endif(NOT CMAKE_BUILD_TYPE)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH  ${CMAKE_BINARY_DIR}/lib)

#-----------------------------------------------------------------------------
# platform specifics

add_definitions(
	-DWIN32
	-DWIN32_LEAN_AND_MEAN
# Windows Server 2008 (not-R2)
	-D_WIN32_WINNT=0x0600
# RFA version
        -DRFA_LIBRARY_VERSION="7.2.1."
)

#-----------------------------------------------------------------------------
# Generated source files

PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS src/provider.proto)

#-----------------------------------------------------------------------------
# source files

set(cxx-sources
	src/client.cc
	src/config.cc
	src/error.cc
	src/main.cc
	src/usagi.cc
	src/provider.cc
	src/rfa.cc
	src/rfa_logging.cc
	src/chromium/chromium_switches.cc
	src/chromium/command_line.cc
	src/chromium/debug/stack_trace.cc
	src/chromium/debug/stack_trace_win.cc
	src/chromium/memory/singleton.cc
	src/chromium/logging.cc
	src/chromium/string_piece.cc
	src/chromium/string_split.cc
	src/chromium/string_util.cc
	src/chromium/synchronization/lock.cc
	src/chromium/synchronization/lock_impl_win.cc
	src/chromium/vlog.cc
)

include_directories(
	include
	${CMAKE_CURRENT_BINARY_DIR}
	${RFA_INCLUDE_DIRS}
	${ZEROMQ_INCLUDE_DIRS}
	${PROTOBUF_INCLUDE_DIRS}
	${Boost_INCLUDE_DIRS}
)

link_directories(
	${RFA_LIBRARY_DIRS}
	${ZEROMQ_LIBRARY_DIRS}
	${Boost_LIBRARY_DIRS}
)

#-----------------------------------------------------------------------------
# output

add_executable(Usagi ${cxx-sources} ${PROTO_SRCS} ${PROTO_HDRS})

target_link_libraries(Usagi
	${RFA_LIBRARIES}
	${Boost_LIBRARIES}
	${ZEROMQ_LIBRARIES}
	${PROTOBUF_LIBRARIES}
	ws2_32.lib
	dbghelp.lib
)

# end of file
