cmake_minimum_required(VERSION 3.7)
project(czmq)

enable_language (C)

set(CMAKE_C_STANDARD 11)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g3")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O0")

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake_files")

find_package(libzmq REQUIRED)
IF (LIBZMQ_FOUND)
    include_directories(${LIBZMQ_INCLUDE_DIRS})
    list(APPEND MORE_LIBRARIES ${LIBZMQ_LIBRARIES})
ELSE (LIBZMQ_FOUND)
    message( FATAL_ERROR "libzmq not found." )
ENDIF (LIBZMQ_FOUND)

########################################################################
# CZMQ dependency
########################################################################
find_package(czmq REQUIRED)
IF (CZMQ_FOUND)
    include_directories(${CZMQ_INCLUDE_DIRS})
    list(APPEND MORE_LIBRARIES ${CZMQ_LIBRARIES})
ELSE (CZMQ_FOUND)
    message( FATAL_ERROR "czmq not found." )
ENDIF (CZMQ_FOUND)

add_executable(peering1 peering1.c)
add_executable(peering2 peering2.c)
add_executable(peering3 peering3.c)

target_link_libraries (peering1 ${CZMQ_LIBRARIES} ${LIBZMQ_LIBRARIES})
target_link_libraries (peering2 ${CZMQ_LIBRARIES} ${LIBZMQ_LIBRARIES})
target_link_libraries (peering3 ${CZMQ_LIBRARIES} ${LIBZMQ_LIBRARIES})

add_executable(peering1_API4 peering1_API4.c)
target_link_libraries (peering1_API4 ${CZMQ_LIBRARIES} ${LIBZMQ_LIBRARIES})

add_executable(peering2_API4 peering2_API4.c)
target_link_libraries (peering2_API4 ${CZMQ_LIBRARIES} ${LIBZMQ_LIBRARIES})

add_executable(peering3_API4 peering3_API4.c)
target_link_libraries (peering3_API4 ${CZMQ_LIBRARIES} ${LIBZMQ_LIBRARIES})
