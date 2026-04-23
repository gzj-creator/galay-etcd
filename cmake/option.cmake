include_guard(GLOBAL)

option(GALAY_ETCD_BUILD_SHARED_LIBS "Build galay-etcd as a shared library" ON)
option(GALAY_ETCD_CXX_EXTENSIONS "Enable compiler specific C++ extensions" OFF)
option(GALAY_ETCD_INSTALL_MODULE_INTERFACE "Install C++ module interface files (*.cppm)" ON)
option(GALAY_ETCD_ENABLE_IMPORT_COMPILATION "Enable C++ import/module compilation path" ON)
option(GALAY_ETCD_BUILD_BENCHMARKS "Build galay-etcd benchmarks" ON)
option(GALAY_ETCD_BUILD_EXAMPLES "Build galay-etcd examples" ON)

set(GALAY_ETCD_BUILD_TESTS_COMPAT FALSE)
if(DEFINED GALAY_ETCD_BUILD_TESTS AND NOT DEFINED BUILD_TESTING)
    set(BUILD_TESTING ${GALAY_ETCD_BUILD_TESTS} CACHE BOOL "Build the testing tree." FORCE)
    set(GALAY_ETCD_BUILD_TESTS_COMPAT TRUE)
elseif(DEFINED BUILD_TESTS AND NOT DEFINED BUILD_TESTING)
    set(BUILD_TESTING ${BUILD_TESTS} CACHE BOOL "Build the testing tree." FORCE)
    set(GALAY_ETCD_BUILD_TESTS_COMPAT TRUE)
elseif(NOT DEFINED BUILD_TESTING)
    set(BUILD_TESTING OFF CACHE BOOL "Build the testing tree." FORCE)
endif()

option(BUILD_TESTS "Compatibility alias of BUILD_TESTING" "${BUILD_TESTING}")
set(BUILD_TESTS "${BUILD_TESTING}" CACHE BOOL "Compatibility alias of BUILD_TESTING" FORCE)
set(GALAY_ETCD_BUILD_TESTS "${BUILD_TESTING}" CACHE BOOL "Legacy alias of BUILD_TESTING" FORCE)

set(GALAY_ETCD_CXX_STANDARD "23" CACHE STRING "C++ standard for galay-etcd")
set_property(CACHE GALAY_ETCD_CXX_STANDARD PROPERTY STRINGS 20 23 26)

if(GALAY_ETCD_BUILD_SHARED_LIBS)
    set(GALAY_ETCD_LIBRARY_TYPE SHARED)
else()
    set(GALAY_ETCD_LIBRARY_TYPE STATIC)
endif()

set(GALAY_ETCD_IMPORT_BUILD_AVAILABLE OFF)
set(GALAY_ETCD_IMPORT_COMPILE_OPTIONS "")
set(GALAY_ETCD_IMPORT_UNAVAILABLE_REASON "")

if(CMAKE_VERSION VERSION_LESS 3.28)
    set(GALAY_ETCD_IMPORT_UNAVAILABLE_REASON "CMake < 3.28")
else()
    if(NOT CMAKE_GENERATOR MATCHES "Ninja|Visual Studio")
        set(GALAY_ETCD_IMPORT_UNAVAILABLE_REASON "unsupported generator; require Ninja or Visual Studio")
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
            set(GALAY_ETCD_IMPORT_UNAVAILABLE_REASON "current stable GNU path is Linux only")
        elseif(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 14)
            set(GALAY_ETCD_IMPORT_UNAVAILABLE_REASON "GCC < 14")
        else()
            set(GALAY_ETCD_IMPORT_BUILD_AVAILABLE ON)
        endif()
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        find_program(GALAY_ETCD_CLANG_SCAN_DEPS NAMES clang-scan-deps)
        if(NOT GALAY_ETCD_CLANG_SCAN_DEPS)
            get_filename_component(_GALAY_ETCD_CLANG_BIN_DIR "${CMAKE_CXX_COMPILER}" DIRECTORY)
            if(EXISTS "${_GALAY_ETCD_CLANG_BIN_DIR}/clang-scan-deps")
                set(GALAY_ETCD_CLANG_SCAN_DEPS "${_GALAY_ETCD_CLANG_BIN_DIR}/clang-scan-deps")
            else()
                find_program(GALAY_ETCD_CLANG_SCAN_DEPS NAMES clang-scan-deps HINTS "${_GALAY_ETCD_CLANG_BIN_DIR}")
            endif()
        endif()
        if(NOT GALAY_ETCD_CLANG_SCAN_DEPS)
            set(GALAY_ETCD_IMPORT_UNAVAILABLE_REASON "clang-scan-deps not found")
        elseif(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
            set(GALAY_ETCD_IMPORT_UNAVAILABLE_REASON "AppleClang is not supported for module dependency scanning in this project")
        else()
            set(GALAY_ETCD_IMPORT_BUILD_AVAILABLE ON)
            list(APPEND GALAY_ETCD_IMPORT_COMPILE_OPTIONS -Xclang -fskip-odr-check-in-gmf)
        endif()
    else()
        set(GALAY_ETCD_IMPORT_UNAVAILABLE_REASON
            "unsupported compiler: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
    endif()
endif()

set(GALAY_ETCD_IMPORT_COMPILATION_ENABLED OFF)
if(GALAY_ETCD_ENABLE_IMPORT_COMPILATION AND GALAY_ETCD_IMPORT_BUILD_AVAILABLE)
    set(GALAY_ETCD_IMPORT_COMPILATION_ENABLED ON)
elseif(GALAY_ETCD_ENABLE_IMPORT_COMPILATION AND NOT GALAY_ETCD_IMPORT_BUILD_AVAILABLE)
    message(STATUS "GALAY_ETCD_ENABLE_IMPORT_COMPILATION=ON but toolchain is unsupported. "
                   "Import compilation is disabled. "
                   "Requirement: CMake >= 3.28, Ninja/Visual Studio generator, Linux + GCC >= 14. "
                   "Current: ${CMAKE_SYSTEM_NAME}, ${CMAKE_GENERATOR}, ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}. "
                   "Reason: ${GALAY_ETCD_IMPORT_UNAVAILABLE_REASON}.")
endif()

function(galay_etcd_apply_cxx target_name)
    set_target_properties(${target_name} PROPERTIES
        CXX_STANDARD ${GALAY_ETCD_CXX_STANDARD}
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS ${GALAY_ETCD_CXX_EXTENSIONS}
    )
endfunction()
