cmake_minimum_required(VERSION 3.20)
project(galay-etcd VERSION 1.1.2 LANGUAGES CXX)

set(CMAKE_FIND_PACKAGE_PREFER_CONFIG ON)
set(CMAKE_NO_SYSTEM_FROM_IMPORTED ON)

set(GALAY_ETCD_LOCAL_KERNEL_PREFIX "${CMAKE_SOURCE_DIR}/../galay-kernel/install-local")
set(GALAY_ETCD_LOCAL_HTTP_PREFIX "${CMAKE_SOURCE_DIR}/../galay-http/install-local")
set(GALAY_ETCD_LOCAL_UTILS_PREFIX "${CMAKE_SOURCE_DIR}/../galay-utils/install-local")

set(GALAY_ETCD_LOCAL_KERNEL_PACKAGE_DIRS
    "${GALAY_ETCD_LOCAL_KERNEL_PREFIX}/lib/cmake/galay-kernel"
    "${CMAKE_SOURCE_DIR}/../galay-kernel/build-codex-release-sweep/galay-kernel"
    "${CMAKE_SOURCE_DIR}/../galay-kernel/build/galay-kernel"
    "${CMAKE_SOURCE_DIR}/../galay-kernel/build-install-release-clean/galay-kernel"
    "${CMAKE_SOURCE_DIR}/../galay-kernel/build-install-release-verify/galay-kernel"
)

foreach(_galay_etcd_kernel_dir IN LISTS GALAY_ETCD_LOCAL_KERNEL_PACKAGE_DIRS)
    if(EXISTS "${_galay_etcd_kernel_dir}/galay-kernel-config.cmake")
        set(galay-kernel_DIR
            "${_galay_etcd_kernel_dir}"
            CACHE PATH "Preferred local galay-kernel package directory" FORCE)
        break()
    endif()
endforeach()

set(GALAY_ETCD_LOCAL_HTTP_PACKAGE_DIRS
    "${GALAY_ETCD_LOCAL_HTTP_PREFIX}/lib/cmake/GalayHttp"
    "${CMAKE_SOURCE_DIR}/../galay-http/build-codex-release-sweep"
    "${CMAKE_SOURCE_DIR}/../galay-http/build"
)

foreach(_galay_etcd_http_dir IN LISTS GALAY_ETCD_LOCAL_HTTP_PACKAGE_DIRS)
    if(EXISTS "${_galay_etcd_http_dir}/GalayHttpConfig.cmake")
        set(GalayHttp_DIR
            "${_galay_etcd_http_dir}"
            CACHE PATH "Preferred local GalayHttp package directory" FORCE)
        break()
    endif()
endforeach()

if(EXISTS "${GALAY_ETCD_LOCAL_UTILS_PREFIX}/lib/cmake/galay-utils/galay-utils-config.cmake")
    set(galay-utils_DIR
        "${GALAY_ETCD_LOCAL_UTILS_PREFIX}/lib/cmake/galay-utils"
        CACHE PATH "Preferred local galay-utils package directory" FORCE)
endif()

set(GALAY_ETCD_LOCAL_PREFIX_HINTS
    "${GALAY_ETCD_LOCAL_KERNEL_PREFIX}"
    "${CMAKE_SOURCE_DIR}/../galay-kernel/build-codex-release-sweep"
    "${CMAKE_SOURCE_DIR}/../galay-kernel/build"
    "${CMAKE_SOURCE_DIR}/../galay-kernel/build-install-release-clean"
    "${CMAKE_SOURCE_DIR}/../galay-kernel/build-install-release-verify"
    "${GALAY_ETCD_LOCAL_HTTP_PREFIX}"
    "${CMAKE_SOURCE_DIR}/../galay-http/build-codex-release-sweep"
    "${CMAKE_SOURCE_DIR}/../galay-http/build"
    "${GALAY_ETCD_LOCAL_UTILS_PREFIX}"
)

set(GALAY_ETCD_LOCAL_DEP_INCLUDE_DIRS "")
foreach(_galay_etcd_local_dep_prefix
        IN ITEMS
        "${GALAY_ETCD_LOCAL_KERNEL_PREFIX}"
        "${GALAY_ETCD_LOCAL_HTTP_PREFIX}"
        "${GALAY_ETCD_LOCAL_UTILS_PREFIX}")
    if(EXISTS "${_galay_etcd_local_dep_prefix}/include")
        list(APPEND GALAY_ETCD_LOCAL_DEP_INCLUDE_DIRS "${_galay_etcd_local_dep_prefix}/include")
    endif()
endforeach()
list(REMOVE_DUPLICATES GALAY_ETCD_LOCAL_DEP_INCLUDE_DIRS)

set(GALAY_ETCD_LOCAL_DEP_LIBRARY_DIRS "")
foreach(_galay_etcd_local_lib_dir
        IN ITEMS
        "${GALAY_ETCD_LOCAL_KERNEL_PREFIX}/lib"
        "${CMAKE_SOURCE_DIR}/../galay-kernel/build-codex-release-sweep/lib"
        "${CMAKE_SOURCE_DIR}/../galay-kernel/build/lib"
        "${CMAKE_SOURCE_DIR}/../galay-kernel/build-install-release-clean/lib"
        "${CMAKE_SOURCE_DIR}/../galay-kernel/build-install-release-verify/lib"
        "${GALAY_ETCD_LOCAL_HTTP_PREFIX}/lib"
        "${CMAKE_SOURCE_DIR}/../galay-http/build-codex-release-sweep/galay-http"
        "${CMAKE_SOURCE_DIR}/../galay-http/build/galay-http"
        "${GALAY_ETCD_LOCAL_UTILS_PREFIX}/lib")
    if(EXISTS "${_galay_etcd_local_lib_dir}")
        list(APPEND GALAY_ETCD_LOCAL_DEP_LIBRARY_DIRS "${_galay_etcd_local_lib_dir}")
    endif()
endforeach()
list(REMOVE_DUPLICATES GALAY_ETCD_LOCAL_DEP_LIBRARY_DIRS)

if(GALAY_ETCD_LOCAL_DEP_LIBRARY_DIRS)
    set(CMAKE_BUILD_RPATH "${GALAY_ETCD_LOCAL_DEP_LIBRARY_DIRS}")
    set(CMAKE_INSTALL_RPATH "${GALAY_ETCD_LOCAL_DEP_LIBRARY_DIRS}")
endif()
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH ON)

foreach(prefix IN LISTS GALAY_ETCD_LOCAL_PREFIX_HINTS)
    if(EXISTS "${prefix}")
        list(PREPEND CMAKE_PREFIX_PATH "${prefix}")
    endif()
endforeach()
list(REMOVE_DUPLICATES CMAKE_PREFIX_PATH)

include(cmake/option.cmake)

message(STATUS "GALAY_ETCD_CXX_STANDARD: ${GALAY_ETCD_CXX_STANDARD}")
message(STATUS "GALAY_ETCD_BUILD_SHARED_LIBS: ${GALAY_ETCD_BUILD_SHARED_LIBS}")
message(STATUS "GALAY_ETCD_INSTALL_MODULE_INTERFACE: ${GALAY_ETCD_INSTALL_MODULE_INTERFACE}")
message(STATUS "GALAY_ETCD_IMPORT_COMPILATION_ENABLED: ${GALAY_ETCD_IMPORT_COMPILATION_ENABLED}")
message(STATUS "GALAY_ETCD_BUILD_TESTS: ${GALAY_ETCD_BUILD_TESTS}")
message(STATUS "GALAY_ETCD_BUILD_BENCHMARKS: ${GALAY_ETCD_BUILD_BENCHMARKS}")
message(STATUS "GALAY_ETCD_BUILD_EXAMPLES: ${GALAY_ETCD_BUILD_EXAMPLES}")

add_subdirectory(galay-etcd)

if(GALAY_ETCD_BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
endif()

if(GALAY_ETCD_BUILD_BENCHMARKS)
    add_subdirectory(benchmark)
endif()

if(GALAY_ETCD_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
