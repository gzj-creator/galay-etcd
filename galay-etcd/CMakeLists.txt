find_package(galay-kernel 3.4.4 CONFIG REQUIRED)
find_package(galay-utils 1.0.3 CONFIG REQUIRED)
find_package(GalayHttp 2.1.0 CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(SIMDJSON REQUIRED IMPORTED_TARGET simdjson)

if(NOT TARGET galay-kernel::galay-kernel)
    message(FATAL_ERROR "galay-kernel::galay-kernel target not found. Please check galay-kernel installation.")
endif()

if(NOT TARGET galay::galay-utils)
    message(FATAL_ERROR "galay::galay-utils target not found. Please check galay-utils installation.")
endif()

if(NOT TARGET GalayHttp::galay-http)
    message(FATAL_ERROR "GalayHttp::galay-http target not found. Please check galay-http installation.")
endif()

file(GLOB SOURCES
    "base/*.cc"
    "async/*.cc"
    "sync/*.cc"
)

add_library(${PROJECT_NAME} ${GALAY_ETCD_LIBRARY_TYPE} ${SOURCES})
set_property(TARGET ${PROJECT_NAME} PROPERTY NO_SYSTEM_FROM_IMPORTED ON)

target_link_libraries(${PROJECT_NAME}
    PUBLIC galay-kernel::galay-kernel
    PUBLIC galay::galay-utils
    PUBLIC GalayHttp::galay-http
    PUBLIC spdlog::spdlog
    PUBLIC pthread
    PRIVATE PkgConfig::SIMDJSON
)

target_include_directories(${PROJECT_NAME}
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    PUBLIC $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
    PUBLIC $<INSTALL_INTERFACE:include>
)

galay_etcd_apply_cxx(${PROJECT_NAME})

if(GALAY_ETCD_IMPORT_COMPILATION_ENABLED AND CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
    add_library(galay-etcd-modules STATIC)
    target_link_libraries(galay-etcd-modules PUBLIC ${PROJECT_NAME})
    target_include_directories(galay-etcd-modules
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
            $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
            $<INSTALL_INTERFACE:include>
    )
    target_sources(galay-etcd-modules
        PUBLIC
            FILE_SET CXX_MODULES
            FILES ${CMAKE_CURRENT_SOURCE_DIR}/module/galay.etcd.cppm
    )
    if(GALAY_ETCD_IMPORT_COMPILE_OPTIONS)
        target_compile_options(galay-etcd-modules PUBLIC ${GALAY_ETCD_IMPORT_COMPILE_OPTIONS})
    endif()
    set_target_properties(galay-etcd-modules PROPERTIES CXX_SCAN_FOR_MODULES ON)
    galay_etcd_apply_cxx(galay-etcd-modules)
endif()

install(TARGETS ${PROJECT_NAME}
    EXPORT galay-etcd-targets
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
    INCLUDES DESTINATION include
)

if(GALAY_ETCD_IMPORT_COMPILATION_ENABLED AND GALAY_ETCD_INSTALL_MODULE_INTERFACE AND CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
    install(TARGETS galay-etcd-modules
        EXPORT galay-etcd-targets
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        INCLUDES DESTINATION include
        FILE_SET CXX_MODULES DESTINATION include/galay-etcd/module
    )
endif()

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/
    DESTINATION include/galay-etcd
    FILES_MATCHING
    PATTERN "*.h"
    PATTERN "*.hpp"
    PATTERN "*.inl"
    PATTERN "base/EtcdInternal.h" EXCLUDE
    PATTERN "build" EXCLUDE
    PATTERN "CMakeFiles" EXCLUDE
    PATTERN "*.cc" EXCLUDE
    PATTERN "*.cmake" EXCLUDE
)

if(GALAY_ETCD_INSTALL_MODULE_INTERFACE AND NOT GALAY_ETCD_IMPORT_COMPILATION_ENABLED)
    file(GLOB GALAY_ETCD_MODULE_INTERFACE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/module/*.cppm")
    if(GALAY_ETCD_MODULE_INTERFACE_FILES)
        install(FILES ${GALAY_ETCD_MODULE_INTERFACE_FILES}
            DESTINATION include/galay-etcd/module
        )
    endif()
endif()

set(GALAY_ETCD_CMAKE_PACKAGE_DIR "lib/cmake/GalayEtcd")
set(GALAY_ETCD_CMAKE_COMPAT_PACKAGE_DIR "lib/cmake/galay-etcd")

install(EXPORT galay-etcd-targets
    FILE GalayEtcdTargets.cmake
    NAMESPACE galay-etcd::
    DESTINATION ${GALAY_ETCD_CMAKE_PACKAGE_DIR}
)

include(CMakePackageConfigHelpers)
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/../cmake/galay-etcd-config.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/GalayEtcdConfig.cmake"
    INSTALL_DESTINATION ${GALAY_ETCD_CMAKE_PACKAGE_DIR}
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/../cmake/galay-etcd-compat-config.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/galay-etcdConfig.cmake"
    @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/../cmake/galay-etcd-compat-config.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/galay-etcd-config.cmake"
    @ONLY
)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/GalayEtcdConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY AnyNewerVersion
)
configure_file(
    "${CMAKE_CURRENT_BINARY_DIR}/GalayEtcdConfigVersion.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/galay-etcdConfigVersion.cmake"
    COPYONLY
)
configure_file(
    "${CMAKE_CURRENT_BINARY_DIR}/GalayEtcdConfigVersion.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/galay-etcd-config-version.cmake"
    COPYONLY
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/GalayEtcdConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/GalayEtcdConfigVersion.cmake"
    DESTINATION ${GALAY_ETCD_CMAKE_PACKAGE_DIR}
)
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/galay-etcdConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/galay-etcd-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/galay-etcdConfigVersion.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/galay-etcd-config-version.cmake"
    DESTINATION ${GALAY_ETCD_CMAKE_COMPAT_PACKAGE_DIR}
)
