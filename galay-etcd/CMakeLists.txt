find_package(galay-kernel REQUIRED)
find_package(galay-utils REQUIRED)
find_package(GalayHttp REQUIRED)
find_package(spdlog REQUIRED)
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
    target_sources(${PROJECT_NAME}
        PUBLIC
            FILE_SET CXX_MODULES
            FILES ${CMAKE_CURRENT_SOURCE_DIR}/module/galay.etcd.cppm
    )
    if(GALAY_ETCD_IMPORT_COMPILE_OPTIONS)
        target_compile_options(${PROJECT_NAME} PUBLIC ${GALAY_ETCD_IMPORT_COMPILE_OPTIONS})
    endif()
endif()

if(GALAY_ETCD_IMPORT_COMPILATION_ENABLED AND GALAY_ETCD_INSTALL_MODULE_INTERFACE AND CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
    install(TARGETS ${PROJECT_NAME}
        EXPORT galay-etcd-targets
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        INCLUDES DESTINATION include
        FILE_SET CXX_MODULES DESTINATION include/galay-etcd/module
    )
else()
    install(TARGETS ${PROJECT_NAME}
        EXPORT galay-etcd-targets
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        INCLUDES DESTINATION include
    )
endif()

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/
    DESTINATION include/galay-etcd
    FILES_MATCHING
    PATTERN "*.h"
    PATTERN "*.hpp"
    PATTERN "*.inl"
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

install(EXPORT galay-etcd-targets
    FILE GalayEtcdTargets.cmake
    NAMESPACE galay-etcd::
    DESTINATION lib/cmake/galay-etcd
)

include(CMakePackageConfigHelpers)
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/../cmake/GalayEtcdConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/GalayEtcdConfig.cmake"
    INSTALL_DESTINATION lib/cmake/galay-etcd
)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/GalayEtcdConfigVersion.cmake"
    VERSION 1.0.0
    COMPATIBILITY AnyNewerVersion
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/GalayEtcdConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/GalayEtcdConfigVersion.cmake"
    DESTINATION lib/cmake/galay-etcd
)
