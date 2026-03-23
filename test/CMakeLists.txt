if(NOT TARGET PkgConfig::SIMDJSON)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(SIMDJSON REQUIRED IMPORTED_TARGET simdjson)
endif()

function(galay_etcd_apply_local_dep_includes target_name)
    if(GALAY_ETCD_LOCAL_DEP_INCLUDE_DIRS)
        target_include_directories(${target_name} BEFORE PRIVATE ${GALAY_ETCD_LOCAL_DEP_INCLUDE_DIRS})
        foreach(_galay_etcd_local_dep_include IN LISTS GALAY_ETCD_LOCAL_DEP_INCLUDE_DIRS)
            target_compile_options(${target_name} BEFORE PRIVATE "-I${_galay_etcd_local_dep_include}")
        endforeach()
    endif()
endfunction()

function(galay_etcd_add_test_target target_name source_name)
    add_executable(${target_name} ${source_name})
    target_link_libraries(${target_name} PRIVATE galay-etcd)
    set_property(TARGET ${target_name} PROPERTY NO_SYSTEM_FROM_IMPORTED ON)
    galay_etcd_apply_local_dep_includes(${target_name})
    galay_etcd_apply_cxx(${target_name})
    add_test(NAME ${target_name} COMMAND $<TARGET_FILE:${target_name}>)
endfunction()

galay_etcd_add_test_target(T1-EtcdSmoke T1-etcd_smoke.cc)
galay_etcd_add_test_target(T2-EtcdPrefixOps T2-etcd_prefix_ops.cc)
galay_etcd_add_test_target(T3-EtcdPipeline T3-etcd_pipeline.cc)
galay_etcd_add_test_target(T4-AsyncEtcdSmoke T4-async_etcd_smoke.cc)
galay_etcd_add_test_target(T5-AsyncEtcdPipeline T5-async_etcd_pipeline.cc)

add_executable(T6-EtcdInternalHelpers T6-etcd_internal_helpers.cc)
target_link_libraries(T6-EtcdInternalHelpers PRIVATE galay-etcd)
target_link_libraries(T6-EtcdInternalHelpers PRIVATE PkgConfig::SIMDJSON)
set_property(TARGET T6-EtcdInternalHelpers PROPERTY NO_SYSTEM_FROM_IMPORTED ON)
galay_etcd_apply_local_dep_includes(T6-EtcdInternalHelpers)
galay_etcd_apply_cxx(T6-EtcdInternalHelpers)
add_test(NAME T6-EtcdInternalHelpers COMMAND $<TARGET_FILE:T6-EtcdInternalHelpers>)

galay_etcd_add_test_target(T7-EtcdAwaitableSurface T7-etcd_awaitable_surface.cc)

add_executable(T8-AsyncBenchmarkSmoke
    T8-async_benchmark_smoke.cc
    ../benchmark/AsyncBenchmarkSupport.cc
)
target_link_libraries(T8-AsyncBenchmarkSmoke PRIVATE galay-etcd)
set_property(TARGET T8-AsyncBenchmarkSmoke PROPERTY NO_SYSTEM_FROM_IMPORTED ON)
galay_etcd_apply_local_dep_includes(T8-AsyncBenchmarkSmoke)
galay_etcd_apply_cxx(T8-AsyncBenchmarkSmoke)
add_test(NAME T8-AsyncBenchmarkSmoke COMMAND $<TARGET_FILE:T8-AsyncBenchmarkSmoke>)
