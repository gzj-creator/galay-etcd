function(galay_etcd_apply_local_dep_includes target_name)
    if(GALAY_ETCD_LOCAL_DEP_INCLUDE_DIRS)
        target_include_directories(${target_name} BEFORE PRIVATE ${GALAY_ETCD_LOCAL_DEP_INCLUDE_DIRS})
        foreach(_galay_etcd_local_dep_include IN LISTS GALAY_ETCD_LOCAL_DEP_INCLUDE_DIRS)
            target_compile_options(${target_name} BEFORE PRIVATE "-I${_galay_etcd_local_dep_include}")
        endforeach()
    endif()
endfunction()

add_executable(B1-EtcdKvBenchmark B1-etcd_kv_benchmark.cc)
target_link_libraries(B1-EtcdKvBenchmark PRIVATE galay-etcd)
set_property(TARGET B1-EtcdKvBenchmark PROPERTY NO_SYSTEM_FROM_IMPORTED ON)
galay_etcd_apply_local_dep_includes(B1-EtcdKvBenchmark)
galay_etcd_apply_cxx(B1-EtcdKvBenchmark)

add_executable(B2-AsyncEtcdKvBenchmark
    B2-async_etcd_kv_benchmark.cc
    AsyncBenchmarkSupport.cc
)
target_link_libraries(B2-AsyncEtcdKvBenchmark PRIVATE galay-etcd)
set_property(TARGET B2-AsyncEtcdKvBenchmark PROPERTY NO_SYSTEM_FROM_IMPORTED ON)
galay_etcd_apply_local_dep_includes(B2-AsyncEtcdKvBenchmark)
galay_etcd_apply_cxx(B2-AsyncEtcdKvBenchmark)
