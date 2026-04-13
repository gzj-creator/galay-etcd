add_executable(B1-EtcdKvBenchmark B1-etcd_kv_benchmark.cc)
target_link_libraries(B1-EtcdKvBenchmark PRIVATE galay-etcd)
set_property(TARGET B1-EtcdKvBenchmark PROPERTY NO_SYSTEM_FROM_IMPORTED ON)
galay_etcd_apply_cxx(B1-EtcdKvBenchmark)

add_executable(B2-AsyncEtcdKvBenchmark
    B2-async_etcd_kv_benchmark.cc
    AsyncBenchmarkSupport.cc
)
target_link_libraries(B2-AsyncEtcdKvBenchmark PRIVATE galay-etcd)
set_property(TARGET B2-AsyncEtcdKvBenchmark PROPERTY NO_SYSTEM_FROM_IMPORTED ON)
galay_etcd_apply_cxx(B2-AsyncEtcdKvBenchmark)
