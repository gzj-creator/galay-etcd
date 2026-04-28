add_executable(B1-EtcdKvBenchmark b1_kv.cc)
target_link_libraries(B1-EtcdKvBenchmark PRIVATE galay-etcd)
set_property(TARGET B1-EtcdKvBenchmark PROPERTY NO_SYSTEM_FROM_IMPORTED ON)
galay_etcd_apply_cxx(B1-EtcdKvBenchmark)

add_executable(B2-AsyncEtcdKvBenchmark
    b2_kv.cc
    bench_support.cc
)
target_link_libraries(B2-AsyncEtcdKvBenchmark PRIVATE galay-etcd)
set_property(TARGET B2-AsyncEtcdKvBenchmark PROPERTY NO_SYSTEM_FROM_IMPORTED ON)
galay_etcd_apply_cxx(B2-AsyncEtcdKvBenchmark)
