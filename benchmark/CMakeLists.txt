add_executable(B1-EtcdKvBenchmark B1-etcd_kv_benchmark.cc)
target_link_libraries(B1-EtcdKvBenchmark PRIVATE galay-etcd)
galay_etcd_apply_cxx(B1-EtcdKvBenchmark)
