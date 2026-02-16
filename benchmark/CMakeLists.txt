add_executable(B1-EtcdKvBenchmark B1-EtcdKvBenchmark.cc)
target_link_libraries(B1-EtcdKvBenchmark PRIVATE galay-etcd)
galay_etcd_apply_cxx(B1-EtcdKvBenchmark)
