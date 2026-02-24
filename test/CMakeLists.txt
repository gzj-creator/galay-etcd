add_executable(T1-EtcdSmoke T1-EtcdSmoke.cc)
target_link_libraries(T1-EtcdSmoke PRIVATE galay-etcd)
galay_etcd_apply_cxx(T1-EtcdSmoke)

add_executable(T2-EtcdPrefixOps T2-EtcdPrefixOps.cc)
target_link_libraries(T2-EtcdPrefixOps PRIVATE galay-etcd)
galay_etcd_apply_cxx(T2-EtcdPrefixOps)

add_executable(T3-EtcdPipeline T3-EtcdPipeline.cc)
target_link_libraries(T3-EtcdPipeline PRIVATE galay-etcd)
galay_etcd_apply_cxx(T3-EtcdPipeline)

add_executable(T4-AsyncEtcdSmoke T4-AsyncEtcdSmoke.cc)
target_link_libraries(T4-AsyncEtcdSmoke PRIVATE galay-etcd)
galay_etcd_apply_cxx(T4-AsyncEtcdSmoke)

add_executable(T5-AsyncEtcdPipeline T5-AsyncEtcdPipeline.cc)
target_link_libraries(T5-AsyncEtcdPipeline PRIVATE galay-etcd)
galay_etcd_apply_cxx(T5-AsyncEtcdPipeline)
