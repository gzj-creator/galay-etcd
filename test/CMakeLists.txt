if(NOT TARGET PkgConfig::SIMDJSON)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(SIMDJSON REQUIRED IMPORTED_TARGET simdjson)
endif()

add_executable(T1-EtcdSmoke T1-etcd_smoke.cc)
target_link_libraries(T1-EtcdSmoke PRIVATE galay-etcd)
galay_etcd_apply_cxx(T1-EtcdSmoke)

add_executable(T2-EtcdPrefixOps T2-etcd_prefix_ops.cc)
target_link_libraries(T2-EtcdPrefixOps PRIVATE galay-etcd)
galay_etcd_apply_cxx(T2-EtcdPrefixOps)

add_executable(T3-EtcdPipeline T3-etcd_pipeline.cc)
target_link_libraries(T3-EtcdPipeline PRIVATE galay-etcd)
galay_etcd_apply_cxx(T3-EtcdPipeline)

add_executable(T4-AsyncEtcdSmoke T4-async_etcd_smoke.cc)
target_link_libraries(T4-AsyncEtcdSmoke PRIVATE galay-etcd)
galay_etcd_apply_cxx(T4-AsyncEtcdSmoke)

add_executable(T5-AsyncEtcdPipeline T5-async_etcd_pipeline.cc)
target_link_libraries(T5-AsyncEtcdPipeline PRIVATE galay-etcd)
galay_etcd_apply_cxx(T5-AsyncEtcdPipeline)

add_executable(T6-EtcdInternalHelpers T6-etcd_internal_helpers.cc)
target_link_libraries(T6-EtcdInternalHelpers PRIVATE galay-etcd)
target_link_libraries(T6-EtcdInternalHelpers PRIVATE PkgConfig::SIMDJSON)
galay_etcd_apply_cxx(T6-EtcdInternalHelpers)
