add_executable(E1-SyncBasic E1-SyncBasic.cc)
target_link_libraries(E1-SyncBasic PRIVATE galay-etcd)
galay_etcd_apply_cxx(E1-SyncBasic)

add_executable(E2-AsyncBasic E2-AsyncBasic.cc)
target_link_libraries(E2-AsyncBasic PRIVATE galay-etcd)
galay_etcd_apply_cxx(E2-AsyncBasic)
