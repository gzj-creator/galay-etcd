#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "galay-etcd::galay-etcd" for configuration "Release"
set_property(TARGET galay-etcd::galay-etcd APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(galay-etcd::galay-etcd PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libgalay-etcd.dylib"
  IMPORTED_SONAME_RELEASE "@rpath/libgalay-etcd.dylib"
  )

list(APPEND _cmake_import_check_targets galay-etcd::galay-etcd )
list(APPEND _cmake_import_check_files_for_galay-etcd::galay-etcd "${_IMPORT_PREFIX}/lib/libgalay-etcd.dylib" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
