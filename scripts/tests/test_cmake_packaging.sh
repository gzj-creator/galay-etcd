#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
TMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/galay-etcd-cmake.XXXXXX")

cleanup() {
    rm -rf "$TMP_ROOT"
}

trap cleanup EXIT INT TERM HUP

fail() {
    printf '%s\n' "FAIL: $*" >&2
    exit 1
}

write_file() {
    path=$1
    shift

    mkdir -p "$(dirname "$path")"
    cat > "$path"
}

DUMMY_PREFIX="$TMP_ROOT/dummy-prefix"
BUILD_DIR="$TMP_ROOT/build"
INSTALL_PREFIX="$TMP_ROOT/install"
CONSUMER="$TMP_ROOT/consumer"

mkdir -p "$DUMMY_PREFIX/lib/cmake" "$DUMMY_PREFIX/lib/pkgconfig" "$DUMMY_PREFIX/include" "$DUMMY_PREFIX/lib"
: > "$DUMMY_PREFIX/lib/libsimdjson.a"

write_file "$DUMMY_PREFIX/lib/cmake/galay-kernel/galay-kernel-config.cmake" <<'EOF'
if(NOT TARGET galay-kernel::galay-kernel)
    add_library(galay-kernel::galay-kernel INTERFACE IMPORTED)
endif()
EOF

write_file "$DUMMY_PREFIX/lib/cmake/galay-kernel/galay-kernel-config-version.cmake" <<'EOF'
set(PACKAGE_VERSION "3.4.4")
set(PACKAGE_VERSION_COMPATIBLE TRUE)
set(PACKAGE_VERSION_EXACT TRUE)
EOF

write_file "$DUMMY_PREFIX/lib/cmake/galay-utils/galay-utils-config.cmake" <<'EOF'
if(NOT TARGET galay::galay-utils)
    add_library(galay::galay-utils INTERFACE IMPORTED)
endif()
EOF

write_file "$DUMMY_PREFIX/lib/cmake/galay-utils/galay-utils-config-version.cmake" <<'EOF'
set(PACKAGE_VERSION "1.0.3")
set(PACKAGE_VERSION_COMPATIBLE TRUE)
set(PACKAGE_VERSION_EXACT TRUE)
EOF

write_file "$DUMMY_PREFIX/lib/cmake/galay-http/galay-http-config.cmake" <<'EOF'
if(NOT TARGET galay-http::galay-http)
    add_library(galay-http::galay-http INTERFACE IMPORTED)
endif()
EOF

write_file "$DUMMY_PREFIX/lib/cmake/galay-http/galay-http-config-version.cmake" <<'EOF'
set(PACKAGE_VERSION "2.1.0")
set(PACKAGE_VERSION_COMPATIBLE TRUE)
set(PACKAGE_VERSION_EXACT TRUE)
EOF

write_file "$DUMMY_PREFIX/lib/cmake/spdlog/spdlogConfig.cmake" <<'EOF'
if(NOT TARGET spdlog::spdlog)
    add_library(spdlog::spdlog INTERFACE IMPORTED)
endif()
EOF

write_file "$DUMMY_PREFIX/lib/cmake/spdlog/spdlogConfigVersion.cmake" <<'EOF'
set(PACKAGE_VERSION "1.0.0")
set(PACKAGE_VERSION_COMPATIBLE TRUE)
EOF

write_file "$DUMMY_PREFIX/lib/pkgconfig/simdjson.pc" <<EOF
prefix=$DUMMY_PREFIX
exec_prefix=$DUMMY_PREFIX
libdir=$DUMMY_PREFIX/lib
includedir=$DUMMY_PREFIX/include
Name: simdjson
Description: dummy simdjson for packaging test
Version: 3.0.0
Libs: -L$DUMMY_PREFIX/lib -lsimdjson
Cflags: -I$DUMMY_PREFIX/include
EOF

PKG_CONFIG_PATH="$DUMMY_PREFIX/lib/pkgconfig" \
cmake -S "$REPO_ROOT" -B "$BUILD_DIR" \
    -DCMAKE_PREFIX_PATH="$DUMMY_PREFIX" \
    -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX" \
    -DBUILD_TESTING=OFF \
    -DGALAY_ETCD_BUILD_BENCHMARKS=OFF \
    -DGALAY_ETCD_BUILD_EXAMPLES=OFF >/dev/null

INSTALL_SCRIPT="$BUILD_DIR/galay-etcd/cmake_install.cmake"
[ -f "$INSTALL_SCRIPT" ] || fail "missing install script: $INSTALL_SCRIPT"

awk -F'"' '
    /TYPE SHARED_LIBRARY FILES|TYPE STATIC_LIBRARY FILES|TYPE ARCHIVE FILES/ {
        for (i = 2; i <= NF; i += 2) {
            print $i
        }
    }
' "$INSTALL_SCRIPT" | while IFS= read -r artifact; do
    [ -n "$artifact" ] || continue
    mkdir -p "$(dirname "$artifact")"
    : > "$artifact"
done

cmake -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX" -P "$INSTALL_SCRIPT" >/dev/null

[ -f "$INSTALL_PREFIX/lib/cmake/galay-etcd/galay-etcd-config.cmake" ] \
    || fail "missing installed config file"
[ -f "$INSTALL_PREFIX/lib/cmake/galay-etcd/galay-etcd-config-version.cmake" ] \
    || fail "missing installed config-version file"
[ -f "$INSTALL_PREFIX/lib/cmake/galay-etcd/galay-etcd-targets.cmake" ] \
    || fail "missing installed targets file"

write_file "$CONSUMER/CMakeLists.txt" <<'EOF'
cmake_minimum_required(VERSION 3.20)
project(galay_etcd_consumer LANGUAGES CXX)

find_package(galay-etcd REQUIRED CONFIG)

if(NOT TARGET galay-etcd::galay-etcd)
    message(FATAL_ERROR "galay-etcd::galay-etcd target is missing from galay-etcd package")
endif()
EOF

cmake -S "$CONSUMER" -B "$TMP_ROOT/consumer-build" \
    -DCMAKE_PREFIX_PATH="$INSTALL_PREFIX;$DUMMY_PREFIX" >/dev/null

printf '%s\n' "ok"
