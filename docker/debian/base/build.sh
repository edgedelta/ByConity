#!/usr/bin/env bash
set -e

#ccache -s # uncomment to display CCache statistics
cd /home/ByConity
mkdir -p /home/ByConity/build_docker
mkdir -p /home/ByConity/build_install
git config --add safe.directory /home/ByConity

git_hash=`git rev-parse HEAD`
VERSION_DATE=`git show -s --format=%cs $git_hash`

sed -i \
    -e "s/set (VERSION_NAME [^) ]*/set (VERSION_NAME "ByConity"/g;" \
    -e "s/set (VERSION_SCM [^) ]*/set (VERSION_SCM $VERSION_SCM/g;" \
    cmake/version.cmake

# Set a default value if BREAKPAD_STATUS is empty or unset
BREAKPAD_STATUS=${BREAKPAD_STATUS:-"OFF"}
echo "Breakpad status: $BREAKPAD_STATUS"

cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} ${CMAKE_FLAGS} -DENABLE_BREAKPAD=${BREAKPAD_STATUS} -DCMAKE_INSTALL_PREFIX=build_install -S . -B build_docker
NUM_JOBS=$(( ($(nproc || grep -c ^processor /proc/cpuinfo) + 1) / 2 ))

ninja -C build_docker -j $NUM_JOBS install
