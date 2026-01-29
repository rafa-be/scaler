#!/bin/bash -e
#
# This script builds and installs in-place the C++ components.
#
# Options:
#   --clean    Remove any existing cached build files before building
#   --release  Build with Release optimizations (default is Debug)
#
# Usage:
#      ./scripts/build.sh [--clean] [--release]

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"   # e.g. linux or darwin
ARCH="$(uname -m)"                              # e.g. x86_64 or arm64

# Parse arguments
CLEAN=false
RELEASE=false

for arg in "$@"; do
    case $arg in
        --clean)
            CLEAN=true
            ;;
        --release)
            RELEASE=true
            ;;
        *)
            echo "Unknown option: $arg"
            echo "Usage: ./scripts/build.sh [--clean] [--release]"
            exit 1
            ;;
    esac
done

# Set build directory and preset based on release flag
if [ "$RELEASE" = true ]; then
    BUILD_DIR="build_${OS}_${ARCH}_release"
    BUILD_PRESET="${OS}-${ARCH}-release"
    BUILD_TYPE="Release"
else
    BUILD_DIR="build_${OS}_${ARCH}"
    BUILD_PRESET="${OS}-${ARCH}"
    BUILD_TYPE="Debug"
fi

if [ "$CLEAN" = true ]; then
    rm -rf $BUILD_DIR
    rm -f src/scaler/protocol/capnp/*.c++
    rm -f src/scaler/protocol/capnp/*.h
fi

echo "Build directory: $BUILD_DIR"
echo "Build preset: $BUILD_PRESET"
echo "Build type: $BUILD_TYPE"

# Configure
cmake --preset $BUILD_PRESET "${CMAKE_ARGS[@]}"

# Build
cmake --build --preset $BUILD_PRESET

# Install
cmake --install $BUILD_DIR
