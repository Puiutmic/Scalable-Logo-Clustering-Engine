cmake_minimum_required(VERSION 3.20)

project(
    LogoClusterer
    VERSION 2.0
    LANGUAGES CXX
)


set(
    CMAKE_CXX_STANDARD
    20
)

set(
    CMAKE_CXX_STANDARD_REQUIRED
    ON
)

set(
    CMAKE_CXX_EXTENSIONS
    OFF
)


option(
    ENABLE_SANITIZERS
    "Enable AddressSanitizer and UndefinedBehaviorSanitizer"
    OFF
)


# ============================================================
# CORE LIBRARY
# ============================================================

add_library(
    clustering_core
    src/clustering.cpp
)


target_include_directories(
    clustering_core
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)


# ============================================================
# CLI EXECUTABLE
# ============================================================

add_executable(
    logo_clusterer
    src/main.cpp
)


target_link_libraries(
    logo_clusterer
    PRIVATE
    clustering_core
)


# ============================================================
# WARNINGS
# ============================================================

if(MSVC)

    target_compile_options(
        clustering_core
        PRIVATE
        /W4
        /permissive-
    )

    target_compile_options(
        logo_clusterer
        PRIVATE
        /W4
        /permissive-
    )

else()

    target_compile_options(
        clustering_core
        PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )

    target_compile_options(
        logo_clusterer
        PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )

endif()


# ============================================================
# SANITIZERS
# Linux / Clang / GCC
# ============================================================

if(
    ENABLE_SANITIZERS
    AND NOT MSVC
)

    target_compile_options(
        clustering_core
        PRIVATE
        -fsanitize=address,undefined
        -fno-omit-frame-pointer
    )

    target_link_options(
        clustering_core
        PRIVATE
        -fsanitize=address,undefined
    )

    target_compile_options(
        logo_clusterer
        PRIVATE
        -fsanitize=address,undefined
        -fno-omit-frame-pointer
    )

    target_link_options(
        logo_clusterer
        PRIVATE
        -fsanitize=address,undefined
    )

endif()


# ============================================================
# TESTING
# ============================================================

include(CTest)

if(BUILD_TESTING)

    include(FetchContent)

    if(MSVC)
        set(
            gtest_force_shared_crt
            ON
            CACHE BOOL
            ""
            FORCE
        )
    endif()


    FetchContent_Declare(
        googletest

        URL
        https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
    )


    FetchContent_MakeAvailable(
        googletest
    )


    add_executable(
        clustering_tests
        tests/test_clustering.cpp
    )


    target_link_libraries(
        clustering_tests
        PRIVATE
        clustering_core
        GTest::gtest_main
    )


    if(
        ENABLE_SANITIZERS
        AND NOT MSVC
    )

        target_compile_options(
            clustering_tests
            PRIVATE
            -fsanitize=address,undefined
            -fno-omit-frame-pointer
        )

        target_link_options(
            clustering_tests
            PRIVATE
            -fsanitize=address,undefined
        )

    endif()


    include(GoogleTest)

    gtest_discover_tests(
        clustering_tests
    )

endif()