# Cedar Arrow/Parquet Build Inputs

Cedar builds the facts-table Parquet runtime from pinned submodules and links
static archives only. The dependency wrapper rejects a different checkout or
an incomplete cache before a Cedar target is configured.

| Input | Pinned revision | Use |
| --- | --- | --- |
| Apache Arrow | `59bea6ec485e7fe351d1aa6753f964f6a6bc353a` (`25.0.0`) | Arrow C++ and Parquet static archives |
| Apache Thrift | `af9ac170f4de895266de4b6f9f3e68a58f113760` (`0.22.0`) | Parquet metadata protocol |
| xsimd | `80c23624ce008d937da7e845e528e82ce0cbf4e0` (`14.2.0`) | Arrow CPU feature abstraction |
| Boost | `199ef13d6034c85232431130142159af3adfce22` (`1.88.0`) | Thrift's header-only dependency |
| RapidJSON | `f54b0e47a08782a6131cc3d60f94d038fa6e0a51` (`1.1.0`) | Parquet metadata implementation |

`cmake/CedarArrowParquet.cmake` builds xsimd from this checkout into the same
fingerprinted cache and makes Arrow resolve it as a local system CMake package.
Thrift is supplied through `FETCHCONTENT_SOURCE_DIR_THRIFT`; Boost and
RapidJSON resolve only from their local pinned checkout. The fingerprint
includes every source revision, local patch digest, compiler, SDK, target
architecture, and the feature set. Builds always use one compiler job.
