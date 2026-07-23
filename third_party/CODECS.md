# Bundled compression codecs

Cedar release builds compile these source snapshots directly and never install
or download codec packages at database startup.

| Codec | Upstream version | Release archive SHA-256 | License |
|---|---:|---|---|
| LZ4 | 1.10.0 | `537512904744b35e232912055ccf8ec66d768639ff3abe5788d90d792ec5f48b` | BSD 2-Clause (`third_party/lz4/LICENSE`) |
| Zstd | 1.5.7 | `37d7284556b20954e56e1ca85b80226768902e2edabd3b649e9e72c0c9012ee3` | BSD 3-Clause / GPLv2 dual license; Cedar uses the BSD terms (`third_party/zstd/LICENSE`) |

Source archives:

- `https://github.com/lz4/lz4/archive/refs/tags/v1.10.0.tar.gz`
- `https://github.com/facebook/zstd/archive/refs/tags/v1.5.7.tar.gz`

The LZ4 snapshot retains the upstream `lib/` directory. The Zstd snapshot
retains the public headers and the `common/`, `compress/`, and `decompress/`
library directories required by Cedar's page codec. Cedar does not compile the
upstream command-line programs, tests, legacy decoders, deprecated API, or
dictionary-builder tools.
