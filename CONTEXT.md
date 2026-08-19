# Cedar Domain Context

## Storage and distribution

| Term | Definition |
| --- | --- |
| PartID | A 32-bit immutable logical virtual-partition identifier. It is not a physical host, process, replica, or tablet identifier. PartID 0 is the explicit default local partition. |
| VertexRef | The globally routable identity `(part_id, vertex_id)`. A bare VertexId is local to one PartID. |
| EdgeRef | The globally routable identity `(home_part_id, edge_id)`. The home PartID is immutable and defaults to the source vertex's PartID. |
| Edge identity | The immutable relationship from an EdgeRef to its source VertexRef, target VertexRef, and edge type. |
| Edge state | A time-varying assertion or retraction of an edge's existence. It does not change edge identity. |
| Fact chain | Facts sharing `(part_id, family, property_id, entity_id)` across valid time and commit sequence. |
| Business version | A Cedar fact event distinguished by valid time and Cedar commit sequence. |
| Storage version | A RocksDB representation version distinguished by RocksDB sequence and value type. It is not a Cedar commit sequence. |
| Authoritative facts | The facts used for recovery and truth. In immutable storage they live only in RocksDB-managed columnar facts files. |
| Derived projection | Rebuildable data used to accelerate a read pattern. It is never authoritative facts. |
