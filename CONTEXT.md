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
| Storage version | An embedded-engine representation version distinguished by engine sequence and value type. It is not a Cedar commit sequence. |
| Authoritative facts | The facts used for recovery and truth. In immutable storage they live only in embedded-engine-managed columnar facts files. |
| Derived projection | Rebuildable data used to accelerate a read pattern. It is never authoritative facts. |

## Temporal query semantics

| Term | Definition |
| --- | --- |
| System time | The Cedar commit sequence fixed by a query Snapshot. It answers which committed database history the query may observe. |
| Valid time | The business-effective time at which a fact is interpreted. It is independent of the Snapshot's system time. |
| Valid-time interval | A half-open business-time interval `[from, to)`. The lower bound is included and the upper bound is excluded. |
| Snapshot-visible correction | For one fact chain and valid-time boundary, the event with the greatest commit sequence not newer than the query Snapshot. Later corrections do not exist in that Snapshot's view. |
| Point state query | Returns the corrected state effective at one valid-time instant in the query Snapshot. |
| Corrected event | The Snapshot-visible PUT or DELETE at one valid-time boundary after competing corrections at that boundary are resolved. It may leave effective state unchanged. |
| Effective change | A corrected event that changes existence or property value relative to the immediately preceding corrected state. |
| Event interval query | Returns corrected events whose valid-time boundary is inside the requested interval. |
| Change interval query | Returns effective changes whose valid-time boundary is inside the requested interval. |
| Overlap state query | Returns entities or properties that are effective at any instant in the requested valid-time interval. |
| Throughout state query | Returns entities or properties that are continuously effective for the entire requested valid-time interval. |
| State history query | Returns corrected maximal effective intervals and values over a requested or unbounded valid-time range in the query Snapshot. |
| Row aggregate | An aggregate over the rows emitted by its input. For interval-bearing input it counts or combines intervals, not an implicit time-varying population. |
| Temporal aggregate | A piecewise history whose value is aggregated from all effective input rows at each valid-time instant and represented as maximal intervals of equal aggregate value. |
| Temporal traversal | One directed traversal from a source VertexRef through an EdgeRef to a target VertexRef, paired with each maximal valid-time interval in which the source, edge, and target are all effective. |
| Overlap expansion | Returns temporal traversals whose effective intervals intersect the requested valid-time interval, clipped to that interval. |
| Throughout expansion | Returns temporal traversals whose effective intervals cover the entire requested valid-time interval. |
| Bidirectional expansion | The union of incoming and outgoing traversals incident to a vertex. A self-loop appears once. |
| Coexisting path | A graph path whose vertices and edges have a non-empty common valid-time interval. The path result retains each maximal common interval. |
| Temporal journey | A sequence of temporal traversals taken at non-decreasing valid times. A journey may wait at a vertex between traversals. |
| Journey wait | The valid-time interval between arrival at a vertex and departure on the next traversal. The vertex must remain effective throughout the wait. |
| Traversal duration | A non-negative valid-time duration explicitly supplied for a journey traversal. The source, edge, and target must remain effective for the traversal, and a missing duration makes that traversal unavailable. |
| Earliest-arrival path | A temporal journey that minimizes arrival valid time from a specified departure bound. Traversal duration is zero unless an explicit duration expression is supplied. |
| Path witness | One concrete path or journey proving a reachability or optimum result. Cedar does not imply enumeration of every qualifying path. |
| Projection coverage | The system-time, schema, partition, key-range, and valid-time region for which a derived projection proves complete query coverage. |
| Projection generation | An immutable, internally consistent set of derived projection data materialized from one authoritative Snapshot. |
| Retired projection generation | A projection generation that accepts no new query readers but remains available to readers that pinned it before retirement. |
| Projection base sequence | The Snapshot commit sequence from which a projection generation was materialized. A newer query needs a complete authoritative delta beyond this sequence. |
| Projection coverage hole | A region for which a projection generation cannot prove completeness. Queries over a hole must use authoritative facts. |
| Adjacency projection | A derived index from a vertex and direction to candidate edge identities and their temporal state. Outgoing and incoming adjacency are distinct projections. |
| Property projection | A derived, property-specific temporal index over entity values and presence. Missing property intervals are not stored facts. |
| Query delta | The complete, commit-ordered authoritative changes after a projection base sequence and through a requested Snapshot. It is rebuildable and is not a second log. |
| Delta coverage | Proof that a query delta contains every committed fact change in one contiguous system-time interval. A gap invalidates projection-plus-delta execution for that interval. |
| Snapshot-correct query | A query whose result reflects its pinned system-time Snapshot exactly. A lagging projection is merged with authoritative fact changes beyond its coverage and never causes a silently stale result. |
| Interactive graph query | A bounded point, adjacency, or small-path query optimized for low first-result latency and prompt cancellation. |
| Columnar analytical query | A range, history, aggregation, or broad graph query optimized for projected batch throughput and page pruning. |
| Missing property | A property with no effective value in the query's bitemporal view. Missing is not a value and is distinct from an Unset history event. |
| Property presence predicate | `IsMissing` or `IsPresent`, used to query property existence explicitly. Ordinary comparisons against a missing property evaluate to false. |
| Temporal predicate interval | A maximal valid-time interval in which an entity is visible and a typed property predicate is true. Applying a predicate intersects this interval with the input row's effective interval. |
| Query scratch | Cedar-owned, non-authoritative temporary storage used by one query for bounded spill. It is neither an embedded-engine file nor a derived projection and is deleted when the query ends or during orphan cleanup. |
| Online query budget | A low-latency query resource policy that bounds memory and work and disallows scratch spill by default. |
| Analytical query budget | A throughput-oriented query resource policy that permits Cedar-controlled scratch spill within explicit memory, disk, I/O, deadline, and cancellation limits. |
| Query result order | Query rows have no implied order. Only an explicit sort establishes a stable result order. |
| Complete query stream | A streamed result is complete only after its cursor reaches a clean end-of-stream. An error or cancellation makes the stream incomplete even if earlier batches were valid prefixes. |
| Query plan statistics | Derived cardinality, distribution, fanout, interval, and size estimates used only to select a physical plan. Missing or inaccurate statistics cannot change query meaning. |
| Query profile | Measurements from one query execution, including its terminal completeness state and the actual work performed by each physical operator. |
