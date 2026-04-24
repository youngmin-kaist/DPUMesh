# gRPC Protobuf DPA Serializer Codegen Framework Design

## 1. Repository findings
### 1.1 Existing gRPC/DPA implementation
Current protobuf/gRPC offload code already lives under `gRPC/`.

Relevant files:
- `gRPC/host_agent.h`, `gRPC/host_agent.cc`
- `gRPC/grpc_dpa_kernel.c`
- `gRPC/dpa_batch_worker.c`
- `gRPC/proto_meta.h`
- `gRPC/dmesh_grpc_api.h`, `gRPC/dmesh_grpc_api.cc`
- `gRPC/arm_grpc_worker.cc`

This codebase already has a useful staged split:
- host-side semantic flattening and task prep in `host_agent.cc`
- DPA-side wire emission in `grpc_dpa_kernel.c`
- ARM/DPU-side framing hooks in `arm_grpc_worker.cc`

That makes `gRPC/` the right root for the new framework.

### 1.2 Existing transport/runtime integration points
Current DPUMesh integration points live under `DPUMesh/`.

Relevant files:
- `DPUMesh/comch_consumer.c`
- `DPUMesh/comch_common.h`
- `DPUMesh/dpa.c`
- `DPUMesh/device/dpa_kernel.c`
- `DPUMesh/host_worker.c`
- `DPUMesh/dpu_worker.c`

These files already provide:
- Host/DPU orchestration
- Comch-based request/response transport
- DPA resident DMA manager path
- validation/completion path

The new codegen framework should not be rooted here. Instead, DPUMesh should consume the framework through thin integration wrappers.

### 1.3 Build/test reality of this repository
Available today:
- DOCA/DPA build environment for existing code
- C/C++ Meson build in `DPUMesh/`
- standalone scripts in `gRPC/`

Unavailable in current environment:
- `protoc`
- protobuf C++ headers
- Python protobuf package

Consequence:
- The framework should define a real descriptor/codegen ingestion interface, but sample generated artifacts must be checked into the repo rather than produced by a live `protoc` invocation.
- The verifier must expose a standard-protobuf adapter boundary, but the default build will need a fallback verifier backend because the standard protobuf runtime is not currently installed.

## 2. Best locations in this repository
### 2.1 Protobuf descriptor ingestion / schema registry
Best location:
- `gRPC/framework/schema/`
- `gRPC/codegen/`

Reason:
- The framework belongs with existing gRPC serialization logic, not generic DPUMesh transport.
- Descriptor ingestion and generated registry types should be shared by generic and specialized backends.

Planned files:
- `gRPC/framework/schema/grpc_schema_desc.h`
- `gRPC/framework/schema/grpc_schema_registry.h`
- `gRPC/framework/schema/grpc_schema_registry.cc`
- `gRPC/codegen/proto_codegen.py` or `proto_codegen_stub.py`
- `gRPC/codegen/generated/demo_request_schema.*`

### 2.2 Host-side semantic lowering
Best location:
- `gRPC/framework/lowering/`

Reason:
- Host lowering is a protobuf/gRPC concern, not a transport concern.
- It should remain independent of DPUMesh `comch_*` code.

Planned files:
- `gRPC/framework/lowering/grpc_exec_plan.h`
- `gRPC/framework/lowering/grpc_lowering.h`
- `gRPC/framework/lowering/grpc_lowering.cc`
- `gRPC/framework/lowering/grpc_selective_policy.h`

### 2.3 Generic ExecPlan IR
Best location:
- `gRPC/framework/ir/`

Reason:
- IR is the contract between Stage 0 lowering and Stage 1 DPA emit.
- It must be usable by both generic and specialized backends.

Planned files:
- `gRPC/framework/ir/grpc_exec_plan.h`
- `gRPC/framework/ir/grpc_emit_ops.h`

### 2.4 Selective offload policy logic
Best location:
- `gRPC/framework/policy/`

Reason:
- Policy should be isolated and swappable.
- This is the right place for threshold heuristics and DMA-safe eligibility checks.

Planned files:
- `gRPC/framework/policy/grpc_selective_policy.h`
- `gRPC/framework/policy/grpc_selective_policy.cc`
- `gRPC/framework/policy/grpc_dma_safety.h`

### 2.5 DPA-side encoder stubs
Best location:
- `gRPC/framework/dpa/`
- generated specialized stubs in `gRPC/codegen/generated/`

Reason:
- Existing DPA wire emitter code is already in `gRPC/grpc_dpa_kernel.c`.
- New generic DPA emit interfaces should live beside it, while per-schema specialized stubs can be generated artifacts.

Planned files:
- `gRPC/framework/dpa/grpc_dpa_emit_iface.h`
- `gRPC/framework/dpa/grpc_dpa_emit_generic.h`
- `gRPC/framework/dpa/grpc_dpa_emit_generic.c`
- `gRPC/codegen/generated/demo_request_dpa_stub.h`
- `gRPC/codegen/generated/demo_request_dpa_stub.c`

### 2.6 ARM/DPU-side framing / transport handoff
Best location:
- `gRPC/framework/runtime/`
- integration wrappers in existing `gRPC/arm_grpc_worker.cc`

Reason:
- Stage 2 should stay separate from wire emission.
- Existing ARM/DPU framing code is already distinct from the DPA serializer.

Planned files:
- `gRPC/framework/runtime/grpc_transport_stage.h`
- `gRPC/framework/runtime/grpc_transport_stage.cc`
- `gRPC/framework/runtime/grpc_completion_stage.h`

### 2.7 Tests, benchmarks, docs
Best location:
- `gRPC/tests/`
- `gRPC/docs/` was an option, but current repo keeps docs at `gRPC/` root; keep consistency and write the main design doc at root.

Planned files:
- `gRPC/tests/grpc_codegen_smoke.cc`
- `gRPC/tests/grpc_codegen_verify.cc`
- `gRPC/tests/grpc_codegen_verify.h`
- `gRPC/build_codegen_demo.sh`
- `gRPC/grpc_proto_codegen_design.md`

## 3. Staged architecture to implement
### Stage 0: host descriptor-driven lowering and selective-offload decision
Input:
- schema registry entry
- runtime host message representation
- selective-offload policy context

Output:
- canonical `ExecPlan`
- ownership/lifetime metadata for copied vs zero-copy fields
- schema/version/hash identifiers

Responsibilities:
- inspect descriptor metadata
- evaluate presence/oneof/map/repeated structure
- decide copy vs zero-copy for bytes/string-like payload-bearing fields at lowering time
- enforce limits (`max_nesting_depth`, `max_encoded_size`, `max_repeated_count`, `max_map_entries`)

### Stage 1: DPA serialization of protobuf wire bytes
Input:
- `ExecPlan`
- message-specific schema metadata

Output:
- protobuf wire bytes
- completion metadata

Responsibilities:
- emit field tags and wire payloads
- keep DPA-side code unaware of protobuf runtime object ABI internals
- support generic plan interpretation and specialized schema stubs

### Stage 2: ARM/DPU-side gRPC prefix + optional HTTP/2 framing + transport handoff
Input:
- protobuf wire bytes or hybrid framing object

Output:
- gRPC-framed message ready for transport

Responsibilities:
- prepend 5-byte gRPC prefix
- optionally wrap for transport handoff
- preserve serialize-and-send style API without forcing an extra generic scatter-gather conversion when not needed

### Stage 3: completion / lifetime release / fallback cleanup
Input:
- completion record
- ownership metadata

Output:
- release or recycle copied/borrowed/zero-copy buffers safely

Responsibilities:
- free copied temp buffers
- decrement refcount / ownership handles for zero-copy references
- perform deterministic fallback cleanup on error

## 4. Concrete implementation plan
### Step 1. Add core schema/IR/policy/runtime scaffolding
Create a framework layer under `gRPC/framework/` with compileable headers and minimal source files for:
- schema descriptions
- plan IR and emit ops
- hybrid refs (`InlineCopyRef`, `ExternalZeroCopyRef`)
- presence and oneof state
- map lowering descriptors
- DMA-safe metadata
- policy interface and default threshold heuristic
- generic DPA emitter interface
- ARM/DPU framing handoff interface

This step establishes the stable internal contracts before any sample-generated code is added.

### Step 2. Add a checked-in sample schema and generated artifacts
Because `protoc` is unavailable, create a checked-in sample generated set for the required demo schema:
- `Inner`
- `AttrEntryValue`
- `Request`

Artifacts to generate/check in:
- schema registry entry
- message/field descriptors
- host lowering helper skeleton
- selective-offload helper hooks
- specialized DPA stub skeleton
- schema hash/version constants

The generator script will be a skeleton that documents expected inputs/outputs, while the generated sample files are committed directly.

### Step 3. Implement generic host-reflection-plan backend skeleton
Provide a generic lowering path that consumes the checked-in schema registry metadata and produces `ExecPlan` without relying on protobuf runtime ABI internals.

This generic path will:
- operate on explicit host view structs rather than protobuf runtime C++ objects
- remain the fallback/correctness path
- be able to feed the generic DPA emitter interface

### Step 4. Implement selective-offload model
Add per-field hybrid representation for payload-bearing fields.

Initial policy:
- if payload length < threshold: copy
- else if payload length >= threshold and buffer is DMA-safe: zero-copy reference
- else: copy fallback

This decision is made during Stage 0 lowering when constructing field plans.

### Step 5. Implement specialized backend for the sample message
Create one optimized sample path for `demo.Request`.

Artifacts:
- specialized lowering helper for `demo.Request`
- specialized DPA emit stub declaration and host-side simulation stub
- schema registry binding to the specialized path

The specialized backend does not replace the generic path. Both remain selectable.

### Step 6. Add verifier harness
Add a verifier harness that compares:
- generic lowering + generic emit bytes
- specialized lowering + specialized emit bytes
- host reference serializer bytes

Because standard protobuf runtime is unavailable in the current repo environment, expose the verifier backend as:
- preferred: standard protobuf serializer adapter when available
- fallback: local host reference serializer implementation clearly marked as non-standard fallback

This preserves a real verification path now while making the missing dependency explicit.

### Step 7. Add build glue and smoke tests
Add a standalone build script for the framework scaffold and verifier tests.

Outputs:
- compileable framework objects
- sample smoke test binary
- sample verify binary

## 5. Comparison note to include in final design
The final design doc and code comments should include a short comparison of:

1. host reflection plan vs schema-specialized codegen
2. always-offload vs selective-offload

Axes:
- performance
- host CPU overhead
- DPA branchiness
- code size
- complexity
- ABI stability
- correctness risk
- cache behavior / bookkeeping overhead
- ease of extension

## 6. Planned deliverables in code
### 6.1 Core abstractions
To implement concretely:
- `MessageDesc`
- `FieldDesc`
- `PresenceRef`
- `OneofState`
- `MapPlan`
- `ExecPlan`
- `EmitOp`
- `InlineCopyRef`
- `ExternalZeroCopyRef`
- DMA-safe buffer metadata and ownership handles
- generic DPA emit interface
- specialized DPA emit interface

### 6.2 Example sample schema coverage
The sample generated path must cover:
- scalar fields
- optional field with presence
- nested message
- oneof
- map as repeated entry submessage
- packed repeated scalar field
- string/bytes selective-offload behavior

### 6.3 Explicit unsupported reporting
Even if some cases are only scaffolded, the framework should explicitly report unsupported cases rather than silently ignoring them.

Examples:
- nesting depth overflow
- map entry count overflow
- encoded size overflow
- unsupported future field kinds

## 7. Immediate file layout to add
Planned additions:
- `gRPC/framework/common/grpc_codegen_common.h`
- `gRPC/framework/schema/grpc_schema_desc.h`
- `gRPC/framework/schema/grpc_schema_registry.h`
- `gRPC/framework/schema/grpc_schema_registry.cc`
- `gRPC/framework/ir/grpc_exec_plan.h`
- `gRPC/framework/lowering/grpc_lowering.h`
- `gRPC/framework/lowering/grpc_lowering.cc`
- `gRPC/framework/policy/grpc_dma_safety.h`
- `gRPC/framework/policy/grpc_selective_policy.h`
- `gRPC/framework/policy/grpc_selective_policy.cc`
- `gRPC/framework/dpa/grpc_dpa_emit_iface.h`
- `gRPC/framework/dpa/grpc_dpa_emit_generic.c`
- `gRPC/framework/runtime/grpc_transport_stage.h`
- `gRPC/framework/runtime/grpc_transport_stage.cc`
- `gRPC/codegen/proto_codegen_stub.py`
- `gRPC/codegen/generated/demo_request_schema.h`
- `gRPC/codegen/generated/demo_request_schema.cc`
- `gRPC/codegen/generated/demo_request_lowering.h`
- `gRPC/codegen/generated/demo_request_lowering.cc`
- `gRPC/codegen/generated/demo_request_dpa_stub.h`
- `gRPC/codegen/generated/demo_request_dpa_stub.c`
- `gRPC/tests/grpc_codegen_verify.h`
- `gRPC/tests/grpc_codegen_verify.cc`
- `gRPC/tests/grpc_codegen_smoke.cc`
- `gRPC/build_codegen_demo.sh`

## 8. Key non-goals for this first scaffold
- Full runtime `protoc` integration in-repo
- Full production transport replacement of existing DPUMesh gRPC demo path
- Full deserialization offload
- Full hardware-specific DOCA/DPA zero-copy execution path

These should be left as clearly marked TODO boundaries.

## 9. Technical comparison note
### 9.1 Host reflection plan vs schema-specialized codegen
| Axis | Host reflection plan | Schema-specialized codegen |
|---|---|---|
| Performance | Lower, more branches and descriptor chasing | Higher, fast path is explicit |
| Host CPU overhead | Higher during lowering | Lower for hot schemas |
| DPA branchiness | Higher if generic interpreter is used | Lower, field order and shape are fixed |
| Code size | Small core, less generated code | Larger due to per-schema emitters |
| Complexity | Simpler control plane, more generic runtime logic | More generator complexity, simpler hot path |
| ABI stability | Better, avoids runtime object ABI coupling | Also stable if generated from descriptors, but regeneration required |
| Correctness risk | Centralized logic, easier to audit once | More generated surface, must verify codegen carefully |
| Cache / bookkeeping behavior | More metadata accesses at runtime | Better locality on hot path |
| Ease of extension | Easier to add broad feature coverage | Easier to optimize known hot messages |

### 9.2 Always-offload vs selective-offload
| Axis | Always-offload | Selective-offload |
|---|---|---|
| Performance | Wastes offload bandwidth on tiny fields | Better when thresholds are well tuned |
| Host CPU overhead | Lower policy complexity, but can add needless prep | Slightly higher policy work, lower wasted work |
| DPA branchiness | Simple but can over-handle cold/small fields | Some extra host-side decision logic, less DPA waste |
| Code size | Smaller | Slightly larger due to hybrid ref handling |
| Complexity | Lower | Higher because ownership and DMA-safety must be tracked |
| ABI stability | Similar | Similar |
| Correctness risk | Lower policy risk, higher runtime waste | Higher policy/lifetime risk if ownership is unclear |
| Cache / bookkeeping overhead | Can defer too much to send time | Better if decisions are made during lowering |
| Ease of extension | Simple baseline | Better foundation for Cornflakes-style field tuning |
