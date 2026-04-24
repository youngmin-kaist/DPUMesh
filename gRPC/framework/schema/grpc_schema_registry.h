#ifndef DMESH_GRPC_SCHEMA_REGISTRY_H
#define DMESH_GRPC_SCHEMA_REGISTRY_H

#include "grpc_schema_desc.h"
#include <vector>

namespace dmesh::grpc_codegen {

class SchemaRegistry {
public:
    void Register(const MessageDesc *desc);
    const MessageDesc *FindByName(const std::string &name) const;
    const std::vector<const MessageDesc *> &All() const { return descs_; }

private:
    std::vector<const MessageDesc *> descs_;
};

}  // namespace dmesh::grpc_codegen

#endif
