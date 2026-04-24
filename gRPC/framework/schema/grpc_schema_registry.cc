#include "grpc_schema_registry.h"

namespace dmesh::grpc_codegen {

void SchemaRegistry::Register(const MessageDesc *desc)
{
    if (desc != nullptr)
        descs_.push_back(desc);
}

const MessageDesc *SchemaRegistry::FindByName(const std::string &name) const
{
    for (const MessageDesc *desc : descs_) {
        if (desc != nullptr && name == desc->full_name)
            return desc;
    }
    return nullptr;
}

}  // namespace dmesh::grpc_codegen
