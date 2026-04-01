#include <iostream>
#include "cedar/storage/cedar_graph_storage.h"

int main() {
    // Open database
    cedar::CedarOptions options;
    options.create_if_missing = true;
    
    cedar::CedarGraphStorage* storage = nullptr;
    cedar::Status s = cedar::CedarGraphStorage::Open(options, "./test_db", &storage);
    
    if (!s.ok()) {
        std::cerr << "Failed to open database: " << s.ToString() << std::endl;
        return 1;
    }
    
    // Store temporal data using Descriptor
    cedar::CedarKey key(100, cedar::EntityType::Vertex, 1, 1700000000000000ULL);
    cedar::Descriptor desc = cedar::Descriptor::InlineInt(1, 42);
    s = storage->Put(key.entity_id(), key.timestamp().value(), desc, cedar::Timestamp(1));
    
    if (!s.ok()) {
        std::cerr << "Failed to put data: " << s.ToString() << std::endl;
        delete storage;
        return 1;
    }
    
    // Read data
    auto result = storage->Get(key.entity_id(), key.timestamp().value());
    
    if (result.has_value()) {
        auto int_val = result->AsInlineInt();
        if (int_val.has_value()) {
            std::cout << "Read value: " << *int_val << std::endl;
        } else {
            std::cout << "Read descriptor (non-integer kind)" << std::endl;
        }
    } else {
        std::cerr << "Failed to get data: not found" << std::endl;
    }
    
    // Cleanup
    delete storage;
    std::cout << "CedarGraph simple example completed successfully!" << std::endl;
    
    return 0;
}
