// Copyright (c) 2025 The CedarGraph Authors. All rights reserved.
// CedarGraph gRPC Server Implementation

#include "grpc/grpc_server.h"

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/types/descriptor.h"

#include <chrono>
#include <sstream>
#include <iostream>

namespace cedar {

// ==================== CedarGraphServiceImpl ====================

CedarGraphServiceImpl::CedarGraphServiceImpl(const GrpcServerConfig& config)
    : config_(config), start_time_(std::chrono::steady_clock::now()) {
}

CedarGraphServiceImpl::~CedarGraphServiceImpl() {}

uint64_t CedarGraphServiceImpl::GetCurrentTimestamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

void CedarGraphServiceImpl::FillServerStats(::cedargrpc::ServerStats* response) const {
    response->set_total_requests(stats_.node_get_requests.load() + 
                                 stats_.node_put_requests.load() + 
                                 stats_.node_delete_requests.load() +
                                 stats_.edge_get_requests.load() +
                                 stats_.edge_put_requests.load() +
                                 stats_.edge_delete_requests.load() +
                                 stats_.cypher_requests.load() +
                                 stats_.neighbors_requests.load());
    response->set_active_connections(0);
    response->set_total_bytes_sent(0);
    response->set_total_bytes_recv(0);
    
    auto now = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - start_time_).count();
    auto hours = duration / 3600;
    auto minutes = (duration % 3600) / 60;
    auto seconds = duration % 60;
    
    std::ostringstream oss;
    oss << hours << "h " << minutes << "m " << seconds << "s";
    response->set_uptime(oss.str());
}

// ==================== 节点 CRUD ====================

::grpc::Status CedarGraphServiceImpl::GetNode(
    ::grpc::ServerContext* context,
    const ::cedargrpc::GetNodeRequest* request,
    ::cedargrpc::Node* response) {
    
    stats_.node_get_requests.fetch_add(1);
    
    if (!storage_) {
        return ::grpc::Status(::grpc::StatusCode::UNAVAILABLE, "Storage not initialized");
    }
    
    uint64_t node_id = request->id();
    uint64_t timestamp = request->as_of().value();
    
    // 获取节点数据 (使用默认类型和列)
    auto result = storage_->Get(node_id, timestamp);
    
    if (!result) {
        return ::grpc::Status(::grpc::StatusCode::NOT_FOUND, "Node not found");
    }
    
    response->set_id(node_id);
    response->set_timestamp(timestamp);
    response->set_version(0);
    
    return ::grpc::Status::OK;
}

::grpc::Status CedarGraphServiceImpl::PutNode(
    ::grpc::ServerContext* context,
    const ::cedargrpc::PutNodeRequest* request,
    ::cedargrpc::GrpcStatus* response) {
    
    stats_.node_put_requests.fetch_add(1);
    
    if (!storage_) {
        response->set_ok(false);
        response->set_code(2);
        response->set_message("Storage not initialized");
        return ::grpc::Status::OK;
    }
    
    uint64_t node_id = request->id();
    const auto& props = request->properties();
    
    // 将属性转换为 Descriptor
    // 简化处理：取第一个属性的值
    Descriptor descriptor;
    if (props.properties_size() > 0) {
        const auto& prop = props.properties(0);
        const auto& value = prop.value();
        
        // 尝试解析为数字，否则作为字符串
        try {
            int64_t int_val = std::stoll(value);
            descriptor = Descriptor::InlineInt(1, static_cast<int32_t>(int_val));
        } catch (...) {
            // 如果不是数字，存储短字符串
            auto opt = Descriptor::InlineShortStr(1, value);
            if (opt) {
                descriptor = *opt;
            }
        }
    } else {
        // 默认值
        descriptor = Descriptor::InlineInt(1, 0);
    }
    
    // 写入存储
    uint64_t tx_time = GetCurrentTimestamp();
    Timestamp txn_version(0);
    
    auto status = storage_->Put(node_id, tx_time, descriptor, txn_version);
    
    if (!status.ok()) {
        response->set_ok(false);
        response->set_code(1);
        response->set_message("Failed to put node: " + status.ToString());
        return ::grpc::Status::OK;
    }
    
    response->set_ok(true);
    response->set_code(0);
    response->set_message("Node created");
    return ::grpc::Status::OK;
}

::grpc::Status CedarGraphServiceImpl::DeleteNode(
    ::grpc::ServerContext* context,
    const ::cedargrpc::DeleteNodeRequest* request,
    ::cedargrpc::GrpcStatus* response) {
    
    stats_.node_delete_requests.fetch_add(1);
    
    if (!storage_) {
        response->set_ok(false);
        response->set_code(2);
        response->set_message("Storage not initialized");
        return ::grpc::Status::OK;
    }
    
    uint64_t node_id = request->id();
    uint64_t tx_time = GetCurrentTimestamp();
    Timestamp txn_version(0);
    
    auto status = storage_->Delete(node_id, tx_time, txn_version);
    
    if (!status.ok()) {
        response->set_ok(false);
        response->set_code(1);
        response->set_message("Failed to delete node: " + status.ToString());
        return ::grpc::Status::OK;
    }
    
    response->set_ok(true);
    response->set_code(0);
    response->set_message("Node deleted");
    return ::grpc::Status::OK;
}

::grpc::Status CedarGraphServiceImpl::QueryNodes(
    ::grpc::ServerContext* context,
    const ::cedargrpc::QueryNodesRequest* request,
    ::cedargrpc::NodeList* response) {
    return ::grpc::Status::OK;
}

// ==================== 边 CRUD ====================

::grpc::Status CedarGraphServiceImpl::GetEdge(
    ::grpc::ServerContext* context,
    const ::cedargrpc::GetEdgeRequest* request,
    ::cedargrpc::Edge* response) {
    
    stats_.edge_get_requests.fetch_add(1);
    
    if (!storage_) {
        return ::grpc::Status(::grpc::StatusCode::UNAVAILABLE, "Storage not initialized");
    }
    
    uint64_t source = request->edge_id().source();
    uint64_t target = request->edge_id().target();
    uint64_t timestamp = request->as_of().value();
    
    // 使用 (source, target) 作为 entity_id 的高/低位
    uint64_t edge_id = (source << 32) | target;
    
    auto result = storage_->Get(edge_id, timestamp);
    
    if (!result) {
        return ::grpc::Status(::grpc::StatusCode::NOT_FOUND, "Edge not found");
    }
    
    response->mutable_edge_id()->set_source(source);
    response->mutable_edge_id()->set_target(target);
    response->set_timestamp(timestamp);
    response->set_version(0);
    
    return ::grpc::Status::OK;
}

::grpc::Status CedarGraphServiceImpl::PutEdge(
    ::grpc::ServerContext* context,
    const ::cedargrpc::PutEdgeRequest* request,
    ::cedargrpc::GrpcStatus* response) {
    
    stats_.edge_put_requests.fetch_add(1);
    
    if (!storage_) {
        response->set_ok(false);
        response->set_code(2);
        response->set_message("Storage not initialized");
        return ::grpc::Status::OK;
    }
    
    uint64_t source = request->edge_id().source();
    uint64_t target = request->edge_id().target();
    const auto& props = request->properties();
    
    // 构建边 ID
    uint64_t edge_id = (source << 32) | target;
    
    // 将属性转换为 Descriptor
    Descriptor descriptor;
    if (props.properties_size() > 0) {
        const auto& prop = props.properties(0);
        const auto& value = prop.value();
        
        try {
            int64_t int_val = std::stoll(value);
            descriptor = Descriptor::InlineInt(1, static_cast<int32_t>(int_val));
        } catch (...) {
            // 如果不是数字，存储短字符串
            auto opt = Descriptor::InlineShortStr(1, value);
            if (opt) {
                descriptor = *opt;
            }
        }
    } else {
        // 默认值
        descriptor = Descriptor::InlineInt(1, 0);
    }
    
    // 写入存储
    uint64_t tx_time = GetCurrentTimestamp();
    Timestamp txn_version(0);
    
    auto status = storage_->Put(edge_id, tx_time, descriptor, txn_version);
    
    if (!status.ok()) {
        response->set_ok(false);
        response->set_code(1);
        response->set_message("Failed to put edge: " + status.ToString());
        return ::grpc::Status::OK;
    }
    
    response->set_ok(true);
    response->set_code(0);
    response->set_message("Edge created");
    return ::grpc::Status::OK;
}

::grpc::Status CedarGraphServiceImpl::DeleteEdge(
    ::grpc::ServerContext* context,
    const ::cedargrpc::DeleteEdgeRequest* request,
    ::cedargrpc::GrpcStatus* response) {
    
    stats_.edge_delete_requests.fetch_add(1);
    
    if (!storage_) {
        response->set_ok(false);
        response->set_code(2);
        response->set_message("Storage not initialized");
        return ::grpc::Status::OK;
    }
    
    uint64_t source = request->edge_id().source();
    uint64_t target = request->edge_id().target();
    uint64_t edge_id = (source << 32) | target;
    uint64_t tx_time = GetCurrentTimestamp();
    Timestamp txn_version(0);
    
    auto status = storage_->Delete(edge_id, tx_time, txn_version);
    
    if (!status.ok()) {
        response->set_ok(false);
        response->set_code(1);
        response->set_message("Failed to delete edge: " + status.ToString());
        return ::grpc::Status::OK;
    }
    
    response->set_ok(true);
    response->set_code(0);
    response->set_message("Edge deleted");
    return ::grpc::Status::OK;
}

::grpc::Status CedarGraphServiceImpl::QueryEdges(
    ::grpc::ServerContext* context,
    const ::cedargrpc::QueryEdgesRequest* request,
    ::cedargrpc::EdgeList* response) {
    return ::grpc::Status::OK;
}

// ==================== 图遍历 ====================

::grpc::Status CedarGraphServiceImpl::GetNeighbors(
    ::grpc::ServerContext* context,
    const ::cedargrpc::GetNeighborsRequest* request,
    ::cedargrpc::GetNeighborsResponse* response) {
    
    stats_.neighbors_requests.fetch_add(1);
    return ::grpc::Status::OK;
}

::grpc::Status CedarGraphServiceImpl::GetInNeighbors(
    ::grpc::ServerContext* context,
    const ::cedargrpc::GetNeighborsRequest* request,
    ::cedargrpc::GetNeighborsResponse* response) {
    
    stats_.neighbors_requests.fetch_add(1);
    return ::grpc::Status::OK;
}

::grpc::Status CedarGraphServiceImpl::ShortestPath(
    ::grpc::ServerContext* context,
    const ::cedargrpc::ShortestPathRequest* request,
    ::cedargrpc::ShortestPathResponse* response) {
    return ::grpc::Status::OK;
}

::grpc::Status CedarGraphServiceImpl::Bfs(
    ::grpc::ServerContext* context,
    const ::cedargrpc::BfsRequest* request,
    ::cedargrpc::BfsResponse* response) {
    return ::grpc::Status::OK;
}

// ==================== Cypher 查询 ====================

::grpc::Status CedarGraphServiceImpl::ExecuteCypher(
    ::grpc::ServerContext* context,
    const ::cedargrpc::CypherQueryRequest* request,
    ::cedargrpc::CypherQueryResponse* response) {
    
    stats_.cypher_requests.fetch_add(1);
    response->set_success(true);
    response->set_result_json("{\"message\":\"Cypher execution not implemented\"}");
    response->set_execution_time_ms(0);
    response->set_row_count(0);
    return ::grpc::Status::OK;
}

::grpc::Status CedarGraphServiceImpl::ExplainCypher(
    ::grpc::ServerContext* context,
    const ::cedargrpc::CypherQueryRequest* request,
    ::cedargrpc::CypherQueryResponse* response) {
    response->set_success(true);
    response->set_result_json("{\"explain\":\"Not implemented\"}");
    return ::grpc::Status::OK;
}

// ==================== 事务 ====================

::grpc::Status CedarGraphServiceImpl::BeginTransaction(
    ::grpc::ServerContext* context,
    const ::cedargrpc::BeginTransactionRequest* request,
    ::cedargrpc::Transaction* response) {
    
    std::string txn_id = "txn-" + std::to_string(GetCurrentTimestamp());
    response->set_txn_id(txn_id);
    response->set_start_time(GetCurrentTimestamp());
    response->set_committed(false);
    return ::grpc::Status::OK;
}

::grpc::Status CedarGraphServiceImpl::Commit(
    ::grpc::ServerContext* context,
    const ::cedargrpc::CommitRequest* request,
    ::cedargrpc::GrpcStatus* response) {
    
    response->set_ok(true);
    response->set_code(0);
    response->set_message("Transaction committed");
    return ::grpc::Status::OK;
}

::grpc::Status CedarGraphServiceImpl::Rollback(
    ::grpc::ServerContext* context,
    const ::cedargrpc::RollbackRequest* request,
    ::cedargrpc::GrpcStatus* response) {
    
    response->set_ok(true);
    response->set_code(0);
    response->set_message("Transaction rolled back");
    return ::grpc::Status::OK;
}

// ==================== 数据库管理 ====================

::grpc::Status CedarGraphServiceImpl::Flush(
    ::grpc::ServerContext* context,
    const ::cedargrpc::FlushRequest* request,
    ::cedargrpc::GrpcStatus* response) {
    
    response->set_ok(true);
    response->set_code(0);
    response->set_message("Database flushed (stub)");
    return ::grpc::Status::OK;
}

::grpc::Status CedarGraphServiceImpl::Compact(
    ::grpc::ServerContext* context,
    const ::cedargrpc::CompactRequest* request,
    ::cedargrpc::GrpcStatus* response) {
    
    response->set_ok(true);
    response->set_code(0);
    response->set_message("Database compacted (stub)");
    return ::grpc::Status::OK;
}

::grpc::Status CedarGraphServiceImpl::GetStats(
    ::grpc::ServerContext* context,
    const ::cedargrpc::GetStatsRequest* request,
    ::cedargrpc::DatabaseStats* response) {
    
    response->set_total_nodes(0);
    response->set_total_edges(0);
    response->set_storage_size_bytes(0);
    response->set_sst_file_count(0);
    response->set_blob_file_count(0);
    response->set_memtable_size_bytes(0);
    response->set_cache_hit_rate(0.0);
    return ::grpc::Status::OK;
}

// ==================== 服务器管理 ====================

::grpc::Status CedarGraphServiceImpl::HealthCheck(
    ::grpc::ServerContext* context,
    const ::cedargrpc::Empty* request,
    ::cedargrpc::HealthCheckResponse* response) {
    
    response->set_healthy(true);
    response->set_timestamp(GetCurrentTimestamp());
    response->set_version("0.1.0");
    response->set_database_connected(false);
    return ::grpc::Status::OK;
}

::grpc::Status CedarGraphServiceImpl::GetServerStats(
    ::grpc::ServerContext* context,
    const ::cedargrpc::Empty* request,
    ::cedargrpc::ServerStats* response) {
    
    FillServerStats(response);
    return ::grpc::Status::OK;
}

// ==================== GrpcServer ====================

GrpcServer::GrpcServer(const GrpcServerConfig& config)
    : config_(config), start_time_(std::chrono::steady_clock::now()) {
}

GrpcServer::~GrpcServer() {
    Stop();
    Wait();
}

bool GrpcServer::Start() {
    if (running_.load()) {
        return false;
    }
    
    service_ = std::make_unique<CedarGraphServiceImpl>(config_);
    
    grpc::ServerBuilder builder;
    builder.AddListeningPort(config_.host + ":" + std::to_string(config_.port),
                            grpc::InsecureServerCredentials());
    builder.RegisterService(service_.get());
    
    server_ = builder.BuildAndStart();
    
    if (!server_) {
        return false;
    }
    
    running_ = true;
    std::cout << "gRPC Server started on " << config_.host << ":" << config_.port << std::endl;
    return true;
}

bool GrpcServer::Stop() {
    if (!running_.load()) {
        return true;
    }
    
    running_ = false;
    
    if (server_) {
        server_->Shutdown();
        server_.reset();
    }
    return true;
}

void GrpcServer::Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return stopped_.load(); });
}

// ==================== GrpcServerFactory ====================

std::unique_ptr<GrpcServer> GrpcServerFactory::Create(const GrpcServerConfig& config) {
    return std::make_unique<GrpcServer>(config);
}

std::unique_ptr<GrpcServer> GrpcServerFactory::CreateDefault() {
    GrpcServerConfig config;
    config.host = "0.0.0.0";
    config.port = 50051;
    config.thread_count = 4;
    return std::make_unique<GrpcServer>(config);
}

void GrpcServer::RegisterStorage(CedarGraphStorage* storage) {
    if (service_) {
        service_->storage_ = storage;
    }
}

}  // namespace cedar
