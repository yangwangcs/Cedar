// Copyright (c) 2025 The CedarGraph Authors. All rights reserved.
// CedarGraph gRPC Server Header

#ifndef FERN_GRPC_GRPC_SERVER_H_
#define FERN_GRPC_GRPC_SERVER_H_

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <atomic>
#include <thread>
#include <chrono>
#include <mutex>
#include <condition_variable>

#include <grpcpp/grpcpp.h>

#include "cedar/cedar_graph.pb.h"
#include "cedar/cedar_graph.grpc.pb.h"

namespace cedar {

// gRPC 服务器配置
struct GrpcServerConfig {
    std::string host = "0.0.0.0";
    uint16_t port = 50051;
    uint32_t thread_count = 4;
    uint32_t max_concurrent_calls = 100;
    uint64_t request_timeout_ms = 30000;
    std::string db_path = "./cedar_graph_db";
    std::string cert_file;
    std::string key_file;
};

// 服务器统计信息
struct GrpcServerStats {
    uint64_t total_requests = 0;
    uint64_t active_connections = 0;
    uint64_t total_bytes_sent = 0;
    uint64_t total_bytes_recv = 0;
    std::string uptime;
    
    std::atomic<uint64_t> node_get_requests{0};
    std::atomic<uint64_t> node_put_requests{0};
    std::atomic<uint64_t> node_delete_requests{0};
    std::atomic<uint64_t> edge_get_requests{0};
    std::atomic<uint64_t> edge_put_requests{0};
    std::atomic<uint64_t> edge_delete_requests{0};
    std::atomic<uint64_t> cypher_requests{0};
    std::atomic<uint64_t> neighbors_requests{0};
    
    GrpcServerStats() = default;
    GrpcServerStats(const GrpcServerStats&) = delete;
    GrpcServerStats& operator=(const GrpcServerStats&) = delete;
};

/**
 * CedarGraph gRPC 服务实现类
 */
class CedarGraphServiceImpl final : public cedargrpc::CedarGraphService::Service {
public:
    CedarGraphServiceImpl(const GrpcServerConfig& config);
    ~CedarGraphServiceImpl();

    CedarGraphServiceImpl(const CedarGraphServiceImpl&) = delete;
    CedarGraphServiceImpl& operator=(const CedarGraphServiceImpl&) = delete;

    void FillServerStats(::cedargrpc::ServerStats* response) const;
    const GrpcServerConfig& GetConfig() const { return config_; }

    // ==================== 节点 CRUD ====================
    
    ::grpc::Status GetNode(::grpc::ServerContext* context,
                          const ::cedargrpc::GetNodeRequest* request,
                          ::cedargrpc::Node* response) override;
    
    ::grpc::Status PutNode(::grpc::ServerContext* context,
                          const ::cedargrpc::PutNodeRequest* request,
                          ::cedargrpc::GrpcStatus* response) override;
    
    ::grpc::Status DeleteNode(::grpc::ServerContext* context,
                             const ::cedargrpc::DeleteNodeRequest* request,
                             ::cedargrpc::GrpcStatus* response) override;
    
    ::grpc::Status QueryNodes(::grpc::ServerContext* context,
                             const ::cedargrpc::QueryNodesRequest* request,
                             ::cedargrpc::NodeList* response) override;

    // ==================== 边 CRUD ====================
    
    ::grpc::Status GetEdge(::grpc::ServerContext* context,
                          const ::cedargrpc::GetEdgeRequest* request,
                          ::cedargrpc::Edge* response) override;
    
    ::grpc::Status PutEdge(::grpc::ServerContext* context,
                          const ::cedargrpc::PutEdgeRequest* request,
                          ::cedargrpc::GrpcStatus* response) override;
    
    ::grpc::Status DeleteEdge(::grpc::ServerContext* context,
                             const ::cedargrpc::DeleteEdgeRequest* request,
                             ::cedargrpc::GrpcStatus* response) override;
    
    ::grpc::Status QueryEdges(::grpc::ServerContext* context,
                             const ::cedargrpc::QueryEdgesRequest* request,
                             ::cedargrpc::EdgeList* response) override;

    // ==================== 图遍历 ====================
    
    ::grpc::Status GetNeighbors(::grpc::ServerContext* context,
                               const ::cedargrpc::GetNeighborsRequest* request,
                               ::cedargrpc::GetNeighborsResponse* response) override;
    
    ::grpc::Status GetInNeighbors(::grpc::ServerContext* context,
                                  const ::cedargrpc::GetNeighborsRequest* request,
                                  ::cedargrpc::GetNeighborsResponse* response) override;
    
    ::grpc::Status ShortestPath(::grpc::ServerContext* context,
                               const ::cedargrpc::ShortestPathRequest* request,
                               ::cedargrpc::ShortestPathResponse* response) override;
    
    ::grpc::Status Bfs(::grpc::ServerContext* context,
                      const ::cedargrpc::BfsRequest* request,
                      ::cedargrpc::BfsResponse* response) override;

    // ==================== Cypher 查询 ====================
    
    ::grpc::Status ExecuteCypher(::grpc::ServerContext* context,
                                 const ::cedargrpc::CypherQueryRequest* request,
                                 ::cedargrpc::CypherQueryResponse* response) override;
    
    ::grpc::Status ExplainCypher(::grpc::ServerContext* context,
                                 const ::cedargrpc::CypherQueryRequest* request,
                                 ::cedargrpc::CypherQueryResponse* response) override;

    // ==================== 事务 ====================
    
    ::grpc::Status BeginTransaction(::grpc::ServerContext* context,
                                   const ::cedargrpc::BeginTransactionRequest* request,
                                   ::cedargrpc::Transaction* response) override;
    
    ::grpc::Status Commit(::grpc::ServerContext* context,
                         const ::cedargrpc::CommitRequest* request,
                         ::cedargrpc::GrpcStatus* response) override;
    
    ::grpc::Status Rollback(::grpc::ServerContext* context,
                           const ::cedargrpc::RollbackRequest* request,
                           ::cedargrpc::GrpcStatus* response) override;

    // ==================== 数据库管理 ====================
    
    ::grpc::Status Flush(::grpc::ServerContext* context,
                        const ::cedargrpc::FlushRequest* request,
                        ::cedargrpc::GrpcStatus* response) override;
    
    ::grpc::Status Compact(::grpc::ServerContext* context,
                          const ::cedargrpc::CompactRequest* request,
                          ::cedargrpc::GrpcStatus* response) override;
    
    ::grpc::Status GetStats(::grpc::ServerContext* context,
                           const ::cedargrpc::GetStatsRequest* request,
                           ::cedargrpc::DatabaseStats* response) override;

    // ==================== 服务器管理 ====================
    
    ::grpc::Status HealthCheck(::grpc::ServerContext* context,
                               const ::cedargrpc::Empty* request,
                               ::cedargrpc::HealthCheckResponse* response) override;
    
    ::grpc::Status GetServerStats(::grpc::ServerContext* context,
                                  const ::cedargrpc::Empty* request,
                                  ::cedargrpc::ServerStats* response) override;

private:
    uint64_t GetCurrentTimestamp();

    GrpcServerConfig config_;
    GrpcServerStats stats_;
    std::chrono::steady_clock::time_point start_time_;
    
    // CedarGraph 存储
    class CedarGraphStorage* storage_ = nullptr;
    
public:
    // 友元类需要访问
    friend class GrpcServer;
};

// gRPC 服务器主类
class GrpcServer {
public:
    GrpcServer(const GrpcServerConfig& config);
    ~GrpcServer();

    GrpcServer(const GrpcServer&) = delete;
    GrpcServer& operator=(const GrpcServer&) = delete;

    bool Start();
    bool Stop();
    void Wait();

    // 注册存储（外部创建后传入）
    void RegisterStorage(class CedarGraphStorage* storage);
    
    // 获取存储
    class CedarGraphStorage* GetStorage() { return service_ ? service_->storage_ : nullptr; }

    bool IsRunning() const { return running_.load(); }
    CedarGraphServiceImpl* GetService() { return service_.get(); }

private:
    GrpcServerConfig config_;
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<CedarGraphServiceImpl> service_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stopped_{false};
    std::vector<std::thread> worker_threads_;
    std::chrono::steady_clock::time_point start_time_;
    std::mutex mutex_;
    std::condition_variable cv_;
};

// 服务器工厂
class GrpcServerFactory {
public:
    static std::unique_ptr<GrpcServer> Create(const GrpcServerConfig& config);
    static std::unique_ptr<GrpcServer> CreateDefault();
};

}  // namespace cedar

#endif  // FERN_GRPC_GRPC_SERVER_H_
