// Copyright (c) 2025 The CedarGraph Authors. All rights reserved.
// CedarGraph HTTP REST API Server

#ifndef FERN_SERVER_HTTP_SERVER_H_
#define FERN_SERVER_HTTP_SERVER_H_

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <functional>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable.h>
#include <event2/event.h>
#include <event2/http.h>
#include <event2/buffer.h>
#include <event2/keyvalq_struct.h>

#include "cedar/db/graph_db.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/cypher/cypher_engine.h"
#include "cedar/core/status.h"

namespace cedar {

// HTTP 请求处理器类型
using HttpHandler = std::function<void(
    struct evhttp_request* req,
    const std::string& body,
    std::string* response
)>;

// API 路由
struct Route {
    std::string method;
    std::string path;
    HttpHandler handler;
};

// 服务器配置
struct HttpServerConfig {
    std::string host = "0.0.0.0";
    uint16_t port = 8080;
    uint32_t thread_count = 4;
    uint32_t request_timeout_sec = 30;
    uint32_t max_body_size = 64 * 1024 * 1024;  // 64MB
    bool enable_cors = true;
    std::string cors_allow_origin = "*";
    std::string log_file;
    int log_level = 1;
};

// JSON 响应构建器
class JsonResponse {
public:
    static std::string Success(const std::string& message);
    static std::string Success(const std::string& message, const std::string& data);
    static std::string Error(int code, const std::string& message);
    static std::string Error(int code, const std::string& message, const std::string& detail);
    static std::string Data(const std::string& key, const std::string& value);
    static std::string Array(const std::string& array_name, const std::string& items);
};

// HTTP 服务器主类
class HttpServer {
public:
    HttpServer(const HttpServerConfig& config);
    ~HttpServer();

    // 禁止拷贝
    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;

    // 启动服务器
    Status Start();

    // 停止服务器
    Status Stop();

    // 等待服务器关闭
    Status Wait();

    // 注册存储实例
    void RegisterStorage(CedarGraphStorage* storage) { storage_ = storage; }

    // 注册数据库实例
    void RegisterDB(CedarGraphDB* db) { db_ = db; }

    // 注册 Cypher 引擎
    void RegisterCypherEngine(CypherEngine* engine) { cypher_engine_ = engine; }

    // 获取运行状态
    bool IsRunning() const { return running_.load(); }

    // 获取配置
    const HttpServerConfig& GetConfig() const { return config_; }

    // 获取统计信息
    struct ServerStats {
        uint64_t total_requests = 0;
        uint64_t active_connections = 0;
        uint64_t total_bytes_sent = 0;
        uint64_t total_bytes_recv = 0;
        std::string uptime;
    };
    ServerStats GetStats() const;

private:
    // 辅助方法声明
    std::vector<std::string> SplitString(const std::string& s, char delimiter);
    std::string ExtractPathParam(const std::string& uri, const std::string& prefix);
    std::map<std::string, std::string> ParseQueryParams(const std::string& uri);
    std::string UrlDecode(const std::string& str);
    uint64_t GetParamValue(const std::map<std::string, std::string>& params, 
                          const std::string& key, uint64_t default_value);
    std::string GetParamValue(const std::map<std::string, std::string>& params,
                             const std::string& key, const std::string& default_value);
    std::string ExtractJsonField(const std::string& json, const std::string& field);
    std::string DescriptorToJson(const Descriptor& desc);
    Descriptor ParseJsonToDescriptor(const std::string& json);
    std::string NodesToJson(const std::vector<uint64_t>& nodes);
    std::string EdgeToJson(const Neighbor& edge);
    std::string EdgesToJson(const std::vector<Neighbor>& edges);
    EdgeProperties ParseJsonToEdgeProperties(const std::string& json);
    std::map<std::string, std::string> ParseJsonToMap(const std::string& json);
    uint64_t GetCurrentTimestamp();
    bool MatchRoute(const std::string& pattern, const std::string& uri);
    void HandleRequest(struct evhttp_request* req);

    // 初始化路由
    void InitRoutes();

    // 初始化 libevent
    Status InitEvent();

    // 注册路由处理器
    void RegisterRoute(const std::string& method, const std::string& path, HttpHandler handler);

    // 请求处理回调
    static void RequestHandler(struct evhttp_request* req, void* ctx);

    // 解析请求体
    std::string ParseRequestBody(struct evhttp_request* req);

    // 发送响应
    void SendResponse(struct evhttp_request* req, int code, 
                     const std::string& content, 
                     const std::string& content_type = "application/json");

    // CORS 处理
    void HandleCors(struct evhttp_request* req);

    // ===== API 路由处理 =====

    // 根路径
    void HandleRoot(struct evhttp_request* req, const std::string& body, std::string* response);

    // 健康检查
    void HandleHealth(struct evhttp_request* req, const std::string& body, std::string* response);

    // 数据库操作
    void HandleDBStats(struct evhttp_request* req, const std::string& body, std::string* response);
    void HandleDBFlush(struct evhttp_request* req, const std::string& body, std::string* response);
    void HandleDBCompact(struct evhttp_request* req, const std::string& body, std::string* response);

    // 节点操作
    void HandleNodeGet(struct evhttp_request* req, const std::string& body, std::string* response);
    void HandleNodePut(struct evhttp_request* req, const std::string& body, std::string* response);
    void HandleNodeDelete(struct evhttp_request* req, const std::string& body, std::string* response);
    void HandleNodeQuery(struct evhttp_request* req, const std::string& body, std::string* response);

    // 边操作
    void HandleEdgeGet(struct evhttp_request* req, const std::string& body, std::string* response);
    void HandleEdgePut(struct evhttp_request* req, const std::string& body, std::string* response);
    void HandleEdgeDelete(struct evhttp_request* req, const std::string& body, std::string* response);
    void HandleEdgeQuery(struct evhttp_request* req, const std::string& body, std::string* response);

    // Cypher 查询
    void HandleCypher(struct evhttp_request* req, const std::string& body, std::string* response);

    // 图遍历
    void HandleGraphNeighbors(struct evhttp_request* req, const std::string& body, std::string* response);
    void HandleGraphShortestPath(struct evhttp_request* req, const std::string& body, std::string* response);

    // 统计信息
    void HandleStats(struct evhttp_request* req, const std::string& body, std::string* response);

    // 配置
    HttpServerConfig config_;

    // libevent 基础
    struct event_base* base_ = nullptr;
    struct evhttp* http_ = nullptr;

    // 数据库和图接口
    CedarGraphDB* db_ = nullptr;
    CedarGraphStorage* storage_ = nullptr;
    cypher::CypherEngine* cypher_engine_ = nullptr;

    // 路由表
    std::vector<Route> routes_;

    // 运行状态
    std::atomic<bool> running_{false};
    std::atomic<bool> stopped_{false};

    // 线程
    std::vector<std::thread> worker_threads_;

    // 统计
    std::atomic<uint64_t> total_requests_{0};
    std::atomic<uint64_t> active_connections_{0};
    std::atomic<uint64_t> total_bytes_sent_{0};
    std::atomic<uint64_t> total_bytes_recv_{0};

    // 启动时间
    std::chrono::steady_clock::time_point start_time_;

    // 互斥锁
    std::mutex mutex_;
    std::condition_variable cv_;
};

// 服务器工厂
class HttpServerFactory {
public:
    static std::unique_ptr<HttpServer> Create(const HttpServerConfig& config);
    static std::unique_ptr<HttpServer> CreateDefault();
};

}  // namespace cedar

#endif  // FERN_SERVER_HTTP_SERVER_H_
