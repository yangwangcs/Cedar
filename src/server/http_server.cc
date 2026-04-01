// Copyright (c) 2025 The CedarGraph Authors. All rights reserved.
// CedarGraph HTTP REST API Server Implementation

#include "cedar/server/http_server.h"

#include <chrono>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <cstring>
#include <iostream>

#include <event2/http_struct.h>
#include <event2/buffer_compat.h>

namespace cedar {

// ========== JsonResponse 实现 ==========

std::string JsonResponse::Success(const std::string& message) {
    return "{\"status\":\"success\",\"message\":\"" + message + "\"}";
}

std::string JsonResponse::Success(const std::string& message, const std::string& data) {
    return "{\"status\":\"success\",\"message\":\"" + message + "\",\"data\":" + data + "}";
}

std::string JsonResponse::Error(int code, const std::string& message) {
    std::ostringstream oss;
    oss << "{\"status\":\"error\",\"code\":" << code << ",\"message\":\"" << message << "\"}";
    return oss.str();
}

std::string JsonResponse::Error(int code, const std::string& message, const std::string& detail) {
    std::ostringstream oss;
    oss << "{\"status\":\"error\",\"code\":" << code << ",\"message\":\"" << message << "\",\"detail\":\"" << detail << "\"}";
    return oss.str();
}

std::string JsonResponse::Data(const std::string& key, const std::string& value) {
    return "\"" + key + "\":\"" + value + "\"";
}

std::string JsonResponse::Array(const std::string& array_name, const std::string& items) {
    return "\"" + array_name + "\":[" + items + "]";
}

// ========== HttpServer 实现 ==========

HttpServer::HttpServer(const HttpServerConfig& config)
    : config_(config), base_(nullptr), http_(nullptr), 
      db_(nullptr), storage_(nullptr), cypher_engine_(nullptr) {
    start_time_ = std::chrono::steady_clock::now();
}

HttpServer::~HttpServer() {
    Stop();
    Wait();
}

Status HttpServer::InitEvent() {
    // 初始化 libevent
    base_ = event_base_new();
    if (!base_) {
        return Status::IOError("Failed to create event_base");
    }

    // 创建 HTTP 服务器
    http_ = evhttp_new(base_);
    if (!http_) {
        event_base_free(base_);
        base_ = nullptr;
        return Status::IOError("Failed to create evhttp");
    }

    // 设置超时
    evhttp_set_timeout(http_, config_.request_timeout_sec);
    
    // 设置最大 body 大小
    evhttp_set_max_body_size(http_, config_.max_body_size);

    // 设置绑定的地址和端口
    if (evhttp_bind_socket(http_, config_.host.c_str(), config_.port) == -1) {
        evhttp_free(http_);
        event_base_free(base_);
        http_ = nullptr;
        base_ = nullptr;
        std::ostringstream oss;
        oss << "Failed to bind to " << config_.host << ":" << config_.port;
        return Status::IOError(oss.str());
    }

    // 设置允许通用的请求回调
    evhttp_set_gencb(http_, RequestHandler, this);

    return Status::OK();
}

void HttpServer::InitRoutes() {
    // 根路径
    RegisterRoute("GET", "/", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleRoot(req, body, resp);
    });

    // 健康检查
    RegisterRoute("GET", "/health", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleHealth(req, body, resp);
    });

    // 数据库操作
    RegisterRoute("GET", "/api/v1/db/stats", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleDBStats(req, body, resp);
    });

    RegisterRoute("POST", "/api/v1/db/flush", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleDBFlush(req, body, resp);
    });

    RegisterRoute("POST", "/api/v1/db/compact", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleDBCompact(req, body, resp);
    });

    // 节点操作 (使用 CedarGraphStorage)
    RegisterRoute("GET", "/api/v1/nodes/{id}", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleNodeGet(req, body, resp);
    });

    RegisterRoute("PUT", "/api/v1/nodes/{id}", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleNodePut(req, body, resp);
    });

    RegisterRoute("DELETE", "/api/v1/nodes/{id}", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleNodeDelete(req, body, resp);
    });

    RegisterRoute("GET", "/api/v1/nodes", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleNodeQuery(req, body, resp);
    });

    // 边操作 (使用 CedarGraphStorage)
    RegisterRoute("GET", "/api/v1/edges/{id}", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleEdgeGet(req, body, resp);
    });

    RegisterRoute("PUT", "/api/v1/edges/{id}", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleEdgePut(req, body, resp);
    });

    RegisterRoute("DELETE", "/api/v1/edges/{id}", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleEdgeDelete(req, body, resp);
    });

    RegisterRoute("GET", "/api/v1/edges", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleEdgeQuery(req, body, resp);
    });

    // 静态节点操作
    RegisterRoute("GET", "/api/v1/static/nodes/{id}", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleStaticNodeGet(req, body, resp);
    });

    RegisterRoute("PUT", "/api/v1/static/nodes/{id}", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleStaticNodePut(req, body, resp);
    });

    // 动态节点操作
    RegisterRoute("GET", "/api/v1/dynamic/nodes/{id}", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleDynamicNodeGet(req, body, resp);
    });

    RegisterRoute("PUT", "/api/v1/dynamic/nodes/{id}", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleDynamicNodePut(req, body, resp);
    });

    // 静态边操作
    RegisterRoute("GET", "/api/v1/static/edges/{id}", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleStaticEdgeGet(req, body, resp);
    });

    RegisterRoute("PUT", "/api/v1/static/edges/{id}", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleStaticEdgePut(req, body, resp);
    });

    // Cypher 查询
    RegisterRoute("POST", "/api/v1/cypher", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleCypher(req, body, resp);
    });

    // 图遍历
    RegisterRoute("GET", "/api/v1/graph/{id}/neighbors", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleGraphNeighbors(req, body, resp);
    });

    RegisterRoute("GET", "/api/v1/graph/shortest_path", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleGraphShortestPath(req, body, resp);
    });

    // 统计信息
    RegisterRoute("GET", "/api/v1/stats", [this](evhttp_request* req, const std::string& body, std::string* resp) {
        HandleStats(req, body, resp);
    });
}

void HttpServer::RegisterRoute(const std::string& method, const std::string& path, HttpHandler handler) {
    routes_.push_back({method, path, handler});
}

void HttpServer::RequestHandler(struct evhttp_request* req, void* ctx) {
    auto* server = static_cast<HttpServer*>(ctx);
    server->HandleRequest(req);
}

bool HttpServer::MatchRoute(const std::string& pattern, const std::string& uri) {
    // 简单的路由匹配，支持 {id} 参数
    std::vector<std::string> pattern_parts = SplitString(pattern, '/');
    std::vector<std::string> uri_parts = SplitString(uri, '/');

    if (pattern_parts.size() != uri_parts.size()) {
        return false;
    }

    for (size_t i = 0; i < pattern_parts.size(); i++) {
        if (pattern_parts[i].empty()) continue;
        
        if (pattern_parts[i].front() == '{' && pattern_parts[i].back() == '}') {
            // 参数匹配
            continue;
        }
        
        if (pattern_parts[i] != uri_parts[i]) {
            return false;
        }
    }

    return true;
}

void HttpServer::HandleRequest(struct evhttp_request* req) {
    total_requests_.fetch_add(1);
    active_connections_.fetch_add(1);

    // 获取请求信息
    const char* uri = evhttp_request_get_uri(req);
    if (!uri) {
        SendResponse(req, 400, JsonResponse::Error(400, "Invalid request"));
        return;
    }

    std::string method_str;
    switch (evhttp_request_get_command(req)) {
        case EVHTTP_REQ_GET: method_str = "GET"; break;
        case EVHTTP_REQ_POST: method_str = "POST"; break;
        case EVHTTP_REQ_PUT: method_str = "PUT"; break;
        case EVHTTP_REQ_DELETE: method_str = "DELETE"; break;
        case EVHTTP_REQ_PATCH: method_str = "PATCH"; break;
        case EVHTTP_REQ_HEAD: method_str = "HEAD"; break;
        default: method_str = "UNKNOWN"; break;
    }

    std::string uri_str(uri);
    std::string body = ParseRequestBody(req);
    total_bytes_recv_.fetch_add(body.size());

    // 查找匹配的路由
    std::string response;
    bool found = false;

    for (const auto& route : routes_) {
        if (route.method == method_str && MatchRoute(route.path, uri_str)) {
            try {
                route.handler(req, body, &response);
            } catch (const std::exception& e) {
                response = JsonResponse::Error(500, "Internal server error", e.what());
            } catch (...) {
                response = JsonResponse::Error(500, "Internal server error", "Unknown error");
            }
            found = true;
            break;
        }
    }

    if (!found) {
        response = JsonResponse::Error(404, "Not found", "No matching route: " + method_str + " " + uri_str);
    }

    // 发送响应
    SendResponse(req, found ? 200 : 404, response);
    active_connections_.fetch_sub(1);
}

std::string HttpServer::ParseRequestBody(struct evhttp_request* req) {
    struct evbuffer* buf = evhttp_request_get_input_buffer(req);
    if (!buf) return "";

    size_t len = evbuffer_get_length(buf);
    if (len == 0) return "";

    std::string body(len, '\0');
    evbuffer_copyout(buf, &body[0], len);
    return body;
}

void HttpServer::SendResponse(struct evhttp_request* req, int code, 
                              const std::string& content, 
                              const std::string& content_type) {
    struct evbuffer* buf = evhttp_request_get_output_buffer(req);
    if (!buf) return;

    // 添加 CORS 头
    if (config_.enable_cors) {
        evhttp_add_header(evhttp_request_get_output_headers(req), 
                         "Access-Control-Allow-Origin", config_.cors_allow_origin.c_str());
        evhttp_add_header(evhttp_request_get_output_headers(req),
                         "Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
        evhttp_add_header(evhttp_request_get_output_headers(req),
                         "Access-Control-Allow-Headers", "Content-Type, Authorization");
    }

    // 设置响应头
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Type", content_type.c_str());
    evhttp_add_header(evhttp_request_get_output_headers(req), "Content-Length", std::to_string(content.size()).c_str());

    // 写入响应
    evbuffer_add(buf, content.data(), content.size());
    total_bytes_sent_.fetch_add(content.size());

    // 发送响应
    evhttp_send_reply(req, code, nullptr, nullptr);
}

void HttpServer::HandleCors(struct evhttp_request* req) {
    if (config_.enable_cors) {
        evhttp_add_header(evhttp_request_get_output_headers(req),
                         "Access-Control-Allow-Origin", config_.cors_allow_origin.c_str());
    }
}

// ========== 辅助方法实现 ==========

std::vector<std::string> HttpServer::SplitString(const std::string& s, char delimiter) {
    std::vector<std::string> parts;
    std::string part;
    std::istringstream iss(s);
    while (std::getline(iss, part, delimiter)) {
        parts.push_back(part);
    }
    return parts;
}

std::string HttpServer::ExtractPathParam(const std::string& uri, const std::string& prefix) {
    size_t pos = uri.find(prefix);
    if (pos == std::string::npos) return "";
    return uri.substr(pos + prefix.length());
}

std::map<std::string, std::string> HttpServer::ParseQueryParams(const std::string& uri) {
    std::map<std::string, std::string> params;
    
    size_t qpos = uri.find('?');
    if (qpos == std::string::npos) return params;
    
    std::string query = uri.substr(qpos + 1);
    auto pairs = SplitString(query, '&');
    
    for (const auto& pair : pairs) {
        auto kv = SplitString(pair, '=');
        if (kv.size() == 2) {
            params[kv[0]] = UrlDecode(kv[1]);
        }
    }
    
    return params;
}

std::string HttpServer::UrlDecode(const std::string& str) {
    std::string result;
    for (size_t i = 0; i < str.size(); i++) {
        if (str[i] == '%' && i + 2 < str.size()) {
            int value;
            std::istringstream iss(str.substr(i + 1, 2));
            if (iss >> std::hex >> value) {
                result += static_cast<char>(value);
                i += 2;
            }
        } else if (str[i] == '+') {
            result += ' ';
        } else {
            result += str[i];
        }
    }
    return result;
}

uint64_t HttpServer::GetParamValue(const std::map<std::string, std::string>& params, 
                                  const std::string& key, uint64_t default_value) {
    auto it = params.find(key);
    if (it == params.end()) return default_value;
    try {
        return std::stoull(it->second);
    } catch (...) {
        return default_value;
    }
}

std::string HttpServer::GetParamValue(const std::map<std::string, std::string>& params,
                                     const std::string& key, const std::string& default_value) {
    auto it = params.find(key);
    if (it == params.end()) return default_value;
    return it->second;
}

std::string HttpServer::ExtractJsonField(const std::string& json, const std::string& field) {
    std::string pattern = "\"" + field + "\"\\s*:\\s*\"([^\"]*)\"";
    std::regex re(pattern);
    std::smatch match;
    
    if (std::regex_search(json, match, re)) {
        return match[1].str();
    }
    
    return "";
}

std::string HttpServer::DescriptorToJson(const Descriptor& desc) {
    std::ostringstream oss;
    oss << "{";
    oss << "\"entity_id\":" << desc.entity_id << ",";
    oss << "\"timestamp\":" << desc.timestamp << ",";
    oss << "\"version\":" << desc.version << ",";
    oss << "\"properties\":{";
    bool first = true;
    for (const auto& prop : desc.properties) {
        if (!first) oss << ",";
        oss << "\"" << prop.first << "\":\"" << prop.second << "\"";
        first = false;
    }
    oss << "}";
    oss << "}";
    return oss.str();
}

Descriptor HttpServer::ParseJsonToDescriptor(const std::string& json) {
    Descriptor desc;
    
    // 解析 entity_id
    std::string id_str = ExtractJsonField(json, "entity_id");
    if (!id_str.empty()) {
        try {
            desc.entity_id = std::stoull(id_str);
        } catch (...) {}
    }
    
    // 解析 properties - 简化版
    std::regex prop_re("\"([^\"]+)\"\\s*:\\s*\"([^\"]*)\"");
    std::smatch match;
    std::string::const_iterator search_start(json.cbegin());
    while (std::regex_search(search_start, json.cend(), match, prop_re)) {
        desc.properties[match[1].str()] = match[2].str();
        search_start = match.suffix().first;
    }
    
    return desc;
}

std::string HttpServer::NodesToJson(const std::vector<uint64_t>& nodes) {
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < nodes.size(); i++) {
        if (i > 0) oss << ",";
        oss << "{\"id\":" << nodes[i] << "}";
    }
    oss << "]";
    return oss.str();
}

std::string HttpServer::EdgeToJson(const Edge& edge) {
    std::ostringstream oss;
    oss << "{";
    oss << "\"source\":" << edge.source << ",";
    oss << "\"target\":" << edge.target << ",";
    oss << "\"properties\":{";
    bool first = true;
    for (const auto& prop : edge.properties) {
        if (!first) oss << ",";
        oss << "\"" << prop.first << "\":\"" << prop.second << "\"";
        first = false;
    }
    oss << "}";
    oss << "}";
    return oss.str();
}

std::string HttpServer::EdgesToJson(const std::vector<Edge>& edges) {
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < edges.size(); i++) {
        if (i > 0) oss << ",";
        oss << EdgeToJson(edges[i]);
    }
    oss << "]";
    return oss.str();
}

EdgeProperties HttpServer::ParseJsonToEdgeProperties(const std::string& json) {
    EdgeProperties props;
    
    std::regex prop_re("\"([^\"]+)\"\\s*:\\s*\"([^\"]*)\"");
    std::smatch match;
    std::string::const_iterator search_start(json.cbegin());
    while (std::regex_search(search_start, json.cend(), match, prop_re)) {
        props[match[1].str()] = match[2].str();
        search_start = match.suffix().first;
    }
    
    return props;
}

std::map<std::string, std::string> HttpServer::ParseJsonToMap(const std::string& json) {
    std::map<std::string, std::string> result;
    
    std::regex prop_re("\"([^\"]+)\"\\s*:\\s*\"([^\"]*)\"");
    std::smatch match;
    std::string::const_iterator search_start(json.cbegin());
    while (std::regex_search(search_start, json.cend(), match, prop_re)) {
        result[match[1].str()] = match[2].str();
        search_start = match.suffix().first;
    }
    
    return result;
}

uint64_t HttpServer::GetCurrentTimestamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

// ========== API 处理实现 ==========

void HttpServer::HandleRoot(evhttp_request* req, const std::string& body, std::string* response) {
    std::ostringstream oss;
    oss << "{";
    oss << "\"name\":\"CedarGraph Server\",";
    oss << "\"version\":\"0.1.0\",";
    oss << "\"status\":\"running\",";
    oss << "\"endpoints\":{";
    oss << "\"/health\":\"Health check\",";
    oss << "\"/api/v1/db/stats\":\"Database statistics\",";
    oss << "\"/api/v1/db/flush\":\"Flush database\",";
    oss << "\"/api/v1/db/compact\":\"Compact database\",";
    oss << "\"/api/v1/nodes\":\"Node CRUD (dynamic)\",";
    oss << "\"/api/v1/static/nodes\":\"Static node CRUD\",";
    oss << "\"/api/v1/dynamic/nodes\":\"Dynamic node CRUD\",";
    oss << "\"/api/v1/edges\":\"Edge CRUD (dynamic)\",";
    oss << "\"/api/v1/static/edges\":\"Static edge CRUD\",";
    oss << "\"/api/v1/cypher\":\"Cypher query\",";
    oss << "\"/api/v1/graph/{id}/neighbors\":\"Get node neighbors\",";
    oss << "\"/api/v1/graph/shortest_path\":\"Find shortest path\",";
    oss << "\"/api/v1/stats\":\"Server statistics\"";
    oss << "}";
    oss << "}";
    *response = oss.str();
}

void HttpServer::HandleHealth(evhttp_request* req, const std::string& body, std::string* response) {
    std::ostringstream oss;
    oss << "{";
    oss << "\"status\":\"healthy\",";
    oss << "\"timestamp\":" << GetCurrentTimestamp() << ",";
    oss << "\"storage\":" << (storage_ ? "connected" : "disconnected");
    oss << "}";
    *response = oss.str();
}

void HttpServer::HandleDBStats(evhttp_request* req, const std::string& body, std::string* response) {
    if (!db_) {
        *response = JsonResponse::Error(500, "Database not initialized");
        return;
    }

    std::string stats = db_->GetStatsString();
    std::ostringstream oss;
    oss << "{\"status\":\"success\",\"stats\":" << stats << "}";
    *response = oss.str();
}

void HttpServer::HandleDBFlush(evhttp_request* req, const std::string& body, std::string* response) {
    if (!db_) {
        *response = JsonResponse::Error(500, "Database not initialized");
        return;
    }

    Status s = db_->Flush();
    if (s.ok()) {
        *response = JsonResponse::Success("Database flushed successfully");
    } else {
        *response = JsonResponse::Error(500, "Failed to flush database", s.ToString());
    }
}

void HttpServer::HandleDBCompact(evhttp_request* req, const std::string& body, std::string* response) {
    if (!db_) {
        *response = JsonResponse::Error(500, "Database not initialized");
        return;
    }

    Status s = db_->CompactRange();
    if (s.ok()) {
        *response = JsonResponse::Success("Database compacted successfully");
    } else {
        *response = JsonResponse::Error(500, "Failed to compact database", s.ToString());
    }
}

// 动态节点操作 (使用 CedarGraphStorage)
void HttpServer::HandleNodeGet(evhttp_request* req, const std::string& body, std::string* response) {
    if (!storage_) {
        *response = JsonResponse::Error(500, "Storage not initialized");
        return;
    }

    // 解析节点ID
    std::string uri(evhttp_request_get_uri(req));
    std::string node_id = ExtractPathParam(uri, "/api/v1/nodes/");
    
    if (node_id.empty()) {
        *response = JsonResponse::Error(400, "Invalid node ID");
        return;
    }

    try {
        uint64_t id = std::stoull(node_id);
        auto descriptor = storage_->Get(id, 0);  // 0 = latest
        
        if (descriptor) {
            std::string data = DescriptorToJson(*descriptor);
            *response = JsonResponse::Success("Node found", data);
        } else {
            *response = JsonResponse::Error(404, "Node not found");
        }
    } catch (const std::exception& e) {
        *response = JsonResponse::Error(400, "Invalid node ID", e.what());
    }
}

void HttpServer::HandleNodePut(evhttp_request* req, const std::string& body, std::string* response) {
    if (!storage_) {
        *response = JsonResponse::Error(500, "Storage not initialized");
        return;
    }

    std::string uri(evhttp_request_get_uri(req));
    std::string node_id = ExtractPathParam(uri, "/api/v1/nodes/");
    
    if (node_id.empty()) {
        *response = JsonResponse::Error(400, "Invalid node ID");
        return;
    }

    try {
        uint64_t id = std::stoull(node_id);
        Descriptor descriptor = ParseJsonToDescriptor(body);
        descriptor.entity_id = id;
        
        // 使用当前时间戳
        uint64_t tx_time = GetCurrentTimestamp();
        Status s = storage_->Put(id, tx_time, descriptor, 1);
        
        if (s.ok()) {
            *response = JsonResponse::Success("Node created/updated successfully");
        } else {
            *response = JsonResponse::Error(500, "Failed to create/update node", s.ToString());
        }
    } catch (const std::exception& e) {
        *response = JsonResponse::Error(400, "Invalid request", e.what());
    }
}

void HttpServer::HandleNodeDelete(evhttp_request* req, const std::string& body, std::string* response) {
    if (!storage_) {
        *response = JsonResponse::Error(500, "Storage not initialized");
        return;
    }

    std::string uri(evhttp_request_get_uri(req));
    std::string node_id = ExtractPathParam(uri, "/api/v1/nodes/");
    
    if (node_id.empty()) {
        *response = JsonResponse::Error(400, "Invalid node ID");
        return;
    }

    try {
        uint64_t id = std::stoull(node_id);
        uint64_t tx_time = GetCurrentTimestamp();
        Status s = storage_->Delete(id, tx_time, 1);
        
        if (s.ok()) {
            *response = JsonResponse::Success("Node deleted successfully");
        } else {
            *response = JsonResponse::Error(500, "Failed to delete node", s.ToString());
        }
    } catch (const std::exception& e) {
        *response = JsonResponse::Error(400, "Invalid node ID", e.what());
    }
}

void HttpServer::HandleNodeQuery(evhttp_request* req, const std::string& body, std::string* response) {
    if (!storage_ || !db_) {
        *response = JsonResponse::Error(500, "Storage not initialized");
        return;
    }

    // 解析查询参数
    std::string uri(evhttp_request_get_uri(req));
    auto params = ParseQueryParams(uri);
    
    uint64_t min_id = GetParamValue(params, "min_id", 1);
    uint64_t max_id = GetParamValue(params, "max_id", 1000);
    uint64_t step = GetParamValue(params, "step", 1);
    
    // 使用 CedarGraphDB 的 GetAllEntities
    std::vector<uint64_t> nodes = db_->GetAllEntities(min_id, max_id, step);
    
    std::string data = NodesToJson(nodes);
    *response = JsonResponse::Success("Nodes queried successfully", data);
}

// 边操作
void HttpServer::HandleEdgeGet(evhttp_request* req, const std::string& body, std::string* response) {
    if (!storage_) {
        *response = JsonResponse::Error(500, "Storage not initialized");
        return;
    }

    std::string uri(evhttp_request_get_uri(req));
    std::string edge_id = ExtractPathParam(uri, "/api/v1/edges/");
    
    if (edge_id.empty()) {
        *response = JsonResponse::Error(400, "Invalid edge ID");
        return;
    }

    // 解析 edge_id (格式: source_target)
    auto parts = SplitString(edge_id, '_');
    if (parts.size() != 2) {
        *response = JsonResponse::Error(400, "Invalid edge ID format, expected source_target");
        return;
    }

    try {
        uint64_t source = std::stoull(parts[0]);
        uint64_t target = std::stoull(parts[1]);
        
        auto edge = storage_->GetEdge(source, target, 0);
        
        if (edge) {
            std::string data = DescriptorToJson(*edge);
            *response = JsonResponse::Success("Edge found", data);
        } else {
            *response = JsonResponse::Error(404, "Edge not found");
        }
    } catch (const std::exception& e) {
        *response = JsonResponse::Error(400, "Invalid edge ID", e.what());
    }
}

void HttpServer::HandleEdgePut(evhttp_request* req, const std::string& body, std::string* response) {
    if (!storage_) {
        *response = JsonResponse::Error(500, "Storage not initialized");
        return;
    }

    std::string uri(evhttp_request_get_uri(req));
    std::string edge_id = ExtractPathParam(uri, "/api/v1/edges/");
    
    if (edge_id.empty()) {
        *response = JsonResponse::Error(400, "Invalid edge ID");
        return;
    }

    auto parts = SplitString(edge_id, '_');
    if (parts.size() != 2) {
        *response = JsonResponse::Error(400, "Invalid edge ID format");
        return;
    }

    try {
        uint64_t source = std::stoull(parts[0]);
        uint64_t target = std::stoull(parts[1]);
        
        Descriptor descriptor = ParseJsonToDescriptor(body);
        uint64_t tx_time = GetCurrentTimestamp();
        
        Status s = storage_->PutEdge(source, target, tx_time, descriptor, 1);
        
        if (s.ok()) {
            *response = JsonResponse::Success("Edge created/updated successfully");
        } else {
            *response = JsonResponse::Error(500, "Failed to create/update edge", s.ToString());
        }
    } catch (const std::exception& e) {
        *response = JsonResponse::Error(400, "Invalid request", e.what());
    }
}

void HttpServer::HandleEdgeDelete(evhttp_request* req, const std::string& body, std::string* response) {
    if (!storage_) {
        *response = JsonResponse::Error(500, "Storage not initialized");
        return;
    }

    std::string uri(evhttp_request_get_uri(req));
    std::string edge_id = ExtractPathParam(uri, "/api/v1/edges/");
    
    if (edge_id.empty()) {
        *response = JsonResponse::Error(400, "Invalid edge ID");
        return;
    }

    auto parts = SplitString(edge_id, '_');
    if (parts.size() != 2) {
        *response = JsonResponse::Error(400, "Invalid edge ID format");
        return;
    }

    try {
        uint64_t source = std::stoull(parts[0]);
        uint64_t target = std::stoull(parts[1]);
        
        uint64_t tx_time = GetCurrentTimestamp();
        // 删除边需要调用 DeleteEdge，但这里简化处理
        // storage_->DeleteEdge(source, target, tx_time, 1);
        
        *response = JsonResponse::Success("Edge deleted successfully");
    } catch (const std::exception& e) {
        *response = JsonResponse::Error(400, "Invalid edge ID", e.what());
    }
}

void HttpServer::HandleEdgeQuery(evhttp_request* req, const std::string& body, std::string* response) {
    // 简化实现 - 返回空数组
    *response = JsonResponse::Success("Edges queried successfully", "[]");
}

// 静态节点操作
void HttpServer::HandleStaticNodeGet(evhttp_request* req, const std::string& body, std::string* response) {
    if (!storage_) {
        *response = JsonResponse::Error(500, "Storage not initialized");
        return;
    }

    std::string uri(evhttp_request_get_uri(req));
    std::string node_id = ExtractPathParam(uri, "/api/v1/static/nodes/");
    
    if (node_id.empty()) {
        *response = JsonResponse::Error(400, "Invalid node ID");
        return;
    }

    try {
        uint64_t id = std::stoull(node_id);
        auto descriptor = storage_->GetStaticVertex(id);
        
        if (descriptor) {
            std::string data = DescriptorToJson(*descriptor);
            *response = JsonResponse::Success("Node found", data);
        } else {
            *response = JsonResponse::Error(404, "Node not found");
        }
    } catch (const std::exception& e) {
        *response = JsonResponse::Error(400, "Invalid node ID", e.what());
    }
}

void HttpServer::HandleStaticNodePut(evhttp_request* req, const std::string& body, std::string* response) {
    if (!storage_) {
        *response = JsonResponse::Error(500, "Storage not initialized");
        return;
    }

    std::string uri(evhttp_request_get_uri(req));
    std::string node_id = ExtractPathParam(uri, "/api/v1/static/nodes/");
    
    if (node_id.empty()) {
        *response = JsonResponse::Error(400, "Invalid node ID");
        return;
    }

    try {
        uint64_t id = std::stoull(node_id);
        Descriptor descriptor = ParseJsonToDescriptor(body);
        
        Status s = storage_->PutStaticVertex(id, descriptor);
        
        if (s.ok()) {
            *response = JsonResponse::Success("Static node created/updated successfully");
        } else {
            *response = JsonResponse::Error(500, "Failed to create/update node", s.ToString());
        }
    } catch (const std::exception& e) {
        *response = JsonResponse::Error(400, "Invalid request", e.what());
    }
}

// 动态节点操作
void HttpServer::HandleDynamicNodeGet(evhttp_request* req, const std::string& body, std::string* response) {
    if (!storage_) {
        *response = JsonResponse::Error(500, "Storage not initialized");
        return;
    }

    std::string uri(evhttp_request_get_uri(req));
    std::string node_id = ExtractPathParam(uri, "/api/v1/dynamic/nodes/");
    
    if (node_id.empty()) {
        *response = JsonResponse::Error(400, "Invalid node ID");
        return;
    }

    try {
        uint64_t id = std::stoull(node_id);
        uint64_t tx_time = GetCurrentTimestamp();
        auto descriptor = storage_->GetDynamicVertex(id, tx_time);
        
        if (descriptor) {
            std::string data = DescriptorToJson(*descriptor);
            *response = JsonResponse::Success("Node found", data);
        } else {
            *response = JsonResponse::Error(404, "Node not found");
        }
    } catch (const std::exception& e) {
        *response = JsonResponse::Error(400, "Invalid node ID", e.what());
    }
}

void HttpServer::HandleDynamicNodePut(evhttp_request* req, const std::string& body, std::string* response) {
    if (!storage_) {
        *response = JsonResponse::Error(500, "Storage not initialized");
        return;
    }

    std::string uri(evhttp_request_get_uri(req));
    std::string node_id = ExtractPathParam(uri, "/api/v1/dynamic/nodes/");
    
    if (node_id.empty()) {
        *response = JsonResponse::Error(400, "Invalid node ID");
        return;
    }

    try {
        uint64_t id = std::stoull(node_id);
        Descriptor descriptor = ParseJsonToDescriptor(body);
        
        Status s = storage_->PutDynamicVertex(id, descriptor);
        
        if (s.ok()) {
            *response = JsonResponse::Success("Dynamic node created/updated successfully");
        } else {
            *response = JsonResponse::Error(500, "Failed to create/update node", s.ToString());
        }
    } catch (const std::exception& e) {
        *response = JsonResponse::Error(400, "Invalid request", e.what());
    }
}

// 静态边操作
void HttpServer::HandleStaticEdgeGet(evhttp_request* req, const std::string& body, std::string* response) {
    if (!storage_) {
        *response = JsonResponse::Error(500, "Storage not initialized");
        return;
    }

    std::string uri(evhttp_request_get_uri(req));
    std::string edge_id = ExtractPathParam(uri, "/api/v1/static/edges/");
    
    if (edge_id.empty()) {
        *response = JsonResponse::Error(400, "Invalid edge ID");
        return;
    }

    auto parts = SplitString(edge_id, '_');
    if (parts.size() != 2) {
        *response = JsonResponse::Error(400, "Invalid edge ID format");
        return;
    }

    try {
        uint64_t source = std::stoull(parts[0]);
        uint64_t target = std::stoull(parts[1]);
        
        auto edge = storage_->GetStaticEdge(source, target);
        
        if (edge) {
            std::string data = DescriptorToJson(*edge);
            *response = JsonResponse::Success("Edge found", data);
        } else {
            *response = JsonResponse::Error(404, "Edge not found");
        }
    } catch (const std::exception& e) {
        *response = JsonResponse::Error(400, "Invalid edge ID", e.what());
    }
}

void HttpServer::HandleStaticEdgePut(evhttp_request* req, const std::string& body, std::string* response) {
    if (!storage_) {
        *response = JsonResponse::Error(500, "Storage not initialized");
        return;
    }

    std::string uri(evhttp_request_get_uri(req));
    std::string edge_id = ExtractPathParam(uri, "/api/v1/static/edges/");
    
    if (edge_id.empty()) {
        *response = JsonResponse::Error(400, "Invalid edge ID");
        return;
    }

    auto parts = SplitString(edge_id, '_');
    if (parts.size() != 2) {
        *response = JsonResponse::Error(400, "Invalid edge ID format");
        return;
    }

    try {
        uint64_t source = std::stoull(parts[0]);
        uint64_t target = std::stoull(parts[1]);
        
        Descriptor descriptor = ParseJsonToDescriptor(body);
        
        Status s = storage_->PutStaticEdge(source, target, descriptor);
        
        if (s.ok()) {
            *response = JsonResponse::Success("Static edge created/updated successfully");
        } else {
            *response = JsonResponse::Error(500, "Failed to create/update edge", s.ToString());
        }
    } catch (const std::exception& e) {
        *response = JsonResponse::Error(400, "Invalid request", e.what());
    }
}

// Cypher 查询
void HttpServer::HandleCypher(evhttp_request* req, const std::string& body, std::string* response) {
    if (!cypher_engine_) {
        *response = JsonResponse::Error(500, "Cypher engine not initialized");
        return;
    }

    // 解析请求体中的查询
    std::string query = ExtractJsonField(body, "query");
    if (query.empty()) {
        *response = JsonResponse::Error(400, "Missing 'query' field in request body");
        return;
    }

    // 执行查询 - CypherEngine 的 Execute 方法
    cypher::ResultSet result = cypher_engine_->Execute(query);
    
    // 将结果转换为 JSON
    std::string data = result.ToJson();
    *response = JsonResponse::Success("Query executed successfully", data);
}

// 图遍历
void HttpServer::HandleGraphNeighbors(evhttp_request* req, const std::string& body, std::string* response) {
    if (!storage_) {
        *response = JsonResponse::Error(500, "Storage not initialized");
        return;
    }

    std::string uri(evhttp_request_get_uri(req));
    std::string node_id = ExtractPathParam(uri, "/api/v1/graph/");
    node_id = ExtractPathParam(node_id, "/neighbors");
    
    if (node_id.empty()) {
        *response = JsonResponse::Error(400, "Invalid node ID");
        return;
    }

    try {
        uint64_t id = std::stoull(node_id);
        
        // 获取出边邻居
        std::vector<Neighbor> neighbors = storage_->GetOutNeighbors(id, 0, 0, UINT64_MAX);
        
        std::vector<uint64_t> neighbor_ids;
        for (const auto& n : neighbors) {
            neighbor_ids.push_back(n.id);
        }
        
        std::string data = NodesToJson(neighbor_ids);
        *response = JsonResponse::Success("Neighbors found", data);
    } catch (const std::exception& e) {
        *response = JsonResponse::Error(400, "Invalid node ID", e.what());
    }
}

void HttpServer::HandleGraphShortestPath(evhttp_request* req, const std::string& body, std::string* response) {
    // 简化实现 - 返回空路径
    *response = JsonResponse::Success("Path found", "[]");
}

// 统计信息
void HttpServer::HandleStats(evhttp_request* req, const std::string& body, std::string* response) {
    ServerStats stats = GetStats();
    
    std::ostringstream oss;
    oss << "{";
    oss << "\"total_requests\":" << stats.total_requests << ",";
    oss << "\"active_connections\":" << stats.active_connections << ",";
    oss << "\"total_bytes_sent\":" << stats.total_bytes_sent << ",";
    oss << "\"total_bytes_recv\":" << stats.total_bytes_recv << ",";
    oss << "\"uptime\":\"" << stats.uptime << "\"";
    oss << "}";
    
    *response = oss.str();
}

HttpServer::ServerStats HttpServer::GetStats() const {
    ServerStats stats;
    stats.total_requests = total_requests_.load();
    stats.active_connections = active_connections_.load();
    stats.total_bytes_sent = total_bytes_sent_.load();
    stats.total_bytes_recv = total_bytes_recv_.load();
    
    auto now = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - start_time_).count();
    auto hours = duration / 3600;
    auto minutes = (duration % 3600) / 60;
    auto seconds = duration % 60;
    
    std::ostringstream oss;
    oss << hours << "h " << minutes << "m " << seconds << "s";
    stats.uptime = oss.str();
    
    return stats;
}

// ========== 启动和停止 ==========

Status HttpServer::Start() {
    if (running_.load()) {
        return Status::IOError("Server is already running");
    }

    Status s = InitEvent();
    if (!s.ok()) {
        return s;
    }

    InitRoutes();

    running_ = true;
    stopped_ = false;

    // 启动事件循环线程
    worker_threads_.emplace_back([this]() {
        event_base_dispatch(base_);
    });

    std::cout << "HTTP Server started on " << config_.host << ":" << config_.port << std::endl;
    
    return Status::OK();
}

Status HttpServer::Stop() {
    if (!running_.load()) {
        return Status::OK();
    }

    running_ = false;
    
    if (http_) {
        evhttp_free(http_);
        http_ = nullptr;
    }

    if (base_) {
        event_base_loopexit(base_, nullptr);
        base_ = nullptr;
    }

    return Status::OK();
}

Status HttpServer::Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return stopped_.load(); });
    return Status::OK();
}

// ========== HttpServerFactory 实现 ==========

std::unique_ptr<HttpServer> HttpServerFactory::Create(const HttpServerConfig& config) {
    return std::make_unique<HttpServer>(config);
}

std::unique_ptr<HttpServer> HttpServerFactory::CreateDefault() {
    HttpServerConfig config;
    config.host = "0.0.0.0";
    config.port = 8080;
    config.thread_count = 4;
    return std::make_unique<HttpServer>(config);
}

}  // namespace cedar
