// Copyright 2026 The Cedar Authors

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>

#include <gtest/gtest.h>

#include "cedar/core/status.h"
#include "kernel/async_submission_executor.h"
#define private public
#include "cedar/database.h"
#undef private
#include "kernel/database_impl.h"

namespace cedar {
namespace {

std::shared_ptr<AsyncSubmissionExecutor::Ticket> MakeTicket(
    uint64_t bytes, std::function<Status()> handoff) {
  auto ticket = std::make_shared<AsyncSubmissionExecutor::Ticket>();
  ticket->estimated_bytes = bytes;
  ticket->handoff = std::move(handoff);
  ticket->fail = [](const Status&) {};
  ticket->release = [] {};
  return ticket;
}

TEST(AsyncSubmissionExecutorTest, RejectsRequestBoundWithoutWaiting) {
  AsyncSubmissionExecutor executor({1, 1, 1 << 20});
  ASSERT_TRUE(executor.Start().ok());

  std::mutex mutex;
  std::condition_variable condition;
  bool started = false;
  bool stop = false;
  bool handoff_done = false;
  auto held = MakeTicket(1, [&] {
    std::unique_lock<std::mutex> lock(mutex);
    started = true;
    condition.notify_all();
    condition.wait(lock, [&] { return stop; });
    handoff_done = true;
    condition.notify_all();
    return Status::OK();
  });
  ASSERT_TRUE(executor.TrySubmit(held).ok());
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(condition.wait_for(lock, std::chrono::seconds(2),
                                   [&] { return started; }));
  }

  auto rejected = MakeTicket(1, [] { return Status::OK(); });
  const auto started_at = std::chrono::steady_clock::now();
  const Status status = executor.TrySubmit(rejected);
  const auto elapsed = std::chrono::steady_clock::now() - started_at;
  EXPECT_TRUE(status.IsResourceExhausted());
  EXPECT_LT(elapsed, std::chrono::milliseconds(20));

  {
    std::lock_guard<std::mutex> lock(mutex);
    stop = true;
  }
  condition.notify_all();
  executor.Stop(Status::ShutdownInProgress("test"));
}

TEST(AsyncSubmissionExecutorTest, ReleaseIsIdempotentAndByteBounded) {
  AsyncSubmissionExecutor executor({1, 4, 4});
  ASSERT_TRUE(executor.Start().ok());
  std::mutex mutex;
  std::condition_variable condition;
  bool started = false;
  bool stop = false;
  bool handoff_done = false;
  uint32_t releases = 0;
  auto held = MakeTicket(4, [&] {
    std::unique_lock<std::mutex> lock(mutex);
    started = true;
    condition.notify_all();
    condition.wait(lock, [&] { return stop; });
    handoff_done = true;
    condition.notify_all();
    return Status::OK();
  });
  held->release = [&] {
    std::lock_guard<std::mutex> lock(mutex);
    ++releases;
    condition.notify_all();
  };
  ASSERT_TRUE(executor.TrySubmit(held).ok());
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(condition.wait_for(lock, std::chrono::seconds(2),
                                   [&] { return started; }));
  }
  EXPECT_TRUE(executor.TrySubmit(MakeTicket(1, [] { return Status::OK(); }))
                  .IsResourceExhausted());

  {
    std::lock_guard<std::mutex> lock(mutex);
    stop = true;
  }
  condition.notify_all();
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(condition.wait_for(lock, std::chrono::seconds(2),
                                   [&] { return handoff_done; }));
  }
  executor.Release(held->id);
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(condition.wait_for(lock, std::chrono::seconds(2),
                                   [&] { return releases == 1; }));
  }
  EXPECT_EQ(executor.ReservedBytes(), 0U);
  executor.Release(held->id);
  EXPECT_EQ(executor.ReservedBytes(), 0U);
  executor.Stop(Status::ShutdownInProgress("test"));
}

TEST(AsyncSubmissionExecutorTest, CancelsQueuedTicketBeforeHandoff) {
  AsyncSubmissionExecutor executor({1, 2, 1 << 20});
  ASSERT_TRUE(executor.Start().ok());

  std::mutex mutex;
  std::condition_variable condition;
  bool first_started = false;
  bool stop_first = false;
  bool second_failed = false;
  bool second_handed_off = false;
  auto first = MakeTicket(1, [&] {
    std::unique_lock<std::mutex> lock(mutex);
    first_started = true;
    condition.notify_all();
    condition.wait(lock, [&] { return stop_first; });
    return Status::OK();
  });
  ASSERT_TRUE(executor.TrySubmit(first).ok());
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(condition.wait_for(lock, std::chrono::seconds(2),
                                   [&] { return first_started; }));
  }

  auto second = MakeTicket(1, [&] {
    std::lock_guard<std::mutex> lock(mutex);
    second_handed_off = true;
    condition.notify_all();
    return Status::OK();
  });
  second->fail = [&](const Status& status) {
    EXPECT_TRUE(status.IsResourceExhausted());
    std::lock_guard<std::mutex> lock(mutex);
    second_failed = true;
    condition.notify_all();
  };
  ASSERT_TRUE(executor.TrySubmit(second).ok());
  executor.Cancel(second->id);
  {
    std::lock_guard<std::mutex> lock(mutex);
    stop_first = true;
  }
  condition.notify_all();
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(condition.wait_for(lock, std::chrono::seconds(2),
                                   [&] { return second_failed; }));
    EXPECT_FALSE(second_handed_off);
  }
  executor.Stop(Status::ShutdownInProgress("test"));
  EXPECT_EQ(executor.ReservedRequests(), 0U);
}

TEST(AsyncSubmissionExecutorTest,
     AppendRequestAndTicketDoNotRetainEachOtherAfterCompletion) {
  std::weak_ptr<Database::Impl::AppendCommitRequest> weak_request;
  std::weak_ptr<AsyncSubmissionExecutor::Ticket> weak_ticket;
  {
    auto request = std::make_shared<Database::Impl::AppendCommitRequest>();
    auto ticket = std::make_shared<AsyncSubmissionExecutor::Ticket>();
    ticket->handoff = [request] { return Status::OK(); };
    ticket->fail = [](const Status&) {};
    ticket->release = [] {};
    request->executor_ticket = ticket;
    weak_request = request;
    weak_ticket = ticket;
  }

  EXPECT_TRUE(weak_request.expired());
  EXPECT_TRUE(weak_ticket.expired());
}

}  // namespace
}  // namespace cedar
