#include "async/async.h"

#include "dispatcher.hpp"
#include "parser.hpp"
#include "subscriber_async.hpp"
#include <chrono>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>

namespace
{

  static std::time_t now_epoch_seconds()
  {
    using clock = std::chrono::system_clock;
    return std::chrono::system_clock::to_time_t(clock::now());
  }

  struct Context
  {
    explicit Context(std::size_t n) : batcher(n)
    {
      batcher.subscribe(std::make_shared<AsyncSubscriber>());
    }
    Batcher batcher;
    std::string buffer;
    std::mutex mtx;
  };

}

namespace async
{

  using CtxPtr = Context *;

  handle_t connect(std::size_t bulk)
  {

    Dispatcher::instance().start();
    auto *ctx = new Context(bulk);
    return reinterpret_cast<handle_t>(ctx);
  }

  void receive(handle_t handle, const char *data, std::size_t size)
  {

    if (!handle || !data || size == 0)
      return;
    auto *ctx = reinterpret_cast<CtxPtr>(handle);
    std::lock_guard<std::mutex> lk(ctx->mtx);
    ctx->buffer.append(data, size);
    std::size_t pos = 0;
    while (true)
    {
      auto nl = ctx->buffer.find('\n', pos);
      if (nl == std::string::npos)
      {
        if (pos > 0)
          ctx->buffer.erase(0, pos);
        break;
      }
      std::string line = ctx->buffer.substr(pos, nl - pos);
      pos = nl + 1;
      ctx->batcher.feed(line, now_epoch_seconds());
    }
  }

  void disconnect(handle_t handle)
  {
    if (!handle)
      return;
    auto *ctx = reinterpret_cast<CtxPtr>(handle);
    {
      std::lock_guard<std::mutex> lk(ctx->mtx);
      if (!ctx->buffer.empty())
      {
        ctx->batcher.feed(ctx->buffer, now_epoch_seconds());
        ctx->buffer.clear();
      }
      ctx->batcher.finish();
    }
    delete ctx;
  }

}