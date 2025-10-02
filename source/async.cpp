#include "async/async.h"

#include "dispatcher.hpp"
#include <chrono>
#include <mutex>
#include <string>
#include <vector>

namespace {

static std::time_t now_epoch_seconds()
{
  using clock = std::chrono::system_clock;
  return std::chrono::system_clock::to_time_t(clock::now());
}

class GlobalStaticAggregator {
public:
  void set_size(std::size_t n)
  {
    std::lock_guard<std::mutex> lk(mtx_);
    N_ = n ? n : 1;
  }

  void feed(const std::string &line, std::time_t ts)
  {
    std::vector<std::string> to_publish;
    std::time_t publish_ts{};
    {
      std::lock_guard<std::mutex> lk(mtx_);
      if (cmds_.empty())
        ts_ = ts;
      cmds_.push_back(line);
      if (cmds_.size() == N_)
      {
        to_publish.swap(cmds_);
        publish_ts = ts_;
      }
    }
    if (!to_publish.empty())
    {
      Dispatcher::instance().publish(Block{to_publish, publish_ts});
    }
  }

  void flush()
  {
    std::vector<std::string> to_publish;
    std::time_t publish_ts{};
    {
      std::lock_guard<std::mutex> lk(mtx_);
      if (!cmds_.empty())
      {
        to_publish.swap(cmds_);
        publish_ts = ts_;
      }
    }
    if (!to_publish.empty())
    {
      Dispatcher::instance().publish(Block{to_publish, publish_ts});
    }
  }

private:
  std::mutex mtx_;
  std::vector<std::string> cmds_;
  std::time_t ts_{};
  std::size_t N_{1};
};

static GlobalStaticAggregator &global_static()
{
  static GlobalStaticAggregator g;
  return g;
}

struct Context {
  explicit Context(std::size_t /*n*/) {}

  std::string buffer;
  std::vector<std::string> dyn_cmds;
  std::time_t dyn_ts{};
  int dyn_depth{0};
  std::mutex mtx;
};

}
namespace async
{

  using CtxPtr = Context *;

  handle_t connect(std::size_t bulk)
  {
    Dispatcher::instance().start();
    global_static().set_size(bulk);
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

      auto now = now_epoch_seconds();
      if (line == "{")
      {
        if (ctx->dyn_depth == 0)
          global_static().flush();
        ++ctx->dyn_depth;
        continue;
      }
      if (line == "}")
      {
        if (ctx->dyn_depth > 0)
        {
          --ctx->dyn_depth;
          if (ctx->dyn_depth == 0)
          {
            if (!ctx->dyn_cmds.empty())
            {
              Dispatcher::instance().publish(Block{ctx->dyn_cmds, ctx->dyn_ts});
              ctx->dyn_cmds.clear();
            }
          }
        }
        continue;
      }

      if (ctx->dyn_depth > 0)
      {
        if (ctx->dyn_cmds.empty())
          ctx->dyn_ts = now;
        ctx->dyn_cmds.push_back(line);
      }
      else
      {
        global_static().feed(line, now);
      }
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
        auto now = now_epoch_seconds();
        if (ctx->dyn_depth > 0)
        {
          if (ctx->dyn_cmds.empty())
            ctx->dyn_ts = now;
          ctx->dyn_cmds.push_back(ctx->buffer);
        }
        else
        {
          global_static().feed(ctx->buffer, now);
        }
        ctx->buffer.clear();
      }

      if (ctx->dyn_depth != 0)
      {
        ctx->dyn_cmds.clear();
        ctx->dyn_depth = 0;
      }
    }
    delete ctx;
  }

}
