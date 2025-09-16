#pragma once
#include <ctime>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

struct BatchSubscriber
{
  virtual ~BatchSubscriber() = default;
  virtual void onBatch(const std::vector<std::string> &cmds,
                       std::time_t ts) = 0;
};

struct ConsoleSubscriber final : BatchSubscriber
{
  void onBatch(const std::vector<std::string> &cmds, std::time_t ts) override
  {
    if (cmds.empty())
      return;

    std::cout << "bulk: ";
    for (std::size_t i = 0; i < cmds.size(); ++i)
    {
      if (i)
        std::cout << ", ";
      std::cout << cmds[i];
    }
    std::cout << '\n';
  }
};

struct FileSubscriber final : BatchSubscriber
{
  void onBatch(const std::vector<std::string> &cmds, std::time_t ts) override
  {
    if (cmds.empty())
      return;
    std::ostringstream name;
    name << "bulk" << ts << ".log";
    std::ofstream out(name.str(), std::ios::out | std::ios::trunc);
    if (!out)
      return;
    for (std::size_t i = 0; i < cmds.size(); ++i)
    {
      if (i)
        out << ", ";
      out << cmds[i];
    }
    out << '\n';
  }
};

class Batcher
{
public:
  explicit Batcher(std::size_t n) : N_(n) { subscribers_.reserve(2); }

  void subscribe(std::shared_ptr<BatchSubscriber> s)
  {
    subscribers_.push_back(std::move(s));
  }

  void feed(const std::string &line, std::time_t now)
  {
    if (line == "{")
    {
      if (dyn_depth_ == 0)
        flushStatic();
      ++dyn_depth_;
      return;
    }
    if (line == "}")
    {
      if (dyn_depth_ > 0)
      {
        --dyn_depth_;
        if (dyn_depth_ == 0)
          flushDynamic();
      }
      return;
    }
    if (dyn_depth_ > 0)
    {
      if (dyn_cmds_.empty())
        dyn_ts_ = now;
      dyn_cmds_.push_back(line);
    }
    else
    {
      if (st_cmds_.empty())
        st_ts_ = now;
      st_cmds_.push_back(line);
      if (st_cmds_.size() == N_)
        flushStatic();
    }
  }

  void finish()
  {
    if (dyn_depth_ == 0)
    {
      flushStatic();
    }
    else
    {
      dyn_cmds_.clear();
      dyn_depth_ = 0;
    }
  }

private:
  void notifyAll(const std::vector<std::string> &cmds, std::time_t ts)
  {
    for (auto &s : subscribers_)
      s->onBatch(cmds, ts);
  }

  void flushStatic()
  {
    if (!st_cmds_.empty())
    {
      notifyAll(st_cmds_, st_ts_);
      st_cmds_.clear();
    }
  }
  void flushDynamic()
  {
    if (!dyn_cmds_.empty())
    {
      notifyAll(dyn_cmds_, dyn_ts_);
      dyn_cmds_.clear();
    }
  }
  const std::size_t N_;
  std::vector<std::shared_ptr<BatchSubscriber>> subscribers_;

  // static packet
  std::vector<std::string> st_cmds_;
  std::time_t st_ts_{};

  // dynamic packet
  std::vector<std::string> dyn_cmds_;
  std::time_t dyn_ts_{};
  int dyn_depth_{0};
};
