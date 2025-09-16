
#include "dispatcher.hpp"
#include <fstream>
#include <iostream>
#include <sstream>
#include <filesystem>

Dispatcher &Dispatcher::instance()
{
  static Dispatcher inst;
  return inst;
}

Dispatcher::~Dispatcher() { stop(); }

void Dispatcher::start()
{
  bool expected = false;
  if (running_.compare_exchange_strong(expected, true))
  {
    t_log_ = std::thread(&Dispatcher::log_worker, this);
    t_file1_ = std::thread(&Dispatcher::file_worker, this, 1);
    t_file2_ = std::thread(&Dispatcher::file_worker, this, 2);
  }
}

void Dispatcher::stop()
{
  bool expected = true;
  if (running_.compare_exchange_strong(expected, false))
  {
    {
      std::lock_guard<std::mutex> lk(mtx_log_);
    }
    cv_log_.notify_all();
    {
      std::lock_guard<std::mutex> lk(mtx_file_);
    }
    cv_file_.notify_all();
    if (t_log_.joinable())
      t_log_.join();
    if (t_file1_.joinable())
      t_file1_.join();
    if (t_file2_.joinable())
      t_file2_.join();
  }
}

void Dispatcher::publish(Block block)
{
  {
    std::lock_guard<std::mutex> lk(mtx_log_);
    q_log_.push(block);
  }
  cv_log_.notify_one();

  {
    std::lock_guard<std::mutex> lk(mtx_file_);
    q_file_.push(std::move(block));
  }
  cv_file_.notify_one();
}

void Dispatcher::log_worker()
{
  for (;;)
  {
    Block blk;
    {
      std::unique_lock<std::mutex> lk(mtx_log_);
      cv_log_.wait(lk, [&]
                   { return !running_ || !q_log_.empty(); });
      if (!running_ && q_log_.empty())
        break;
      blk = std::move(q_log_.front());
      q_log_.pop();
    }
    if (!blk.cmds.empty())
    {
      std::cout << "bulk: ";
      for (std::size_t i = 0; i < blk.cmds.size(); ++i)
      {
        if (i)
          std::cout << ", ";
        std::cout << blk.cmds[i];
      }
      std::cout << '\n';
    }
  }
}

void Dispatcher::file_worker(int file_index)
{
  for (;;)
  {
    Block blk;
    {
      std::unique_lock<std::mutex> lk(mtx_file_);
      cv_file_.wait(lk, [&]
                    { return !running_ || !q_file_.empty(); });
      if (!running_ && q_file_.empty())
        break;
      blk = std::move(q_file_.front());
      q_file_.pop();
    }
    if (blk.cmds.empty())
      continue;

    unsigned long long seq = seq_.fetch_add(1, std::memory_order_relaxed);
    std::ostringstream name;
    name << "bulk" << blk.ts << '_' << file_index << '_' << seq << ".log";
    const std::string dir = "log";
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    std::ofstream out(dir + "/" + name.str(), std::ios::out | std::ios::trunc);
    if (!out)
      continue;
    for (std::size_t i = 0; i < blk.cmds.size(); ++i)
    {
      if (i)
        out << ", ";
      out << blk.cmds[i];
    }
    out << '\n';
  }
}
