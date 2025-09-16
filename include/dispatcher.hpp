#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

struct Block
{
  std::vector<std::string> cmds;
  std::time_t ts{};
};

class Dispatcher
{
public:
  static Dispatcher &instance();

  void start();
  void stop();

  void publish(Block block);

private:
  Dispatcher() = default;
  ~Dispatcher();
  Dispatcher(const Dispatcher &) = delete;
  Dispatcher &operator=(const Dispatcher &) = delete;

  void log_worker();
  void file_worker(int file_index);

  std::mutex mtx_log_;
  std::condition_variable cv_log_;
  std::queue<Block> q_log_;

  std::mutex mtx_file_;
  std::condition_variable cv_file_;
  std::queue<Block> q_file_;

  std::thread t_log_;
  std::thread t_file1_;
  std::thread t_file2_;
  std::atomic<bool> running_{false};
  std::atomic<uint64_t> seq_{0};
};
