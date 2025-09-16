
#include "parser.hpp"
#include "subscriber_async.hpp"
#include "dispatcher.hpp"
#include <chrono>
#include <iostream>
#include <string>

static std::time_t now_epoch_seconds()
{
  using clock = std::chrono::system_clock;
  return std::chrono::system_clock::to_time_t(clock::now());
}

int main(int argc, char *argv[])
{

  if (argc != 2)
  {
    std::cerr << "Usage: bulk <N>\n";
    return 1;
  }

  std::size_t N{};
  try
  {
    N = static_cast<std::size_t>(std::stoul(argv[1]));
  }
  catch (...)
  {
    std::cerr << "Invalid N\n";
  }

  Dispatcher::instance().start();

  Batcher batcher(N);
  batcher.subscribe(std::make_shared<AsyncSubscriber>());

  std::string line;
  while (std::getline(std::cin, line))
  {
    batcher.feed(line, now_epoch_seconds());
  }
  batcher.finish();
  Dispatcher::instance().stop();

  return 0;
}
