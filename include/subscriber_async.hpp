#pragma once

#include "parser.hpp"
#include "dispatcher.hpp"

struct AsyncSubscriber final : BatchSubscriber
{
  void onBatch(const std::vector<std::string> &cmds, std::time_t ts) override
  {

    Dispatcher::instance().publish(Block{cmds, ts});
  }
};
