#include <boost/asio.hpp>
#include <iostream>
#include <memory>
#include <array>

#include "async/async.h"

using boost::asio::ip::tcp;

class Session : public std::enable_shared_from_this<Session>
{
public:
  Session(tcp::socket socket, std::size_t bulk_size)
      : socket_(std::move(socket)), bulk_size_(bulk_size)
  {
  }

  void start()
  {
    handle_ = async::connect(bulk_size_);
    do_read();
  }

private:
  void do_read()
  {
    auto self = shared_from_this();
    socket_.async_read_some(
        boost::asio::buffer(buf_),
        [this, self](boost::system::error_code ec, std::size_t bytes)
        {
          if (!ec)
          {
            if (bytes > 0)
              async::receive(handle_, buf_.data(), bytes);
            do_read();
          }
          else
          {
            if (handle_)
            {
              async::disconnect(handle_);
              handle_ = nullptr;
            }
            boost::system::error_code ignore;
            socket_.shutdown(tcp::socket::shutdown_both, ignore);
            socket_.close(ignore);
          }
        });
  }

  tcp::socket socket_;
  async::handle_t handle_{nullptr};
  std::size_t bulk_size_{};
  std::array<char, 4096> buf_{};
};

class Server
{
public:
  Server(boost::asio::io_context &io, unsigned short port, std::size_t bulk)
      : acceptor_(io, tcp::endpoint(tcp::v4(), port)), bulk_size_(bulk)
  {
    do_accept();
  }

private:
  void do_accept()
  {
    acceptor_.async_accept(
        [this](boost::system::error_code ec, tcp::socket socket)
        {
          if (!ec)
          {
            std::make_shared<Session>(std::move(socket), bulk_size_)->start();
          }
          do_accept();
        });
  }

  tcp::acceptor acceptor_;
  std::size_t bulk_size_{};
};

int main(int argc, char *argv[])
{
  if (argc != 3)
  {
    std::cerr << "Usage: bulk_server <port> <bulk_size>\n";
    return 1;
  }

  try
  {
    const auto port = static_cast<unsigned short>(std::stoul(argv[1]));
    const auto bulk_size = static_cast<std::size_t>(std::stoul(argv[2]));

    boost::asio::io_context io;
    Server srv(io, port, bulk_size);
    io.run();
  }
  catch (const std::exception &e)
  {
    std::cerr << "Error: " << e.what() << "\n";
    return 2;
  }
  return 0;
}


