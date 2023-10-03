#include "client_manager.h"
#include <glog/logging.h>

using asio::ip::tcp;

void ClientManager::Start(tcp::socket socket) {
  auto id = NextClientId();
  auto client =
    std::make_shared<Client>(id, std::move(socket), multi_paxos_, this,
                             is_from_client_);
  {
    std::scoped_lock lock(mu_);
    auto [it, ok] = clients_.insert({id, client});
    CHECK(ok);
  }
  DLOG(INFO) << " client_manager started client " << id;
  client->Start();
}

client_ptr ClientManager::Get(int64_t id) {
  std::scoped_lock lock(mu_);
  auto it = clients_.find(id);
  if (it == clients_.end())
    return nullptr;
  return it->second;
}

void ClientManager::Stop(int64_t id) {
  std::scoped_lock lock(mu_);
  DLOG(INFO) << " client_manager stopped client " << id;
  auto it = clients_.find(id);
  CHECK(it != clients_.end());
  it->second->Stop();
  clients_.erase(it);
}

void ClientManager::StopAll() {
  std::scoped_lock lock(mu_);
  for (auto& [id, client] : clients_) {
    DLOG(INFO) << " client_manager stopping all clients " << id;
    client->Stop();
  }
  clients_.clear();
  //thread_pool_.join();
}
