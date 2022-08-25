#include "client_manager.h"
#include <glog/logging.h>

using asio::ip::tcp;

void ClientManager::Start(tcp::socket socket) {
  auto id = NextClientId();
  auto client =
      std::make_shared<Client>(id, std::move(socket), multi_paxos_, this);
  auto [it, ok] = clients_.insert({id, client});
  CHECK(ok);
  client->Start();
}

client_ptr ClientManager::Get(int64_t id) {
  auto it = clients_.find(id);
  if (it == clients_.end())
    return nullptr;
  return it->second;
}

void ClientManager::Stop(int64_t id) {
  auto it = clients_.find(id);
  CHECK(it != clients_.end());
  it->second->Stop();
  clients_.erase(it);
}

void ClientManager::StopAll() {
  for (auto& [id, client] : clients_)
    client->Stop();
  clients_.clear();
}
