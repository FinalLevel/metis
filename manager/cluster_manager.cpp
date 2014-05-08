///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis cluster control class implementation
///////////////////////////////////////////////////////////////////////////////

#include "cluster_manager.hpp"
#include "../metis_log.hpp"
#include "storage_cmd_event.hpp"

using namespace fl::metis;

namespace EManagerFlds
{
	enum EManagerFlds
	{
		ID,
		CMD_IP,
		CMD_PORT,
		STATUS,
	};
};

const char * const MANAGER_SQL = "SELECT id, cmdIp, cmdPort, status FROM manager";

ManagerNode::ManagerNode(MysqlResult *res)
	: _id(res->get<decltype(_id)>(EManagerFlds::ID)), 
		_ip(Socket::ip2Long(res->get(EManagerFlds::CMD_IP))),
		_port(res->get<decltype(_port)>(EManagerFlds::CMD_PORT)),  
		_status(res->get<decltype(_status)>(EManagerFlds::STATUS))
{
}

bool ClusterManager::_loadManagers(Mysql &sql)
{
	auto res = sql.query(MANAGER_SQL);
	if (!res)	{
		log::Error::L("Cannot load information about manager nodes\n");
		return false;
	}
	while (res->next()) {
		TManagerNodePtr manager(new ManagerNode(res.get()));
		_managers.insert(TManagerNodeMap::value_type(manager->id(), manager));
	}
	log::Info::L("Load %u managers\n", _managers.size());
	return true;
}

namespace EStorageFlds
{
	enum EStorageFlds
	{
		ID,
		GROUPID,
		IP,
		PORT,
		STATUS,
	};
};

const char * const  STORAGE_SQL = "SELECT id, groupID, ip, port, status FROM storage";

StorageNode::StorageNode(MysqlResult *res)
	: _id(res->get<decltype(_id)>(EStorageFlds::ID)), 
		_groupID(res->get<decltype(_groupID)>(EStorageFlds::GROUPID)), 
		_ip(Socket::ip2Long(res->get(EStorageFlds::IP))),
		_port(res->get<decltype(_port)>(EStorageFlds::PORT)),  
		_status(res->get<decltype(_status)>(EStorageFlds::STATUS)),
		_weight(rand()), _leftSpace(0), _errors(0)
{
	
}

bool StorageNode::balanceStorage(StorageNode *a, StorageNode *b)
{
	return a->_weight < b->_weight;
}

bool StorageNode::canPut(const TSize size) const
{
	if ((_status & (ST_ACTIVE | ST_CAN_PUT | ST_DOWN)) == (ST_ACTIVE | ST_CAN_PUT))
		return (size < _leftSpace);
	else
		return false;
}


void StorageNode::ping(StoragePingAnswer &storageAnswer)
{
	_leftSpace = storageAnswer.leftSpace;
	_errors = 0;
	if (_status & ST_DOWN) {
		_status &= (~ST_DOWN);
		log::Warning::L("Storage %s:%u (%u) is UP\n", Socket::ip2String(_ip).c_str(), _port, _id);
	}
}

void StorageNode::error()
{
	if (_errors >= MAX_ERRORS_BEFORE_DOWN) {
		if (!(_status & ST_DOWN)) {
			_status |= ST_DOWN;
			log::Warning::L("Storage %s:%u (%u) is DOWN\n", Socket::ip2String(_ip).c_str(), _port, _id);
		}
	} else {
		_errors++;
	}
}

bool ClusterManager::_loadStorages(Mysql &sql)
{
	auto res = sql.query(STORAGE_SQL);
	if (!res)	{
		log::Error::L("Cannot load information about storage nodes\n");
		return false;
	}
	while (res->next()) {
		TStorageNodePtr storage(new StorageNode(res.get()));
		_storages.insert(TStorageNodeMap::value_type(storage->id(), storage));
	}
	log::Info::L("Load %u storages\n", _storages.size());
	return true;
}

bool ClusterManager::loadAll(Mysql &sql)
{
	if (!_loadManagers(sql))
		return false;

	if (!_loadStorages(sql))
		return false;

	return true;
}

void ClusterManager::findStorages(TServerIDList &storageIds, TStorageList &storages)
{
	AutoMutex autoSync(&_sync);
	for (auto storageID = storageIds.begin(); storageID != storageIds.end(); storageID++) {
		auto storage = _storages.find(*storageID);
		if (storage == _storages.end()) {
			log::Warning::L("Can't find storage %u\n", *storageID);
		} else {
			storages.push_back(storage->second.get());
		}
	}
}

TServerID ClusterManager::findFreeManager()
{
	AutoMutex autoSync(&_sync);
	TManagerList managers;
	for (auto manager = _managers.begin(); manager != _managers.end(); manager++) {
		if (manager->second->isFree()) {
			managers.push_back(manager->second.get());
		}
	}
	if (managers.empty())
		return 0;
	return managers[rand() % managers.size()]->id();
}

bool ClusterManager::findFreeStorages(const size_t minimumCopies, TServerIDList &storageIDs, const int64_t minLeftSpace)
{
	AutoMutex autoSync(&_sync);
	TStorageList freeStorages;
	for (auto storage = _storages.begin(); storage != _storages.end(); storage++) {
		if (storage->second->canPut(minLeftSpace)) {
			bool newGroup = true;
			for (auto freeStorage = freeStorages.begin(); freeStorage != freeStorages.end(); freeStorage++) {
				if ((*freeStorage)->groupID() == storage->second->groupID()) {
					newGroup = false;
					break;
				}
			}
			if (newGroup)
				freeStorages.push_back(storage->second.get());
		}
	}
	if (freeStorages.size() <  minimumCopies)
		return false;
	std::sort(freeStorages.begin(), freeStorages.end(), StorageNode::balanceStorage); 
	auto storage = freeStorages.begin();
	for (size_t c = 0; c < minimumCopies; c++) {
		storageIDs.push_back((*storage)->id());
		storage++;
	}
	return true;
}

bool ClusterManager::startStoragesPinging(EPollWorkerThread *thread)
{
	_storagesPinging = new StorageCMDPinging(this, thread);
	return _storagesPinging->start();
}

TStorageList ClusterManager::storages()
{
	TStorageList storagesVector;
	for (auto s = _storages.begin(); s != _storages.end(); s++)
		storagesVector.push_back(s->second.get());
	return storagesVector;
}

ClusterManager::ClusterManager()
	: _storagesPinging(NULL)
{
}
