#pragma once
#ifndef __FL_MANAGER_CLUSTER_MANAGER_HPP
#define	__FL_MANAGER_CLUSTER_MANAGER_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis cluster control class implementation
///////////////////////////////////////////////////////////////////////////////

#include "../types.hpp"
#include "socket.hpp"
#include "config.hpp"
#include "mysql.hpp"
#include "mutex.hpp"
#include <map>

namespace fl {
	namespace metis {
		using fl::network::TIPv4;
		using fl::db::MysqlResult;
		using fl::threads::Mutex;
		using fl::threads::AutoMutex;
		
		class ManagerNode
		{
		public:
			ManagerNode(MysqlResult *res);
			const TServerID id() const
			{
				return _id;
			}
			const bool isFree() const
			{
				return true;
			}	
		private:
			TServerID _id;
			TIPv4 _ip;
			uint32_t _port;
			TManagerStatus _status;
		};
		
		class StorageNode
		{
		public:
			StorageNode(MysqlResult *res);
			
			const TServerID id() const
			{
				return _id;
			}
			const TStorageGroupID groupID() const
			{
				return _groupID;
			}
			const bool isFree() const
			{
				return true;
			}
			const TIPv4 ip() const
			{
				return _ip;
			}
			const uint32_t port() const
			{
				return _port;
			}
			static bool balanceStorage(StorageNode *a, StorageNode *b);
			bool canPut(const TSize size)
			{
				return true;
			}
		private:
			TServerID _id;
			TStorageGroupID _groupID;
			TIPv4 _ip;
			uint32_t _port;
			TStorageStatus _status;
			uint32_t _weight;
		};
		
		typedef std::vector<TServerID> TServerIDList;
		typedef std::vector<StorageNode*> TStorageList;
		typedef std::vector<ManagerNode*> TManagerList;
		
		class ClusterManager
		{
		public:
			bool loadAll(Mysql &sql);
			bool findFreeStorages(const size_t minimumCopies, TServerIDList &storageIDs);
			TServerID findFreeManager();
			void findStorages(TServerIDList &storageIds, TStorageList &storages);
			void pingStorages();
		private:
			bool _loadManagers(Mysql &sql);
			bool _loadStorages(Mysql &sql);
			
			typedef std::shared_ptr<ManagerNode> TManagerNodePtr;
			typedef std::map<TServerID, TManagerNodePtr> TManagerNodeMap;
			TManagerNodeMap _managers;
			
			typedef std::shared_ptr<StorageNode> TStorageNodePtr;
			typedef std::map<TServerID, TStorageNodePtr> TStorageNodeMap;
			TStorageNodeMap _storages;
			
			Mutex _sync;
		};
	};
};

#endif	// __FL_MANAGER_CLUSTER_MANAGER_HPP
