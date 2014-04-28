#pragma once
#ifndef __FL_MANAGER_STORAGE_CMD_EVENT_HPP
#define	__FL_MANAGER_STORAGE_CMD_EVENT_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis storage communication event class
///////////////////////////////////////////////////////////////////////////////
#include <map>
#include "event_queue.hpp"
#include "types.hpp"
#include "cluster_manager.hpp"
#include "network_buffer.hpp"
#include "event_thread.hpp"

namespace fl {
	namespace metis {
		using namespace fl::events;
		using fl::network::NetworkBuffer;
		
		namespace EStorageResult
		{
			enum EStorageResult : uint8_t
			{
				ERROR,
			};
		}
		
		class StorageCMDEventInterface
		{
		public:
			virtual bool result(const EStorageResult::EStorageResult res, class StorageCMDEvent *storageEvent) = 0; 
		};
		
		class StorageCMDEvent : public Event
		{
		public:
			StorageCMDEvent(StorageNode *storage, EPollWorkerThread *thread);
			virtual ~StorageCMDEvent();
			bool setCMD(const EStorageCMD cmd, StorageCMDEventInterface *interface, ItemHeader &item);
			virtual const ECallResult call(const TEvents events);
			EPollWorkerThread *thread()
			{
				return _thread;
			}
			bool isNormalState()
			{
				return _state == WAIT_CONNECTION;
			}
			StorageNode *storage()
			{
				return _storage;
			}
			bool removeFromPoll();
		private:
			bool _makeCMD();
			bool _send();
			Socket _socket;
			EPollWorkerThread *_thread;
			StorageCMDEventInterface *_interface;
			NetworkBuffer _buffer;
			StorageNode *_storage;
			enum EState : uint8_t
			{
				WAIT_CONNECTION,
				SEND_REQUEST,
				WAIT_ANSWER,
			};
			EState _state;
			EStorageCMD _cmd;
		};
		
		typedef std::vector<StorageCMDEvent*> TStorageCMDEventVector;
		class StorageCMDEventPool
		{
		public:
			StorageCMDEventPool(const size_t maxConnectionPerStorage);
			bool get(const TStorageList &storages, TStorageCMDEventVector &storageEvents, EPollWorkerThread *thread);
			void free(StorageCMDEvent *se);
			~StorageCMDEventPool();
		private:
			size_t _maxConnectionPerStorage;
			typedef std::map<TServerID, TStorageCMDEventVector> TStorageCMDEventMap;
			TStorageCMDEventMap _freeEvents;
		};
	};
};

#endif	// __FL_MANAGER_STORAGE_CMD_EVENT_HPP
