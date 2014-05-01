#pragma once
#ifndef __FL_MANAGER_STORAGE_CMD_EVENT_HPP
#define __FL_MANAGER_STORAGE_CMD_EVENT_HPP

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
#include "file.hpp"

namespace fl {
	namespace metis {
		using namespace fl::events;
		using fl::network::NetworkBuffer;
		using fl::fs::File;
		
		class StorageCMDEventInterface
		{
		public:
			virtual void itemInfo(const EStorageAnswerStatus res, class StorageCMDEvent *storageEvent, 
				const ItemHeader *item)
			{
			}
			virtual void itemPut(const EStorageAnswerStatus res, class StorageCMDEvent *storageEvent)
			{
			}
			virtual bool getMorePutData(class StorageCMDEvent *storageEvent, NetworkBuffer &buffer)
			{
				return false;
			}
		};
		
		typedef std::vector<class StorageCMDEvent*> TStorageCMDEventVector;
		
		class BasicStorageCMD
		{
		public:
			BasicStorageCMD() {}
			virtual ~BasicStorageCMD() {}
		protected:
		};
		
		class StorageCMDGet : public BasicStorageCMD
		{
		public:
			StorageCMDGet(class StorageCMDEvent *storageEvent, const TItemSize size, class StorageCMDEventPool *pool);
			virtual ~StorageCMDGet();
		private:
			class StorageCMDEvent *_storageEvent;
			class StorageCMDEventPool *_pool;
			TItemSize _leftSize;
			TItemSize _curSeek;
		};
		
		class StorageCMDPut : public BasicStorageCMD
		{
		public:
			StorageCMDPut(class StorageCMDEvent *storageEvent, class StorageCMDEventPool *pool, File *postTmpFile, 
				const TSize size);
			virtual ~StorageCMDPut();
			bool getMoreData(class StorageCMDEvent *storageEvent, NetworkBuffer &buffer);
			const TSize size() const
			{
				return _size;
			}
		private:
			class StorageCMDEvent *_storageEvent;
			class StorageCMDEventPool *_pool;
			File *_postTmpFile;
			TSize _size;
		};
		
		class StorageCMDItemInfo : public BasicStorageCMD
		{
		public:
			StorageCMDItemInfo(const TStorageCMDEventVector &storageCMDEvents, class StorageCMDEventPool *pool);
			virtual ~StorageCMDItemInfo();
			bool addAnswer(const EStorageAnswerStatus res, class StorageCMDEvent *storageEvent, 
	const ItemHeader *item);
			StorageNode *getPutStorage(const TSize size);
			StorageCMDEventPool &pool()
			{
				return *_pool;
			}
			bool getStoragesAndFillItem(ItemHeader &item, TStoragePtrList &storageNodes);
		private:
			class StorageCMDEventPool *_pool;
			void _clearEvents();
			bool _isComplete();
			struct Answer
			{
				Answer(StorageCMDEvent *event, StorageNode *storage)
					: _status(STORAGE_ANSWER_OK), _event(event), _storage(storage)
				{
					bzero(&_header, sizeof(_header));
				}
				EStorageAnswerStatus _status;
				ItemHeader _header;
				StorageCMDEvent *_event;
				StorageNode *_storage;
			};
			typedef std::vector<Answer> TAnswerVector;
			TAnswerVector _answers;
		};
		
		
		class StorageCMDEvent : public Event
		{
		public:
			StorageCMDEvent(StorageNode *storage, EPollWorkerThread *thread, StorageCMDEventInterface *interface);
			bool setCMD(const EStorageCMD cmd, const ItemHeader &item);
			bool setPutCMD(const ItemHeader &item, File *postTmpFile, BString &putData, TSize &leftSize);
			virtual const ECallResult call(const TEvents events);
			EPollWorkerThread *thread()
			{
				return _thread;
			}
			bool isCompletedState()
			{
				return _state == COMPLETED;
			}
			void setWaitState()
			{
				_state = WAIT_CONNECTION;
			}
			StorageNode *storage()
			{
				return _storage;
			}
			bool removeFromPoll();
			void set(EPollWorkerThread *thread, StorageCMDEventInterface *interface)
			{
				_thread = thread;
				_interface = interface;
			}
			void addToDelete();
		private:
			friend class EPollWorkerThread;
			friend class StorageCMDEventPool;
			virtual ~StorageCMDEvent();
			bool _makeCMD();
			bool _send();
			void _error();
			bool _read();
			void _cmdReady(const StorageAnswer &sa, const char *data);
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
				ERROR,
				COMPLETED,
			};
			EState _state;
			EStorageCMD _cmd;
		};
		
		class StorageCMDEventPool
		{
		public:
			StorageCMDEventPool(const size_t maxConnectionPerStorage);
			bool get(const TStorageList &storages, TStorageCMDEventVector &storageEvents, EPollWorkerThread *thread, 
				StorageCMDEventInterface *interface);
			StorageCMDEvent *get(StorageNode *storageNode, EPollWorkerThread *thread, StorageCMDEventInterface *interface);
			void free(StorageCMDEvent *se);
			
			StorageCMDItemInfo *mkStorageItemInfo(const TStorageList &storages, EPollWorkerThread *thread, 
				StorageCMDEventInterface *interface, const ItemHeader &item);
			
			StorageCMDPut *mkStorageCMDPut(const ItemHeader &item, StorageNode *storageNode, EPollWorkerThread *thread, 
				StorageCMDEventInterface *interface, File *postTmpFile, BString &putData, const TSize size);
			
			StorageCMDGet *mkStorageCMDGet(const ItemHeader &item, StorageNode *storageNode, EPollWorkerThread *thread, 
				StorageCMDEventInterface *interface, BString &networkBuffer);
			~StorageCMDEventPool();
		private:
			size_t _maxConnectionPerStorage;
			typedef std::map<TServerID, TStorageCMDEventVector> TStorageCMDEventMap;
			TStorageCMDEventMap _freeEvents;
		};
	};
};

#endif	// __FL_MANAGER_STORAGE_CMD_EVENT_HPP
