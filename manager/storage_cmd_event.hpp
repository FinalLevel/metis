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
#include "timer_event.hpp"
#include "compatibility.hpp"

namespace fl {
	namespace metis {
		using namespace fl::events;
		using fl::network::NetworkBuffer;
		using fl::fs::File;
		
		const uint8_t MAX_STORAGE_RECONNECTS = 3;
		
		
		typedef std::vector<class StorageCMDEvent*> TStorageCMDEventVector;
		
		class BasicStorageCMD
		{
		public:
			BasicStorageCMD() {}
			virtual ~BasicStorageCMD() {}
			virtual bool getMoreDataToSend(class StorageCMDEvent *ev)
			{
				return false;
			}
			virtual void ready(class StorageCMDEvent *ev, const StorageAnswer &sa) = 0;
			virtual void repeat(class StorageCMDEvent *ev) = 0;
		protected:
		};
		
		class StorageCMDGetInterface 
		{
		public:
			virtual ~StorageCMDGetInterface() {};
			virtual void itemGetChunkReady(class StorageCMDGet *cmd, NetworkBuffer &buffer, const bool isSended) = 0;
			virtual void itemGetChunkError(class StorageCMDGet *cmd, const bool isSended) = 0;
		};
		class StorageCMDGet : public BasicStorageCMD
		{
		public:
			StorageCMDGet(const TStorageList &storages, class StorageCMDEventPool *pool, const ItemHeader &item, 
				const TItemSize chunkSize);
			virtual ~StorageCMDGet();
			bool start(EPollWorkerThread *thread, StorageCMDGetInterface *interface);

			bool canFinish();
			bool getNextChunk(EPollWorkerThread *thread);
			virtual void ready(class StorageCMDEvent *ev, const StorageAnswer &sa);
			virtual void repeat(class StorageCMDEvent *ev);
		private:
			void _fillCMD();
			void _error();
			class StorageCMDEvent *_storageEvent;
			class StorageCMDEventPool *_pool;
			class StorageCMDGetInterface *_interface;
			TStorageList _storages;
			ItemIndex _item;
			TItemSize _itemSize;
			TItemSize _chunkSize;
			TItemSize _remainingSize;
			uint8_t _reconnects;
		};
		
		class StorageCMDPutInterface
		{
		public:
			virtual ~StorageCMDPutInterface() {};
			virtual void itemPut(class StorageCMDPut *cmd, const bool isCompleted) =  0;
		};
		
		class StorageCMDPut : public BasicStorageCMD
		{
		public:
			StorageCMDPut(const ItemHeader &item, class StorageCMDEventPool *pool, File *postTmpFile, BString &putData);
			virtual ~StorageCMDPut();
			bool start(TStorageList &storages, EPollWorkerThread *thread, StorageCMDPutInterface *interface);
			
			virtual bool getMoreDataToSend(class StorageCMDEvent *ev) override;
			virtual void ready(class StorageCMDEvent *ev, const StorageAnswer &sa) override;
			virtual void repeat(class StorageCMDEvent *ev) override;
		private:
			void _error(class StorageCMDEvent *ev);
			void _clearEvents();
			bool _fillCMD(class StorageCMDEvent *storageEvent, TItemSize &seek);
			ItemHeader _item;
			class StorageCMDEventPool *_pool;
			class StorageCMDPutInterface *_interface;
			File *_postTmpFile;
			BString &_putData;
			struct StorageRequest
			{
				StorageRequest(StorageCMDEvent *event, StorageNode *storage, const TItemSize seek)
					: _status(STORAGE_NO_ANSWER), _event(event), _storage(storage), _seek(seek), _reconnects(0)
				{
				}
				StorageRequest(const EStorageAnswerStatus status, StorageNode *storage) 
					: _status(status), _event(NULL), _storage(storage), _seek(0), _reconnects(0)
				{
				}
				EStorageAnswerStatus _status;
				StorageCMDEvent *_event;
				StorageNode *_storage;
				TItemSize _seek;
				uint8_t _reconnects;
			};
			typedef std::vector<StorageRequest> TStorageRequestVector;
			TStorageRequestVector _requests;
		};
		
		class StorageCMDItemInfoInterface 
		{
		public:
			virtual ~StorageCMDItemInfoInterface() {};
			virtual void itemInfo(class StorageCMDItemInfo *cmd) = 0;
		};
		
		class StorageCMDItemInfo : public BasicStorageCMD, TimerEventInterface
		{
		public:
			StorageCMDItemInfo(StorageCMDEventPool *pool, const ItemIndex &item, EPollWorkerThread *thread);
			virtual ~StorageCMDItemInfo();
			bool start(const TStorageList &storages, StorageCMDItemInfoInterface *interface);

			TStorageList getPutStorages(const TSize size, const size_t minimumCopies);
			bool getStoragesAndFillItem(ItemHeader &item, TStorageList &storageNodes);
			
			virtual void ready(class StorageCMDEvent *ev, const StorageAnswer &sa) override;
			virtual void repeat(class StorageCMDEvent *ev) override;
			virtual void timerCall(class TimerEvent *te) override;
		private:
			class StorageCMDEventPool *_pool;
			ItemIndex _item;
			EPollWorkerThread *_thread;
			StorageCMDItemInfoInterface *_interface;
			TimerEvent *_timer;
			void _fillCMD(class StorageCMDEvent *storageEvent);
			void _error(class StorageCMDEvent *ev);
			struct StorageRequest
			{
				StorageRequest(StorageCMDEvent *event, StorageNode *storage)
					: _status(STORAGE_NO_ANSWER), _event(event), _storage(storage), _reconnects(0)
				{
					bzero(&_item, sizeof(_item));
				}
				StorageRequest(const EStorageAnswerStatus status, StorageNode *storage) 
					: _status(status), _event(NULL), _storage(storage), _reconnects(0)
				{
					bzero(&_item, sizeof(_item));
				}
				EStorageAnswerStatus _status;
				GetItemInfoAnswer _item;
				StorageCMDEvent *_event;
				StorageNode *_storage;
				uint8_t _reconnects;
			};
			typedef std::vector<StorageRequest> TStorageRequestVector;
			TStorageRequestVector _requests;
		};
		
		
		class StorageCMDEvent : public Event
		{
		public:
			StorageCMDEvent(StorageNode *storage, EPollWorkerThread *thread, BasicStorageCMD *interface);
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
			void setStorage(StorageNode *storageNode)
			{
				_storage = storageNode;
			}
			NetworkBuffer &networkBuffer()
			{
				return _buffer;
			}
			bool removeFromPoll();
			void set(EPollWorkerThread *thread, BasicStorageCMD *interface)
			{
				_thread = thread;
				_interface = interface;
			}
			void addToDelete();
			bool makeCMD();
			void reopen();
		private:
			friend class EPollWorkerThread;
			friend class StorageCMDEventPool;
			virtual ~StorageCMDEvent();
			bool _send();
			void _error();
			bool _read();
			void _cmdReady(const StorageAnswer &sa, const char *data);
			Socket _socket;
			EPollWorkerThread *_thread;
			BasicStorageCMD *_interface;
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
		};
		
		class StorageCMDEventPool
		{
		public:
			StorageCMDEventPool(const size_t maxConnectionPerStorage);
			StorageCMDEvent *get(StorageNode *storageNode, EPollWorkerThread *thread, BasicStorageCMD *interface);
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
