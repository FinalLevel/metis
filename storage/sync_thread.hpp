#pragma once
#ifndef __FL_METIS_STORAGE_SYNC_THREAD_HPP
#define	__FL_METIS_STORAGE_SYNC_THREAD_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis' storage Synchronization thread
///////////////////////////////////////////////////////////////////////////////

#include <list>
#include "../types.hpp"
#include "thread.hpp"
#include "mutex.hpp"
#include "socket.hpp"
#include "buffer.hpp"
#include "config.hpp"

namespace fl {
	namespace metis {
		using fl::threads::Thread;
		using fl::threads::Mutex;
		using fl::threads::AutoMutex;
		using fl::network::Socket;
		using fl::utils::Buffer;
		
		class SyncThread : public Thread
		{
		public:
			SyncThread(class Storage *storage, Config *config);
			virtual ~SyncThread();
			bool add(const TServerID managerID, const TRangeID rangeID, TIndexSyncEntryVector &syncs);
			bool checkActive(const TRangeID rangeID);
		private:
			bool _syncItem(Socket &conn, const ItemHeader &header, Buffer &buffer);
			virtual void run();
			class Storage *_storage;
			Config *_config;
			struct SyncTaskGroup
			{
				SyncTaskGroup()
					: managerID(0), rangeID(0)
				{
				}
				SyncTaskGroup(const TServerID managerID, const TRangeID rangeID, TIndexSyncEntryVector &syncs)
					: managerID(managerID), rangeID(rangeID), syncs(std::move(syncs))
				{
				}
				TServerID managerID;
				TRangeID rangeID;
				TIndexSyncEntryVector syncs;
			};
			typedef std::list<SyncTaskGroup> TSyncTaskGroupList;
			TSyncTaskGroupList _tasks;
			SyncTaskGroup _currentTask;
			Mutex _sync;
			
			bool _checkActive(const TRangeID rangeID);
		};
	};
};

#endif	// __FL_METIS_STORAGE_SYNC_THREAD_HPP
