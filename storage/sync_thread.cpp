///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis' storage Synchronization thread implementation
///////////////////////////////////////////////////////////////////////////////


#include "sync_thread.hpp"
#include "../metis_log.hpp"
#include "storage.hpp"


using namespace fl::metis;
using namespace fl::network;

SyncThread::SyncThread(class Storage *storage, Config *config)
	: _storage(storage), _config(config)
{
	static const uint32_t SYNC_THREAD_STACK_SIZE = 100000;
	setStackSize(SYNC_THREAD_STACK_SIZE);
	if (!create()) {
		log::Fatal::L("Can't create an index thread\n");
		throw std::exception();
	}
}

SyncThread::~SyncThread()
{
}

bool SyncThread::checkActive(const TRangeID rangeID)
{
	AutoMutex autoSync(&_sync);
	return _checkActive(rangeID);
}

bool SyncThread::_checkActive(const TRangeID rangeID)
{
	if (_currentTask.rangeID == rangeID) {
		return false;
	}
	for (auto task = _tasks.begin(); task != _tasks.end(); task++) {
		if (task->rangeID == rangeID) {
			return false;
		}
	}
	return true;
}

bool SyncThread::add(const TServerID managerID, const TRangeID rangeID, TIndexSyncEntryVector &syncs)
{
	AutoMutex autoSync(&_sync);
	if (!_checkActive(rangeID)) {
		log::Error::L("Sync thread already has task on range %u, from manager %u\n", rangeID, managerID);
		return false;
	}
	_tasks.push_back(SyncTaskGroup(managerID, rangeID, syncs));
	return true;
}

bool SyncThread::_syncItem(Socket &conn, const ItemHeader &item, Buffer &fileBuffer)
{
	GetItemChunkRequest getRequest;
	getRequest.rangeID = item.rangeID;
	getRequest.itemKey = item.itemKey;
	getRequest.chunkSize = _config->maxMemmoryChunk();
	Buffer request(sizeof(getRequest) + sizeof(StorageCmd) + 1);
	fileBuffer.clear();
	TItemSize leftSize = item.size;
	while (leftSize > 0) {	
		if (getRequest.chunkSize > leftSize)
			getRequest.chunkSize = leftSize;
		getRequest.seek = item.size - leftSize;

		request.clear();
		StorageCmd &storageCmd = *(StorageCmd*)request.reserveBuffer(sizeof(StorageCmd));
		storageCmd.cmd = EStorageCMD::STORAGE_GET_ITEM_CHUNK;
		storageCmd.size = sizeof(getRequest);
		request.add(&getRequest, sizeof(getRequest));
		if (!conn.pollAndSendAll(request.begin(), request.writtenSize())) {
			log::Warning::L("SyncThread: Can't send\n");
			return false;
		}
		request.clear();
		StorageAnswer &answer = *(StorageAnswer*)request.reserveBuffer(sizeof(StorageAnswer));
		if (!conn.pollAndRecvAll(&answer, sizeof(answer))) {
			log::Warning::L("SyncThread: Can't read answer\n");
			return false;
		}
		if ((answer.status != EStorageAnswerStatus::STORAGE_ANSWER_OK) || (answer.size != getRequest.chunkSize)) {
			log::Warning::L("SyncThread: Receive bad answer status %u, size %u\n", answer.status, answer.size);
			return false;
		}
		if (!conn.pollAndRecvAll(fileBuffer.reserveBuffer(getRequest.chunkSize), getRequest.chunkSize)) {
			log::Warning::L("SyncThread: Can't read data\n");
			return false;
		}
		leftSize -= getRequest.chunkSize;
	}
	return _storage->add((char*)fileBuffer.begin(), item);
}

void SyncThread::run()
{
	log::Warning::L("Storage %u SyncThread has been started\n", _config->serverID());
	while (true) {
		_sync.lock();
		if (_tasks.empty()) {
			_sync.unLock();
			sleep(1);
			continue;
		}
		std::swap(_currentTask, _tasks.front());
		_tasks.pop_front();
		_sync.unLock();
		Socket conn;
		TServerID lastServerID = 0;
		Buffer buffer;
		for (auto sync = _currentTask.syncs.begin(); sync != _currentTask.syncs.end(); sync++) {
			if (sync->fromServer != lastServerID) {
				lastServerID = 0;
				conn.reopen();
				if (!conn.connect(sync->ip, sync->port)) {
					log::Warning::L("SyncThread: Can't connect to %s:%u (%u)\n", Socket::ip2String(sync->ip).c_str(), sync->port,
						sync->fromServer);
					continue;
				}
				lastServerID = sync->fromServer;
			}
			if (_syncItem(conn, sync->header, buffer)) {
				log::Warning::L("SyncThread: Sync %u/%u from %s:%u (%u)\n", sync->header.itemKey, sync->header.rangeID, 
					Socket::ip2String(sync->ip).c_str(), sync->port, sync->fromServer);
			} else {
				log::Warning::L("SyncThread: Can't sync %u/%u from %s:%u (%u)\n", sync->header.itemKey, sync->header.rangeID, 
					Socket::ip2String(sync->ip).c_str(), sync->port, sync->fromServer);
				lastServerID = 0;
				continue;
			}
		}
		_currentTask.managerID = 0;
		_currentTask.rangeID = 0;
		_currentTask.syncs.clear();
	}
}
