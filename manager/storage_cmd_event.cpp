///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis storage communication event class
///////////////////////////////////////////////////////////////////////////////

#include "storage_cmd_event.hpp"
#include "network_buffer.hpp"
#include "metis_log.hpp"
#include "webdav_interface.hpp"
#include "buffer.hpp"
#include "manager.hpp"

using namespace fl::metis;
using fl::utils::Buffer;

StorageCMDSync::StorageCMDSync(class StorageCMDRangeIndexCheck *parent, EPollWorkerThread *thread)
	: _parent(parent), _thread(thread), _operationTimer(new TimerEvent())
{
}

StorageCMDSync::~StorageCMDSync()
{
	delete _operationTimer;
	_clearEvents();
}

void StorageCMDSync::_clearEvents()
{
	for (auto a = _requests.begin(); a != _requests.end(); a++) {
		if (!a->_event)
			continue;
		_thread->addToDeletedNL(a->_event);
		a->_event = NULL;
	}
}

void StorageCMDSync::_fillCMD(StorageCMDEvent *ev, TIndexSyncEntryVector &syncs)
{
	NetworkBuffer &buffer = ev->networkBuffer();
	buffer.clear();
	StorageCmd &storageCmd = *(StorageCmd*)buffer.reserveBuffer(sizeof(StorageCmd));
	storageCmd.cmd = EStorageCMD::STORAGE_SYNC;
	RangeSyncHeader rangeHeader;
	rangeHeader.count = syncs.size();
	rangeHeader.managerID = _parent->managerID();
	rangeHeader.rangeID = _parent->rangeID();
	storageCmd.size = sizeof(rangeHeader) + (sizeof(RangeSyncEntry) * rangeHeader.count);
	buffer.add((char*)&rangeHeader, sizeof(rangeHeader));
	for (auto sync = syncs.begin(); sync != syncs.end(); sync++)
		buffer.add((char*)&*sync, sizeof(RangeSyncEntry));
}

bool StorageCMDSync::start(TStorageSyncMap &syncs)
{
	bool haveActiveRequests = false;
	for (auto sync = syncs.begin(); sync != syncs.end(); sync++) {
		StorageCMDEvent* storageEvent = new StorageCMDEvent(sync->first, _thread, this);
		if (storageEvent) {
			_fillCMD(storageEvent, sync->second);
			if (storageEvent->makeCMD()) {
				haveActiveRequests = true;
				_requests.push_back(StorageRequest(storageEvent, sync->first, EStorageAnswerStatus::STORAGE_NO_ANSWER, 
					sync->second));
				continue;
			} else {
				_thread->addToDeletedNL(storageEvent);
			}
		}
		_requests.push_back(StorageRequest(NULL, sync->first, EStorageAnswerStatus::STORAGE_ANSWER_ERROR, sync->second));
	}
	if (haveActiveRequests) {
		static const uint32_t SYNC_WAIT_TIME = 60; // 1 minute;	
		if (!_operationTimer->setTimer(SYNC_WAIT_TIME, 0, 0, 0, this))
			return false;
		if (!_thread->ctrl(_operationTimer)) {
			log::Error::L("StorageCMDSync: Can't add a timer event to the pool\n");
			return false;
		}
		return true;
	} else {
		return false;
	}
}

void StorageCMDSync::timerCall(class TimerEvent *te)
{
	te->stop();
	bool allOkey = true;
	for (auto r = _requests.begin(); r != _requests.end(); r++) {
		if (r->_status != EStorageAnswerStatus::STORAGE_ANSWER_OK)
			allOkey = false;
	}
	_parent->syncFinished(allOkey);
}

void StorageCMDSync::ready(class StorageCMDEvent *ev, const StorageAnswer &sa)
{
	bool completed = true;
	for (auto r = _requests.begin(); r != _requests.end(); r++) {
		if (r->_event == ev) {
			r->_status = sa.status;
			_thread->addToDeletedNL(r->_event);
			r->_event = NULL;
		}
		if (r->_status == EStorageAnswerStatus::STORAGE_NO_ANSWER)
			completed = false;
	}
	if (completed)
		timerCall(_operationTimer);
}

void StorageCMDSync::repeat(class StorageCMDEvent *ev)
{
	bool completed = true;
	for (auto r = _requests.begin(); r != _requests.end(); r++) {
		if (r->_event == ev) {
			if (r->_reconnects < MAX_STORAGE_RECONNECTS) {
				r->_reconnects++;
				ev->reopen();
				_fillCMD(ev, r->_syncs);
				if (ev->makeCMD())
					return;
			}
			r->_status = EStorageAnswerStatus::STORAGE_ANSWER_ERROR;
			_thread->addToDeletedNL(r->_event);
			r->_event = NULL;
		}
		if (r->_status == EStorageAnswerStatus::STORAGE_NO_ANSWER)
			completed = false;
	}
	if (completed)
		timerCall(_operationTimer);
}

StorageCMDRangeIndexCheck::StorageCMDRangeIndexCheck(Manager *manager, EPollWorkerThread *thread)
	: _manager(manager), _thread(thread), _operationTimer(new TimerEvent()), _recheckTimer(new TimerEvent()),
		_storageCMDSync(NULL)
{
}

StorageCMDRangeIndexCheck::~StorageCMDRangeIndexCheck()
{
}

void StorageCMDRangeIndexCheck::_fillCMD(StorageCMDEvent *ev)
{
	NetworkBuffer &buffer = ev->networkBuffer();
	buffer.clear();
	StorageCmd &storageCmd = *(StorageCmd*)buffer.reserveBuffer(sizeof(StorageCmd));
	storageCmd.cmd = EStorageCMD::STORAGE_GET_RANGE_ITEMS;
	RangeItemsRequest rangeItemRequest;
	rangeItemRequest.serverID = ev->storage()->id();
	rangeItemRequest.rangeID = _currentRange->rangeID();
	storageCmd.size = sizeof(rangeItemRequest);
	buffer.add((char*)&rangeItemRequest, sizeof(rangeItemRequest));
}

bool StorageCMDRangeIndexCheck::_parse(class StorageCMDEvent *ev)
{
	NetworkBuffer &data = ev->networkBuffer();
	Buffer dataBuffer(std::move(data));
	try
	{
		dataBuffer.skip(sizeof(StorageAnswer));
		RangeItemsHeader header;
		dataBuffer.get(&header, sizeof(header));
		if (header.rangeID != _currentRange->rangeID()) {
			log::Error::L("Receive a bad rangeID %u, wait for %u\n", header.rangeID, _currentRange->rangeID());
			return false;
		}
		RangeItemEntry ie;
		TItemEntryVector emptyVector;
		for (decltype(header.count) i = 0; i < header.count; i++) {
			dataBuffer.get(&ie, sizeof(ie));
			auto itemKey = ie.itemKey;
			auto res = _items.insert(TItemEntryMap::value_type(itemKey, emptyVector));
			res.first->second.push_back(ItemEntry(ev->storage(), ie.size, ie.timeTag));
		}
		return true;
	}
	catch (Buffer::Error &er)
	{
		log::Error::L("Receive a bad storage range items answer\n");
	}
	return false;
}

void StorageCMDRangeIndexCheck::ready(class StorageCMDEvent *ev, const StorageAnswer &sa)
{
	bool completed = true;
	for (auto r = _requests.begin(); r != _requests.end(); r++) {
		if (r->_event == ev) {
			if (sa.status == EStorageAnswerStatus::STORAGE_ANSWER_OK) {
				if (_parse(ev))
					r->_status = sa.status;
				else
					r->_status = EStorageAnswerStatus::STORAGE_ANSWER_ERROR;
			}
			else
				r->_status = sa.status;
			_thread->addToDeletedNL(r->_event);
			r->_event = NULL;
		}
		if (r->_status == EStorageAnswerStatus::STORAGE_NO_ANSWER)
			completed = false;
	}
	if (completed)
		timerCall(_operationTimer);
}

void StorageCMDRangeIndexCheck::repeat(class StorageCMDEvent *ev)
{
	bool completed = true;
	for (auto r = _requests.begin(); r != _requests.end(); r++) {
		if (r->_event == ev) {
			if (r->_reconnects < MAX_STORAGE_RECONNECTS) {
				r->_reconnects++;
				ev->reopen();
				_fillCMD(ev);
				if (ev->makeCMD())
					return;
			}
			r->_status = EStorageAnswerStatus::STORAGE_ANSWER_ERROR;
			_thread->addToDeletedNL(r->_event);
			r->_event = NULL;
		}
		if (r->_status == EStorageAnswerStatus::STORAGE_NO_ANSWER)
			completed = false;
	}
	if (completed)
		timerCall(_operationTimer);
}


void StorageCMDRangeIndexCheck::_checkItems()
{
	if (_storageCMDSync) {
		log::Error::L("_checkItems has been received not null _storageCMDSync\n");
		return;
	}
	log::Warning::L("Check range %u (items: %u, servers: %u)\n", _currentRange->rangeID(), _items.size(), 
		_requests.size());
	
	RangeSyncEntry syncEntry;
	bzero(&syncEntry, sizeof(syncEntry));
	syncEntry.header.rangeID = _currentRange->rangeID();
	syncEntry.header.level = _currentRange->level();
	syncEntry.header.subLevel = _currentRange->subLevel();
		
	static TIndexSyncEntryVector emptySyncVector;
	TStorageSyncMap syncs;

	for (auto item = _items.begin(); item != _items.end(); item++) {
		TSize size = 0;
		ModTimeTag timeTag;
		timeTag.tag = 0;
		TStorageList storages;
		for (auto itemEntry = item->second.begin();  itemEntry != item->second.end();  itemEntry++) {
			if (itemEntry->timeTag.tag > timeTag.tag) {
				timeTag = itemEntry->timeTag;
				size = itemEntry->size;
				storages.clear();
				if (size > 0) {
					storages.push_back(itemEntry->storage);
				}
			} else if (itemEntry->timeTag.tag == timeTag.tag) {
				if (size != itemEntry->size) {
					log::Error::L("Item %u/%u has different size on storage %u\n", item->first, _currentRange->rangeID(), 
						itemEntry->storage->id());
				} else {
					storages.push_back(itemEntry->storage);
				}
			}
		}
		syncEntry.header.itemKey = item->first;
	
		if (storages.size() != item->second.size()) { // need clear old or repeat delete command
			for (auto itemEntry = item->second.begin();  itemEntry != item->second.end();  itemEntry++) {
				if (itemEntry->timeTag.tag < timeTag.tag) {
					syncEntry.header.size = 0;
					syncEntry.fromServer = 0;
					if (size == 0) { // need delete
						log::Info::L("Delete Item %u/%u from %u\n", item->first, _currentRange->rangeID(), 
							itemEntry->storage->id());
						
						syncEntry.header.timeTag = timeTag;
					} else { // need update
						syncEntry.header.timeTag = itemEntry->timeTag;
						syncEntry.header.timeTag.op++;
						if (itemEntry->timeTag.tag >= timeTag.tag)
							continue;
						log::Info::L("Delete old item %u/%u from %u\n", item->first, _currentRange->rangeID(), 
							itemEntry->storage->id());
					}
					auto res = syncs.insert(TStorageSyncMap::value_type(itemEntry->storage, emptySyncVector));
					res.first->second.push_back(syncEntry);
				}
			}
		}
		if (storages.empty()) {
			log::Info::L("Item %u/%u doesn't have active copies\n", item->first, _currentRange->rangeID());
			continue;
		}
			
		if ((size > 0) && (storages.size() < _manager->config()->minimumCopies())) { // Item is not deleted and need more copies
			log::Info::L("Item %u/%u has only %u copies, but needs %u\n", item->first, _currentRange->rangeID(), 
				storages.size(), _manager->config()->minimumCopies());
			StorageNode *copyStorage = _manager->getStorageForCopy(_currentRange->rangeID(), size, storages);
			if (copyStorage) {
				StorageNode *fromStorage =  storages[rand() % storages.size()];
				syncEntry.fromServer = fromStorage->id();
				syncEntry.ip = fromStorage->ip();
				syncEntry.port = fromStorage->port();
				
				log::Info::L("Item %u/%u has found storage %u for copying from %u\n", item->first, _currentRange->rangeID(), 
					copyStorage->id(), syncEntry.fromServer);
				
				syncEntry.header.size = size;
				syncEntry.header.timeTag = timeTag;
				auto res = syncs.insert(TStorageSyncMap::value_type(copyStorage, emptySyncVector));
				res.first->second.push_back(syncEntry);
			} else {
				log::Error::L("Item %u/%u can't find storage for copying\n", item->first, _currentRange->rangeID());
			}
		}
	}
	if (!syncs.empty()) {
		_storageCMDSync = new StorageCMDSync(this, _thread);
		if (!_storageCMDSync->start(syncs)) {
			log::Error::L("Range %u couldn't start sync process\n", _currentRange->rangeID());
			delete _storageCMDSync;
			_storageCMDSync = NULL;
		}
	}
}

void StorageCMDRangeIndexCheck::_checkRange()
{
	bool isReadyToReplication = true;
	for (auto r = _requests.begin(); r != _requests.end(); r++) {
		if (r->_event) {
			_thread->addToDeletedNL(r->_event);
			r->_event = NULL;
		}
		if (r->_storage->isActive() && (r->_status != EStorageAnswerStatus::STORAGE_ANSWER_OK) && 
			(r->_status != EStorageAnswerStatus::STORAGE_ANSWER_NOT_FOUND)) {
			isReadyToReplication = false;
		}
	}
	if (isReadyToReplication) {
		_checkItems();
	}
	else {
		log::Warning::L("Range %u not ready to replication\n", _currentRange->rangeID());
	}
	_requests.clear();
	_items.clear();
}

void StorageCMDRangeIndexCheck::timerCall(class TimerEvent *te)
{
	if (te == _operationTimer) {
		if (te->descr() == INVALID_EVENT) {
			log::Warning::L("Received timer call from stopped _operationTimer\n");
			return;
		}
		te->stop();
		_checkRange();
		if (_ranges.empty()) { // ranges check finished
			while (!_setRecheckTimer()) {
				log::Fatal::L("Can't reset recheck timer\n");
				sleep(1);
			}
			return;
		}
	} else if (te == _recheckTimer) {
		if (te->descr() == INVALID_EVENT) {		
			log::Warning::L("Received timer call from stopped _recheckTimer\n");
			return;
		}
		_operationTimer->stop();
		_recheckTimer->stop();
	} else {
		log::Error::L("Received an unknown timer call\n");
		return;
	}
	while (!start()) {
		log::Fatal::L("Can't restart StorageCMDRangeIndexCheck\n");
		sleep(1);
	}
}

void StorageCMDRangeIndexCheck::syncFinished(const bool status)
{
	log::Error::L("Range %u has %s finished sync process\n", _currentRange->rangeID(), 
		status ? "successfully" : "unsuccessfully");
	delete _storageCMDSync;
	_storageCMDSync = NULL;
}

TRangeID StorageCMDRangeIndexCheck::rangeID() const
{
	return _currentRange->rangeID();
}

TServerID StorageCMDRangeIndexCheck::managerID() const
{
	return _manager->config()->serverID();
}

bool StorageCMDRangeIndexCheck::_setRecheckTimer()
{
	static const uint32_t STORAGES_RECHECK_TIME = 60 * 60; // 1 hour;	
	if (!_recheckTimer->setTimer(STORAGES_RECHECK_TIME, 0, 0, 0, this))
		return false;
	if (!_thread->ctrl(_recheckTimer)) {
		log::Error::L("StorageCMDRangeIndexCheck: Can't add a timer event to the pool\n");
		return false;
	}
	return true;	
}

bool StorageCMDRangeIndexCheck::start()
{
	if (!_manager->clusterManager().isReady()) {
		static const uint32_t STORAGES_INIT_WAIT_TIME_SEC_TIME = 1; // 1 second
		if (!_recheckTimer->setTimer(STORAGES_INIT_WAIT_TIME_SEC_TIME, 0, 0, 0, this))
			return false;
		if (!_thread->ctrl(_recheckTimer)) {
			log::Error::L("StorageCMDRangeIndexCheck: Can't add a timer event to the pool\n");
			return false;
		}
		return true;
	}
	if (_storageCMDSync) { // wait until last sync commands will be finished
		static const uint32_t STORAGES_SYNC_WAIT_TIME_SEC_TIME = 5; // 5 second
		if (!_recheckTimer->setTimer(STORAGES_SYNC_WAIT_TIME_SEC_TIME, 0, 0, 0, this))
			return false;
		if (!_thread->ctrl(_recheckTimer)) {
			log::Error::L("StorageCMDRangeIndexCheck: Can't add a timer event to the pool\n");
			return false;
		}
		return true;
	}
	if (_ranges.empty()) { // start new range check
		_ranges = _manager->index().getControlledRanges();
		if (_ranges.empty())
			return _setRecheckTimer();
	}
	_currentRange = _ranges.back();
	_ranges.pop_back();
	auto storages = _currentRange->storages();
	for (auto storage = storages.begin(); storage != storages.end(); storage++) {
		StorageCMDEvent *storageEvent = new StorageCMDEvent(*storage, _thread, this);
		_fillCMD(storageEvent);
		EStorageAnswerStatus status =  EStorageAnswerStatus::STORAGE_ANSWER_ERROR;
		if (storageEvent->makeCMD()) {
			status = EStorageAnswerStatus::STORAGE_NO_ANSWER;
		} else {
			_thread->addToDeletedNL(storageEvent);
			storageEvent = NULL;
		}
		_requests.push_back(StorageRequest(storageEvent, *storage, status));
	}
	
	static const uint32_t STORAGES_MAXIMUM_WAIT_TIME_SEC_TIME = 5; // 5 seconds	
	if (!_operationTimer->setTimer(STORAGES_MAXIMUM_WAIT_TIME_SEC_TIME, 0, 0, 0, this))
		return false;
	if (!_thread->ctrl(_operationTimer)) {
		log::Error::L("StorageCMDRangeIndexCheck: Can't add a timer event to the pool\n");
		return false;
	}
	return true;
}

StorageCMDPinging::StorageCMDPinging(ClusterManager *manager, EPollWorkerThread *thread)
	: _manager(manager), _thread(thread), _timer(NULL)
{
}

StorageCMDPinging::~StorageCMDPinging()
{
}


void StorageCMDPinging::_fillCMD(StorageCMDEvent *ev)
{
	NetworkBuffer &buffer = ev->networkBuffer();
	buffer.clear();
	StorageCmd &storageCmd = *(StorageCmd*)buffer.reserveBuffer(sizeof(StorageCmd));
	storageCmd.cmd = EStorageCMD::STORAGE_PING;
	TServerID serverID = ev->storage()->id();
	storageCmd.size = sizeof(serverID);
	buffer.add((char*)&serverID, sizeof(serverID));
}

void StorageCMDPinging::ready(class StorageCMDEvent *ev, const StorageAnswer &sa)
{
	for (auto r = _requests.begin(); r != _requests.end(); r++) {
		if (r->_event == ev) {
			if (sa.status == EStorageAnswerStatus::STORAGE_ANSWER_OK) {
				NetworkBuffer &data = ev->networkBuffer();
				if ((size_t)data.size() >= (sizeof(StorageAnswer) + sizeof(StoragePingAnswer))) {
					r->_status = sa.status;
					StoragePingAnswer storageAnswer;
					memcpy(&storageAnswer, data.c_str() + sizeof(StorageAnswer), sizeof(storageAnswer));
					ev->storage()->ping(storageAnswer);
				} else {
					log::Error::L("Receive a bad storage ping answer - the sizes are mismatch\n");
					ev->storage()->error();
				}
			}
			else
				ev->storage()->error();
			r->_status = sa.status;
			_thread->addToDeletedNL(r->_event);
			r->_event = NULL;
			return;
		}
	}
}

void StorageCMDPinging::repeat(class StorageCMDEvent *ev)
{
	for (auto r = _requests.begin(); r != _requests.end(); r++) {
		if (r->_event == ev) {
			ev->storage()->error();
			r->_status = EStorageAnswerStatus::STORAGE_ANSWER_ERROR;
			_thread->addToDeletedNL(r->_event);
			r->_event = NULL;
			return;
		}
	}
}

void StorageCMDPinging::timerCall(class TimerEvent *te)
{
	for (auto r = _requests.begin(); r != _requests.end(); r++) {
		if (r->_event) {
			r->_event->storage()->error();
			_thread->addToDeletedNL(r->_event);
		}
	}
	_requests.clear();
	_ping();
}

void StorageCMDPinging::_ping()
{
	TStorageList storages = _manager->storages();
	for (auto s = storages.begin(); s != storages.end(); s++) {
		StorageCMDEvent *ev = NULL;
		EStorageAnswerStatus status = EStorageAnswerStatus::STORAGE_NO_ANSWER;
		if ((*s)->isActive()) {
			ev = new StorageCMDEvent(*s, _thread, this);
			_fillCMD(ev);
			if (!ev->makeCMD()) {
				status = EStorageAnswerStatus::STORAGE_ANSWER_ERROR;
			}
		}
		_requests.push_back(StorageRequest(ev, *s, status));
	}
}

bool StorageCMDPinging::start()
{
	if (!_timer) {
		_timer = new TimerEvent();
	}
	static const uint32_t STORAGES_PING_SEC_TIME = 15; // each 15 seconds
	static const uint32_t WAIT_BEFORE_FIRST_PING_NANO_SEC = 100000000; // 100ms
	if (!_timer->setTimer(0, WAIT_BEFORE_FIRST_PING_NANO_SEC, STORAGES_PING_SEC_TIME, 0, this))
		return false;
	if (!_thread->ctrl(_timer)) {
		log::Error::L("StorageCMDPinging: Can't add a timer event to the pool\n");
		return false;
	}
	return true;
}

StorageCMDGet::StorageCMDGet(const TStorageList &storages, StorageCMDEventPool *pool, const ItemInfo &item, 
	const TItemSize chunkSize)
	: _storageEvent(NULL), _pool(pool), _interface(NULL), _storages(storages), _item(item.index), 
	_itemSize(item.size), _chunkSize(chunkSize), _remainingSize(item.size), _reconnects(0)
{
	
}

StorageCMDGet::~StorageCMDGet()
{
	if (_storageEvent)
		_pool->free(_storageEvent);	
}


void StorageCMDGet::_fillCMD()
{
	GetItemChunkRequest getRequest;
	getRequest.rangeID = _item.rangeID;
	getRequest.itemKey = _item.itemKey;
	if (_chunkSize > _remainingSize)
		_chunkSize = _remainingSize;
	getRequest.chunkSize = _chunkSize;
	getRequest.seek = _itemSize - _remainingSize;
	NetworkBuffer &buffer = _storageEvent->networkBuffer();

	buffer.clear();
	StorageCmd &storageCmd = *(StorageCmd*)buffer.reserveBuffer(sizeof(StorageCmd));
	storageCmd.cmd = EStorageCMD::STORAGE_GET_ITEM_CHUNK;
	storageCmd.size = sizeof(getRequest);
	buffer.add((char*)&getRequest, sizeof(getRequest));
}

bool StorageCMDGet::start(EPollWorkerThread *thread, StorageCMDGetInterface *interface)
{
	for (auto storage = _storages.begin(); storage != _storages.end(); storage++) {
		_storageEvent = _pool->get(*storage, thread, this);
		if (_storageEvent)
			break;
	}
	if (!_storageEvent)
		return false;

	_fillCMD();
	if (_storageEvent->makeCMD()) {
		_interface = interface;
		return true;
	} else {
		_pool->free(_storageEvent);
		_storageEvent = NULL;
		return false;
	}
}


void StorageCMDGet::ready(class StorageCMDEvent *ev, const StorageAnswer &sa)
{
	if (sa.status == EStorageAnswerStatus::STORAGE_ANSWER_OK) {
		NetworkBuffer &buffer = ev->networkBuffer();
		bool isSended = (_remainingSize < _itemSize);
		_remainingSize -= _chunkSize;
		buffer.setSended(sizeof(StorageAnswer));
		_interface->itemGetChunkReady(this, buffer, isSended);
	} else {
		auto f = std::find(_storages.begin(), _storages.end(), ev->storage());
		if (f != _storages.end())
			_storages.erase(f);
		if (_storages.empty())
			_error();
		else
			repeat(ev);
	}
}

void StorageCMDGet::repeat(class StorageCMDEvent *ev)
{
	if (_storageEvent != ev) {
		log::Fatal::L("StorageCMDGet::repeat: Receive another event\n");
		throw std::exception();
	}
	if (_reconnects >= MAX_STORAGE_RECONNECTS) {
		auto f = std::find(_storages.begin(), _storages.end(), ev->storage());
		if (f != _storages.end())
			_storages.erase(f);
		_reconnects = 0;
		if (_storages.empty()) {
			_error();
			return;
		}
	} else {
		_reconnects++;
	}
	for (auto storage = _storages.begin(); storage != _storages.end(); storage++) {
		ev->reopen();
		_fillCMD();
		ev->setStorage(*storage);
		if (ev->makeCMD())
			return;
	}
	_error();
}

void StorageCMDGet::_error()
{
	if (_interface)
		_interface->itemGetChunkError(this, (_remainingSize < _itemSize));
}


bool StorageCMDGet::canFinish()
{
	if (_remainingSize > 0) {
		if (_storageEvent) {
			_pool->free(_storageEvent);	
			_storageEvent = NULL;
		}
		return false;
	} else {
		return true;
	}
}

bool StorageCMDGet::getNextChunk(EPollWorkerThread *thread)
{
	return start(thread, _interface);
}

StorageCMDPut::StorageCMDPut(const ItemHeader &item, class StorageCMDEventPool *pool, File *postTmpFile, 
	BString &putData)
	: _item(item), _pool(pool), _interface(NULL), _postTmpFile(postTmpFile), _putData(putData)
{
}

void StorageCMDPut::_clearEvents()
{
	for (auto a = _requests.begin(); a != _requests.end(); a++) {
		if (!a->_event)
			continue;
		_pool->free(a->_event);
		a->_event = NULL;
	}
}

StorageCMDPut::~StorageCMDPut()
{
	_clearEvents();
}

bool StorageCMDPut::_fillCMD(class StorageCMDEvent *storageEvent, TItemSize &seek)
{
	NetworkBuffer &buffer = storageEvent->networkBuffer();
	buffer.clear();
	StorageCmd &storageCmd = *(StorageCmd*)buffer.reserveBuffer(sizeof(StorageCmd));
	storageCmd.cmd = EStorageCMD::STORAGE_PUT;
	storageCmd.size = _item.size + sizeof(_item);
	buffer.add((char*)&_item, sizeof(_item));
	if (_postTmpFile) {
		TSize chunkSize = fl::http::WebDavInterface::maxPostInMemmorySize();
		if (chunkSize > _item.size) {
			chunkSize = _item.size;
		}
		_postTmpFile->seek(0, SEEK_SET);
		if (_postTmpFile->read(buffer.reserveBuffer(chunkSize), chunkSize) != (ssize_t)chunkSize) {
			log::Error::L("StorageCMDPut::_fillCMD: Can't read %u from postTmpFile\n", chunkSize);
			return false;
		}
		seek = chunkSize;
		return true;
	} else {
		buffer << _putData;
		seek = _putData.size();
		if (seek != _item.size) {
			log::Error::L("StorageCMDPut::_fillCMD: seek and _size are different\n");
			return false;
		}
		return true;
	}
}

bool StorageCMDPut::start(TStorageList &storages, EPollWorkerThread *thread, StorageCMDPutInterface *interface)
{
	bool haveActiveRequests = false;
	for (auto storage = storages.begin(); storage != storages.end(); storage++) {
		StorageCMDEvent* storageEvent = _pool->get(*storage, thread, this);
		if (storageEvent) {
			TItemSize curSeek = 0;
			if (_fillCMD(storageEvent, curSeek)) {
				_requests.push_back(StorageRequest(storageEvent, *storage, curSeek));
				if (storageEvent->makeCMD()) {
					haveActiveRequests = true;
				} else {
					_pool->free(_requests.back()._event);
					_requests.back()._event = NULL;
					_requests.back()._status = EStorageAnswerStatus::STORAGE_ANSWER_ERROR;
				}
				continue;
			} else {
				_pool->free(storageEvent);
			}
		}
		_requests.push_back(StorageRequest(EStorageAnswerStatus::STORAGE_ANSWER_ERROR, *storage));
	}
	if (haveActiveRequests)
		_interface = interface;
	return haveActiveRequests;	
}


bool StorageCMDPut::getMoreDataToSend(class StorageCMDEvent *ev)
{
	for (auto request = _requests.begin(); request != _requests.end(); request++) {
		if (request->_event == ev) {
			if (request->_seek >= _item.size)
				return false;
			NetworkBuffer &buffer = ev->networkBuffer();
			buffer.clear();
			TSize chunkSize = fl::http::WebDavInterface::maxPostInMemmorySize();
			TSize remainingSize = _item.size - request->_seek;
			if (chunkSize > remainingSize) {
				chunkSize = remainingSize;
			}
			if (_postTmpFile->pread(buffer.reserveBuffer(chunkSize), chunkSize, request->_seek) != (ssize_t)chunkSize) {
				log::Error::L("getMoreData: Can't read %u from postTmpFile\n", chunkSize);
				return false;
			}
			request->_seek += chunkSize;
			return true;
		}
	}
	return false;
}

void StorageCMDPut::ready(class StorageCMDEvent *ev, const StorageAnswer &sa)
{
	bool isComplete = true;
	bool haveFullSended = false;

	for (auto request = _requests.begin(); request != _requests.end(); request++) {
		if (request->_event == ev) {
			request->_status = sa.status;
			_pool->free(request->_event);
			request->_event = NULL;
		} else if (request->_event) {
			isComplete = false;
			continue;
		}	
		if (request->_status == EStorageAnswerStatus::STORAGE_ANSWER_OK) {
			if (request->_seek >= _item.size)
				haveFullSended = true;
		}
	}
	if (isComplete)
		_interface->itemPut(this, haveFullSended);
}
	
void StorageCMDPut::_error(class StorageCMDEvent *ev)
{
	bool isComplete = true;
	bool haveFullSended = false;
	for (auto request = _requests.begin(); request != _requests.end(); request++) {
		if (request->_event == ev) {
			request->_status = EStorageAnswerStatus::STORAGE_ANSWER_ERROR;
			_pool->free(request->_event);
			request->_event = NULL;
		} else if (request->_event) {
			isComplete = false;
		} else if (request->_status == EStorageAnswerStatus::STORAGE_ANSWER_OK) {
			if (request->_seek >= _item.size)
				haveFullSended = true;
		}
	}
	if (isComplete)
		_interface->itemPut(this, haveFullSended);
}

void StorageCMDPut::repeat(class StorageCMDEvent *ev)
{
	for (auto request = _requests.begin(); request != _requests.end(); request++) {
		if (request->_event == ev) {
			if (request->_reconnects < MAX_STORAGE_RECONNECTS) {
				request->_reconnects++;
				ev->reopen();
				request->_seek = 0;
				if (_fillCMD(ev, request->_seek) && ev->makeCMD())
					return;
			}
			_error(ev);
			return;
		}
	}
}

StorageCMDItemInfo::StorageCMDItemInfo(StorageCMDEventPool *pool, const ItemIndex &item, EPollWorkerThread *thread)
	: _pool(pool), _item(item), _thread(thread), _interface(NULL), _timer(NULL)
{
}

void StorageCMDItemInfo::_fillCMD(StorageCMDEvent *storageEvent)
{
	NetworkBuffer &buffer = storageEvent->networkBuffer();
	buffer.clear();
	StorageCmd &storageCmd = *(StorageCmd*)buffer.reserveBuffer(sizeof(StorageCmd));
	storageCmd.cmd = EStorageCMD::STORAGE_ITEM_INFO;
	storageCmd.size = sizeof(_item);
	buffer.add((char*)&_item, sizeof(_item));
}

bool StorageCMDItemInfo::start(const TStorageList &storages, StorageCMDItemInfoInterface *interface)
{
	bool haveActiveRequests = false;
	for (auto storage = storages.begin(); storage != storages.end(); storage++) {
		auto storageEvent = _pool->get(*storage, _thread, this);
		if (storageEvent) {
			_fillCMD(storageEvent);
			if (storageEvent->makeCMD()) {
				_requests.push_back(StorageRequest(storageEvent, *storage));
				haveActiveRequests = true;
				continue;
			} else {
				_pool->free(storageEvent);
			}
		}
		_requests.push_back(StorageRequest(EStorageAnswerStatus::STORAGE_ANSWER_ERROR, *storage));
	}
	if (haveActiveRequests) {
		_interface = interface;
		if (!_timer) {
			_timer = new TimerEvent();
		}
		static const uint32_t STORAGES_WAIT_NONOSEC_TIME = 70000000; // 70 ms
		if (!_timer->setTimer(0, STORAGES_WAIT_NONOSEC_TIME, 0, 0, this))
			return false;
		if (!_thread->ctrl(_timer)) {
			log::Error::L("_formGet: Can't add a timer event to the pool\n");
			return false;
		}
	}
	return haveActiveRequests;
}


bool StorageCMDItemInfo::getStoragesAndFillItem(ItemInfo &item, TStorageList &storageNodes)
{
	bool isThereErrorStorages = false;
	for (auto a = _requests.begin(); a != _requests.end(); a++) {
		if (a->_status == EStorageAnswerStatus::STORAGE_ANSWER_OK) {
			if (item.timeTag.tag) {
				if ((item.timeTag.tag != a->_item.timeTag.tag) || (item.size != a->_item.size)) {
					log::Warning::L("Time tags or sizes is different choose latest\n");
					if (item.timeTag.tag < a->_item.timeTag.tag) {
						item.timeTag.tag = a->_item.timeTag.tag;
						item.size = a->_item.size;
						storageNodes.clear();
					}
					else
						continue;
				}
			} else {
				item.timeTag.tag = a->_item.timeTag.tag;
				item.size = a->_item.size;
			}
			if (item.size > 0)
				storageNodes.push_back(a->_storage);
		} else if (a->_status != EStorageAnswerStatus::STORAGE_ANSWER_NOT_FOUND) {
			isThereErrorStorages = true;
		}
	}
	if (isThereErrorStorages)
		return ! storageNodes.empty();
	else
		return true;
}

TStorageList StorageCMDItemInfo::getPutStorages(const TSize size, const size_t minimumCopies)
{
	TStorageList storages;
	for (auto a = _requests.begin(); a != _requests.end(); a++) {
		if (a->_status == EStorageAnswerStatus::STORAGE_ANSWER_OK) {
			if (a->_storage->canPut(size)) {
				bool found = false;
				for (auto existsStorage = storages.begin(); existsStorage != storages.end(); existsStorage++) {
					if ((*existsStorage)->groupID() == a->_storage->groupID()) {
						found = true;
						break;
					}
				}
				if (!found) {	
					storages.push_back(a->_storage);
					if (storages.size() >= minimumCopies)
						return storages;
				}
			}
		}
	}
	return storages;
}

void StorageCMDItemInfo::timerCall(class TimerEvent *te)
{
	log::Warning::L("StorageCMDItemInfo::timerCal\n");
	_timer->stop();
	_interface->itemInfo(this);
}

void StorageCMDItemInfo::repeat(class StorageCMDEvent *ev)
{
	for (auto a = _requests.begin(); a != _requests.end(); a++) {
		if (a->_event == ev) {
			if (a->_reconnects < MAX_STORAGE_RECONNECTS) {
				a->_reconnects++;
				ev->reopen();
				_fillCMD(ev);
				if (ev->makeCMD())
					return;
			}
			_error(ev);
			return;
		}
	}
}

void StorageCMDItemInfo::_error(class StorageCMDEvent *ev)
{
	bool isComplete = true;
	for (auto a = _requests.begin(); a != _requests.end(); a++) {
		if (a->_event == ev) {
			a->_status = EStorageAnswerStatus::STORAGE_ANSWER_ERROR;
			_pool->free(a->_event);
			a->_event = NULL;
		} else if (a->_event) {
			isComplete = false;
		}
	}
	if (isComplete)
		_interface->itemInfo(this);
}

void StorageCMDItemInfo::ready(class StorageCMDEvent *ev, const StorageAnswer &sa)
{
	bool isComplete = true;
	for (auto a = _requests.begin(); a != _requests.end(); a++) {
		if (a->_event == ev) {
			if (sa.status == EStorageAnswerStatus::STORAGE_ANSWER_OK) {
				NetworkBuffer &data = ev->networkBuffer();
				if ((size_t)data.size() >= (sizeof(StorageAnswer) + sizeof(a->_item))) {
					a->_status = sa.status;
					memcpy(&a->_item, data.c_str() + sizeof(StorageAnswer), sizeof(a->_item));
				} else {
					log::Error::L("Receive a bad item info answer - the sizes are mismatch\n");
					a->_status = EStorageAnswerStatus::STORAGE_ANSWER_ERROR;
				}
			} else {
				a->_status = EStorageAnswerStatus::STORAGE_ANSWER_ERROR;
			}
			_pool->free(a->_event);
			a->_event = NULL;
		} else if (a->_event) {
			isComplete = false;
		}
	}
	if (isComplete)
		_interface->itemInfo(this);	
}

StorageCMDItemInfo::~StorageCMDItemInfo()
{
	if (_timer) {
		_timer->stop();
		_thread->addToDeletedNL(_timer);
	}
	for (auto a = _requests.begin(); a != _requests.end(); a++) {
		if (!a->_event)
			continue;
		_pool->free(a->_event);
		a->_event = NULL;
	}
}

StorageCMDDeleteItem::StorageCMDDeleteItem(StorageCMDEventPool *pool, const ItemHeader &item)
	: _pool(pool), _item(item)
{
}

StorageCMDDeleteItem::~StorageCMDDeleteItem()
{
	for (auto a = _requests.begin(); a != _requests.end(); a++) {
		if (!a->_event)
			continue;
		_pool->free(a->_event);
		a->_event = NULL;
	}	
}

void StorageCMDDeleteItem::_fillCMD(class StorageCMDEvent *storageEvent)
{
	NetworkBuffer &buffer = storageEvent->networkBuffer();
	buffer.clear();
	StorageCmd &storageCmd = *(StorageCmd*)buffer.reserveBuffer(sizeof(StorageCmd));
	storageCmd.cmd = EStorageCMD::STORAGE_DELETE_ITEM;
	storageCmd.size = sizeof(_item);
	buffer.add((char*)&_item, sizeof(_item));	
}

void StorageCMDDeleteItem::repeat(class StorageCMDEvent *ev)
{
	for (auto a = _requests.begin(); a != _requests.end(); a++) {
		if (a->_event == ev) {
			if (a->_reconnects < MAX_STORAGE_RECONNECTS) {
				a->_reconnects++;
				ev->reopen();
				_fillCMD(ev);
				if (ev->makeCMD())
					return;
			}
			_error(ev);
			return;
		}
	}
}

void StorageCMDDeleteItem::_error(class StorageCMDEvent *ev)
{
	bool isComplete = true;
	bool haveNormalyFinished = false;
	for (auto a = _requests.begin(); a != _requests.end(); a++) {
		if (a->_event == ev) {
			a->_status = EStorageAnswerStatus::STORAGE_ANSWER_ERROR;
			_pool->free(a->_event);
			a->_event = NULL;
		} else if (a->_event) {
			isComplete = false;
		}
		if (a->_status == EStorageAnswerStatus::STORAGE_ANSWER_OK)
			haveNormalyFinished = true;
	}
	if (isComplete)
		_interface->deleteItem(this, haveNormalyFinished);
}

void StorageCMDDeleteItem::ready(class StorageCMDEvent *ev, const StorageAnswer &sa)
{
	bool isComplete = true;
	bool haveNormalyFinished = false;
	for (auto a = _requests.begin(); a != _requests.end(); a++) {
		if (a->_event == ev) {
			a->_status = sa.status;
			_pool->free(a->_event);
			a->_event = NULL;
		} else if (a->_event) {
			isComplete = false;
		}
		if (a->_status == EStorageAnswerStatus::STORAGE_ANSWER_OK)
			haveNormalyFinished = true;
	}
	if (isComplete)
		_interface->deleteItem(this, haveNormalyFinished);		
}


bool StorageCMDDeleteItem::start(const TStorageList &storages, StorageCMDDeleteItemInterface *interface, 
	EPollWorkerThread *thread)
{
	bool haveActiveRequests = false;
	for (auto storage = storages.begin(); storage != storages.end(); storage++) {
		auto storageEvent = _pool->get(*storage, thread, this);
		if (storageEvent) {
			_fillCMD(storageEvent);
			if (storageEvent->makeCMD()) {
				_requests.push_back(StorageRequest(storageEvent, *storage));
				haveActiveRequests = true;
				continue;
			} else {
				_pool->free(storageEvent);
			}
		}
		_requests.push_back(StorageRequest(EStorageAnswerStatus::STORAGE_ANSWER_ERROR, *storage));
	}
	if (haveActiveRequests)
		_interface = interface;
	return haveActiveRequests;
}

StorageCMDEventPool::~StorageCMDEventPool()
{
	for (auto storageVector = _freeEvents.begin(); storageVector != _freeEvents.end(); storageVector++)
		for (auto storage = storageVector->second.begin(); storage != storageVector->second.end(); storage++)
			delete *storage;
}

StorageCMDEventPool::StorageCMDEventPool(const size_t maxConnectionPerStorage) 
	: _maxConnectionPerStorage(maxConnectionPerStorage)
{

}

void StorageCMDEventPool::free(StorageCMDEvent *se)
{
	if (se->isCompletedState()) {
		static TStorageCMDEventVector emptyVector;
		auto res = _freeEvents.insert(TStorageCMDEventMap::value_type(se->storage()->id(), emptyVector));
		if (res.first->second.size() < _maxConnectionPerStorage) {
			if (se->removeFromPoll()) {
				res.first->second.push_back(se);
				return;
			}
		}	
	}
	se->addToDelete();
}

StorageCMDEvent *StorageCMDEventPool::get(StorageNode *storageNode, EPollWorkerThread *thread, 
	BasicStorageCMD *interface)
{
	auto f = _freeEvents.find(storageNode->id());
	if ((f == _freeEvents.end()) || f->second.empty()) {
		return new StorageCMDEvent(storageNode, thread, interface);
	} else {
		auto storageEvent = f->second.back();
		storageEvent->setWaitState();
		storageEvent->set(thread, interface);
		f->second.pop_back();
		return storageEvent;
	}
}

StorageCMDEvent::StorageCMDEvent(StorageNode *storage, EPollWorkerThread *thread, BasicStorageCMD *interface)
	: Event(0), _thread(thread), _interface(interface), _storage(storage), _state(WAIT_CONNECTION)
{
	_socket.setNonBlockIO();
	_descr = _socket.descr();
}

StorageCMDEvent::~StorageCMDEvent()
{
	
}

void StorageCMDEvent::reopen()
{
	_socket.reopen();
	_descr = _socket.descr();
	_state = WAIT_CONNECTION;
	_op = EPOLL_CTL_ADD;	
}

bool StorageCMDEvent::_send()
{
	_state = SEND_REQUEST;
	auto sendResult = _buffer.send(_descr);
	if (sendResult == NetworkBuffer::CONNECTION_CLOSE) {
		log::Warning::L("StorageCMDEvent: Reset closed connection to %s:%u\n", Socket::ip2String(_storage->ip()).c_str(), 
			_storage->port());
		reopen();
		return false;
	}
	if (sendResult == NetworkBuffer::ERROR)
		return false;
	if (sendResult == NetworkBuffer::OK) {
		_buffer.clear();
		if (_interface->getMoreDataToSend(this)) {
			setWaitSend();
		} else {
			_state = WAIT_ANSWER;
			setWaitRead();
		}
	} else {
		setWaitSend();
	}
	if (_thread->ctrl(this)) {
		return true;
	} else {
		log::Error::L("StorageCMDEvent::send Can't add an event to the event thread %u/%u\n", _state, _op);
		return false;
	}	
}

bool StorageCMDEvent::makeCMD()
{
	while (true) {
		auto res = _socket.connectNonBlock(_storage->ip(), _storage->port());
		if (res == Socket::CN_ERORR) {
			log::Warning::L("StorageCMDEvent: Can't connect to %s:%u\n", Socket::ip2String(_storage->ip()).c_str(), 
				_storage->port());
			return false;
		}
		if (res == Socket::CN_NEED_RESET) {
			log::Warning::L("StorageCMDEvent: Reset connection to %s:%u\n", Socket::ip2String(_storage->ip()).c_str(), 
				_storage->port());
			reopen();
			continue;
		}

		if (res == Socket::CN_CONNECTED) {
			if (_send()) {
				return true;
			} else if (_state != WAIT_CONNECTION)
				return false;
			else
				continue;
		} else if (res == Socket::CN_NOT_READY) {
			_events = E_OUTPUT | E_ERROR | E_HUP;
			if (_thread->ctrl(this)) {
				return true;
			} else {
				log::Error::L("StorageCMDEvent::makeCMD Can't add an event to the event thread\n");
				return false;
			}
		}
	}
	return false;
}

void StorageCMDEvent::addToDelete()
{
	_state = COMPLETED;
	_thread->addToDeletedNL(this);
}
bool StorageCMDEvent::removeFromPoll()
{
	if (_op == EPOLL_CTL_ADD)
		return true;
	
	_op = EPOLL_CTL_DEL;
	if (_thread->ctrl(this)) {
		_op = EPOLL_CTL_ADD;
		return true;
	} else {
		return false;
	}
}

void StorageCMDEvent::_cmdReady(const StorageAnswer &sa, const char *data)
{
	removeFromPoll();
	_state = COMPLETED;
	_interface->ready(this, sa);	
}

bool StorageCMDEvent::_read()
{
	auto res = _buffer.read(_descr);
	if ((res == NetworkBuffer::ERROR) || (res == NetworkBuffer::CONNECTION_CLOSE))
		return false;
	else if (res == NetworkBuffer::IN_PROGRESS)
		return true;
	if ((size_t)_buffer.size() >= sizeof(StorageAnswer)) {
		StorageAnswer sa = *(StorageAnswer*)_buffer.c_str();
		if ((size_t)_buffer.size() >= (sa.size + sizeof(StorageAnswer))) {
			_cmdReady(sa, _buffer.c_str() + sizeof(StorageAnswer));
			return true;
		}
	}
	return true;
}

const Event::ECallResult StorageCMDEvent::call(const TEvents events)
{
	if (_state == COMPLETED)
		return SKIP;
	
	if (((events & E_HUP) == E_HUP) || ((events & E_ERROR) == E_ERROR)) {
		log::Error::L("Storage %s:%u (%u) dropped connection\n", Socket::ip2String(_storage->ip()).c_str(), 
			_storage->port(), _storage->id());
		_interface->repeat(this);
		return SKIP;
	}
	if (events & E_INPUT)
	{
		if (_state == WAIT_ANSWER) {
			if (!_read()) {
				log::Error::L("Can't read from storage %s:%u (%u) dropped connection\n", 
					Socket::ip2String(_storage->ip()).c_str(), _storage->port(), _storage->id());
				_interface->repeat(this);
			}
			return SKIP;
		}
	}
	if (events & E_OUTPUT) {
		if (_state == WAIT_CONNECTION) {
			if (!makeCMD())
				_interface->repeat(this);
			return SKIP;
		} 
		if (_state == SEND_REQUEST) {
			if (!_send()) {
				_interface->repeat(this);
				return SKIP;
			}
		}
	}
	return SKIP;
}

