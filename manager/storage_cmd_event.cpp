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

using namespace fl::metis;

StorageCMDGet::StorageCMDGet(const TStorageList &storages, StorageCMDEventPool *pool, const ItemHeader &item, 
	const TItemSize chunkSize)
	: _storageEvent(NULL), _pool(pool), _interface(NULL), _storages(storages), _item(item.rangeID, item.itemKey), 
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
			if (!_fillCMD(storageEvent, curSeek))
			{
				_pool->free(storageEvent);
				break;
			}
			_requests.push_back(StorageRequest(storageEvent, *storage, curSeek));
			if (storageEvent->makeCMD()) {
				haveActiveRequests = true;
			} else {
				_pool->free(_requests.back()._event);
				_requests.back()._event = NULL;
				_requests.back()._status = EStorageAnswerStatus::STORAGE_ANSWER_ERROR;
			}
			continue;
		}
		_requests.push_back(StorageRequest(EStorageAnswerStatus::STORAGE_ANSWER_ERROR, *storage));
	}
	if (haveActiveRequests)
		_interface = interface;
	return haveActiveRequests;
	
}


bool StorageCMDPut::getMoreDataToSend(class StorageCMDEvent *ev) override
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
			if (request->_reconnects >= MAX_STORAGE_RECONNECTS)
				return;
			request->_reconnects++;
			ev->reopen();
			request->_seek = 0;
			if (_fillCMD(ev, request->_seek) && ev->makeCMD())
				return;
			_error(ev);
			return;
		}
	}
}

StorageCMDItemInfo::StorageCMDItemInfo(StorageCMDEventPool *pool, const ItemIndex &item, EPollWorkerThread *thread)
	: _pool(pool), _item(item), _thread(thread), _interface(NULL)
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


bool StorageCMDItemInfo::getStoragesAndFillItem(ItemHeader &item, TStorageList &storageNodes)
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
		log::Error::L("StorageCMDEvent: Can't add an event to the event thread\n");
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
			_socket.reopen();
			_descr = _socket.descr();
			_op = EPOLL_CTL_ADD;
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
				log::Error::L("StorageCMDEvent: Can't add an event to the event thread\n");
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
		StorageAnswer &sa = *(StorageAnswer*)_buffer.c_str();
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

