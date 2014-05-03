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

StorageCMDGet::StorageCMDGet(StorageCMDEvent* storageEvent, StorageCMDEventPool* pool, 
	const GetItemChunkRequest &getRequest, const TItemSize lastSize)
	: _storageEvent(storageEvent), _pool(pool), _getRequest(getRequest), _lastSize(lastSize)
{
}

StorageCMDGet::~StorageCMDGet()
{
	if (_storageEvent)
		_pool->free(_storageEvent);	
}

StorageCMDPut::StorageCMDPut(class StorageCMDEvent *storageEvent, class StorageCMDEventPool *pool, File *postTmpFile, 
	const TSize size)
	: _storageEvent(storageEvent), _pool(pool), _postTmpFile(postTmpFile),  _size(size)
{
}

StorageCMDPut::~StorageCMDPut()
{
	if (_storageEvent)
		_pool->free(_storageEvent);
}

bool StorageCMDPut::getMoreData(class StorageCMDEvent *storageEvent, NetworkBuffer &buffer)
{
	if (!_size)
		return false;
	buffer.clear();
	TSize chunkSize = fl::http::WebDavInterface::maxPostInMemmorySize();
	if (chunkSize > _size) {
		chunkSize = _size;
	}
	if (_postTmpFile->read(buffer.reserveBuffer(chunkSize), chunkSize) != (ssize_t)chunkSize) {
		log::Error::L("getMoreData: Can't read %u from postTmpFile\n", chunkSize);
		return false;
	}
	_size -= chunkSize;
	return true;
}

StorageCMDItemInfo::StorageCMDItemInfo(const TStorageCMDEventVector &storageCMDEvents, StorageCMDEventPool *pool)
 : _pool(pool)
{
	for (auto s = storageCMDEvents.begin(); s != storageCMDEvents.end(); s++) {
		_answers.push_back(Answer(*s, (*s)->storage()));
	}
}

bool StorageCMDItemInfo::getStoragesAndFillItem(ItemHeader &item, TStoragePtrList &storageNodes)
{
	bool isThereErrorStorages = false;
	for (auto a = _answers.begin(); a != _answers.end(); a++) {
		if (a->_status == EStorageAnswerStatus::STORAGE_ANSWER_OK) {
			if (item.timeTag.tag) {
				if ((item.timeTag.tag != a->_header.timeTag.tag) || (item.size != a->_header.size)) {
					log::Warning::L("Time tags or sizes is different choose latest\n");
					if (item.timeTag.tag < a->_header.timeTag.tag) {
						item.timeTag.tag = a->_header.timeTag.tag;
						item.size = a->_header.size;
						storageNodes.clear();
					}
					else
						continue;
				}
			} else {
				item.timeTag.tag = a->_header.timeTag.tag;
				item.size = a->_header.size;
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

StorageNode *StorageCMDItemInfo::getPutStorage(const TSize size)
{
	for (auto a = _answers.begin(); a != _answers.end(); a++) {
		if (a->_status == EStorageAnswerStatus::STORAGE_ANSWER_OK) {
			if (a->_storage->canPut(size))
				return a->_storage;
		}
	}
	return NULL;
}

bool StorageCMDItemInfo::addAnswer(const EStorageAnswerStatus res, class StorageCMDEvent *storageEvent, 
	const ItemHeader *item)
{
	for (auto a = _answers.begin(); a != _answers.end(); a++) {
		if (a->_event == storageEvent) {
			a->_status = res;
			if (item)
				a->_header = *item;
			_pool->free(a->_event);
			a->_event = NULL;
		}
	}
	return _isComplete();
}

bool StorageCMDItemInfo::_isComplete()
{
	for (auto a = _answers.begin(); a != _answers.end(); a++) {
		if (a->_event != NULL)
			return false;
	}
	return true;
}

void StorageCMDItemInfo::_clearEvents()
{
	for (auto a = _answers.begin(); a != _answers.end(); a++) {
		if (!a->_event)
			continue;
		_pool->free(a->_event);
		a->_event = NULL;
	}
}

StorageCMDItemInfo::~StorageCMDItemInfo()
{
	_clearEvents();
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
	StorageCMDEventInterface *interface)
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

bool StorageCMDEventPool::get(const TStorageList &storages, TStorageCMDEventVector &storageEvents, 
	EPollWorkerThread *thread, StorageCMDEventInterface *interface)
{
	for (auto s = storages.begin(); s != storages.end(); s++) {
		storageEvents.push_back(get(s->get(), thread, interface));
	}
	return true;
}

StorageCMDGet *StorageCMDEventPool::mkStorageCMDGet(const ItemHeader &item, StorageNode *storageNode, 
	EPollWorkerThread *thread, StorageCMDEventInterface *interface, const TItemSize chunkSize)
{
	StorageCMDEvent *storageEvent = get(storageNode, thread, interface);
	if (!storageEvent)
		return NULL;

	GetItemChunkRequest getRequest;
	getRequest.rangeID = item.rangeID;
	getRequest.itemKey = item.itemKey;
	getRequest.chunkSize = chunkSize;
	getRequest.seek = 0;
	if (getRequest.chunkSize > item.size)
		getRequest.chunkSize = item.size;
	
	if (storageEvent->setGetCMD(getRequest)) {
		delete storageEvent;
		return NULL;
	}
	return new StorageCMDGet(storageEvent, this, getRequest, item.size - getRequest.chunkSize);
}

StorageCMDPut *StorageCMDEventPool::mkStorageCMDPut(const ItemHeader &item, StorageNode *storageNode, 
	EPollWorkerThread *thread, StorageCMDEventInterface *interface, File *postTmpFile, BString &putData, const TSize size)
{
	StorageCMDEvent *storageEvent = get(storageNode, thread, interface);
	if (!storageEvent)
		return NULL;
	TSize leftSize = size;
	if (!storageEvent->setPutCMD(item, postTmpFile, putData, leftSize)) {
		delete storageEvent;
		return NULL;
	}
	return new StorageCMDPut(storageEvent, this, postTmpFile, leftSize);
}

StorageCMDItemInfo *StorageCMDEventPool::mkStorageItemInfo(const TStorageList &storages, EPollWorkerThread *thread, 
	StorageCMDEventInterface *interface, const ItemHeader &item)
{
	TStorageCMDEventVector events;
	if (!get(storages, events, thread, interface))
		return NULL;
	
	TStorageCMDEventVector workEvents;
	for (auto ev = events.begin(); ev != events.end(); ev++) {
		if ((*ev)->setCMD(EStorageCMD::STORAGE_ITEM_INFO, item)) {
			workEvents.push_back(*ev);
		} else {
			delete (*ev);
		}
	}
	if (workEvents.empty())
		return false;
	
	return new StorageCMDItemInfo(workEvents, this);
}

StorageCMDEvent::StorageCMDEvent(StorageNode *storage, EPollWorkerThread *thread, StorageCMDEventInterface *interface)
	: Event(0), _thread(thread), _interface(interface), _storage(storage), _state(WAIT_CONNECTION), _cmd(STORAGE_NO_CMD)
{
	_socket.setNonBlockIO();
	_descr = _socket.descr();
}

StorageCMDEvent::~StorageCMDEvent()
{
	
}

bool StorageCMDEvent::setGetCMD(const GetItemChunkRequest &getRequest)
{
	_cmd = EStorageCMD::STORAGE_GET_ITEM_CHUNK;
	_buffer.clear();
	StorageCmd &storageCmd = *(StorageCmd*)_buffer.reserveBuffer(sizeof(StorageCmd));
	storageCmd.cmd = _cmd;
	storageCmd.size = sizeof(getRequest);
	_buffer.add((char*)&getRequest, sizeof(getRequest));
	return _makeCMD();
}

bool StorageCMDEvent::setPutCMD(const ItemHeader &item, File *postTmpFile, BString &putData, TSize &leftSize)
{
	_cmd = EStorageCMD::STORAGE_PUT;
	_buffer.clear();
	StorageCmd &storageCmd = *(StorageCmd*)_buffer.reserveBuffer(sizeof(StorageCmd));
	storageCmd.cmd = _cmd;
	storageCmd.size = leftSize + sizeof(item);
	_buffer.add((char*)&item, sizeof(item));
	if (postTmpFile) {
		TSize chunkSize = fl::http::WebDavInterface::maxPostInMemmorySize();
		if (chunkSize > leftSize) {
			chunkSize = leftSize;
		}
		postTmpFile->seek(0, SEEK_SET);
		if (postTmpFile->read(_buffer.reserveBuffer(chunkSize), chunkSize) != (ssize_t)chunkSize) {
			log::Error::L("setPutCMD: Can't read %u from postTmpFile\n", chunkSize);
			return false;
		}
		leftSize -= chunkSize;
		return _makeCMD();;
	} else {
		_buffer << putData;
		leftSize = 0;
		return _makeCMD();;
	}
}

bool StorageCMDEvent::setCMD(const EStorageCMD cmd, const ItemHeader &item)
{
	_cmd = cmd;
	_buffer.clear();
	StorageCmd &storageCmd = *(StorageCmd*)_buffer.reserveBuffer(sizeof(StorageCmd));
	storageCmd.cmd = cmd;
	storageCmd.size = sizeof(item);
	_buffer.add((char*)&item, sizeof(item));
	return _makeCMD();
}

bool StorageCMDEvent::_send()
{
	_state = SEND_REQUEST;
	auto sendResult = _buffer.send(_descr);
	if (sendResult == NetworkBuffer::CONNECTION_CLOSE) {
		log::Warning::L("StorageCMDEvent: Reset closed connection to %s:%u\n", Socket::ip2String(_storage->ip()).c_str(), 
			_storage->port());
		_socket.reopen();
		_descr = _socket.descr();
		_state = WAIT_CONNECTION;
		_op = EPOLL_CTL_ADD;
		return false;
	}
	if (sendResult == NetworkBuffer::ERROR)
		return false;
	if (sendResult == NetworkBuffer::OK) {
		_buffer.clear();
		if ((_cmd == EStorageCMD::STORAGE_PUT) && (_interface->getMorePutData(this, _buffer))) {
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

bool StorageCMDEvent::_makeCMD()
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

void StorageCMDEvent::_error()
{
	_state = ERROR;
	removeFromPoll();
	switch (_cmd)
	{
		case EStorageCMD::STORAGE_GET_ITEM_CHUNK:
			_interface->itemGet(STORAGE_ANSWER_ERROR, this);
		break;
		case EStorageCMD::STORAGE_ITEM_INFO:
			_interface->itemInfo(STORAGE_ANSWER_ERROR, this, NULL);
		break;
		case EStorageCMD::STORAGE_PUT:
			_interface->itemPut(STORAGE_ANSWER_ERROR, this);
		break;
		case EStorageCMD::STORAGE_NO_CMD:
		break;
	};
}

void StorageCMDEvent::_cmdReady(const StorageAnswer &sa, const char *data)
{
	removeFromPoll();
	_state = COMPLETED;
	switch (_cmd)
	{
		case EStorageCMD::STORAGE_GET_ITEM_CHUNK:
			_interface->itemGet(sa.status, this);
		break;
		case EStorageCMD::STORAGE_ITEM_INFO:
			if (sa.status == EStorageAnswerStatus::STORAGE_ANSWER_NOT_FOUND) 
				_interface->itemInfo(sa.status, this, NULL);
			else if (sa.size >=  sizeof(ItemHeader)) {
				ItemHeader &ih = *(ItemHeader*)data;
				_interface->itemInfo(sa.status, this, &ih);
			}
		break;
		case EStorageCMD::STORAGE_PUT:
			_interface->itemPut(sa.status, this);
		break;
		case EStorageCMD::STORAGE_NO_CMD:
		break;
	};
	
}

bool StorageCMDEvent::_read()
{
	auto res = _buffer.read(_descr);
	if ((res == NetworkBuffer::ERROR) || (res == NetworkBuffer::CONNECTION_CLOSE))
		return false;
	else if (res == NetworkBuffer::IN_PROGRESS)
		return true;
	if ((size_t)_buffer.size() > sizeof(StorageAnswer)) {
		StorageAnswer &sa = *(StorageAnswer*)_buffer.c_str();
		if (sa.status == EStorageAnswerStatus::STORAGE_ANSWER_ERROR) {
			log::Warning::L("Manager has received an error status from storage\n");
			return false;
		}
		if (sa.size + sizeof(StorageAnswer) >= (size_t)_buffer.size()) {
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
		_error();
		return SKIP;
	}
	if (events & E_INPUT)
	{
		if (_state == WAIT_ANSWER) {
			if (!_read())
				_error();
			return SKIP;
		}
	}
	if (events & E_OUTPUT) {
		if (_state == WAIT_CONNECTION) {
			if (!_makeCMD())
				_error();
			return SKIP;
		} if (_state == SEND_REQUEST) {
			if (!_send()) {
				_error();
				return SKIP;
			}
		}
	}
	return SKIP;
}

