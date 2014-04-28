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

using namespace fl::metis;

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
	if (se->isNormalState()) {
		static TStorageCMDEventVector emptyVector;
		auto res = _freeEvents.insert(TStorageCMDEventMap::value_type(se->storage()->id(), emptyVector));
		if (res.first->second.size() < _maxConnectionPerStorage) {
			if (se->removeFromPoll()) {
				res.first->second.push_back(se);
				return;
			}
		}	
	}
	delete se;
}

bool StorageCMDEventPool::get(const TStorageList &storages, TStorageCMDEventVector &storageEvents, 
	EPollWorkerThread *thread)
{
	for (auto s = storages.begin(); s != storages.end(); s++) {
		auto f = _freeEvents.find((*s)->id());
		if (f == _freeEvents.end()) {
			storageEvents.push_back(new StorageCMDEvent(s->get(), thread));
		} else if (!f->second.empty()) {
			storageEvents.push_back(f->second.back());
			f->second.pop_back();
		}
	}
	return true;
}

StorageCMDEvent::StorageCMDEvent(StorageNode *storage, EPollWorkerThread *thread)
	: Event(0), _thread(thread), _interface(NULL), _storage(storage), _state(WAIT_CONNECTION), _cmd(STORAGE_NO_CMD)
{
	_socket.setNonBlockIO();
	_descr = _socket.descr();
}

StorageCMDEvent::~StorageCMDEvent()
{
	
}

bool StorageCMDEvent::setCMD(const EStorageCMD cmd, StorageCMDEventInterface *interface, ItemHeader &item)
{
	_interface = interface;
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
		_state = WAIT_ANSWER;
		_events = E_INPUT | E_ERROR | E_HUP;
	} else {
		_events = E_OUTPUT | E_ERROR | E_HUP;
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

const Event::ECallResult StorageCMDEvent::call(const TEvents events)
{
	if (((events & E_HUP) == E_HUP) || ((events & E_ERROR) == E_ERROR)) {
		removeFromPoll();
		_interface->result(EStorageResult::ERROR, this);
		return SKIP;
	}
	if (events & E_INPUT)
	{
		if (_state == WAIT_ANSWER) {
		}
	}
	if (events & E_OUTPUT) {
		if (_state == SEND_REQUEST) {
			if (!_send()) {
				removeFromPoll();
				_interface->result(EStorageResult::ERROR, this);
				return SKIP;
			}
		}
	}
	return SKIP;
}

