///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis Storage event system implementation classes
///////////////////////////////////////////////////////////////////////////////

#include "storage_event.hpp"
#include "slice.hpp"
#include "config.hpp"
#include "metis_log.hpp"
#include "storage.hpp"

using namespace fl::metis;


Storage *StorageEvent::_storage = NULL;
Config *StorageEvent::_config = NULL;
bool StorageEvent::_isReady = false;

void StorageEvent::setInited(Storage *storage, class Config *config)
{
	_storage = storage;
	_config = config;
	_isReady = true;
}


StorageEvent::StorageEvent(const TEventDescriptor descr, const time_t timeOutTime)
	: WorkEvent(descr, timeOutTime), _networkBuffer(NULL), _curState(ST_WAIT_REQUEST)
{
	setWaitRead();
	bzero(&_cmd, sizeof(_cmd));
}

StorageEvent::~StorageEvent()
{
	_endWork();
}

void StorageEvent::_endWork()
{
	_curState = ST_FINISHED;
	if (_descr != 0)
		close(_descr);
	if (_networkBuffer)
	{
		auto threadSpecData = static_cast<StorageThreadSpecificData*>(_thread->threadSpecificData());
		threadSpecData->bufferPool.free(_networkBuffer);
		_networkBuffer = NULL;
	}
}

bool StorageEvent::_reset()
{
	_curState = ST_WAIT_REQUEST;
	_networkBuffer->clear();
	setWaitRead();
	bzero(&_cmd, sizeof(_cmd));
	_putTmpFile.close();
	if (_thread->ctrl(this)) {
		_updateTimeout();
		return true;
	}
	else
		return false;
}

void StorageEvent::_updateTimeout()
{
	_timeOutTime = EPollWorkerGroup::curTime.unix() + _config->cmdTimeout();
}

StorageEvent::ECallResult StorageEvent::_nopCmd()
{
	_networkBuffer->clear();
	StorageAnswer &sa = *(StorageAnswer*)_networkBuffer->reserveBuffer(sizeof(StorageAnswer));
	sa.status = STORAGE_ANSWER_OK;
	sa.size = 0;
	return _send();
}

StorageEvent::ECallResult StorageEvent::_sendStatus(const EStorageAnswerStatus status)
{
	_networkBuffer->clear();
	StorageAnswer &sa = *(StorageAnswer*)_networkBuffer->reserveBuffer(sizeof(StorageAnswer));
	sa.status = status;
	sa.size = 0;
	return _send();
}


StorageEvent::ECallResult StorageEvent::_itemGetChunk(const char *data)
{
	if (_cmd.size < sizeof(GetItemChunkRequest)) {
		log::Error::L("StorageEvent::_itemInfo has received cmd.size < sizeof(ItemHeader)\n");
		return FINISHED;
	}
	GetItemChunkRequest itemRequest = *(GetItemChunkRequest*)data;
	_networkBuffer->clear();
	_networkBuffer->reserveBuffer(sizeof(StorageAnswer));
	if (_storage->get(itemRequest, *_networkBuffer)) {
		StorageAnswer &sa = *(StorageAnswer*)_networkBuffer->c_str();
		sa.status = STORAGE_ANSWER_OK;
		sa.size = _networkBuffer->size() - sizeof(StorageAnswer);
	} else {
		_networkBuffer->clear();
		StorageAnswer &sa = *(StorageAnswer*)_networkBuffer->reserveBuffer(sizeof(StorageAnswer));
		sa.status = STORAGE_ANSWER_NOT_FOUND;
		sa.size = 0;
	}
	return _send();
}

StorageEvent::ECallResult StorageEvent::_deleteItem(const char *data)
{
	if (_cmd.size < sizeof(ItemHeader)) {
		log::Error::L("StorageEvent::_deleteItem has received cmd.size < sizeof(ItemHeader)\n");
		return FINISHED;
	}
	ItemHeader ih = *(ItemHeader*)data;
	_networkBuffer->clear();
	StorageAnswer &sa = *(StorageAnswer*)_networkBuffer->reserveBuffer(sizeof(StorageAnswer));
	sa.size = 0;
	if (_storage->remove(ih)) {
		sa.status = STORAGE_ANSWER_OK;
	} else {
		sa.status = STORAGE_ANSWER_NOT_FOUND;
	}
	return _send();
}

StorageEvent::ECallResult StorageEvent::_getRangeItems(const char *data)
{
	if (_cmd.size < sizeof(RangeItemsRequest)) {
		log::Error::L("StorageEvent::_getRangeItems has received cmd.size < sizeof(RangeItemsRequest)\n");
		return FINISHED;
	}
	RangeItemsRequest request = *(RangeItemsRequest*)data;
	if (_config->serverID() == request.serverID) {
		_networkBuffer->clear();
		_networkBuffer->reserveBuffer(sizeof(StorageAnswer));
		auto currentBufferSize = _networkBuffer->size();
		if (_storage->getRangeItems(request.rangeID, *_networkBuffer)) {
			StorageAnswer &sa = *(StorageAnswer*)_networkBuffer->c_str();
			sa.status = STORAGE_ANSWER_OK;
			sa.size = _networkBuffer->size() - currentBufferSize;
			return _send();
		}
	}
	_networkBuffer->clear();
	StorageAnswer &sa = *(StorageAnswer*)_networkBuffer->reserveBuffer(sizeof(StorageAnswer));
	sa.status = STORAGE_ANSWER_ERROR;
	sa.size = 0;
	return _send();
}

StorageEvent::ECallResult StorageEvent::_ping(const char *data)
{
	if (_cmd.size < sizeof(TServerID)) {
		log::Error::L("StorageEvent::_ping has received cmd.size < sizeof(TServerID)\n");
		return FINISHED;
	}
	TServerID requestServerID = *(TServerID*)data;
	_networkBuffer->clear();
	StorageAnswer &sa = *(StorageAnswer*)_networkBuffer->reserveBuffer(sizeof(StorageAnswer));
	sa.status = STORAGE_ANSWER_ERROR;
	sa.size = 0;

	StoragePingAnswer storageAnswer;
	if (_config->serverID() == requestServerID) {
		if (_storage->ping(storageAnswer)) {
			storageAnswer.serverID = _config->serverID();
			sa.status = STORAGE_ANSWER_OK;
			sa.size = sizeof(storageAnswer);
			_networkBuffer->add((char*)&storageAnswer, sizeof(storageAnswer));
		}
	} else {
		log::Error::L("StorageEvent::_ping has been requested %u, but it is %u\n", requestServerID, _config->serverID());
	}
	return _send();
}

StorageEvent::ECallResult StorageEvent::_itemInfo(const char *data)
{
	if (_cmd.size < sizeof(ItemIndex)) {
		log::Error::L("StorageEvent::_itemInfo has received cmd.size < sizeof(ItemIndex)\n");
		return FINISHED;
	}
	ItemIndex itemIndex = *(ItemIndex*)data;
	_networkBuffer->clear();
	StorageAnswer &sa = *(StorageAnswer*)_networkBuffer->reserveBuffer(sizeof(StorageAnswer));
	sa.status = STORAGE_ANSWER_NOT_FOUND;
	sa.size = 0;
	GetItemInfoAnswer itemInfo;
	if (_storage->findAndFill(itemIndex, itemInfo)) {
		sa.status = STORAGE_ANSWER_OK;
		sa.size = sizeof(itemInfo);
		_networkBuffer->add((char*)&itemInfo, sizeof(itemInfo));
	}
	return _send();
}

StorageEvent::ECallResult StorageEvent::_parseCmd(const char *data)
{
	switch (_cmd.cmd) 
	{
		case EStorageCMD::STORAGE_GET_ITEM_CHUNK:
			return _itemGetChunk(data);
		case EStorageCMD::STORAGE_ITEM_INFO:
			return _itemInfo(data);
		case EStorageCMD::STORAGE_DELETE_ITEM:
			return _deleteItem(data);
		case EStorageCMD::STORAGE_PING:
			return _ping(data);
		case EStorageCMD::STORAGE_GET_RANGE_ITEMS:
			return _getRangeItems(data);
		case EStorageCMD::STORAGE_NO_CMD:
			return _nopCmd();
		case EStorageCMD::STORAGE_PUT: // never come here
			log::Error::L("Should not come here %u\n", _cmd.cmd);
		break;
	};
	log::Error::L("Unsupported command %u\n", _cmd.cmd);
	_endWork();
	return FINISHED;
}

StorageEvent::ECallResult  StorageEvent::_send()
{
	_curState = ST_WAIT_SEND;
	auto res = _networkBuffer->send(_descr);
	if (res == NetworkBuffer::IN_PROGRESS) {
		setWaitSend();
		if (_thread->ctrl(this)) {
			_updateTimeout();
			return CHANGE;
		}
		else
			return FINISHED;
	} else if (res == NetworkBuffer::OK) {
		if (_reset())
			return CHANGE;
	}
	return FINISHED;
}

StorageEvent::ECallResult StorageEvent::_parsePut()
{
	ssize_t writeSize = _networkBuffer->size();
	size_t skipSize = 0;
	if (_putTmpFile.descr() == 0) {
		if (_cmd.size < sizeof(ItemHeader)) {
			log::Error::L("Put cmd can't have size less than %u, but its size is %u\n", sizeof(ItemHeader), _cmd.size);
			return _sendStatus(EStorageAnswerStatus::STORAGE_ANSWER_ERROR);
		}
		_putTmpFile.createUnlinkedTmpFile(_config->getTmpDir());
		writeSize -= sizeof(StorageCmd);
		skipSize = sizeof(StorageCmd);
	}
	
	if (_putTmpFile.write(_networkBuffer->c_str() + skipSize, writeSize) != writeSize) {
		log::Error::L("Can't write to put temporary file in %s\n", _config->getTmpDir());
		return _sendStatus(EStorageAnswerStatus::STORAGE_ANSWER_ERROR);
	}
	if (_putTmpFile.seek(0, SEEK_CUR) >= _cmd.size) {
		ItemHeader itemHeader;
		_putTmpFile.seek(0, SEEK_SET);
		if (_putTmpFile.read(&itemHeader, sizeof(itemHeader)) !=  sizeof(itemHeader)) {
			log::Error::L("Can't read an item header from put temporary file in %s\n", _config->getTmpDir());
			return _sendStatus(EStorageAnswerStatus::STORAGE_ANSWER_ERROR);
		}
		_cmd.size -= sizeof(itemHeader);
		if (itemHeader.size != _cmd.size) {
			log::Error::L("Item and cmd's sizes are different %u != %u\n", itemHeader.size != _cmd.size);
			return _sendStatus(EStorageAnswerStatus::STORAGE_ANSWER_ERROR);
		}
		if (_storage->add(itemHeader, _putTmpFile, *_networkBuffer)) {
			return _sendStatus(EStorageAnswerStatus::STORAGE_ANSWER_OK);
		} else {
			log::Error::L("Can't put a temporary file into the storage\n");
			return _sendStatus(EStorageAnswerStatus::STORAGE_ANSWER_ERROR);
		}
	}
	_networkBuffer->clear();
	_updateTimeout();
	return CHANGE;
}

StorageEvent::ECallResult StorageEvent::_read()
{
	if (!_networkBuffer) {
		auto threadSpecData = static_cast<StorageThreadSpecificData*>(_thread->threadSpecificData());
		_networkBuffer = threadSpecData->bufferPool.get();
	}
		
	auto res = _networkBuffer->read(_descr);
	if ((res == NetworkBuffer::ERROR) || (res == NetworkBuffer::CONNECTION_CLOSE))
	{
		_endWork();
		return FINISHED;
	}
	else if (res == NetworkBuffer::IN_PROGRESS)
		return SKIP;
	if ((size_t)_networkBuffer->size() >= sizeof(StorageCmd)) {
		if (!_cmd.size)
			_cmd = *(StorageCmd*)_networkBuffer->c_str();
		if (_cmd.cmd  == EStorageCMD::STORAGE_PUT)
		{
			return _parsePut();
		} else if ((size_t)_networkBuffer->size() >= (_cmd.size + sizeof(StorageCmd)))
			return _parseCmd(_networkBuffer->c_str() + sizeof(StorageCmd));
	}
	_updateTimeout();
	return CHANGE;
}

const StorageEvent::ECallResult StorageEvent::call(const TEvents events)
{
	if (_curState == ST_FINISHED)
		return FINISHED;
	if (!_isReady)
		return FINISHED;
	
	if (((events & E_HUP) == E_HUP) || ((events & E_ERROR) == E_ERROR)) {
		_endWork();
		return FINISHED;
	}
	
	if (events & E_INPUT) {
		if (_curState == ST_WAIT_REQUEST) {
			return _read();
		}
	}
	
	if (events & E_OUTPUT) {
		if (_curState == ST_WAIT_SEND) {
			return _send();
		}
	}
	return SKIP;
}


StorageThreadSpecificData::StorageThreadSpecificData(Config *config)
	: bufferPool(config->bufferSize(), config->maxFreeBuffers())
{
	
}

StorageThreadSpecificDataFactory::StorageThreadSpecificDataFactory(Config *config)
	: _config(config)
{
}

ThreadSpecificData *StorageThreadSpecificDataFactory::create()
{
	return new StorageThreadSpecificData(_config);
}

StorageEventFactory::StorageEventFactory(Config *config)
	: _config(config)
{
}

WorkEvent *StorageEventFactory::create(const TEventDescriptor descr, const TIPv4 ip, 
	const time_t timeOutTime, Socket* acceptSocket)
{
	return new StorageEvent(descr, EPollWorkerGroup::curTime.unix() + _config->cmdTimeout());
}
