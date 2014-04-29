///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis manager a WebDav interface class implementation
///////////////////////////////////////////////////////////////////////////////

#include "webdav.hpp"
#include "config.hpp"
#include "manager.hpp"
#include "metis_log.hpp"

using namespace fl::metis;

bool ManagerWebDavInterface::_isReady = false;
Manager *ManagerWebDavInterface::_manager = NULL;

void ManagerWebDavInterface::setInited(Manager *manager)
{
	_manager = manager;
	_isReady = true;
}

ManagerWebDavInterface::ManagerWebDavInterface()
	: _storageCmd(NULL)
{
}

ManagerWebDavInterface::~ManagerWebDavInterface()
{
	delete _storageCmd;
}

bool ManagerWebDavInterface::reset()
{
	if (WebDavInterface::reset()) {
		delete _storageCmd;
		_storageCmd = NULL;
		return true;
	} else
		return false;
}

bool ManagerWebDavInterface::parseURI(const char* cmdStart, const EHttpVersion::EHttpVersion version, 
	const std::string& host, const std::string& fileName, const std::string& query)
{
	if (!_isReady) {
		_error = ERROR_503_SERVICE_UNAVAILABLE;
		return false;
	}
	if (!WebDavInterface::parseURI(cmdStart, version, host, fileName, query))
		return false;

	TCrc urlCrc;
	if (!_manager->index().parseURL(host, fileName, _item, urlCrc)) {
		_error = ERROR_400_BAD_REQUEST;
		return false;
	}
	return true;
}

bool ManagerWebDavInterface::_mkCOL()
{
	if (!_manager->addLevel(_item.level, _item.subLevel)) {
		_error = ERROR_405_METHOD_NOT_ALLOWED;
		return false;
	}
	return true;
}

WebDavInterface::EFormResult ManagerWebDavInterface::_formPut(BString &networkBuffer, class HttpEvent *http)
{
	ManagerCmdThreadSpecificData *threadSpec = (ManagerCmdThreadSpecificData *)http->thread()->threadSpecificData();
	bool wasAdded = false;
	TRangePtr range;
	if (!_manager->fillAndAdd(_item, range, wasAdded)) {
		_error = ERROR_409_CONFLICT;
		return EFormResult::RESULT_ERROR;
	};
	if (!wasAdded) { // need check previous copy
		_storageCmd = threadSpec->storageCmdEventPool.mkStorageItemInfo(range->storages(), http->thread(), this, _item);
		if (!_storageCmd) {
			_error = ERROR_503_SERVICE_UNAVAILABLE;
			log::Error::L("_formPut: Can't make StorageItemInfo from the pool\n");
			return EFormResult::RESULT_ERROR;
		}
		_error = ERROR_503_SERVICE_UNAVAILABLE;
		return EFormResult::RESULT_OK_WAIT;	
	}
	return _put(networkBuffer, threadSpec->storageCmdEventPool);
}

WebDavInterface::EFormResult ManagerWebDavInterface::_put(BString &networkBuffer, StorageCMDEventPool &pool)
{
	TSize size = _putData.size();
	if (_status & ST_POST_SPLITED) {
		size = _postTmpFile.fileSize();
	}

	StorageNode *storageNode = NULL;
	if (_storageCmd) {
		storageNode = static_cast<StorageCMDItemInfo*>(_storageCmd)->getPutStorage(size);
		delete _storageCmd;
		_storageCmd = NULL;
	}

	if (!storageNode) {
		storageNode = _manager->getPutStorage(_item.rangeID, size);
		if (!storageNode) {
			log::Error::L("Put: Can't find storage to fit %u\n", size);
			_error = ERROR_507_INSUFFICIENT_STORAGE;
			return EFormResult::RESULT_ERROR;
		}	
	}
	
	_storageCmd = pool.mkStorageCMDPut(_item, storageNode, _httpEvent->thread(), this, 
			(_status & ST_POST_SPLITED) ?	&_postTmpFile : NULL, _putData, size);
	if (!_storageCmd) {
		_error = ERROR_503_SERVICE_UNAVAILABLE;
		log::Error::L("_formPut: Can't make StorageCMDPut from the pool\n");
		return EFormResult::RESULT_ERROR;
	}

	//_storageCmd = new StorageCMDItemInfo
	
	return EFormResult::RESULT_ERROR;
}

void ManagerWebDavInterface::itemInfo(const EStorageAnswerStatus res, class StorageCMDEvent *storageEvent, 
	const ItemHeader *item)
{
	bool isComplete = static_cast<StorageCMDItemInfo*>(_storageCmd)->addAnswer(res, storageEvent, item);
	if (!isComplete)
		return;

	auto putResult = _put(*_httpEvent->networkBuffer(), static_cast<StorageCMDItemInfo*>(_storageCmd)->pool());
	if (putResult != EFormResult::RESULT_OK_WAIT)
		_httpEvent->sendAnswer(putResult);
}


ManagerWebDavEventFactory::ManagerWebDavEventFactory(class Config *config)
	: _config(config)
{
}

WorkEvent *ManagerWebDavEventFactory::create(const TEventDescriptor descr, const TIPv4 ip, const time_t timeOutTime, 
	Socket *acceptSocket)
{
	auto interface = new ManagerWebDavInterface();
	HttpEvent *httpEvent = new HttpEvent(descr, EPollWorkerGroup::curTime.unix() + _config->webDavTimeout(), interface);
	interface->setHttpEvent(httpEvent);
	return httpEvent;
}
