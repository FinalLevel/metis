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
#include "http_answer.hpp"

using namespace fl::metis;
using namespace fl::http;

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
		_timerEvent.stop();
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

WebDavInterface::EFormResult ManagerWebDavInterface::_formGet(BString &networkBuffer, class HttpEvent *http)
{
	_error = ERROR_404_NOT_FOUND;
	TRangePtr range;
	if (!_manager->findAndFill(_item, range)) {
		return EFormResult::RESULT_ERROR;
	}
	ManagerCmdThreadSpecificData *threadSpec = (ManagerCmdThreadSpecificData *)http->thread()->threadSpecificData();
	_storageCmd = threadSpec->storageCmdEventPool.mkStorageItemInfo(range->storages(), http->thread(), this, _item);
	_error = ERROR_503_SERVICE_UNAVAILABLE;
	if (!_storageCmd) {
		log::Error::L("_formGet: Can't make StorageItemInfo from the pool\n");
		return EFormResult::RESULT_ERROR;
	}
	static const uint32_t STORAGES_WAIT_NONOSEC_TIME = 70000000; // 70 ms
	if (!_timerEvent.setTimer(0, STORAGES_WAIT_NONOSEC_TIME, 0, 0, this))
		return EFormResult::RESULT_ERROR;
	if (!_httpEvent->thread()->ctrl(&_timerEvent)) {
		log::Error::L("_formGet: Can't add a timer event to the pool\n");
		return EFormResult::RESULT_ERROR;
	}
	return EFormResult::RESULT_OK_WAIT;
}

WebDavInterface::EFormResult ManagerWebDavInterface::_get(TStoragePtrList &storageNodes, BString &networkBuffer, 
	StorageCMDEventPool &pool)
{
	for (auto storage = storageNodes.begin(); storage != storageNodes.end(); storage++) {
		_storageCmd = pool.mkStorageCMDGet(_item, *storage, _httpEvent->thread(), this, maxPostInMemmorySize());
		if (_storageCmd)
			return EFormResult::RESULT_OK_WAIT;
	}
	return EFormResult::RESULT_ERROR;
}

WebDavInterface::EFormResult ManagerWebDavInterface::_gotItemInfo()
{
	_timerEvent.stop();
	
	TStoragePtrList storageNodes;
	if (!static_cast<StorageCMDItemInfo*>(_storageCmd)->getStoragesAndFillItem(_item, storageNodes)) {
		log::Error::L("Can't get item info\n");
		return EFormResult::RESULT_ERROR;
	}
	StorageCMDEventPool &pool = static_cast<StorageCMDItemInfo*>(_storageCmd)->pool();
	delete _storageCmd;
	_storageCmd = NULL;
	if (storageNodes.empty())
	{
		_error = ERROR_404_NOT_FOUND;
		return EFormResult::RESULT_ERROR;
	}
	return _get(storageNodes, *_httpEvent->networkBuffer(), pool);
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
		_error = ERROR_503_SERVICE_UNAVAILABLE;
		if (!_storageCmd) {
			log::Error::L("_formPut: Can't make StorageItemInfo from the pool\n");
			return EFormResult::RESULT_ERROR;
		}
		return EFormResult::RESULT_OK_WAIT;	
	}
	return _put(networkBuffer, threadSpec->storageCmdEventPool);
}

WebDavInterface::EFormResult ManagerWebDavInterface::_put(BString &networkBuffer, StorageCMDEventPool &pool)
{
	_item.size = _putData.size();
	if (_status & ST_POST_SPLITED) {
		_item.size = _postTmpFile.fileSize();
	}
	
	StorageNode *storageNode = NULL;
	if (_storageCmd) {
		storageNode = static_cast<StorageCMDItemInfo*>(_storageCmd)->getPutStorage(_item.size);
		delete _storageCmd;
		_storageCmd = NULL;
	}

	if (!storageNode) {
		storageNode = _manager->getPutStorage(_item.rangeID, _item.size);
		if (!storageNode) {
			log::Error::L("Put: Can't find storage to fit %u\n", _item.size);
			_error = ERROR_507_INSUFFICIENT_STORAGE;
			return EFormResult::RESULT_ERROR;
		}	
	}
	
	_storageCmd = pool.mkStorageCMDPut(_item, storageNode, _httpEvent->thread(), this, 
			(_status & ST_POST_SPLITED) ?	&_postTmpFile : NULL, _putData);
	if (!_storageCmd) {
		_error = ERROR_503_SERVICE_UNAVAILABLE;
		log::Error::L("_formPut: Can't make StorageCMDPut from the pool\n");
		return EFormResult::RESULT_ERROR;
	}
	if (!static_cast<StorageCMDPut*>(_storageCmd)->makeCMD()) {
		_error = ERROR_503_SERVICE_UNAVAILABLE;
		log::Error::L("_formPut: Can't run storage put cmd\n");
		return EFormResult::RESULT_ERROR;
	}
	return EFormResult::RESULT_OK_WAIT;
}

ManagerWebDavInterface::EFormResult ManagerWebDavInterface::getMoreDataToSend(BString &networkBuffer, 
	class HttpEvent *http) override
{
	StorageCMDGet *getCMD = static_cast<StorageCMDGet*>(_storageCmd);
	if (!getCMD) {
		log::Error::L("Receive NULL _storageCmd in getMoreDataToSend\n");
		return EFormResult::RESULT_FINISH;
	}
	if (getCMD->getNextChunk(http->thread(), this))
		return EFormResult::RESULT_OK_WAIT;
	else
		return EFormResult::RESULT_FINISH;
}

void ManagerWebDavInterface::itemChunkGet(const EStorageAnswerStatus res, class StorageCMDEvent *storageEvent)
{
	StorageCMDGet *getCMD = static_cast<StorageCMDGet*>(_storageCmd);
	bool isSended = getCMD->isSended();
	if (res == EStorageAnswerStatus::STORAGE_ANSWER_OK) {
		auto networkBuffer = _httpEvent->networkBuffer();
		networkBuffer->clear();
		if (isSended) {
			storageEvent->moveItemData(*networkBuffer);
		} else {
			auto contentType = MimeType::getMimeTypeStrFromFileName(_fileName);
			HttpAnswer answer(*networkBuffer, _ERROR_STRINGS[ERROR_200_OK], contentType, (_status & ST_KEEP_ALIVE)); 
			answer.setContentLength(_item.size);
			storageEvent->addItemData(*networkBuffer);
		}
		if (getCMD->canFinish()) {
			delete _storageCmd;
			_storageCmd = NULL;
			_httpEvent->sendAnswer(_keepAliveState()); 
		} else {
			_httpEvent->sendAnswer(EFormResult::RESULT_OK_PARTIAL_SEND);
		}
		return;
	} 
	if (isSended) { // if data was sent then close connection
		_httpEvent->sendAnswer(EFormResult::RESULT_FINISH);
		return;
	}
		
	if (res == EStorageAnswerStatus::STORAGE_ANSWER_NOT_FOUND) {
		_error = ERROR_404_NOT_FOUND; 
	}
	_httpEvent->sendAnswer(EFormResult::RESULT_ERROR);
}

void ManagerWebDavInterface::itemInfo(const EStorageAnswerStatus res, class StorageCMDEvent *storageEvent, 
	const ItemHeader *item)
{
	bool isComplete = static_cast<StorageCMDItemInfo*>(_storageCmd)->addAnswer(res, storageEvent, item);
	if (!isComplete)
		return;
	
	EFormResult result = EFormResult::RESULT_ERROR;
	if (_requestType == ERequestType::GET) {
		result = _gotItemInfo();
	} else if (_requestType == ERequestType::PUT) {
		result = _put(*_httpEvent->networkBuffer(), static_cast<StorageCMDItemInfo*>(_storageCmd)->pool());
	}
	if (result != EFormResult::RESULT_OK_WAIT)
		_httpEvent->sendAnswer(result);
}

void ManagerWebDavInterface::itemPut(const EStorageAnswerStatus res, class StorageCMDEvent *storageEvent)
{
	if ((res == EStorageAnswerStatus::STORAGE_ANSWER_OK) && (static_cast<StorageCMDPut*>(_storageCmd)->size() == 0)) {
		auto putResult = WebDavInterface::_formPut(*_httpEvent->networkBuffer(), _httpEvent);
		_httpEvent->sendAnswer(putResult);
	} else {
		log::Error::L("itemPut has received an error from the storage\n");
		_error = ERROR_503_SERVICE_UNAVAILABLE;
		_httpEvent->sendAnswer(EFormResult::RESULT_ERROR);
	}
	delete _storageCmd;
	_storageCmd = NULL;
}

bool ManagerWebDavInterface::getMorePutData(class StorageCMDEvent *storageEvent, NetworkBuffer &buffer)
{
	return static_cast<StorageCMDPut*>(_storageCmd)->getMoreData(storageEvent, buffer);
}

void  ManagerWebDavInterface::timerCall(class TimerEvent *te)
{
	EFormResult result = EFormResult::RESULT_ERROR;
	_timerEvent.stop();
	if (_requestType == ERequestType::GET) {
		if (_item.timeTag.tag == 0) // timer was called while item info waiting
			result = _gotItemInfo();
	}
	if (result != EFormResult::RESULT_OK_WAIT)
		_httpEvent->sendAnswer(result);
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
