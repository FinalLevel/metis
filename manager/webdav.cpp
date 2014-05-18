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
	: _storageCmd(NULL), _httpEvent(NULL)
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
	
	ItemLevelIndex levelIndex;
	if (!_manager->index().parseURL(host, fileName, levelIndex, urlCrc)) {
		_error = ERROR_400_BAD_REQUEST;
		return false;
	}
	bzero(&_item, sizeof(_item));
	_item.level = levelIndex.level;
	_item.subLevel = levelIndex.subLevel;
	_item.itemKey = levelIndex.itemKey;
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

WebDavInterface::EFormResult ManagerWebDavInterface::_formDelete(BString &networkBuffer, class HttpEvent *http)
{
	_error = ERROR_404_NOT_FOUND;
	TRangePtr range;
	if (!_manager->index().find(ItemLevelIndex(_item), range)) {
		return EFormResult::RESULT_ERROR;
	}
	_item.rangeID = range->rangeID();
	_item.timeTag = _manager->index().genNewTimeTag();
	
	ManagerCmdThreadSpecificData *threadSpec = (ManagerCmdThreadSpecificData *)http->thread()->threadSpecificData();
	std::unique_ptr<StorageCMDDeleteItem> storageCmd(new StorageCMDDeleteItem(&threadSpec->storageCmdEventPool, _item));
	_error = ERROR_503_SERVICE_UNAVAILABLE;
	if (!storageCmd->start(range->storages(), this, http->thread())) {
		log::Error::L("_formDelete: Can't make StorageItemInfo from the pool\n");
		return EFormResult::RESULT_ERROR;
	}
	_storageCmd = storageCmd.release();
	return EFormResult::RESULT_OK_WAIT;
}

void ManagerWebDavInterface::deleteItem(class StorageCMDDeleteItem *cmd, const bool haveNormalyFinished)
{
	if (_storageCmd != cmd) {
		log::Fatal::L("deleteItem: Receive notify from another handler\n");
		throw std::exception();
	}
	delete _storageCmd;
	_storageCmd = NULL;
	if (haveNormalyFinished) {
		_manager->cache().remove(ItemIndex(_item.rangeID, _item.itemKey));
		auto putResult = WebDavInterface::_formDelete(*_httpEvent->networkBuffer(), _httpEvent);
		_httpEvent->sendAnswer(putResult);
	} else {
		log::Error::L("deleteItem has received an error from the storage\n");
		_error = ERROR_503_SERVICE_UNAVAILABLE;
		_httpEvent->sendAnswer(EFormResult::RESULT_ERROR);
	}
}

WebDavInterface::EFormResult ManagerWebDavInterface::_formGet(BString &networkBuffer, class HttpEvent *http)
{
	_error = ERROR_404_NOT_FOUND;
	TRangePtr range;
	if (!_manager->index().find(ItemLevelIndex(_item), range)) {
		return EFormResult::RESULT_ERROR;
	}
	_item.rangeID = range->rangeID();
	
	ManagerCmdThreadSpecificData *threadSpec = (ManagerCmdThreadSpecificData *)http->thread()->threadSpecificData();
	std::unique_ptr<StorageCMDItemInfo> storageCmd(new StorageCMDItemInfo(&threadSpec->storageCmdEventPool, 
		ItemIndex(_item.rangeID, _item.itemKey), http->thread()));
	_error = ERROR_503_SERVICE_UNAVAILABLE;
	if (!storageCmd->start(range->storages(), this)) {
		log::Error::L("_formGet: Can't make StorageItemInfo from the pool\n");
		return EFormResult::RESULT_ERROR;
	}
	_storageCmd = storageCmd.release();
	return EFormResult::RESULT_OK_WAIT;
}

WebDavInterface::EFormResult ManagerWebDavInterface::_get(TStorageList &storages)
{
	ManagerCmdThreadSpecificData *threadSpec = (ManagerCmdThreadSpecificData *)_httpEvent->thread()->threadSpecificData();
	std::unique_ptr<StorageCMDGet> storageCmd(new StorageCMDGet(storages, &threadSpec->storageCmdEventPool, 
		ItemInfo(_item), _manager->config()->maxMemmoryChunk()));
	if (storageCmd->start(_httpEvent->thread(), this)) {
		_storageCmd = storageCmd.release();
		return EFormResult::RESULT_OK_WAIT;
	}
	return EFormResult::RESULT_ERROR;
}

WebDavInterface::EFormResult ManagerWebDavInterface::_get(StorageCMDItemInfo *cmd)
{
	TStorageList storageNodes;
	ItemInfo itemInfo(_item);
	if (!cmd->getStoragesAndFillItem(itemInfo, storageNodes)) {
		log::Error::L("Can't get item info\n");
		return EFormResult::RESULT_ERROR;
	}
	delete _storageCmd;
	_storageCmd = NULL;
	if (storageNodes.empty()) {
		_error = ERROR_404_NOT_FOUND;
		return EFormResult::RESULT_ERROR;
	}
	_item.size = itemInfo.size;
	_item.timeTag = itemInfo.timeTag;
	return _get(storageNodes);
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
	_item.size = _putData.size();
	if (_status & ST_POST_SPLITED) {
		_item.size = _postTmpFile.fileSize();
	}

	if (!wasAdded) { // need check previous copy
		_storageCmd = new StorageCMDItemInfo(&threadSpec->storageCmdEventPool, ItemIndex(_item.rangeID, _item.itemKey), 
			http->thread());
		_error = ERROR_503_SERVICE_UNAVAILABLE;
		if (!static_cast<StorageCMDItemInfo*>(_storageCmd)->start(range->storages(), this)) {
			log::Error::L("_formPut: Can't make StorageItemInfo from the pool\n");
			return EFormResult::RESULT_ERROR;
		}
		return EFormResult::RESULT_OK_WAIT;	
	}
	TStorageList storages;
	if (!_manager->getPutStorages(_item.rangeID, _item.size, storages)) {
		log::Error::L("Put: Can't find storage to fit %u\n", _item.size);
		_error = ERROR_507_INSUFFICIENT_STORAGE;
		return EFormResult::RESULT_ERROR;		
	}
	return _put(storages);
}

WebDavInterface::EFormResult ManagerWebDavInterface::_put(TStorageList &storages)
{	
	ManagerCmdThreadSpecificData *threadSpec = (ManagerCmdThreadSpecificData *)_httpEvent->thread()->threadSpecificData();
	std::unique_ptr<StorageCMDPut> storageCmd(new StorageCMDPut(_item, &threadSpec->storageCmdEventPool,   
			(_status & ST_POST_SPLITED) ?	&_postTmpFile : NULL, _putData));
	if (!storageCmd->start(storages, _httpEvent->thread(), this)) {
		_error = ERROR_503_SERVICE_UNAVAILABLE;
		log::Error::L("_formPut: Can't make StorageCMDPut from the pool\n");
		return EFormResult::RESULT_ERROR;
	}
	_storageCmd = storageCmd.release();
	return EFormResult::RESULT_OK_WAIT;
}

ManagerWebDavInterface::EFormResult ManagerWebDavInterface::getMoreDataToSend(BString &networkBuffer, 
	class HttpEvent *http)
{
	StorageCMDGet *getCMD = static_cast<StorageCMDGet*>(_storageCmd);
	if (!getCMD) {
		log::Error::L("Receive NULL _storageCmd in getMoreDataToSend\n");
		return EFormResult::RESULT_FINISH;
	}
	if (getCMD->getNextChunk(http->thread()))
		return EFormResult::RESULT_OK_WAIT;
	else
		return EFormResult::RESULT_FINISH;
}

void ManagerWebDavInterface::itemGetChunkError(class StorageCMDGet *cmd, const bool isSended)
{
	if (isSended) { // if data was sent then close connection
		_httpEvent->sendAnswer(EFormResult::RESULT_FINISH);
	} else {
		_error = ERROR_503_SERVICE_UNAVAILABLE;
		_httpEvent->sendAnswer(EFormResult::RESULT_ERROR);
	}
}

void ManagerWebDavInterface::itemGetChunkReady(class StorageCMDGet *cmd, NetworkBuffer &buffer, const bool isSended)
{
	if (_storageCmd != cmd) {
		log::Fatal::L("itemGetChunkReady: Receive notify from another handler\n");
		throw std::exception();
	}
	auto networkBuffer = _httpEvent->networkBuffer();
	networkBuffer->clear();
	if (isSended) {
		*networkBuffer = std::move(buffer);
	} else {
		auto contentType = MimeType::getMimeTypeStrFromFileName(_fileName);
		HttpAnswer answer(*networkBuffer, _ERROR_STRINGS[ERROR_200_OK], contentType, (_status & ST_KEEP_ALIVE)); 
		answer.setContentLength(_item.size);
		networkBuffer->add(buffer.c_str() + buffer.sended(), buffer.size() - buffer.sended());
	}
	
	if (cmd->canFinish()) {
		delete _storageCmd;
		_storageCmd = NULL;
		_httpEvent->sendAnswer(_keepAliveState()); 
	} else {
		_httpEvent->sendAnswer(EFormResult::RESULT_OK_PARTIAL_SEND);
	}
}

void ManagerWebDavInterface::itemInfo(class StorageCMDItemInfo *cmd)
{
	if (_storageCmd != cmd) {
		log::Fatal::L("itemInfo: Receive notify from another handler\n");
		throw std::exception();
	}
	
	EFormResult result = EFormResult::RESULT_ERROR;
	if (_requestType == ERequestType::GET) {
		result = _get(cmd);
	} else if (_requestType == ERequestType::PUT) {
		TStorageList storages = cmd->getPutStorages(_item.size, _manager->config()->minimumCopies());
		if (storages.size() < _manager->config()->minimumCopies()) {
			_manager->getPutStorages(_item.rangeID, _item.size, storages);
		}
		if (storages.empty()) {
			_error = ERROR_507_INSUFFICIENT_STORAGE;
			log::Error::L("Can't find storage for putting data\n");
		}
		else {
			delete _storageCmd;
			_storageCmd = NULL;
			result = _put(storages);
		}
	}
	if (result != EFormResult::RESULT_OK_WAIT)
		_httpEvent->sendAnswer(result);
}

void ManagerWebDavInterface::itemPut(StorageCMDPut *cmd, const bool isCompleted)
{
	if (_storageCmd != cmd) {
		log::Fatal::L("itemPut: Receive notify from another handler\n");
		throw std::exception();
	}
	delete _storageCmd;
	_storageCmd = NULL;
	if (isCompleted) {
		_manager->cache().clear(ItemIndex(_item.rangeID, _item.itemKey));
		auto putResult = WebDavInterface::_formPut(*_httpEvent->networkBuffer(), _httpEvent);
		_httpEvent->sendAnswer(putResult);
	} else {
		log::Error::L("itemPut has received an error from the storage\n");
		_error = ERROR_503_SERVICE_UNAVAILABLE;
		_httpEvent->sendAnswer(EFormResult::RESULT_ERROR);
	}
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
