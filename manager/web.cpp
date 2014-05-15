///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis manager http event system classes implementation
///////////////////////////////////////////////////////////////////////////////

#include <memory>
#include "web.hpp"
#include "config.hpp"
#include "manager.hpp"

using namespace fl::metis;
using fl::http::HttpAnswer;
using fl::http::MimeType;


bool ManagerHttpInterface::_isReady = false;
class Manager *ManagerHttpInterface::_manager = NULL;

void ManagerHttpInterface::setInited(Manager *manager)
{
	_manager = manager;
	_isReady = true;
}

ManagerHttpInterface::ManagerHttpInterface()
	: _storageCmd(NULL), _httpEvent(NULL), _status(0)
{
}

bool ManagerHttpInterface::reset()
{
	if (_status & ST_KEEP_ALIVE) {	
		_status = 0;
		delete _storageCmd;
		_storageCmd = NULL;
		return true;
	} else {
		return false;
	}
}

ManagerHttpInterface::~ManagerHttpInterface()
{
	delete _storageCmd;
}

bool ManagerHttpInterface::parseURI(const char *cmdStart, const EHttpVersion::EHttpVersion version, 
	const std::string &host, const std::string &fileName, const std::string &query)
{
	if (version == EHttpVersion::HTTP_1_1) {
		_status |= ST_KEEP_ALIVE;
	}
	if (!_isReady) {
		return false;
	}
	static const std::string CMD_HEAD("HEAD");
	if (!strncasecmp(cmdStart, CMD_HEAD.c_str(), CMD_HEAD.size())) {
		_status |= ST_HEAD_REQUEST;
		return true;				
	}
	
	TCrc urlCrc;
	if (!_manager->index().parseURL(host, fileName, _item, urlCrc)) {
		_status |= ST_ERROR_NOT_FOUND;
		return false;
	}
	_contentType = MimeType::getMimeTypeFromFileName(fileName);
	return true;
}

bool ManagerHttpInterface::formError(class BString &result, class HttpEvent *http)
{
	EError error = ERROR_503_SERVICE_UNAVAILABLE;
	if (_status & ST_ERROR_NOT_FOUND)
		error = ERROR_404_NOT_FOUND;
	HttpAnswer answer(result, _ERROR_STRINGS[error], "text/html; charset=\"utf-8\"", (_status & ST_KEEP_ALIVE));
	answer.setContentLength();
	if (_status & ST_KEEP_ALIVE)
		http->setKeepAlive();
	return true;
}

bool ManagerHttpInterface::parseHeader(const char *name, const size_t nameLength, const char *value, 
	const size_t valueLen, const char *pEndHeader)
{
	if (_parseHost(name, nameLength, value, valueLen, _host)) {		
	} else {
		bool isKeepAlive = false;
		if (_parseKeepAlive(name, nameLength, value, isKeepAlive)) {
			if (isKeepAlive)
				_status |= ST_KEEP_ALIVE;
			else
				_status &= (~ST_KEEP_ALIVE);
		}
	}
	return true;
}

ManagerHttpInterface::EFormResult ManagerHttpInterface::formResult(BString &networkBuffer, class HttpEvent *http)
{
	TRangePtr range;
	if (!_manager->findAndFill(_item, range)) {
		_status |= ST_ERROR_NOT_FOUND;
		return EFormResult::RESULT_ERROR;
	}
	ManagerHttpThreadSpecificData *threadSpec = (ManagerHttpThreadSpecificData *)http->thread()->threadSpecificData();
	std::unique_ptr<StorageCMDItemInfo> storageCmd(new StorageCMDItemInfo(&threadSpec->storageCmdEventPool, 
		ItemIndex(_item.rangeID, _item.itemKey), http->thread()));
	
	if (!storageCmd->start(range->storages(), this)) {
		log::Error::L("_formGet: Can't make StorageItemInfo from the pool\n");
		return EFormResult::RESULT_ERROR;
	}
	_storageCmd = storageCmd.release();
	return EFormResult::RESULT_OK_WAIT;
}

ManagerHttpInterface::EFormResult ManagerHttpInterface::_get(TStorageList &storages)
{
	ManagerHttpThreadSpecificData *threadSpec = (ManagerHttpThreadSpecificData *)_httpEvent->thread()->threadSpecificData();
	std::unique_ptr<StorageCMDGet> storageCmd(new StorageCMDGet(storages, &threadSpec->storageCmdEventPool, _item, 
		_manager->config()->maxMemmoryChunk()));
	if (storageCmd->start(_httpEvent->thread(), this)) {
		_storageCmd = storageCmd.release();
		return EFormResult::RESULT_OK_WAIT;
	}
	return EFormResult::RESULT_ERROR;
}

ManagerHttpInterface::EFormResult ManagerHttpInterface::_get(StorageCMDItemInfo *cmd)
{
	TStorageList storageNodes;
	if (!cmd->getStoragesAndFillItem(_item, storageNodes)) {
		log::Error::L("Can't get item info\n");
		return EFormResult::RESULT_ERROR;
	}
	delete _storageCmd;
	_storageCmd = NULL;
	if (storageNodes.empty()) {
		_status |= ST_ERROR_NOT_FOUND;
		return EFormResult::RESULT_ERROR;
	}
	return _get(storageNodes);
}

void ManagerHttpInterface::itemGetChunkError(class StorageCMDGet *cmd, const bool isSended)
{
	if (isSended) { // if data was sent then close connection
		_httpEvent->sendAnswer(EFormResult::RESULT_FINISH);
	} else {
		_httpEvent->sendAnswer(EFormResult::RESULT_ERROR);
	}
}

void ManagerHttpInterface::itemGetChunkReady(class StorageCMDGet *cmd, NetworkBuffer &buffer, const bool isSended)
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
		auto contentType = MimeType::getMimeTypeStr(_contentType);
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

ManagerHttpInterface::EFormResult ManagerHttpInterface::getMoreDataToSend(BString &networkBuffer, 
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

void ManagerHttpInterface::itemInfo(class StorageCMDItemInfo *cmd)
{
	if (_storageCmd != cmd) {
		log::Fatal::L("itemInfo: Receive notify from another handler\n");
		throw std::exception();
	}
	
	EFormResult result = _get(cmd);
	if (result != EFormResult::RESULT_OK_WAIT)
		_httpEvent->sendAnswer(result);
}


ManagerEventFactory::ManagerEventFactory(Config *config)
	: _config(config)
{
}

WorkEvent *ManagerEventFactory::create(const TEventDescriptor descr, const TIPv4 ip, const time_t timeOutTime, 
	Socket *acceptSocket)
{
	auto interface = new ManagerHttpInterface();
	auto httpEvent = new HttpEvent(descr, EPollWorkerGroup::curTime.unix() + _config->webTimeout(), interface);
	interface->setHttpEvent(httpEvent);
	return httpEvent;
}

ManagerHttpThreadSpecificDataFactory::ManagerHttpThreadSpecificDataFactory(class Config *config)
	: _config(config)
{
}

ThreadSpecificData *ManagerHttpThreadSpecificDataFactory::create()
{
	return new ManagerHttpThreadSpecificData(_config);
}

ManagerHttpThreadSpecificData::ManagerHttpThreadSpecificData(class Config *config)
	: config(config), storageCmdEventPool(config->maxConnectionPerStorage())
{
}

