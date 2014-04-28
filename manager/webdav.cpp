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
{
}

ManagerWebDavInterface::~ManagerWebDavInterface()
{
	_clearStorageEvents();
}

void ManagerWebDavInterface::_clearStorageEvents()
{
	for (auto s = _storageCMDEvents.begin(); s != _storageCMDEvents.end(); s++) {
		if (*s) {
			ManagerCmdThreadSpecificData *threadSpec = (ManagerCmdThreadSpecificData *)(*s)->thread()->threadSpecificData();
			threadSpec->storageCmdEventPool.free(*s);
		}
	}
	_storageCMDEvents.clear();
}

bool ManagerWebDavInterface::reset()
{
	if (WebDavInterface::reset()) {
		_clearStorageEvents();
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
	TRangePtr range;
	bool wasAdded = false;
	if (!_manager->fillAndAdd(_item, range, wasAdded)) {
		_error = ERROR_409_CONFLICT;
		return EFormResult::RESULT_ERROR;
	};
	if (!wasAdded) { // need check previous copy
		if (!threadSpec->storageCmdEventPool.get(range->storages(), _storageCMDEvents, http->thread())) {
			_error = ERROR_503_SERVICE_UNAVAILABLE;
			log::Error::L("_formPut: Can't get StorageCmd events from a pool\n");
			return EFormResult::RESULT_ERROR;
		}
		for (auto ev = _storageCMDEvents.begin(); ev != _storageCMDEvents.end(); ev++) {
			if (!(*ev)->setCMD(EStorageCMD::STORAGE_ITEM_INFO, this, _item)) {
				delete (*ev);
				*ev = NULL;
			}
		}
		return EFormResult::RESULT_OK_WAIT;	
	}
	return EFormResult::RESULT_ERROR;
}

bool ManagerWebDavInterface::result(const EStorageResult::EStorageResult res, class StorageCMDEvent *storageEvent)
{
	return false;
}


ManagerWebDavEventFactory::ManagerWebDavEventFactory(class Config *config)
	: _config(config)
{
}

WorkEvent *ManagerWebDavEventFactory::create(const TEventDescriptor descr, const TIPv4 ip, const time_t timeOutTime, 
	Socket *acceptSocket)
{
	return new HttpEvent(descr, EPollWorkerGroup::curTime.unix() + _config->webDavTimeout(), 
		new ManagerWebDavInterface());
}
