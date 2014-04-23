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

bool ManagerWebDavInterface::_put(const char *dataStart)
{
	if (!_manager->fillAndAdd(_item)) {
		_error = ERROR_409_CONFLICT;
		return false;
	};
	return false;
}

bool ManagerWebDavInterface::_putFile()
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
