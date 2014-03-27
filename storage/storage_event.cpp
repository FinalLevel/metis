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

using namespace fl::metis;


SliceManager *StorageEvent::_sliceManager = NULL;
bool StorageEvent::_isReady = false;

void StorageEvent::setInited(SliceManager *sliceManager)
{
	_sliceManager = sliceManager;
	_isReady = true;
}


StorageEvent::StorageEvent(const TEventDescriptor descr, const time_t timeOutTime)
	: WorkEvent(descr, timeOutTime), _networkBuffer(NULL), _curState(ST_WAIT_REQUEST)
{
	setWaitRead();
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

const StorageEvent::ECallResult StorageEvent::call(const TEvents events)
{
	if (_curState == ST_FINISHED)
		return FINISHED;
	
	if (((events & E_HUP) == E_HUP) || ((events & E_ERROR) == E_ERROR)) {
		_endWork();
		return FINISHED;
	}
	
	if (events & E_INPUT) {
	}
	
	if (events & E_OUTPUT) {
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
