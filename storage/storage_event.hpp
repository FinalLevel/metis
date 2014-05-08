#pragma once
#ifndef __FL_METIS_STORAGE_EVENT_HPP
#define	__FL_METIS_STORAGE_EVENT_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis Storage event system implementation classes
///////////////////////////////////////////////////////////////////////////////

#include "event_thread.hpp"
#include "config.hpp"
#include "network_buffer.hpp"
#include "file.hpp"

namespace fl {
	namespace metis {
		using namespace fl::events;
		using fl::fs::File;
		
		class StorageEvent : public WorkEvent
		{
		public:
			enum EStorageState : u_int8_t
			{
				ER_PARSE = 1,
				ST_WAIT_REQUEST,
				ST_WAIT_SEND,
				ST_FINISHED,
			};

			StorageEvent(const TEventDescriptor descr, const time_t timeOutTime);
			virtual ~StorageEvent();
			virtual const ECallResult call(const TEvents events);
			static void setInited(class Storage *storage, class Config *config);
			static void exitFlush();
		private:
			void _endWork();
			ECallResult _read();
			ECallResult _send();
			ECallResult _sendStatus(const EStorageAnswerStatus status);
			bool _reset();
			void _updateTimeout();
			ECallResult _parseCmd(const char *data);
			ECallResult _parsePut();
			
			ECallResult _nopCmd();
			ECallResult _itemInfo(const char *data);
			ECallResult _itemGetChunk(const char *data);
			ECallResult _deleteItem(const char *data);
			ECallResult _ping(const char *data);
			ECallResult _getRangeItems(const char *data);
			static bool _isReady;
			static class Storage *_storage;
			static class Config *_config;
			NetworkBuffer *_networkBuffer;
			EStorageState _curState;
			StorageCmd _cmd;
			File _putTmpFile;
		};

		
		class StorageThreadSpecificData : public ThreadSpecificData
		{
		public:
			StorageThreadSpecificData(Config *config);
			virtual ~StorageThreadSpecificData() {}
			NetworkBufferPool bufferPool;
		};
		
		class StorageThreadSpecificDataFactory : public ThreadSpecificDataFactory
		{
		public:
			StorageThreadSpecificDataFactory(Config *config);
			virtual ThreadSpecificData *create();
			virtual ~StorageThreadSpecificDataFactory() {};
		private:
			Config *_config;
		};

		class StorageEventFactory : public WorkEventFactory 
		{
		public:
			StorageEventFactory(Config *config);
			virtual WorkEvent *create(const TEventDescriptor descr, const TIPv4 ip, const time_t timeOutTime, 
				Socket *acceptSocket);
			virtual ~StorageEventFactory() {};
		private:
			Config *_config;
		};
		
	};
};

#endif	// __FL_METIS_STORAGE_EVENT_HPP
