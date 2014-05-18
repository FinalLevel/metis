#pragma once
#ifndef __FL_METIS_EVENT_HPP
#define	__FL_METIS_EVENT_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis manager http event system classes
///////////////////////////////////////////////////////////////////////////////

#include "http_event.hpp"
#include "storage_cmd_event.hpp"
#include "http_answer.hpp"

namespace fl {
	namespace metis {
		using namespace fl::events;
		using fl::http::MimeType;
		class ManagerHttpInterface : public HttpEventInterface, StorageCMDItemInfoInterface, StorageCMDGetInterface
		{
		public:
			ManagerHttpInterface();
			// StorageCMDItemInfoInterface
			virtual void itemInfo(class StorageCMDItemInfo *cmd) override;
			
			//StorageCMDGetInterface
			virtual void itemGetChunkReady(class StorageCMDGet *cmd, NetworkBuffer &buffer, const bool isSended) override;
			virtual void itemGetChunkError(class StorageCMDGet *cmd, const bool isSended) override;
			
			// HttpEventInterface
			virtual bool parseURI(const char *cmdStart, const EHttpVersion::EHttpVersion version,
				const std::string &host, const std::string &fileName, const std::string &query) override;
			virtual EFormResult formResult(BString &networkBuffer, class HttpEvent *http) override;
			virtual bool formError(class BString &result, class HttpEvent *http) override;
			virtual bool parseHeader(const char *name, const size_t nameLength, const char *value, const size_t valueLen, 
				const char *pEndHeader) override;
			virtual bool reset() override;
			virtual EFormResult getMoreDataToSend(BString &networkBuffer, class HttpEvent *http) override;
			virtual ~ManagerHttpInterface();
			void setHttpEvent(HttpEvent *httpEvent)
			{
				_httpEvent = httpEvent;
			}
			static void setInited(class Manager *manager);
		private:
			static bool _isReady;
			static class Manager *_manager;
			ItemInfo _item;
			BasicStorageCMD *_storageCmd;
			HttpEvent *_httpEvent;
			typedef uint8_t TStatus;
			TStatus _status; 
			static const TStatus ST_KEEP_ALIVE = 0x1;
			static const TStatus ST_HEAD_REQUEST = 0x2;
			static const TStatus ST_ERROR_NOT_FOUND = 0x4;
			MimeType::EMimeType _contentType;
			TRangePtr _range;
			
			EFormResult _get(TStorageList &storages);
			EFormResult _get(StorageCMDItemInfo *cmd);
			EFormResult _keepAliveState()
			{
				return (_status & ST_KEEP_ALIVE) ? EFormResult::RESULT_OK_KEEP_ALIVE : EFormResult::RESULT_OK_CLOSE;
			}
			EFormResult _formHead(BString &networkBuffer);
		};
	
		class ManagerEventFactory : public WorkEventFactory 
		{
		public:
			ManagerEventFactory(class Config *config);
			virtual WorkEvent *create(const TEventDescriptor descr, const TIPv4 ip, const time_t timeOutTime, 
				Socket *acceptSocket);
			virtual ~ManagerEventFactory() {};
		private:
			class Config *_config;
		};

		class ManagerHttpThreadSpecificData : public HttpThreadSpecificData
		{
		public:
			ManagerHttpThreadSpecificData(class Config *config);
			virtual ~ManagerHttpThreadSpecificData() 
			{
			}
			class Config *config;
			StorageCMDEventPool storageCmdEventPool;
		};
		
		class ManagerHttpThreadSpecificDataFactory : public ThreadSpecificDataFactory
		{
		public:
			ManagerHttpThreadSpecificDataFactory(class Config *config);
			virtual ThreadSpecificData *create();
			virtual ~ManagerHttpThreadSpecificDataFactory() {};
		private:
			class Config *_config;
		};

	};
};

#endif	// __FL_METIS_EVENT_HPP
