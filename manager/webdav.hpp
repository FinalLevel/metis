#pragma once
#ifndef __FL_METIS_MANAGER_WEBDAV_INTERFACE_HPP
#define	__FL_METIS_MANAGER_WEBDAV_INTERFACE_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis manager a WebDav interface class
///////////////////////////////////////////////////////////////////////////////

#include "cmd_event.hpp"
#include "webdav_interface.hpp"
#include "types.hpp"
#include "compatibility.hpp"
#include "storage_cmd_event.hpp"
#include "index.hpp"
#include "timer_event.hpp"

namespace fl {
	namespace metis {
		using fl::http::WebDavInterface;
		
		class ManagerWebDavInterface : public WebDavInterface, StorageCMDEventInterface, TimerEventInterface
		{
		public:
			ManagerWebDavInterface();
			virtual ~ManagerWebDavInterface();
			virtual bool parseURI(const char *cmdStart, const EHttpVersion::EHttpVersion version,
				const std::string &host, const std::string &fileName, const std::string &query);
			static void setInited(class Manager *manager);
			virtual bool reset(); 
			virtual EFormResult getMoreDataToSend(BString &networkBuffer, class HttpEvent *http) override;
			
			// StorageCMDEventInterface
			virtual void itemInfo(const EStorageAnswerStatus res, class StorageCMDEvent *storageEvent, 
				const ItemHeader *item);
			virtual void itemPut(const EStorageAnswerStatus res, class StorageCMDEvent *storageEvent);
			virtual bool getMorePutData(class StorageCMDEvent *storageEvent, NetworkBuffer &buffer);
			virtual void itemChunkGet(const EStorageAnswerStatus res, class StorageCMDEvent *storageEvent);
			
			// TimerEventInterface
			virtual void timerCall(class TimerEvent *te) override;
			
			void setHttpEvent(HttpEvent *httpEvent)
			{
				_httpEvent = httpEvent;
			}
		protected:
			static bool _isReady;
			static class Manager *_manager;
			virtual EFormResult _formPut(BString &networkBuffer, class HttpEvent *http) override;
			virtual EFormResult _formGet(BString &networkBuffer, class HttpEvent *http) override;
			virtual bool _mkCOL() override;
			
			EFormResult _put(BString &networkBuffer, StorageCMDEventPool &pool);
			EFormResult _get(TStoragePtrList &storageNodes, BString &networkBuffer, StorageCMDEventPool &pool);
			EFormResult _gotItemInfo();
			ItemHeader _item;
			TimerEvent _timerEvent;
			BasicStorageCMD *_storageCmd;
			HttpEvent *_httpEvent;
		};
		
		class ManagerWebDavEventFactory : public WorkEventFactory 
		{
		public:
			ManagerWebDavEventFactory(class Config *config);
			virtual WorkEvent *create(const TEventDescriptor descr, const TIPv4 ip, const time_t timeOutTime, 
				Socket *acceptSocket);
			virtual ~ManagerWebDavEventFactory() {};
		private:
			class Config *_config;
		};
	};
};

#endif	// __FL_METIS_MANAGER_WEBDAV_INTERFACE_HPP
