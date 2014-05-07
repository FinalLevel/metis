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

namespace fl {
	namespace metis {
		using fl::http::WebDavInterface;
		
		class ManagerWebDavInterface : public WebDavInterface, StorageCMDItemInfoInterface, StorageCMDGetInterface, 
																		StorageCMDPutInterface, StorageCMDDeleteItemInterface
		{
		public:
			ManagerWebDavInterface();
			virtual ~ManagerWebDavInterface();
			virtual bool parseURI(const char *cmdStart, const EHttpVersion::EHttpVersion version,
				const std::string &host, const std::string &fileName, const std::string &query);
			static void setInited(class Manager *manager);
			virtual bool reset(); 
			virtual EFormResult getMoreDataToSend(BString &networkBuffer, class HttpEvent *http) override;
			
			// StorageCMDItemInfoInterface
			virtual void itemInfo(class StorageCMDItemInfo *cmd) override;
			
			// StorageCMDPutInterface
			virtual void itemPut(class StorageCMDPut *cmd, const bool isCompleted) override;
			
			//StorageCMDGetInterface
			virtual void itemGetChunkReady(class StorageCMDGet *cmd, NetworkBuffer &buffer, const bool isSended) override;
			virtual void itemGetChunkError(class StorageCMDGet *cmd, const bool isSended) override;
			
			//StorageCMDDeleteItemInterface
			virtual void deleteItem(class StorageCMDDeleteItem *cmd, const bool haveNormalyFinished);
			
			void setHttpEvent(HttpEvent *httpEvent)
			{
				_httpEvent = httpEvent;
			}
		protected:
			static bool _isReady;
			static class Manager *_manager;
			virtual EFormResult _formPut(BString &networkBuffer, class HttpEvent *http) override;
			virtual EFormResult _formGet(BString &networkBuffer, class HttpEvent *http) override;
			virtual EFormResult _formDelete(BString &networkBuffer, class HttpEvent *http) override;
			virtual bool _mkCOL() override;
			
			EFormResult _put(TStorageList &storages);
			EFormResult _get(TStorageList &storages);
			EFormResult _get(StorageCMDItemInfo *cmd);
			ItemHeader _item;
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
