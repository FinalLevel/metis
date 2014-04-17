#pragma once
#ifndef __FL_METIS_STORAGE_CONFIG_HPP
#define	__FL_METIS_STORAGE_CONFIG_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis server's storage configuration class
///////////////////////////////////////////////////////////////////////////////

#include <boost/property_tree/ptree.hpp>

#include <string>
#include <vector>
#include "socket.hpp"
#include "../types.hpp"
#include "mysql.hpp"

namespace fl {
	namespace metis {
		using fl::network::Socket;
		using fl::db::Mysql;

		const char * const DEFAULT_CONFIG = SYSCONFDIR "/metis.cnf";
		const size_t MAX_BUF_SIZE = 300000;
		
		const int DEFAULT_SOCKET_TIMEOUT = 60;
		const size_t DEFAULT_SOCKET_QUEUE_LENGTH = 10000;
		const size_t EPOLL_WORKER_STACK_SIZE = 100000;
		const size_t DEFAULT_WORKERS_COUNT = 2;
		
		const size_t DEFAULT_BUFFER_SIZE = 32000;
		const size_t DEFAULT_MAX_FREE_BUFFERS = 500;
		
		const uint32_t DEFAULT_CMD_PORT = 7008;

		const double DEFAULT_MIN_DISK_FREE = 0.05; // 5%
		const TSize DEFAULT_MAX_SLICE_SIZE = 1024 * 1024 * 1024; // 1GB
		
		class Config
		{
		public:
			Config(int argc, char *argv[]);
			const TServerID serverID() const
			{
				return _serverID;
			}
			const std::string &dataPath() const
			{
				return _dataPath;
			}
			
			const std::string &logPath() const
			{
				return _logPath;
			}
			int logLevel() const
			{
				return _logLevel;
			}
			typedef uint32_t TStatus;
			static const TStatus ST_LOG_STDOUT = 0x1;
			const bool isLogStdout() const
			{
				return _status & ST_LOG_STDOUT;
			}
			const int cmdTimeout() const
			{
				return _cmdTimeout;
			}
			
			const size_t workerQueueLength() const
			{
				return _workerQueueLength;
			}
			const size_t workers() const
			{
				return _workers;
			}
			const size_t bufferSize() const
			{
				return _bufferSize;
			}
			const size_t maxFreeBuffers() const
			{
				return _maxFreeBuffers;
			}
			bool connectDb(Mysql &sql);
			bool initNetwork();
			typedef uint8_t TStorageStatus;
			static const TStorageStatus ST_STORAGE_ACTIVE = 0x1;
			Socket &listenSocket()
			{
				return _listenSocket;
			}
			double minDiskFree() const
			{
				return _minDiskFree;
			}
			TSize maxSliceSize() const
			{
				return _maxSliceSize;
			}
			void setProcessUserAndGroup();
		private:
			void _usage();
			void _loadFromDB();
			void _parseDBParams(boost::property_tree::ptree &pt);
			void _parseUserGroupParams(boost::property_tree::ptree &pt);
	
			std::string _userName;
			uint32_t _uid;
			std::string _groupName;
			uint32_t _gid;

			TServerID _serverID;
			TStatus _status;
			std::string _logPath;
			int _logLevel;
			
			std::string _dataPath;
			
			std::string _dbHost;
			std::string _dbUser;
			std::string _dbPassword;
			std::string _dbName;
			uint16_t _dbPort;
			
			int _cmdTimeout;
			size_t _workerQueueLength;
			size_t _workers;
			
			size_t _bufferSize;
			size_t _maxFreeBuffers;
			
			std::string _listenIp;
			uint32_t _port;
			TStorageStatus _storageStatus;
			Socket _listenSocket;
			
			double _minDiskFree;
			TSize _maxSliceSize;
		};
	}
}

#endif // __FL_METIS_STORAGE_CONFIG_HPP