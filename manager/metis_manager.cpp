///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis Storage is a CDN and high available http server, which is especially useful for storing 
// and delivering of a huge number of small and medium size objects (images, js files, etc) 
///////////////////////////////////////////////////////////////////////////////


#include <memory>
#include <signal.h>
#include "socket.hpp"
#include "config.hpp"
#include "metis_log.hpp"
#include "time.hpp"
#include "accept_thread.hpp"
#include "web.hpp"
#include "webdav.hpp"
#include "cmd_event.hpp"
#include "manager.hpp"



using fl::network::Socket;
using fl::chrono::Time;
using namespace fl::metis;
using namespace fl::events;

void sigInt(int sig)
{
	log::Fatal::L("Interruption signal (%d) has been received - flushing data\n", sig);
	static fl::threads::Mutex sigSync;
	if (!sigSync.tryLock())
		return;
	
	exit(0);
}

void setSignals()
{
	signal(SIGINT, sigInt);
	signal(SIGTERM, sigInt);
}

int main(int argc, char *argv[])
{
	std::unique_ptr<Config> config;
	std::unique_ptr<EPollWorkerGroup> webWorkerGroup;
	std::unique_ptr<EPollWorkerGroup> cmdWorkerGroup;
	std::unique_ptr<Manager> manager;
	try
	{
		config.reset(new Config(argc, argv));
		if (!log::MetisLogSystem::init(config->logLevel(), config->logPath(), config->isLogStdout()))
			return -1;
		if (!config->initNetwork())
			return -1;
		config->setProcessUserAndGroup();

		ManagerHttpThreadSpecificDataFactory *httpDataFactory = new ManagerHttpThreadSpecificDataFactory(config.get());
		webWorkerGroup.reset(new EPollWorkerGroup(httpDataFactory, config->webWorkers(), config->webWorkerQueueLength(), 
			EPOLL_WORKER_STACK_SIZE));
		AcceptThread httpThread(webWorkerGroup.get(), &config->webSocket(), new ManagerEventFactory(config.get()));
		
		ManagerCmdThreadSpecificDataFactory *cmdDataFactory = new ManagerCmdThreadSpecificDataFactory(config.get());
		cmdWorkerGroup.reset(new EPollWorkerGroup(cmdDataFactory, config->cmdWorkers(), config->cmdWorkerQueueLength(), 
			EPOLL_WORKER_STACK_SIZE));
		AcceptThread webDavThread(cmdWorkerGroup.get(), &config->webDavSocket(), 
			new ManagerWebDavEventFactory(config.get()));

		log::Warning::L("Starting Metis Manager server %u\n", config->serverID());
		manager.reset(new Manager(config.get()));
		if (!manager->loadAll())
			return -1;
		if (!manager->clusterManager().startStoragesPinging(cmdWorkerGroup.get()->getThread(0))) {
			log::Fatal::L("Manager can't start a process of the storages pinging\n");
			return -1;
		}
		if (!manager->startRangesChecking(cmdWorkerGroup.get()->getThread(1 % config->cmdWorkers()))) {
			log::Fatal::L("Manager can't start a process of the ranges checking\n");
			return -1;
		}
		
		ManagerWebDavInterface::setInited(manager.get());
		setSignals();
		webWorkerGroup->waitThreads();
		cmdWorkerGroup->waitThreads();
	}
	catch (...)	
	{
		return -1;
	}
	return 0;
};
