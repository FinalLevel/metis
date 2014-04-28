///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis manager command event system classes implementation
///////////////////////////////////////////////////////////////////////////////

#include "cmd_event.hpp"

using namespace fl::metis;

ManagerCmdThreadSpecificDataFactory::ManagerCmdThreadSpecificDataFactory(class Config *config)
	: _config(config)
{
}

ThreadSpecificData *ManagerCmdThreadSpecificDataFactory::create()
{
	return new ManagerCmdThreadSpecificData(_config);
}

ManagerCmdThreadSpecificData::ManagerCmdThreadSpecificData(Config* config)
	: config(config), storageCmdEventPool(config->maxConnectionPerStorage())
{
	
}
