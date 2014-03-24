///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Storage slice managment class
///////////////////////////////////////////////////////////////////////////////

#include "slice.hpp"

using namespace fl::metis;

SliceManager::SliceManager(const char *path)
	: _path(path)
{
}

bool SliceManager::init()
{
	return false;
}
