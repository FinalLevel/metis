///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Storage Index class implementation
///////////////////////////////////////////////////////////////////////////////

#include "index.hpp"
#include "dir.hpp"
#include "metis_log.hpp"
#include "bstring.hpp"

using namespace fl::metis;
using fl::fs::Directory;
using fl::strings::BString;

TopLevelIndex::TopLevelIndex(const char *path)
	: _sliceManager(path)
{
	
}

bool TopLevelIndex::init()
{
	if (!_sliceManager.init())
		return false;
	return true;
}

Index::Index(const std::string &path, const double minFree)
	: _path(path), _minFree(minFree), _leftSpace(0)
{
	
}

bool Index::init()
{
	if (!_recalcSpace())
		return false;
	try
	{
		Directory dir(_path.c_str());
		BString levelPath;
		while (dir.next()) {
			if (dir.name()[0] == '.') // skip system
				continue;
			TLevel levelID = strtoul(dir.name(), NULL, 10);
			levelPath.sprintfSet("%s/%s", _path.c_str(), dir.name());
			TTopLevelIndexPtr topLevel(new TopLevelIndex(levelPath.c_str()));
			if (!topLevel->init())
				return false;
			_index.emplace(levelID, topLevel);
		}
		log::Info::L("Load %u top levels\n", _index.size());
		return true;
	}
	catch (Directory::Error &er)
	{
		log::Error::L("Can't open metis storage index %s\n", _path.c_str());
	}
	return false;
}

bool Index::_recalcSpace()
{
	uint64_t totalSpace;
	uint64_t freeSpace;
	if (!Directory::getDiskSize(_path.c_str(), totalSpace, freeSpace))
		return false;
	
	uint64_t reserveSpace = (totalSpace * _minFree);
	if (freeSpace < reserveSpace)
		_leftSpace = 0;
	else
		_leftSpace = freeSpace - reserveSpace;
	return true;
}

