///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description:  Range index controlling class implementation
///////////////////////////////////////////////////////////////////////////////

#include <limits>
#include "range_index.hpp"

using namespace fl::metis;

Range::Range()
	: _minID(std::numeric_limits<decltype(_minID)>::max()), _maxID(0)
{
}

bool Range::remove(const ItemHeader &itemHeader)
{
	const TItemKey itemKey = itemHeader.itemKey;
	if ((itemKey < _minID) || (itemKey > _maxID))
		return false;
	AutoMutex autoSync(&_sync);
	auto f = _items.find(itemKey);
	if (f == _items.end())
		return false;
	if (f->second.timeTag <= itemHeader.timeTag)
		_items.erase(f);
	return true;
}

bool Range::find(const TItemKey itemKey, Entry &ie)
{
	if ((itemKey < _minID) || (itemKey > _maxID))
		return false;
	AutoMutex autoSync(&_sync);
	auto f = _items.find(itemKey);
	if (f == _items.end())
		return false;
	ie = f->second;
	return true;
}

void Range::addNoLock(const IndexEntry &ie)
{
	const TItemKey itemKey = ie.header.itemKey;
	if (_minID > itemKey)
		_minID = itemKey;
	if (_maxID < itemKey)
		_maxID = itemKey;
	
	Entry entry(ie);
	auto res = _items.insert(TItemHash::value_type(itemKey, entry));
	if (!res.second)
		res.first->second = entry;
}

bool Index::remove(const ItemHeader &itemHeader)
{
	AutoMutex autoSync(&_sync);
	auto f = _ranges.find(itemHeader.rangeID);
	if (f == _ranges.end())
		return false;
	TRangePtr rangePtr  = f->second;
	autoSync.unLock();
	return rangePtr->remove(itemHeader);
}

bool Index::find(const TRangeID rangeID, const TItemKey itemKey, Range::Entry &ie)
{
	AutoMutex autoSync(&_sync);
	auto f = _ranges.find(rangeID);
	if (f == _ranges.end())
		return false;
	TRangePtr rangePtr  = f->second;
	autoSync.unLock();
	return rangePtr->find(itemKey, ie);
}

void Index::add(const IndexEntry &ie)
{
	TRangePtr rangePtr;
	AutoMutex autoSync(&_sync);
	auto res = _ranges.insert(TRangeHash::value_type(ie.header.rangeID, rangePtr));
	if (res.second) {
		rangePtr.reset(new Range());
		res.first->second = rangePtr;
	}
	else
		rangePtr = res.first->second;
	autoSync.unLock();
	
	rangePtr->add(ie);
}

void Index::addNoLock(const IndexEntry &ie)
{
	static TRangePtr nullRange;
	auto res = _ranges.insert(TRangeHash::value_type(ie.header.rangeID, nullRange));
	if (res.second)
		res.first->second.reset(new Range());
	res.first->second->addNoLock(ie);
}
