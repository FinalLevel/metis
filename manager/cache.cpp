///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis's manager cache system classes implementation
///////////////////////////////////////////////////////////////////////////////

#include "cache.hpp"
#include "metis_log.hpp"
#include <limits>
#include <cstdlib>
#include <map>

using namespace fl::metis;


void ItemCache::hitAndFill(ItemInfo &item)
{
	if (_hits < std::numeric_limits<decltype(_hits)>::max())
		_hits++;
	item.size = _size;
	item.timeTag = _timeTag;
}

void ItemCache::fill(TStorageList &storages)
{
	storages.clear();
	for (uint8_t s = 0; s < MAX_STORAGES; s++) {
		if (_nodes[s]) {
			storages.push_back(_nodes[s]);
		} else {
			break;
		}
	}
}

bool ItemCache::fillBuffer(BString &buffer)
{
	if (_data) {
		buffer.add(_data, _size);
		return true;
	} else {
		return false;
	}
}

ItemCache::~ItemCache()
{
	free();
}

size_t ItemCache::free()
{
	if (_data) {
		::free(_data);
		_data = NULL;
		return _size;
	} else {
		return 0;
	}
}

void ItemCache::update(const ItemInfo &item, const TStorageList &storages, size_t &freedMem)
{
	if (item.timeTag.tag != _timeTag.tag) {
		freedMem += free();
		_size = item.size;
		_timeTag = item.timeTag;
	}
	auto storage = storages.begin();
	for (uint8_t i = 0; i < MAX_STORAGES; i++) {
		if (storage == storages.end()) {
			_nodes[i] = NULL;
		}
		else {
			_nodes[i] = *storage;
			storage++;
		}
	}
}

void ItemCache::set(TCacheLineIndex cacheIndex, const ItemInfo &item, const TStorageList &storages)
{
	_cacheIndex = cacheIndex;
	_hits = 0;
	_size = item.size;
	_timeTag = item.timeTag;
	auto storage = storages.begin();
	for (auto i = 0; i < MAX_STORAGES; i++) {
		if (storage == storages.end()) {
			_nodes[i] = NULL;
		}
		else {
			_nodes[i] = *storage;
			storage++;
		}
	}
}

bool ItemCache::replaceData(const ItemInfo &item, const char *data, int64_t &usedMem, 
	const uint32_t minHitsToCache)
{
	auto freedMem = free();
	if (!freedMem && (_hits < minHitsToCache))
		return false;
	usedMem -= freedMem;
	_data = (char*)malloc(item.size);
	if (!_data) {
		log::Error::L("ItemCache can't allocate %u bytes\n", item.size);
		return false;
	}
	_size = item.size;
	_timeTag = item.timeTag;
	memcpy(_data, data, _size);
	usedMem += _size;
	return true;
}

void CacheLine::resize(const TCacheLineIndex countItemInfoItems, const uint32_t minHitsToCache)
{
	AutoMutex autoSync(&_sync);
	_minHitsToCache = minHitsToCache;
	_items.clear();
	_itemsCache.resize(countItemInfoItems);
	_freeIndexes.resize(countItemInfoItems);
	for (TCacheLineIndex i = 0; i < countItemInfoItems; i++) {
		_freeIndexes[i] = i;
	}
}

void CacheLine::_free(ItemCache *ic, size_t &freedMem)
{
	_freeIndexes.push_back(ic->cacheIndex());
	freedMem += ic->free();
}

ECacheFindResult CacheLine::findAndFill(ItemInfo &item, TStorageList &storages, BString &buffer)
{
	AutoMutex autoSync(&_sync);
	if (_notFoundItems.find(item.index) != _notFoundItems.end())
		return ECacheFindResult::FIND_NOT_FOUND;
	auto f = _items.find(item.index);
	if (f == _items.end())
		return ECacheFindResult::NOT_IN_CACHE;
	
	f->second->hitAndFill(item);
	if (f->second->fillBuffer(buffer))
		return ECacheFindResult::FIND_FULL;
	else {
		f->second->fill(storages);
		return ECacheFindResult::FIND_HEADER_ONLY;
	}
}

bool CacheLine::replaceData(const ItemInfo &item, const char *data, int64_t &usedMem)
{
	AutoMutex autoSync(&_sync);
	auto f = _items.find(item.index);
	if (f == _items.end())
		return false;
	return f->second->replaceData(item, data, usedMem, _minHitsToCache);
}


bool CacheLine::clear(const ItemIndex &index, size_t &freedMem)
{
	AutoMutex autoSync(&_sync);
	auto f = _items.find(index);
	if (f != _items.end()) {
		_free(f->second, freedMem);
		_items.erase(f);
	}
	_notFoundItems.erase(index);
	return true;
}

bool CacheLine::remove(const ItemIndex &index, size_t &freedMem)
{
	AutoMutex autoSync(&_sync);
	auto f = _items.find(index);
	if (f != _items.end()) {
		_free(f->second, freedMem);
		_items.erase(f);
	}
	if (_notFoundItems.size() > _itemsCache.size())
		return false;

	_notFoundItems.insert(index);
	return true;	
}

bool CacheLine::replace(const ItemInfo &item, const TStorageList &storages, size_t &freedMem)
{
	AutoMutex autoSync(&_sync);

	_notFoundItems.erase(item.index);
	auto f = _items.find(item.index);
	if (f != _items.end()) {
		f->second->update(item, storages, freedMem);
		return true;
	}
	if (_freeIndexes.empty())
		return false;
	auto cacheIndex = _freeIndexes.back();
	_freeIndexes.pop_back();
		
	ItemCache &ic = _itemsCache[cacheIndex];
	ic.set(cacheIndex, item, storages);
	_items.insert(TItemCacheMap::value_type(item.index, &ic));
	return true;
}

size_t CacheLine::recycle(const size_t needFree)
{
	AutoMutex autoSync(&_sync);
	_notFoundItems.clear();
	
	size_t needFreeIndexes = 0;
	if (_freeIndexes.size() >= (_itemsCache.size() / 4)) // min 25% of the indexes must be free
	{
		if (!needFree) { // there are enough free indexes
			return 0;
		}
	} else {
		needFreeIndexes = (_itemsCache.size() / 4) - _freeIndexes.size();
	}
	
	typedef std::multimap<TCacheHits, ItemCache*> THitsMap;
	THitsMap hitsMap;
	size_t freedMemory = 0;
	for (auto item = _items.begin(); item != _items.end(); item++) {
		ItemCache *ic = item->second;
		if (!ic->haveData() && !needFreeIndexes) { // only header item can't free memory and free indexes are not required
			continue;
		}
		hitsMap.insert(THitsMap::value_type(ic->hits(), ic));
	}
	typedef unordered_set<TCacheLineIndex> TCacheLineIndexSet;
	TCacheLineIndexSet markedToFreeSet;
	for (auto hit = hitsMap.begin(); hit != hitsMap.end(); hit++) {
		ItemCache *ic = hit->second;
		if (needFreeIndexes > 0) {
			needFreeIndexes--;
		} else if (freedMemory >= needFree)
			break;
		if (ic->haveData()) {
			freedMemory += ic->size();
		}
		markedToFreeSet.insert(ic->cacheIndex());
	}
	TItemCacheMap savedItems;
	for (auto item = _items.begin(); item != _items.end(); item++) {
		ItemCache *ic = item->second;
		if (markedToFreeSet.find(ic->cacheIndex()) == markedToFreeSet.end()) {
			ic->divideHits();
			savedItems.insert(TItemCacheMap::value_type(item->first, ic));
		} else {
			size_t temp;
			_free(ic, temp);
		}
	}
	std::swap(savedItems, _items);
	return freedMemory;
}

Cache::Cache(const size_t cacheSize, const size_t itemHeadersCacheSize, const TCacheLineIndex itemsInLine, 
	const uint32_t minHitsToCache)
	: _leftMem(cacheSize), 
		_minFreeMem(cacheSize / 4) // minimum 25% of memory must be free for new objects
{
	size_t countItemInfoItems = itemHeadersCacheSize / ((sizeof(TCacheLineIndex) * 2) + sizeof(ItemCache));
	if (!countItemInfoItems) {
		log::Fatal::L("Can't create cache - not enough memory for cache lines. Cache is turned off\n");
		return;
	}
	size_t cacheLines = (countItemInfoItems / itemsInLine) + 1;
	if (cacheLines > 1)
		countItemInfoItems = itemsInLine;
	
	_lines.resize(cacheLines);
	for (auto line = _lines.begin(); line != _lines.end(); line++) {
		line->resize(countItemInfoItems, minHitsToCache);
	}
	log::Info::L("Cache: %u cache lines was created with %u items in each, free data memory: %lld\n", 
		(uint32_t)cacheLines, (uint32_t)countItemInfoItems, _leftMem);
}


bool Cache::clear(const ItemIndex &index)
{
	if (_lines.empty())
		return false;
	auto lineNumber = index.itemKey % _lines.size();
	size_t freedMem = 0;
	auto res = _lines[lineNumber].clear(index, freedMem);
	if (freedMem)
		__sync_add_and_fetch(&_leftMem, freedMem);
	return res;
	
}

bool Cache::remove(const ItemIndex &index)
{
	if (_lines.empty())
		return false;
	auto lineNumber = index.itemKey % _lines.size();
	size_t freedMem = 0;
	auto res = _lines[lineNumber].remove(index, freedMem);
	if (freedMem)
		__sync_add_and_fetch(&_leftMem, freedMem);
	return res;
}

bool Cache::replace(const ItemInfo &item, const TStorageList &storages)
{
	if (_lines.empty())
		return false;
	if (storages.empty()) {
		log::Error::L("Cache: can't replace item with empty storages - use remove\n");
		return false;
	}
	
	auto lineNumber = item.index.itemKey % _lines.size();
	size_t freedMem = 0;
	auto res = _lines[lineNumber].replace(item, storages, freedMem);
	if (freedMem)
		__sync_add_and_fetch(&_leftMem, freedMem);
	return res;
}

bool Cache::replaceData(const ItemInfo &item, const char *data)
{
	if (_leftMem < static_cast<int64_t>(item.size))
		return false;
	
	auto lineNumber = item.index.itemKey % _lines.size();
	int64_t usedMem = 0;
	auto res = _lines[lineNumber].replaceData(item, data, usedMem);
	if (usedMem)
		__sync_sub_and_fetch(&_leftMem, usedMem);
	return res;	
}

ECacheFindResult Cache::findAndFill(ItemInfo &item, TStorageList &storages, BString &buffer)
{
	if (_lines.empty())
		return ECacheFindResult::NOT_IN_CACHE;
	
	auto lineNumber = item.index.itemKey % _lines.size();
	return _lines[lineNumber].findAndFill(item, storages, buffer);	
}

void Cache::recycle()
{
	if (_lines.empty())
		return;
	
	size_t needFree = 0;
	if (_leftMem < (int64_t)_minFreeMem) {
		needFree = (_minFreeMem - _leftMem);
	}
	size_t freedTotalMemory = 0;
	for (auto line = _lines.begin(); line !=  _lines.end(); line++) {
		size_t freedMemory = line->recycle(needFree / _lines.size());
		freedTotalMemory += freedMemory;
		if (freedMemory >= needFree)
			break;
		else
			needFree -=  freedMemory;
	}
	if (freedTotalMemory) {
		__sync_add_and_fetch(&_leftMem, freedTotalMemory);
		log::Info::L("Cache::recycle has been finished freedTotalMemory=%lld, leftMem=%lld\n", (int64_t)freedTotalMemory, 
			_leftMem);
	}
}