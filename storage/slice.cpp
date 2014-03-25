///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Storage slice management class
///////////////////////////////////////////////////////////////////////////////

#include "slice.hpp"
#include "metis_log.hpp"
#include "dir.hpp"
#include "manager/config.hpp"
#include "buffer.hpp"
#include "config.hpp"



using namespace fl::metis;
using fl::fs::Directory;
using fl::utils::Buffer;

void Slice::_openDataFile(BString &dataFileName)
{
	if (!_dataFd.open(dataFileName.c_str(), O_CREAT | O_RDWR)) {
		log::Fatal::L("Can't open slice dataFile %s\n", dataFileName.c_str());
		throw SliceError("Can't open slice dataFile");
	}
	TSize curSeek = _dataFd.seek(0, SEEK_END);
	if (curSeek < sizeof(SliceDataHeader)) 	{ // create new file
		_dataFd.seek(0, SEEK_SET);
		SliceDataHeader sh;
		sh.version = SliceDataHeader::CURRENT_VERSION;
		sh.sliceID = _sliceID;
		if (_dataFd.write(&sh, sizeof(sh)) != sizeof(sh)) {
			log::Fatal::L("Can't write slice dataFile header %s\n", dataFileName.c_str());
			throw SliceError("Can't write slice dataFile header");
		}
		_size = sizeof(sh);
	}	else {
		_dataFd.seek(0, SEEK_SET);
		SliceDataHeader sh;
		if (_dataFd.read(&sh, sizeof(sh)) != sizeof(sh)) {
			log::Fatal::L("Can't read slice dataFile header %s\n", dataFileName.c_str());
			throw SliceError("Can't read slice dataFile header");
		}
		if (sh.sliceID != _sliceID) {
			log::Fatal::L("SliceID mismatch in %s, %u != %u\n", dataFileName.c_str(), _sliceID, sh.sliceID);
			throw SliceError("SliceID mismatch");
		}
		_size = _dataFd.seek(0, SEEK_END);
	}
}

void Slice::_rebuildIndexFromData(BString &indexFileName)
{
	_indexFd.truncate(0);
	SliceIndexHeader sh;
	sh.version = SliceIndexHeader::CURRENT_VERSION;
	sh.sliceID = _sliceID;
	if (_indexFd.write(&sh, sizeof(sh)) != sizeof(sh)) {
		log::Fatal::L("Can't write slice indexFile header %s\n", indexFileName.c_str());
		throw SliceError("Can't write slice indexFile header");
	}
	if (_size <= sizeof(SliceDataHeader))
		return;
	
	log::Warning::L("Begin rebuild slice index file %s\n", indexFileName.c_str());

	Buffer buf(MAX_BUF_SIZE + sizeof(SliceDataHeader) * 2);
	_dataFd.seek(sizeof(SliceDataHeader), SEEK_SET);
	TSeek curSeek = sizeof(SliceDataHeader);
	while (curSeek < _size)	{
		ItemHeader &ih = *(ItemHeader*)buf.reserveBuffer(sizeof(ih));
		if (_dataFd.read(&ih, sizeof(ih)) != sizeof(ih)) {
			log::Fatal::L("Can't read an item header from slice dataFile\n");
			throw SliceError("Can't read an item header from slice dataFile");
		}
		TSeek itemSeek = curSeek;
		TSeek moveSize = ih.size + sizeof(ih);
		_dataFd.seek(moveSize, SEEK_CUR);
		curSeek += moveSize;
		buf.add(itemSeek);
		if ((buf.writtenSize() >= MAX_BUF_SIZE) || (curSeek >= _size)) {
			if (_indexFd.write(buf.begin(), buf.writtenSize()) != (ssize_t)buf.writtenSize()) {
				log::Fatal::L("Can't write data to slice indexFile header %s\n", indexFileName.c_str());
				throw SliceError("Can't write data to slice indexFile header");
			}
			buf.clear();
		}
	}
}

void Slice::_openIndexFile(BString &indexFileName)
{
	if (!_indexFd.open(indexFileName.c_str(), O_CREAT | O_RDWR)) {
		log::Fatal::L("Can't open slice dataFile %s\n", indexFileName.c_str());
		throw SliceError("Can't open slice dataFile");
	}
	TSize curSeek = _indexFd.seek(0, SEEK_END);
	if (curSeek < sizeof(SliceIndexHeader)) {
		_rebuildIndexFromData(indexFileName);
	}	else {
		_indexFd.seek(0, SEEK_SET);
		try
		{
			SliceIndexHeader sh;
			if (_indexFd.read(&sh, sizeof(sh)) != sizeof(sh)) {
				log::Fatal::L("Can't read slice indexFile header %s\n", indexFileName.c_str());
				throw SliceError("Can't read slice indexFile header");
			}
			if (sh.sliceID != _sliceID) {
				log::Fatal::L("SliceID mismatch in %s, %u != %u\n", indexFileName.c_str(), _sliceID, sh.sliceID);
				throw SliceError("SliceID mismatch");
			}
			_indexFd.seek(0, SEEK_END);
		}
		catch (SliceError &er)
		{
			_rebuildIndexFromData(indexFileName);
		}
	}
}

Slice::Slice(const TSliceID sliceID, BString &dataFileName, BString &indexFileName)
	: _sliceID(sliceID)
{
	_openDataFile(dataFileName);
	_openIndexFile(indexFileName);
}

bool Slice::_writeItem(const char *data, const ItemHeader &itemHeader, ItemPointer &pointer)
{
	if (_dataFd.seek(_size, SEEK_SET) != (off_t)_size) {
		log::Fatal::L("Can't seek slice dataFile %u\n", _sliceID);
		return false;
	}
	if (_dataFd.write(&itemHeader, sizeof(itemHeader)) != sizeof(itemHeader)) {
		log::Fatal::L("Can't write header to slice dataFile %u\n", _sliceID);
		return false;
	}
	if (_dataFd.write(data, itemHeader.size) != (ssize_t)itemHeader.size) {
		log::Fatal::L("Can't write data to slice dataFile %u\n", _sliceID);
		return false;
	}
	if (_indexFd.write(&itemHeader, sizeof(itemHeader)) != sizeof(itemHeader)) {
		log::Fatal::L("Can't write itemHeader to slice indexFile %u\n", _sliceID);
		return false;
	}

	pointer.sliceID = _sliceID;
	pointer.seek = _size;
	if (_indexFd.write(&pointer.seek, sizeof(pointer.seek)) != sizeof(pointer.seek))	{
		log::Fatal::L("Can't write seek data to slice indexFile %u\n", _sliceID);
		return false;
	}
	_size = _dataFd.seek(0, SEEK_CUR);
	return true;
}

bool Slice::add(const char *data, const ItemHeader &itemHeader, ItemPointer &pointer)
{
	AutoReadWriteLockWrite autoSyncWrite(&_sync);
	TSeek curIndexSeek = _indexFd.seek(0, SEEK_CUR);
	if (!_writeItem(data, itemHeader, pointer)) {
		_dataFd.truncate(_size);
		_indexFd.truncate(curIndexSeek);
		return false;
	}
	return true;
}

bool Slice::get(BString &data, const ItemRequest &item)
{
	AutoReadWriteLockRead autoSyncRead(&_sync);
	if (item.pointer.seek >= _size) {
		log::Fatal::L("Can't get out of range seek %u\n", _sliceID, item.pointer.seek);
		return false;
	}
	ssize_t readSize = item.size + sizeof(ItemHeader);
	if (_dataFd.pread(data.reserveBuffer(readSize), readSize, item.pointer.seek) != readSize) {
		log::Fatal::L("Can't read data file from seek %u\n", _sliceID, item.pointer.seek);
		return false;
	}
	ItemHeader &itemHeader = *(ItemHeader*)data.c_str();
	if (itemHeader.level == 0)
		return false;
	return true;
}

bool Slice::loadIndex(BString &data, TSeek &seek)
{
	AutoReadWriteLockWrite autoSyncWrite(&_sync);
	TSeek indexSize = _indexFd.seek(0, SEEK_END);
	if (seek >= indexSize)
		return true;
	
	if (seek < sizeof(SliceIndexHeader))
		seek = sizeof(SliceIndexHeader);
	TSize packetSize = sizeof(ItemHeader) + sizeof(TSeek);
	while (seek < indexSize)
	{
		ssize_t maxSize = (((ssize_t)MAX_BUF_SIZE - data.size()) / packetSize) * packetSize;
		if (!maxSize)
			return true;
	
		ssize_t readSize = indexSize - seek;
		if (readSize > maxSize)
			readSize = maxSize;
		
		if (_dataFd.pread(data.reserveBuffer(readSize), readSize, seek) != readSize) {
			log::Fatal::L("Can't read data index from seek %u\n", _sliceID, seek);
			return false;
		}
		seek += readSize;
	}
	return true;
}

SliceManager::SliceManager(const char *path, const double minFree, const TSize maxSliceSize)
	: _path(path), _minFree(minFree), _leftSpace(0), _maxSliceSize(maxSliceSize)
{
	if (!_recalcSpace())
		throw SliceError("Can't recalculate disk space");
	_init();
}


void SliceManager::_formDataPath(BString &path)
{
	path.sprintfSet("%s/data", _path.c_str());
}

void SliceManager::_formIndexPath(BString &path)
{
	path.sprintfSet("%s/index", _path.c_str());
}

void SliceManager::_init()
{
	BString dataPath;
	_formDataPath(dataPath);
	Directory::makeDirRecursive(dataPath.c_str());
	
	BString indexPath;
	_formIndexPath(indexPath);
	Directory::makeDirRecursive(indexPath.c_str());
	
	try
	{
		Directory dir(dataPath.c_str());
		BString dataFileName;
		BString indexFileName;
		while (dir.next()) {
			if (dir.name()[0] == '.')
				continue;
			TSliceID sliceID = strtoul(dir.name(), NULL, 10);
			dataFileName.sprintfSet("%s/%u", dataPath.c_str(), sliceID);
			indexFileName.sprintfSet("%s/%u", indexPath.c_str(), sliceID);
			TSlicePtr slice(new Slice(sliceID, dataFileName, indexFileName));
			if (sliceID >= _slices.size())
				_slices.resize(sliceID + 1);
			_slices[sliceID] = slice;
		}
		return;
	}
	catch (Directory::Error &er)
	{
		log::Error::L("Can't open slice storage data path %s\n", dataPath.c_str());
	}
	throw SliceError("Can't initialize sliceManager");
}

bool SliceManager::add(const char *data, const ItemHeader &itemHeader, ItemPointer &pointer)
{
	if ((int64_t)itemHeader.size > _leftSpace)
		return false;

	AutoMutex autoSync(&_sync);
	
	if ((_writeSlice.get() == NULL) || ((_writeSlice->size() + itemHeader.size) > _maxSliceSize))	{
		if (!_addWriteSlice(itemHeader.size))
			return false;
	}
	autoSync.unLock();
	if (_writeSlice->add(data, itemHeader, pointer))
	{
		_leftSpace = __sync_sub_and_fetch(&_leftSpace, itemHeader.size);
		return true;
	} else {
		return false;
	}
}

bool SliceManager::get(BString &data, const ItemRequest &item)
{
	AutoMutex autoSync(&_sync);
	if (item.pointer.sliceID >= _slices.size())
	{
		log::Error::L("Can't get slice %u\n", item.pointer.sliceID);
		return false;
	}
	TSlicePtr slice = _slices[item.pointer.sliceID];
	autoSync.unLock();
	return slice->get(data, item);
}

bool SliceManager::loadIndex(BString &data, TSliceID &sliceID, TSeek &seek)
{
	data.clear();
	data.reserveBuffer(sizeof(TSize));
	bool finished = true;
	AutoMutex autoSync(&_sync);
	for (; sliceID < _slices.size(); sliceID++)
	{
		TSlicePtr slice = _slices[sliceID];
		autoSync.unLock();
		if (!slice->loadIndex(data, seek))
			return false;
		if ((size_t)data.size() > MAX_BUF_SIZE) {
			finished = false;
			break;
		}
		autoSync.lock(&_sync);
		seek = 0;
	}
	TSize &answerSize = *(TSize*)data.data();
	answerSize = data.size() - sizeof(TSize);
	if (finished)
		answerSize |= PACKET_FINISHED_FLAG;
	return true;
}

bool SliceManager::_addWriteSlice(const TSize size)
{
	TSliceID sliceID = 0;
	for (auto slice = _slices.begin(); slice != _slices.end(); slice++, sliceID++) {
		if (slice->get() == NULL)
			break;
		if ((slice->get()->size() + size) < _maxSliceSize) {
			_writeSlice = *slice;
			return true;
		}
	}
	BString dataFileName;
	_formDataPath(dataFileName);
	dataFileName.sprintfAdd("/%u", sliceID);

	BString indexFileName;
	_formIndexPath(indexFileName);
	indexFileName.sprintfAdd("/%u", sliceID);

	TSlicePtr slicePtr(new Slice(sliceID, dataFileName, indexFileName));
	if (sliceID >= _slices.size())
		_slices.resize(sliceID + 1);
	_slices[sliceID] = slicePtr;
	_writeSlice = slicePtr;
	return true;
}

bool SliceManager::_recalcSpace()
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
