#pragma once
#ifndef __FL_METIS_TYPES_HPP
#define	__FL_METIS_TYPES_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis internal types
///////////////////////////////////////////////////////////////////////////////

#include <cstdint>

namespace fl {
	namespace metis
	{
		typedef uint32_t TServerID;
		
		typedef uint8_t TStatus;
		static const TStatus ST_ITEM_DELETED = 0x80;
		
		typedef uint32_t TLevel;
		typedef uint32_t TSubLevel;
		typedef uint32_t TItemKey;
		typedef uint32_t TItemSize;
		typedef uint32_t TItemModTime;
		
		struct ItemHeader
		{
			TStatus status;
			TLevel level;
			TSubLevel subLevel;
			TItemKey itemKey;
			TItemSize size;
			union ModTimeTag
			{
				struct
				{
					uint32_t op;
					TItemModTime modTime;
				};
				uint64_t tag;
			};
			ModTimeTag timeTag;
		}  __attribute__((packed));
		
		typedef uint32_t TSeek;
		typedef uint32_t TSize;
		typedef uint16_t TSliceID;
		
		struct ItemPointer
		{
			TSliceID sliceID;
			TSeek seek;
		} __attribute__((packed));
		
		struct ItemRequest
		{
			ItemPointer pointer;
			TSize size;
		} __attribute__((packed));

		struct IndexEntry
		{
			ItemHeader header;
			ItemPointer pointer;
		} __attribute__((packed));		
	};
};

#endif	// __FL_METIS_TYPES_HPP
