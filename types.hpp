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
		typedef uint32_t TLevel;
		typedef uint32_t TSubLevel;
		typedef uint64_t TItemKey;
		typedef uint32_t TItemSize;
		typedef uint32_t TItemModTime;
		
		struct ItemHeader
		{
			TLevel level;
			TSubLevel subLevel;
			TItemKey itemKey;
			TItemSize size;
			union ModTimeTag
			{
				struct
				{
					u_int32_t op;
					TItemModTime modTime;
				};
				u_int64_t tag;
			};
			ModTimeTag timeTag;
		};
	};
};

#endif	// __FL_METIS_TYPES_HPP
