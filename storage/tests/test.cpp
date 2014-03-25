#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK 
#define BOOST_TEST_MAIN

#include <boost/test/unit_test.hpp>

struct InitTests {
    InitTests()
		{ 
			srand(time(NULL));
		}
    ~InitTests()  
		{ 
		}
};
BOOST_GLOBAL_FIXTURE( InitTests );

