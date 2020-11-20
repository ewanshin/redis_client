#include <WinSock2.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <hiredis/hiredis.h>
#include "TestBase.hpp"
#include "TestGeneric.hpp"
#include "TestString.hpp"
#include "TestList.hpp"
#include "TestSet.hpp"
#include "TestHash.hpp"
#include "TestZset.hpp"
#include "TestConcur.hpp"
#include "TestMulti.hpp"

#ifdef HIREDIS_WIN
#define snprintf sprintf_s
#endif
#include <spdlog/spdlog.h>

const int PORT = 17001;
const char *const AUTH_CMD = NULL;

int main(void) 
{
	std::string strHost = "127.0.0.1";
	unsigned int port = 30001;
	//std::string strHost = "10.113.113.36";	
	//unsigned int port = 17001;

	while (1)
	{
		//CTestBase testBase;
		//if (!testBase.StartTest(strHost, port))
		//	break;

		//CTestGeneric testKeys;
		//if (!testKeys.StartTest(strHost))
		//    break;

/*		CTestString testStr;
		if (!testStr.StartTest(strHost, port))
			break;*/

		//CTestList testList;
		//if (!testList.StartTest(strHost))
		//    break;

		//CTestSet testSet;
		//if (!testSet.StartTest(strHost))
		//    break;

		//CTestHash testHash;
		//if (!testHash.StartTest(strHost))
		//    break;

/*		CTestZset testZset;
		if (!testZset.StartTest(strHost, port))
			break;*/

		//CTestConcur testConcur;
		//if (!testConcur.StartTest(strHost, port))
		//	break;

		CTestMulti testMulti;
		if (!testMulti.StartTest(strHost, port))
			break;

		if (0 == getchar())
			return 0;
	}
	return 0;
}
