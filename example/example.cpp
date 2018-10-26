#include <stdio.h>
#include <stdlib.h>
//#include <string.h>
#include <string>

#include "hiredis/hiredis.h"
#include "TestBase.hpp"
#ifdef HIREDIS_WIN
#define snprintf sprintf_s
#endif

const int PORT = 30001;
const char *const AUTH_CMD = NULL;

int main(void) 
{
	std::string strHost = "10.75.17.135";
	unsigned int port = 30001;

	while (1)
	{
		CTestBase testBase;
		if (!testBase.StartTest(strHost, port))
			break;

		//CTestGeneric testKeys;
		//if (!testKeys.StartTest(strHost))
		//    break;

		/*		CTestString testStr;
		if (!testStr.StartTest(strHost))
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
		if (!testZset.StartTest(strHost))
		break;*/

		//CTestConcur testConcur;
		//if (!testConcur.StartTest(strHost))
		//    break;

		break;
	}
	return 0;
}