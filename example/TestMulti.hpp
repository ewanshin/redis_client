#ifndef _TEST_MULTI_H_
#define _TEST_MULTI_H_

#include "TestClient.hpp"
#include <spdlog/spdlog.h>
class CTestMulti : public CTestClient
{
public:
	CTestMulti();
	virtual bool StartTest(const std::string &strHost, int port);

private:
	void Test_Multi();

private:
	bool m_bExit;
	std::mutex m_mutex;
};

#endif
