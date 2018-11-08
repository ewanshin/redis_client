#ifndef TEST_CONCUR_H
#define TEST_CONCUR_H

#include "TestClient.hpp"
#include <spdlog/spdlog.h>
class CTestConcur : public CTestClient
{
public:
    CTestConcur();
	~CTestConcur()
	{
		int a = 0;
		a++;
		a++;
	};
	bool StartTest(const std::string &strHost, int port);

	std::shared_ptr<spdlog::logger> console_;
	std::shared_ptr<spdlog::logger> file_logger_;

private:
    void Test_GetS();
    void Test_Get();
    void Test_Set();

private:
	bool m_bExit;
	std::mutex m_mutex;
};

#endif
