#include "TestClient.hpp"
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/async.h>

CTestClient::CTestClient()
{
}

CTestClient::~CTestClient()
{
	//spdlog::shutdown();
}

bool CTestClient::PrintResult(const std::string &strCmd, bool bSuccess)
{
	std::cout << "test " << strCmd << (bSuccess ? " success" : " failed") << std::endl;
	return bSuccess;
}

bool IsInContainer(const std::set<std::string> &setVal, const std::string &strVal)
{
	return setVal.find(strVal) != setVal.end();
}

bool CTestClient::InitStringEnv(int nDel, int nSet)
{
	int nRet = RC_SUCCESS;
	for (int i = 0; i < nDel && nRet >= 0; ++i)
	{
		std::stringstream ss;
		ss << "tk_str_" << i + 1;

		auto connection = m_redis.AttachConnection(m_redis.HASH_SLOT(ss.str()));
		RedisResult result;
		nRet = m_redis.Del(connection, ss.str(), &result);
		//log_debug("CTestClient::InitStringEnv [Del tk_str_", (i + 1), "]");
		log_debug("CTestClient::InitStringEnv [Del tk_str_", std::to_string(i), "]");
		m_redis.DetachConnection(m_redis.HASH_SLOT(ss.str()), connection);
	}

	for (int i = 0; i < nSet && nRet >= 0; ++i)
	{
		std::stringstream ssKey, ssVal;
		auto connection = m_redis.AttachConnection(m_redis.HASH_SLOT(ssKey.str()));
		ssKey << "tk_str_" << i + 1;
		ssVal << "value_" << i + 1;
		nRet = m_redis.Set(connection, ssKey.str(), ssVal.str());
		log_debug("CTestClient::InitStringEnv [Set ", ssKey.str(), " ", ssVal.str(), "]");
		m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);
	}

	return nRet >= 0;
}

bool CTestClient::InitListEnv(int nDel, int nSet)
{
	int nRet = RC_SUCCESS;
	//for (int i = 0; i < nDel && nRet >= 0; ++i)
	//{
	//	std::stringstream ss;
	//	ss << "tk_list_" << i + 1;
	//	nRet = m_redis.Del(ss.str());
	//}

	//for (int i = 0; i < nSet && nRet >= 0; ++i)
	//{
	//	std::stringstream ssKey;
	//	ssKey << "tk_list_" << i + 1;
	//	for (int j = 0; j < i + 1 && nRet >= 0; ++j)
	//	{
	//		std::stringstream ssVal;
	//		ssVal << "value_" << j + 1;
	//		nRet = m_redis.Rpush(ssKey.str(), ssVal.str());
	//	}
	//}
	return nRet >= 0;
}

bool CTestClient::InitSetEnv(int nDel, int nSet)
{
	int nRet = RC_SUCCESS;
	//for (int i = 0; i < nDel && nRet >= 0; ++i)
	//{
	//	std::stringstream ss;
	//	ss << "tk_set_" << i + 1;
	//	nRet = m_redis.Del(ss.str());
	//}

	//for (int i = 0; i < nSet && nRet >= 0; ++i)
	//{
	//	std::stringstream ssKey;
	//	ssKey << "tk_set_" << i + 1;
	//	for (int j = 0; j < i + 1 && nRet >= 0; ++j)
	//	{
	//		std::stringstream ssVal;
	//		ssVal << "value_" << j + 1;
	//		nRet = m_redis.Sadd(ssKey.str(), ssVal.str());
	//	}
	//}
	return nRet >= 0;
}

bool CTestClient::InitZsetEnv(int nDel, int nSet)
{
	int nRet = RC_SUCCESS;
	//for (int i = 0; i < nDel && nRet >= 0; ++i)
	//{
	//	std::stringstream ss;
	//	ss << "tk_zset_" << i + 1;
	//	nRet = m_redis.Del(ss.str());
	//}

	//for (int i = 0; i < nSet && nRet >= 0; ++i)
	//{
	//	std::stringstream ssKey;
	//	ssKey << "tk_zset_" << i + 1;
	//	for (int j = 0; j < i + 1 && nRet >= 0; ++j)
	//	{
	//		std::stringstream ssVal;
	//		ssVal << "value_" << j + 1;
	//		nRet = m_redis.Zadd(ssKey.str(), j + 1, ssVal.str());
	//	}
	//}
	return nRet >= 0;
}

bool CTestClient::InitHashEnv(int nDel, int nSet)
{
	int nRet = RC_SUCCESS;
	//for (int i = 0; i < nDel && nRet >= 0; ++i)
	//{
	//	std::stringstream ss;
	//	ss << "tk_hash_" << i + 1;
	//	nRet = m_redis.Del(ss.str());
	//}

	//for (int i = 0; i < nSet && nRet >= 0; ++i)
	//{
	//	std::stringstream ssKey;
	//	ssKey << "tk_hash_" << i + 1;
	//	for (int j = 0; j < i + 1 && nRet >= 0; ++j)
	//	{
	//		std::stringstream ssFld;
	//		std::stringstream ssVal;
	//		ssFld << "field_" << j + 1;
	//		ssVal << "value_" << j + 1;
	//		nRet = m_redis.Hset(ssKey.str(), ssFld.str(), ssVal.str());
	//	}
	//}
	return nRet >= 0;
}

bool CTestClient::GetTime(struct timeval &tmVal)
{
	//return m_redis.Time(&tmVal) == RC_SUCCESS;
	return false;
}

bool CTestClient::StartTest(const std::string &strHost, int port)
{
	console_logger_ = spdlog::stdout_color_mt("console");
	file_logger_ = spdlog::daily_logger_mt<spdlog::async_factory>("result", "result.log");

	spdlog::set_level(spdlog::level::trace);
	log_debug("Logger created");
	return true;
}
