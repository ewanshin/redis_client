#include "stdafx.h"
#include "TestConcur.hpp"

//#define NUM_DEF 150
//#define NUM_DEF 50
//#define NUM_DEF 0
const int print_interval = 0;

CTestConcur::CTestConcur()
{
}

bool CTestConcur::StartTest(const std::string &strHost, int port)
{
	CTestClient::StartTest(strHost, port);


	//if (!m_redis.Initialize(strHost, port, 3, 3, 20))
	if (!m_redis.Initialize(strHost, port, 3, 3, 20))
	{
		log_error("Connect to redis failed [ip:", strHost, "][port:", port, "]");
		return false;
	}

	if (!InitStringEnv(100, 10))
	{
		log_error("Initialize environment failed [ip:", strHost, "][port:", port, "]");
		return false;
	}

	m_bExit = false;
	const int nGetTrdNum = 5;
	const int nSetTrdNum = 5;
	const int nMutliTrdNum = 1;
	//const int nGetTrdNum = 1;
	//const int nSetTrdNum = 1;
	//const int nMutliTrdNum = 1;
	std::thread *pthreadGet[nGetTrdNum] = {nullptr};
	std::thread *pthreadSet[nSetTrdNum] = { nullptr };
	std::thread *pthreadMulti[nMutliTrdNum] = { nullptr };
	for (int i = 0; i < nGetTrdNum; ++i)
	    pthreadGet[i] = new std::thread(std::bind(&CTestConcur::Test_Get, this));
	for (int i = 0; i < nSetTrdNum; ++i)
		pthreadSet[i] = new std::thread(std::bind(&CTestConcur::Test_Set, this));
	for (int i = 0; i < nMutliTrdNum; ++i)
		pthreadMulti[i] = new std::thread(std::bind(&CTestConcur::Test_Multi, this));

	for (int i = 0; i < nGetTrdNum; ++i)
	{
	    if (pthreadGet[i])
	        pthreadGet[i]->join();
	}
	for (int i = 0; i < nSetTrdNum; ++i)
	{
		if (pthreadSet[i])
			pthreadSet[i]->join();
	}
	for (int i = 0; i < nMutliTrdNum; ++i)
	{
		if (pthreadMulti[i])
			pthreadMulti[i]->join();
	}

	log_info("TestFinish");
	return true;
}

void CTestConcur::Test_GetS()
{
	std::string strVal;
	while (!m_bExit)
	{
		int nRet = m_redis.Get("tk_str_1", &strVal);
		if (nRet != RC_SUCCESS)
		{
			if (nRet == RC_NO_RESOURCE)
			{
				log_error("No resource: ", time(nullptr));
			}
			else
			{
				log_error("Get Failed [error:", nRet, "]");
			}
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
}

void CTestConcur::Test_Get()
{
	//struct timeval tv;
	//struct timezone tz;

	int nIndex = 1;
	int test_count = 0;
	int npc = 0;
	while (!m_bExit)
	{
		std::string strVal;
		std::stringstream ss;
		ss << "tk_str_" << nIndex++;
		int nRet = m_redis.Get(ss.str(), &strVal);
		if (nRet == RC_SUCCESS)
		{
			m_mutex.lock();
			if (++npc > print_interval)
			{
				log_trace("Get OK [key:", ss.str(), "][value:", strVal, "]");
				npc = 0;
			}
			++test_count;
			m_mutex.unlock();
			if (150 < test_count)
			{
				return;
			}
		}
		else
		{
			//gettimeofday(&tv, &tz);
			m_mutex.lock();
			if (++npc > print_interval)
			{
				if (nRet == RC_NO_RESOURCE)
				{
					log_error("No resource : ", strVal);
				}
				else
				{
					log_error("CTestConcur::Test_Get Failed:", strVal, "][errorcode:", nRet, "]");
				}
				npc = 0;
			}
			m_mutex.unlock();
			//m_bExit = true;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		if (nIndex > 10)
			nIndex = 1;
	}
}

void CTestConcur::Test_Set()
{
	int nCounter = 2;
	int nIndex = 1;
	int test_count = 0;
	int npc = 0;
	//struct timeval tv;
	//struct timezone tz;
	while (!m_bExit)
	{
		auto start_time = std::chrono::system_clock::now();
		std::stringstream ssKey, ssVal;
		ssKey << "tk_str_" << nIndex++;
		ssVal << "value_" << nCounter++;

		int nRet = m_redis.Set(ssKey.str(), ssVal.str());
		if (nRet == RC_SUCCESS)
		{
			m_mutex.lock();
			if (++npc > print_interval)
			{
				auto end_time = std::chrono::system_clock::now();
				std::chrono::duration<double, std::ratio<1, 10>> diff;
				diff = end_time - start_time;
				log_debug("Set OK [key:", ssKey.str(), "][value:", ssVal.str(), "][Time:", diff.count(), "ms]");
				npc = 0;
			}
			++test_count;
			m_mutex.unlock();
			if (150 < test_count)
			{
				return;
			}

		}
		else
		{
			//gettimeofday(&tv, &tz);
			m_mutex.lock();
			if (++npc > print_interval)
			{	
				if (nRet == RC_NO_RESOURCE)
				{
					log_error("No resource ");
				}
				else
				{
					log_error("Set Failed: [error:", nRet, "]");
				}
				npc = 0;
			}
			m_mutex.unlock();
			//m_bExit = true;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		if (nIndex > 10)
			nIndex = 1;
	}
}

void CTestConcur::Test_Multi()
{
	int nCounter = 2;
	int nIndex = 1;
	int test_count = 0;
	int npc = 0;
	//struct timeval tv;
	//struct timezone tz;
	while (!m_bExit)
	{
		auto start_time = std::chrono::system_clock::now();
		std::stringstream ssKey, ssVal;
		ssKey << "tk_str_" << nIndex++;
		ssVal << "value_" << nCounter++;

		auto key = m_redis.HASH_SLOT(ssKey.str());
		//auto connection = m_redis.AttachConnection(m_redis.HASH_SLOT(ssKey.str()));
		auto connection = m_redis.AttachConnection(key);
		if (nullptr == connection)
		{
			log_error("attach connection is null");
			return;
		}
		int nRet = m_redis.Watch(connection, ssKey.str());
		if (nRet != RC_SUCCESS)
		{
			log_error("watch failed [error:", nRet, "]");
			m_redis.Unwatch(connection, ssKey.str());
			m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);
			return;
		}
		nRet = m_redis.Multi(connection, ssKey.str());
		if (nRet != RC_SUCCESS)
		{
			log_error("multi failed [error:", nRet, "]");
			m_redis.Unwatch(connection, ssKey.str());
			m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);
			return;
		}

		nRet = m_redis.Set(ssKey.str(), ssVal.str());
		if (nRet != RC_SUCCESS)
		{
			log_error("multi failed [error:", nRet, "]");
			m_redis.Unwatch(connection, ssKey.str());
			m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);
			return;
		}

		nRet = m_redis.Exec(connection, ssKey.str());

		if (nRet == RC_SUCCESS)
		{
			m_mutex.lock();
			++test_count;
			if (++npc > print_interval)
			{
				auto end_time = std::chrono::system_clock::now();
				std::chrono::duration<double, std::ratio<1, 10>> diff;
				diff = end_time - start_time;
				//std::chrono::time_point<std::chrono::system_clock> end_time(std::chrono::system_clock::now());
				log_debug("Multi OK [count:", test_count, "][key:", ssKey.str(), "][value:", ssVal.str(), "][Time:", diff.count(), "ms]");
				npc = 0;
			}
			m_mutex.unlock();
			if (150 < test_count)
			{
				return;
			}

		}
		else
		{
			m_redis.Unwatch(connection, ssKey.str());
			m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);

			//gettimeofday(&tv, &tz);
			m_mutex.lock();
			if (++npc > print_interval)
			{
				if (nRet == RC_NO_RESOURCE)
				{
					//std::cout << "No resource: " << tv.tv_usec << std::endl;
					log_error("No resource ");
				}
				else
				{
					//std::cout << "Get Failed: " << tv.tv_usec << std::endl;
					log_error("Multi Failed: [error:", nRet, "]");
				}
				npc = 0;
			}
			m_mutex.unlock();
			//m_bExit = true;
		}
		m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		if (nIndex > 10)
			nIndex = 1;
		//m_mutex.unlock();
	}
}
