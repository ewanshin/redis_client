#include <redis_client/RedisClient.hpp>
#include "TestMulti.hpp"

//#define NUM_DEF 150
//#define NUM_DEF 50
//#define NUM_DEF 0
const int print_interval = 0;

CTestMulti::CTestMulti()
{
}

bool CTestMulti::StartTest(const std::string &strHost, int port)
{
	CTestClient::StartTest(strHost, port);


	if (!m_redis.Initialize(strHost, port, 3, 3, 20))
	{
		log_error("Connect to redis failed [ip:", strHost, "][port:", port, "]");
		return false;
	}

	m_bExit = false;
	const int nMutliTrdNum = 1;
	std::thread *pthreadMulti[nMutliTrdNum] = { nullptr };
	for (int i = 0; i < nMutliTrdNum; ++i)
		pthreadMulti[i] = new std::thread(std::bind(&CTestMulti::Test_Multi, this));

	for (int i = 0; i < nMutliTrdNum; ++i)
	{
		if (pthreadMulti[i])
			pthreadMulti[i]->join();
	}

	log_info("TestFinish");
	return true;
}

void CTestMulti::Test_Multi()
{
	int nCounter = 2;
	int nIndex = 1;
	int test_count = 0;
	int npc = 0;
	int nRet = RC_SUCCESS;
	//struct timeval tv;
	//struct timezone tz;
	while (!m_bExit)
	{
		auto start_time = std::chrono::system_clock::now();
		std::stringstream ssKey, ssVal;
		ssKey << "tk_str_" << nIndex++;
		ssVal << "value_" << nCounter++;

		auto key = m_redis.HASH_SLOT(ssKey.str());

		log_trace("set " + ssKey.str() + " " + ssVal.str());
		nRet = m_redis.Set(ssKey.str(), ssVal.str());
		if (RC_SUCCESS != nRet)
		{
			log_error("set failed [error:", nRet, "]");
			return;
		}

		log_trace("del " + ssKey.str());
		nRet = m_redis.Del(ssKey.str());
		if (RC_SUCCESS != nRet)
		{
			log_error("del failed [error:", nRet, "]");
			return;
		}

		auto connection = m_redis.AttachConnection(key);
		if (nullptr == connection)
		{
			log_error("attach connection is null");
			return;
		}
		log_debug("attach connection [addr:", (long long)&connection, "]");

		log_trace("watch " + ssKey.str());
		nRet = m_redis.Watch(connection, ssKey.str());
		if (nRet != RC_SUCCESS)
		{
			log_error("watch failed [error:", nRet, "]");
			m_redis.Unwatch(connection, ssKey.str());
			m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);
			return;
		}

		log_trace("get " + ssKey.str());
		std::string return_value;
		nRet = m_redis.Get(connection, ssKey.str(), &return_value);
		if (nRet != RC_SUCCESS)
		{
			log_error("get failed [error:", nRet, "]");
			m_redis.Unwatch(connection, ssKey.str());
			m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);
			return;
		}

		log_trace("multi");
		nRet = m_redis.Multi(connection, ssKey.str());
		if (nRet != RC_SUCCESS)
		{
			log_error("multi failed [error:", nRet, "]");
			m_redis.Unwatch(connection, ssKey.str());
			m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);
			return;
		}

		log_trace("get " + ssKey.str());
		nRet = m_redis.Get(connection, ssKey.str(), &return_value);
		if (nRet != RC_SUCCESS)
		{
			log_error("get failed [error:", nRet);
			m_redis.Unwatch(connection, ssKey.str());
			m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);
			return;
		}

		log_trace("setnx " + ssKey.str() + " " + ssVal.str());
		nRet = m_redis.Setnx(connection, ssKey.str(), ssVal.str());
		if (nRet != RC_SUCCESS)
		{
			log_error("setnx failed [error:", nRet, "]");
			m_redis.Unwatch(connection, ssKey.str());
			m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);
			return;
		}

		log_trace("set " + ssKey.str() + " " + ssVal.str());
		nRet = m_redis.Set(connection, ssKey.str(), ssVal.str(), 1000);
		if (nRet != RC_SUCCESS)
		{
			log_error("set failed [error:", nRet, "]");
			m_redis.Unwatch(connection, ssKey.str());
			m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);
			return;
		}

		log_trace("expire " + ssKey.str() + " 3");
		long expire_return_val = 0;
		nRet = m_redis.Expire(connection, ssKey.str(), 3, &expire_return_val);
		if (nRet != RC_SUCCESS)
		{
			log_error("expire failed [error:", nRet, "]");
			m_redis.Unwatch(connection, ssKey.str());
			m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);
			return;
		}

		log_trace("get " + ssKey.str());
		nRet = m_redis.Get(connection, ssKey.str(), &return_value);
		if (nRet != RC_SUCCESS)
		{
			log_error("get failed [error:", nRet, "]");
			m_redis.Unwatch(connection, ssKey.str());
			m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);
			return;
		}

		log_trace("del " + ssKey.str());
		nRet = m_redis.Del(connection, ssKey.str());
		if (RC_SUCCESS != nRet)
		{
			log_error("del failed [error:", nRet, "]");
			m_redis.Unwatch(connection, ssKey.str());
			m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);
			return;
		}
		log_trace("exec ");
		RedisResult result;
		nRet = m_redis.Exec(connection, ssKey.str(), &result);
		if (nRet == RC_SUCCESS && REDIS_REPLY_NIL != result.type)
		{
			for (auto elm : result.m_arrayVal)
			{
				log_debug("elem [type:", elm.type, "][integer:", elm.m_llVal, "][string:", elm.m_strVal, "]");
			}
			m_mutex.lock();
			++test_count;
			if (++npc > print_interval)
			{
				auto end_time = std::chrono::system_clock::now();
				std::chrono::duration<double, std::ratio<1, 10>> diff;
				diff = end_time - start_time;
				//std::chrono::time_point<std::chrono::system_clock> end_time(std::chrono::system_clock::now());

				if ((long)1 == result.m_arrayVal[2].m_llVal && (long)1 == result.m_arrayVal[3].m_llVal)
				{
					log_debug("Exec OK [count:", test_count, "][Time:", diff.count(), "s][string:", result.m_arrayVal[3].m_strVal, "]");
				}
				else
				{
					log_error("Exec failed [count:", test_count, "][value:", result.m_arrayVal[2].m_llVal, "][Time:", diff.count(), "ms]");
				}

				npc = 0;
			}
			m_mutex.unlock();
			if (10 < test_count)
			{
				return;
			}

		}
		else
		{
			//m_redis.Unwatch(connection, ssKey.str());
			//m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);


			m_mutex.lock();
			if (++npc > print_interval)
			{
				if (nRet == RC_NO_RESOURCE)
				{
					log_error("No resource ");
				}
				else
				{
					log_error("Exec Failed: [error:", nRet, "]");
				}
				npc = 0;
			}
			m_mutex.unlock();
			//m_bExit = true;
		}
		m_redis.DetachConnection(m_redis.HASH_SLOT(ssKey.str()), connection);
		log_debug("detach connection [addr:", (long long)&connection, "]");
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		if (nIndex > 10)
			nIndex = 1;
		//m_mutex.unlock();
	}
}
