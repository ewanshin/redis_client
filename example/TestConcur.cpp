#include "TestConcur.hpp"

//#define NUM_DEF 150
#define NUM_DEF 0

CTestConcur::CTestConcur()
{
}

bool CTestConcur::StartTest(const std::string &strHost, int port)
{
	spdlog::set_async_mode(8192, spdlog::async_overflow_policy::block_retry,
		nullptr, std::chrono::seconds(1), nullptr);
	console_ = spdlog::stdout_color_mt("console");
	spdlog::set_level(spdlog::level::trace);
	spdlog::get("console")->debug("Logger created");
	spdlog::get("console")->flush();
    if (!m_redis.Initialize(strHost, port, 5, 5, 100))
    {
        //std::cout << "Connect to redis failed" << std::endl;
		console_->info("Connect to redis failed");
        return false;
    }

    if (!InitStringEnv(100, 10))
    {
        //std::cout << "Initialize environment failed" << std::endl;
		console_->info("Initialize environment failed");
        return false;
    }

    m_bExit = false;
    const int nGetTrdNum = 20;
    const int nSetTrdNum = 20;
	//const int nGetTrdNum = 1;
	//const int nSetTrdNum = 1;
	std::thread *pthreadGet[nGetTrdNum] = {nullptr};
    std::thread *pthreadSet[nSetTrdNum] = {nullptr};
    for (int i = 0; i < nGetTrdNum; ++i)
        pthreadGet[i] = new std::thread(std::bind(&CTestConcur::Test_Get, this));
    for (int i = 0; i < nSetTrdNum; ++i)
        pthreadSet[i] = new std::thread(std::bind(&CTestConcur::Test_Set, this));

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

    //std::cout << "TestFinish" << std::endl;
	console_->info("TestFinish");
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
				//std::cout << "No resource: " << time(nullptr) << std::endl;
				console_->error("No resource: ", time(nullptr));
			}
			else
			{
				//std::cout << "Get Failed: " << time(nullptr) << std::endl;
				console_->error("Get Failed: ", time(nullptr));
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
            if (++npc > NUM_DEF)
            {
                //std::cout << "Get OK: " << strVal << std::endl;
				std::stringstream stream;
				stream << std::this_thread::get_id();
				int thread_id = std::stoull(stream.str());
				spdlog::get("console")->trace("[thread:" + std::to_string(thread_id) + "]Get OK [key:" + ss.str() + "][value:" + strVal + "]");
                npc = 0;
            }
            m_mutex.unlock();
        }
        else
        {
			//gettimeofday(&tv, &tz);
			m_mutex.lock();
			if (++npc > NUM_DEF)
			{
				if (nRet == RC_NO_RESOURCE)
				{
					//std::cout << "No resource: " << tv.tv_usec << std::endl;
					spdlog::get("console")->error("No resource: " + strVal);
				}
				else
				{
					//std::cout << "Get Failed: " << std::endl;
					spdlog::get("console")->error("Get Failed: " + strVal);
				}
                npc = 0;
            }
            m_mutex.unlock();
            //m_bExit = true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (nIndex > 10)
            nIndex = 1;
    }
}

void CTestConcur::Test_Set()
{
	int nCounter = 2;
	int nIndex = 1;
	int npc = 0;
	//struct timeval tv;
	//struct timezone tz;
    while (!m_bExit)
    {
        std::stringstream ssKey, ssVal;
        ssKey << "tk_str_" << nIndex++;
        ssVal << "value_" << nCounter++;
        int nRet = m_redis.Set(ssKey.str(), ssVal.str());
        if (nRet == RC_SUCCESS)
        {
            m_mutex.lock();
            if (++npc > NUM_DEF)
            {
				std::stringstream stream;
				stream << std::this_thread::get_id();
				int thread_id = std::stoull(stream.str());
				spdlog::get("console")->debug("[thread:" + std::to_string(thread_id) + "]Set OK [key:" + ssKey.str() + "][value:" + ssVal.str() + "]");

                //std::cout << "Set OK" << std::endl;
                npc = 0;
            }
            m_mutex.unlock();
        }
        else
        {
            //gettimeofday(&tv, &tz);
            m_mutex.lock();
            if (++npc > NUM_DEF)
            {
				if (nRet == RC_NO_RESOURCE)
				{
					//std::cout << "No resource: " << tv.tv_usec << std::endl;
					spdlog::get("console")->debug("No resource: ");
				}
				else
				{
					//std::cout << "Get Failed: " << tv.tv_usec << std::endl;
					spdlog::get("console")->debug("Get Failed: ");
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
