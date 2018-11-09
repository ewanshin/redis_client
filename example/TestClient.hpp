#ifndef TEST_CLIENT_H
#define TEST_CLIENT_H

#include "redis_client/RedisClient.hpp"
#include <iostream>
//#include <unistd.h>
#include <time.h>
//#include <cmath>
#include <algorithm>
#include <spdlog/spdlog.h>

#define FLOAT_ZERO 0.000001

template <typename T>
bool IsInContainer(const T &container, const typename T::value_type &value)
{
    return std::find(container.begin(), container.end(), value) != container.end();
}

template <typename K, typename V>
bool IsPairInMap(const K &key, const V &val, const std::map<K, V> &mapVal)
{
    auto iter = mapVal.find(key);
    if (iter == mapVal.end() || iter->second != val)
        return false;
    return true;
}

template <> bool IsInContainer(const std::set<std::string> &setVal, const std::string &strVal);

class CTestClient
{
public:
    CTestClient();

	template<typename ... Args>	inline void log_trace(Args const& ... args) { log(spdlog::level::trace, args...); }
	template<typename ... Args>	inline void log_debug(Args const& ... args) { log(spdlog::level::debug, args...); }
	template<typename ... Args>	inline void log_info(Args const& ... args) { log(spdlog::level::info, args...); }
	template<typename ... Args>	inline void log_warn(Args const& ... args) { log(spdlog::level::warn, args...); }
	template<typename ... Args>	inline void log_error(Args const& ... args) { log(spdlog::level::err, args...); }
	template<typename ... Args>	inline void log_critical(Args const& ... args) { log(spdlog::level::critical, args...); }

	template<typename ... Args>
	void log(spdlog::level::level_enum level, Args const& ... args)
	{
		std::stringstream thread_stream;
		thread_stream << std::this_thread::get_id();
		unsigned int thread_id = std::stoull(thread_stream.str());

		std::ostringstream stream;
		using List = int[];
		//stream << "[thread:" << thread_id << "]";
		//(void)List{
		//	3, ((void)(stream << args), 0) ...
		//};
		(void)List{
			0, ((void)(stream << args), 0) ...
		};

		//console_logger_->log(level, stream.str());
		//file_logger_->log(level, stream.str());
		spdlog::get("console")->log(level, stream.str());
		spdlog::get("result")->log(level, stream.str());
	}

protected:
	virtual bool StartTest(const std::string &strHost, int port);

    static bool PrintResult(const std::string &strCmd, bool bSuccess);
    bool InitStringEnv(int nDel, int nSet);
    bool InitListEnv(int nDel, int nSet);
    bool InitSetEnv(int nDel, int nSet);
    bool InitZsetEnv(int nDel, int nSet);
    bool InitHashEnv(int nDel, int nSet);
    bool GetTime(struct timeval &tmVal);

protected:
    CRedisClient m_redis;
	std::shared_ptr<spdlog::logger> console_logger_;
	std::shared_ptr<spdlog::logger> file_logger_;
};

#endif
