#ifndef REDIS_CLIENT_H
#define REDIS_CLIENT_H

#include "hiredis/hiredis.h"
#include <string>
#include <vector>
#include <list>
#include <map>
#include <set>
#include <queue>
#include <sstream>
#include <functional>
#include <thread>
#include <condition_variable>
#include <iostream>
#include <algorithm>
#include <string.h>
#include <synchapi.h>
#include <atomic>

#define RC_RESULT_EOF       5
#define RC_NO_EFFECT        4
#define RC_OBJ_NOT_EXIST    3
#define RC_OBJ_EXIST        2
#define RC_PART_SUCCESS     1
#define RC_SUCCESS          0
#define RC_PARAM_ERR        -1
#define RC_REPLY_ERR        -2
#define RC_RQST_ERR         -3
#define RC_NO_RESOURCE      -4
#define RC_NOT_SUPPORT      -6
#define RC_SLOT_CHANGED     -100

#define RQST_RETRY_TIMES    3
#define WAIT_RETRY_TIMES    60

#define FUNC_DEF_CONV       [](int nRet, redisReply *) { return nRet; }

typedef std::function<int (redisReply *)> TFuncFetch;
typedef std::function<int (int, redisReply *)> TFuncConvert;

class RedisResult
{
public:
	RedisResult()
	{
		Clear();
	}
	enum Type
	{
		NIL = 0,
		INTEGER,
		STRING,
		ARRAY,
	};

	void Clear()
	{
		type = NIL;
		memset(m_strVal, '\0', 1024);
		//m_stringVal.clear();
		m_llVal = 0;
		m_arrayVal.clear();

	}

	Type type = NIL;
	//std::string					m_stringVal;
	char						m_strVal[1024];
	long						m_llVal = 0;
	std::vector<RedisResult>	m_arrayVal;
};

class CSafeLock
{
public:
	CSafeLock(PSRWLOCK pLock) : m_pLock(pLock), m_bLocked(false) {}
	//~CSafeLock() {
	//	WriteUnlock();
	//	ReadUnlock();
	//}
	~CSafeLock() {};

	inline bool ReadLock() 
	{
		AcquireSRWLockShared(m_pLock);
		m_bLocked = true;
		return m_bLocked;
	}

	inline bool WriteLock()
	{
		AcquireSRWLockExclusive(m_pLock);
		m_bLocked = true;
		return m_bLocked;
	}

	inline bool TryReadLock(){ return (m_bLocked = (TryAcquireSRWLockShared(m_pLock) == TRUE)); }
	inline bool TryWriteLock(){ return (m_bLocked = (TryAcquireSRWLockExclusive(m_pLock) == TRUE)); }

	inline void WriteUnlock() { if (true == m_bLocked) ReleaseSRWLockExclusive(m_pLock); };
	inline void ReadUnlock() { if (m_bLocked)ReleaseSRWLockShared(m_pLock); }

	inline void lock() { WriteLock(); }
	inline void unlock() { WriteUnlock(); }

private:
	PSRWLOCK m_pLock = nullptr;
	bool m_bLocked;
};

class CRedisServer;
struct SlotRegion
{
    int nStartSlot;
    int nEndSlot;
    std::string strHost;
    int nPort;
    CRedisServer *pRedisServ;
};

class CRedisCommand
{
public:
    CRedisCommand(const std::string &strCmd, bool bShareMem = true);
    virtual ~CRedisCommand() { ClearArgs(); }
    void ClearArgs();
    void DumpArgs() const;
    void DumpReply() const;

    int GetSlot() const { return m_nSlot; }
    const redisReply * GetReply() const { return m_pReply; }
    std::string FetchErrMsg() const;
    bool IsMovedErr() const;

    void SetSlot(int nSlot) { m_nSlot = nSlot; }
    void SetConvFunc(TFuncConvert funcConv) { m_funcConv = funcConv; }

    void SetArgs();
    void SetArgs(const std::string &strArg);
    void SetArgs(const std::vector<std::string> &vecArg);
    void SetArgs(const std::string &strArg1, const std::string &strArg2);
    void SetArgs(const std::string &strArg1, const std::vector<std::string> &vecArg2);
    void SetArgs(const std::string &strArg1, const std::set<std::string> &setArg2);
    void SetArgs(const std::vector<std::string> &vecArg1, const std::string &strArg2);
    void SetArgs(const std::vector<std::string> &vecArg1, const std::vector<std::string> &vecArg2);
    void SetArgs(const std::map<std::string, std::string> &mapArg);
    void SetArgs(const std::string &strArg1, const std::map<std::string, std::string> &mapArg2);
    void SetArgs(const std::string &strArg1, const std::string &strArg2, const std::string &strArg3);
    void SetArgs(const std::string &strArg1, const std::string &strArg2, const std::vector<std::string> &vecArg2);
    void SetArgs(const std::string &strArg1, const std::vector<std::string> &vecArg2, const std::vector<std::string> &vecArg3);
    void SetArgs(const std::string &strArg1, const std::string &strArg2, const std::string &strArg3, const std::string &strArg4);

    int CmdRequest(redisContext *pContext);
    int CmdAppend(redisContext *pContext);
    int CmdReply(redisContext *pContext);
    int FetchResult(const TFuncFetch &funcFetch);

private:
    void InitMemory(int nArgs);
    void AppendValue(const std::string &strVal);

protected:
    std::string m_strCmd;
    bool m_bShareMem;

    int m_nArgs;
    int m_nIdx;
    char **m_pszArgs;
    size_t *m_pnArgsLen;
    redisReply *m_pReply;

    int m_nSlot;
    TFuncConvert m_funcConv;
};

class CRedisServer;
class CRedisConnection
{
public:
    CRedisConnection(CRedisServer *pRedisServ);
    ~CRedisConnection();
    bool IsValid() { return m_pContext != nullptr; }
    int ConnRequest(CRedisCommand *pRedisCmd);
    int ConnRequest(std::vector<CRedisCommand *> &vecRedisCmd);

private:
    bool ConnectToRedis(const std::string &strHost, int nPort, int nTimeout);
    bool Reconnect();

private:
    redisContext *m_pContext;
    time_t m_nUseTime;
    CRedisServer *m_pRedisServ;
};

class CRedisServer
{
    friend class CRedisConnection;
    friend class CRedisClient;
public:
    CRedisServer(const std::string &strHost, int nPort, int nClientTimeout, int nServerTimeout,int nConnNum);
    virtual ~CRedisServer();

    void SetSlave(const std::string &strHost, int nPort);

    std::string GetHost() const { return m_strHost; }
    int GetPort() const { return m_nPort; }
	bool IsValid() const { return m_queIdleConn.size() > 0 ? true : false; }

    // for the blocking request
    int ServRequest(CRedisCommand *pRedisCmd);
	int ServRequest(CRedisConnection* connection, CRedisCommand *pRedisCmd);

private:
    bool Initialize();
    CRedisConnection *FetchConnection();
    void ReturnConnection(CRedisConnection *pRedisConn);
    void CleanConn();

private:
	std::string m_strHost;
	int m_nPort;
	int m_nCliTimeout;
	int m_nSerTimeout;
	int m_nConnNum;

    std::queue<CRedisConnection *> m_queIdleConn;
    std::vector<std::pair<std::string, int> > m_vecHosts;
    std::mutex m_mutexConn;
	std::condition_variable _wait;
};

class CRedisClient
{
public:
	CRedisClient();
	~CRedisClient();

	bool Initialize(const std::string &strHost, int nPort, int nClientTimeout, int nServerTimeout, int nConnNum);
	bool IsCluster() { return m_bCluster; }

	CRedisConnection* AttachConnection(int slot);
	void DetachConnection(int slot, CRedisConnection* connection);
	uint32_t HASH_SLOT(const std::string &strKey);

	/* interfaces for generic */
	int Del(const std::string &strKey, long *pnVal = nullptr);
	int Del(CRedisConnection* connection, const std::string &strKey, long *pnVal = nullptr);
	int Dump(const std::string &strKey, std::string *pstrVal);
	int Exists(const std::string &strKey, long *pnVal);
	int Expire(const std::string &strKey, long nSec, long *pnVal = nullptr);
	int Expire(CRedisConnection* connection, const std::string &strKey, long nSec, long *pnVal = nullptr);
	int Expireat(const std::string &strKey, long nTime, long *pnVal = nullptr);
	int Keys(const std::string &strPattern, std::vector<std::string> *pvecVal);
	int Persist(const std::string &strKey, long *pnVal = nullptr);
	int Pexpire(const std::string &strKey, long nMilliSec, long *pnVal = nullptr);
	int Pexpireat(const std::string &strKey, long nMilliTime, long *pnVal = nullptr);
	int Pttl(const std::string &strKey, long *pnVal);
	int Randomkey(std::string *pstrVal);
	int Rename(const std::string &strKey, const std::string &strNewKey);
	int Renamenx(const std::string &strKey, const std::string &strNewKey);
	int Restore(const std::string &strKey, long nTtl, const std::string &strVal);
	int Scan(long *pnCursor, const std::string &strPattern, long nCount, std::vector<std::string> *pvecVal);
	int Ttl(const std::string &strKey, long *pnVal);
	int Type(const std::string &strKey, std::string *pstrVal);

	/* interfaces for string */
	int Append(const std::string &strKey, const std::string &strVal, long *pnVal = nullptr);
	int Bitcount(const std::string &strKey, long *pnVal);
	int Bitcount(const std::string &strKey, long nStart, long nEnd, long *pnVal);
	int Bitop(const std::string &strDestKey, const std::string &strOp, const std::vector<std::string> &vecKey, long *pnVal = nullptr);
	int Bitpos(const std::string &strKey, long nBitVal, long *pnVal);
	int Bitpos(const std::string &strKey, long nBitVal, long nStart, long nEnd, long *pnVal);
	int Decr(const std::string &strKey, long *pnVal = nullptr);
	int Decrby(const std::string &strKey, long nDecr, long *pnVal = nullptr);
	int Get(const std::string &strKey, std::string *pstrVal);
	int Get(CRedisConnection* connection, const std::string &strKey, std::string *pstrVal);
	int Getbit(const std::string &strKey, long nOffset, long *pnVal);
	int Getrange(const std::string &strKey, long nStart, long nEnd, std::string *pstrVal);
	int Getset(const std::string &strKey, std::string *pstrVal);
	int Incr(const std::string &strKey, long *pnVal);
	int Incrby(const std::string &strKey, long nIncr, long *pnVal);
	int Incrbyfloat(const std::string &strKey, double dIncr, double *pdVal);
	int Mget(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal);
	int Mset(const std::vector<std::string> &vecKey, const std::vector<std::string> &vecVal);
	int Psetex(const std::string &strKey, long nMilliSec, const std::string &strVal);
	int Set(const std::string &strKey, const std::string &strVal, unsigned int expired = 0);
	int Set(CRedisConnection* connection, const std::string &strKey, const std::string &strVal, unsigned int expired = 0);
	int Setbit(const std::string &strKey, long nOffset, bool bVal);
	int Setex(const std::string &strKey, long nSec, const std::string &strVal);
	int Setex(CRedisConnection* connection, const std::string &strKey, long nSec, const std::string &strVal);
	int Setnx(const std::string &strKey, const std::string &strVal);
	int Setnx(CRedisConnection* connection, const std::string &strKey, const std::string &strVal);
	int Setrange(const std::string &strKey, long nOffset, const std::string &strVal, long *pnVal = nullptr);
	int Strlen(const std::string &strKey, long *pnVal);

	/* interfaces for list */
	int Blpop(const std::string &strKey, long nTimeout, std::vector<std::string> *pvecVal);
	int Blpop(const std::vector<std::string> &vecKey, long nTimeout, std::vector<std::string> *pvecVal);
	int Brpop(const std::string &strKey, long nTimeout, std::vector<std::string> *pvecVal);
	int Brpop(const std::vector<std::string> &vecKey, long nTimeout, std::vector<std::string> *pvecVal);
	int Lindex(const std::string &strKey, long nIndex, std::string *pstrVal);
	int Linsert(const std::string &strKey, const std::string &strPos, const std::string &strPivot, const std::string &strVal, long *pnVal);
	int Llen(const std::string &strKey, long *pnVal);
	int Lpop(const std::string &strKey, std::string *pstrVal);
	int Lpush(const std::string &strKey, const std::string &strVal, long *pnVal = nullptr);
	//int Lpush(const std::string &strKey, const std::vector<std::string> &vecVal);
	int Lpushx(const std::string &strKey, const std::string &strVal, long *pnVal = nullptr);
	int Lrange(const std::string &strKey, long nStart, long nStop, std::vector<std::string> *pvecVal);
	int Lrem(const std::string &strKey, long nCount, const std::string &strVal, long *pnVal = nullptr);
	int Lset(const std::string &strKey, long nIndex, const std::string &strVal);
	int Ltrim(const std::string &strKey, long nStart, long nStop);
	int Rpop(const std::string &strKey, std::string *pstrVal);
	int Rpush(const std::string &strKey, const std::string &strVal, long *pnVal = nullptr);
	//int Rpush(const std::string &strKey, const std::vector<std::string> &vecVal);
	int Rpushx(const std::string &strKey, const std::string &strVal, long *pnVal = nullptr);

	/* interfaces for set */
	int Sadd(const std::string &strKey, const std::string &strVal, long *pnVal = nullptr);
	int Scard(const std::string &strKey, long *pnVal);
	//int Sdiff(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal);
	//int Sinter(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal);
	int Sismember(const std::string &strKey, const std::string &strVal, long *pnVal);
	int Smembers(const std::string &strKey, std::vector<std::string> *pvecVal);
	int Spop(const std::string &strKey, std::string *pstrVal);
	//int Srandmember(const std::string &strKey, long nCount, std::vector<std::string> *pvecVal);
	int Srem(const std::string &strKey, const std::string &strVal, long *pnVal = nullptr);
	int Srem(const std::string &strKey, const std::vector<std::string> &vecVal, long *pnVal = nullptr);
	//int Sunion(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal);

	/* interfaces for hash */
	int Hdel(const std::string &strKey, const std::string &strField, long *pnVal = nullptr);
	int Hexists(const std::string &strKey, const std::string &strField, long *pnVal);
	int Hget(const std::string &strKey, const std::string &strField, std::string *pstrVal);
	int Hgetall(const std::string &strKey, std::map<std::string, std::string> *pmapFv);
	int Hincrby(const std::string &strKey, const std::string &strField, long nIncr, long *pnVal);
	int Hincrbyfloat(const std::string &strKey, const std::string &strField, double dIncr, double *pdVal);
	int Hkeys(const std::string &strKey, std::vector<std::string> *pvecVal);
	int Hlen(const std::string &strKey, long *pnVal);
	int Hmget(const std::string &strKey, const std::vector<std::string> &vecField, std::vector<std::string> *pvecVal);
	int Hmget(const std::string &strKey, const std::vector<std::string> &vecField, std::map<std::string, std::string> *pmapVal);
	//int Hmget(const std::string &strKey, const std::set<std::string> &setField, std::map<std::string, std::string> *pmapVal);
	int Hmset(const std::string &strKey, const std::vector<std::string> &vecField, const std::vector<std::string> &vecVal);
	int Hmset(const std::string &strKey, const std::map<std::string, std::string> &mapFv);
	//int Hscan(const std::string &strKey, long *pnCursor, const std::string &strMatch, long nCount, std::vector<std::string> *pvecVal);
	int Hset(const std::string &strKey, const std::string &strField, const std::string &strVal);
	int Hsetnx(const std::string &strKey, const std::string &strField, const std::string &strVal);
	int Hvals(const std::string &strKey, std::vector<std::string> *pvecVal);

	/* interfaces for sorted set */
	int Zadd(const std::string &strKey, double dScore, const std::string &strElem, long *pnVal = nullptr);
	int Zcard(const std::string &strKey, long *pnVal);
	int Zcount(const std::string &strKey, double dMin, double dMax, long *pnVal);
	int Zincrby(const std::string &strKey, double dIncr, const std::string &strElem, double *pdVal);
	int Zlexcount(const std::string &strKey, const std::string &strMin, const std::string &strMax, long *pnVal);
	int Zrange(const std::string &strKey, long nStart, long nStop, std::vector<std::string> *pvecVal);
	int Zrangewithscore(const std::string &strKey, long nStart, long nStop, std::map<std::string, std::string> *pmapVal);
	int Zrangebylex(const std::string &strKey, const std::string &strMin, const std::string &strMax, std::vector<std::string> *pvecVal);
	int Zrangebyscore(const std::string &strKey, double dMin, double dMax, std::vector<std::string> *pvecVal);
	int Zrangebyscore(const std::string &strKey, double dMin, double dMax, std::map<std::string, double> *pmapVal);
	int Zrank(const std::string &strKey, const std::string &strElem, long *pnVal);
	int Zrem(const std::string &strKey, const std::string &strElem, long *pnVal = nullptr);
	int Zrem(const std::string &strKey, const std::vector<std::string> &vecElem, long *pnVal = nullptr);
	int Zremrangebylex(const std::string &strKey, const std::string &strMin, const std::string &strMax, long *pnVal = nullptr);
	int Zremrangebyrank(const std::string &strKey, long nStart, long nStop, long *pnVal = nullptr);
	int Zremrangebyscore(const std::string &strKey, double dMin, double dMax, long *pnVal = nullptr);
	int Zrevrange(const std::string &strKey, long nStart, long nStop, std::vector<std::string> *pvecVal);
	int Zrevrangebyscore(const std::string &strKey, double dMax, double dMin, std::vector<std::string> *pvecVal);
	int Zrevrangebyscore(const std::string &strKey, double dMax, double dMin, std::map<std::string, double> *pmapVal);
	int Zrevrank(const std::string &strKey, const std::string &strElem, long *pnVal);
	int Zscore(const std::string &strKey, const std::string &strElem, double *pdVal);

	/* interfaces for system */
	int Time(struct timeval *ptmVal);

	/* interface for transaction */
	int Watch(CRedisConnection* connection, const std::string &strKey);
	int Multi(CRedisConnection* connection, const std::string &strKey);
	int Exec(CRedisConnection* connection, const std::string &strKey, OUT RedisResult* result);
	int Unwatch(CRedisConnection* connection, const std::string &strKey);
	int Discard(CRedisConnection* connection, const std::string &strKey);

private:
	static bool ConvertToMapInfo(const std::string &strVal, std::map<std::string, std::string> &mapVal);
	static bool GetValue(redisReply *pReply, std::string &strVal);
	static bool GetArray(redisReply *pReply, std::vector<std::string> &vecVal);
	static CRedisServer * FindServer(const std::vector<CRedisServer *> *vecRedisServ, const std::string &strHost, int nPort);

    void operator()();
    void CleanServer();
    CRedisServer * FindServer(int nSlot) const;
    bool InSameNode(const std::string &strKey1, const std::string &strKey2);
    CRedisServer * GetMatchedServer(const CRedisCommand *pRedisCmd) const;

    bool LoadSlaveInfo(const std::map<std::string, std::string> &mapInfo);
    bool LoadClusterSlots();
    bool WaitForRefresh();
    int Execute(CRedisCommand *pRedisCmd);
	int ExecutePool(CRedisConnection* connection, CRedisCommand *pRedisCmd);
    int SimpleExecute(CRedisCommand *pRedisCmd);
	int SimpleExecute(CRedisConnection* connection, CRedisCommand *pRedisCmd);

    int ExecuteImpl(const std::string &strCmd, int nSlot,
		TFuncFetch funcFetch, TFuncConvert funcConv = FUNC_DEF_CONV);
	int ExecuteImplPool(CRedisConnection* connection, const std::string &strCmd, int nSlot,
		TFuncFetch funcFetch, TFuncConvert funcConv = FUNC_DEF_CONV);

  //  template <typename P>
  //  int ExecuteImpl(const std::string &strCmd, const P &tArg, int nSlot, Pipeline ppLine,
  //                 TFuncFetch funcFetch, TFuncConvert funcConv = FUNC_DEF_CONV)
  //  {
		//CRedisCommand *pRedisCmd = new CRedisCommand(command, !ppLine);
  //      //CRedisCommand *pRedisCmd = new CRedisCommand(strCmd, !ppLine);
  //      pRedisCmd->SetArgs(tArg);
  //      pRedisCmd->SetSlot(nSlot);
  //      pRedisCmd->SetConvFunc(funcConv);
  //      int nRet = Execute(pRedisCmd, ppLine);
  //      if (nRet == RC_SUCCESS && !ppLine)
  //          nRet = pRedisCmd->FetchResult(funcFetch);
  //      if (!ppLine)
  //          delete pRedisCmd;
  //      return nRet;
  //  }

    //template <typename P1, typename P2>
    //int ExecuteImpl(const std::string &strCmd, const P1 &tArg1, const P2 &tArg2, int nSlot, Pipeline ppLine,
    //               TFuncFetch funcFetch, TFuncConvert funcConv = FUNC_DEF_CONV)
    //{
    //    CRedisCommand *pRedisCmd = new CRedisCommand(strCmd, !ppLine);
    //    pRedisCmd->SetArgs(tArg1, tArg2);
    //    pRedisCmd->SetSlot(nSlot);
    //    pRedisCmd->SetConvFunc(funcConv);
    //    int nRet = Execute(pRedisCmd, ppLine);
    //    if (nRet == RC_SUCCESS && !ppLine)
    //        nRet = pRedisCmd->FetchResult(funcFetch);
    //    if (!ppLine)
    //        delete pRedisCmd;
    //    return nRet;
    //}

    //template <typename P1, typename P2, typename P3>
    //int ExecuteImpl(const std::string &strCmd, const P1 &tArg1, const P2 &tArg2, const P3 &tArg3, int nSlot, Pipeline ppLine,
    //               TFuncFetch funcFetch, TFuncConvert funcConv = FUNC_DEF_CONV)
    //{
    //    CRedisCommand *pRedisCmd = new CRedisCommand(strCmd, !ppLine);
    //    pRedisCmd->SetArgs(tArg1, tArg2, tArg3);
    //    pRedisCmd->SetSlot(nSlot);
    //    pRedisCmd->SetConvFunc(funcConv);
    //    int nRet = Execute(pRedisCmd, ppLine);
    //    if (nRet == RC_SUCCESS && !ppLine)
    //        nRet = pRedisCmd->FetchResult(funcFetch);
    //    if (!ppLine)
    //        delete pRedisCmd;
    //    return nRet;
    //}

    //template <typename P1, typename P2, typename P3, typename P4>
    //int ExecuteImpl(const std::string &strCmd, const P1 &tArg1, const P2 &tArg2, const P3 &tArg3, const P4 &tArg4, int nSlot, Pipeline ppLine,
    //               TFuncFetch funcFetch, TFuncConvert funcConv = FUNC_DEF_CONV)
    //{
    //    CRedisCommand *pRedisCmd = new CRedisCommand(strCmd, !ppLine);
    //    pRedisCmd->SetArgs(tArg1, tArg2, tArg3, tArg4);
    //    pRedisCmd->SetSlot(nSlot);
    //    pRedisCmd->SetConvFunc(funcConv);
    //    int nRet = Execute(pRedisCmd, ppLine);
    //    if (nRet == RC_SUCCESS && !ppLine)
    //        nRet = pRedisCmd->FetchResult(funcFetch);
    //    if (!ppLine)
    //        delete pRedisCmd;
    //    return nRet;
    //}

private:
	std::string m_strHost;
	int m_nPort;
	int m_nClientTimeout;
	int m_nServerTimeout;
	int m_nConnNum;
	bool m_bCluster;
	bool m_bValid;
	bool m_bExit;

	std::vector<SlotRegion> m_vecSlot;
	//std::vector<CRedisServer *> m_vecRedisServ;
	//std::atomic<std::vector<SlotRegion>*>		m_vecSlot;
	std::atomic<std::vector<CRedisServer*>*>	m_vecRedisServ;
	std::vector<std::vector<CRedisServer*>*>	m_oldServerInfo;

#if defined(linux) || defined(__linux) || defined(__linux__)
	pthread_rwlockattr_t m_rwAttr;
#endif
	//pthread_rwlock_t m_rwLock;
	SRWLOCK				m_rwLock;
	std::condition_variable_any m_condAny;
	std::thread *m_pThread;

//#ifdef _DEBUG
//public:
//	template<typename ... Args>	inline void client_log_trace(Args const& ... args) { client_log(spdlog::level::trace, args...); }
//	template<typename ... Args>	inline void client_log_debug(Args const& ... args) { client_log(spdlog::level::debug, args...); }
//	template<typename ... Args>	inline void client_log_info(Args const& ... args) { client_log(spdlog::level::info, args...); }
//	template<typename ... Args>	inline void client_log_warn(Args const& ... args) { client_log(spdlog::level::warn, args...); }
//	template<typename ... Args>	inline void client_log_error(Args const& ... args) { client_log(spdlog::level::err, args...); }
//	template<typename ... Args>	inline void client_log_critical(Args const& ... args) { client_log(spdlog::level::critical, args...); }
//
//	template<typename ... Args>
//	void client_log(spdlog::level::level_enum level, Args const& ... args)
//	{
//		std::stringstream thread_stream;
//		thread_stream << std::this_thread::get_id();
//		unsigned int thread_id = std::stoull(thread_stream.str());
//
//		std::ostringstream stream;
//		using List = int[];
//		(void)List {
//			0, ((void)(stream << args), 0) ...
//		};
//
//		//console_logger_->log(level, stream.str());
//		//file_logger_->log(level, stream.str());
//		auto console = spdlog::get("console");
//		if (nullptr != console.get())
//		{
//			spdlog::get("console")->log(level, "[thread:" + thread_stream.str() + "] " + stream.str());
//		}
//		auto file = spdlog::get("result");
//		if (nullptr != console.get())
//		{
//			spdlog::get("result")->log(level, "[thread:" + thread_stream.str() + "] " + stream.str());
//		}
//	}
//
//#endif
};

#endif
