#include <atomic>
#include <iterator>
#include "redis_client/RedisClient.hpp"

#define BIND_INT(val) std::bind(&FetchInteger, std::placeholders::_1, val)
#define BIND_STR(val) std::bind(&FetchString, std::placeholders::_1, val)
#define BIND_VINT(val) std::bind(&FetchIntegerArray, std::placeholders::_1, val)
#define BIND_VSTR(val) std::bind(&FetchStringArray, std::placeholders::_1, val)
#define BIND_MAP(val) std::bind(&FetchMap, std::placeholders::_1, val)
#define BIND_TIME(val) std::bind(&FetchTime, std::placeholders::_1, val)
#define BIND_SLOT(val) std::bind(&FetchSlot, std::placeholders::_1, val)
#define BIND_MULTI(val) std::bind(&FetchMulti, std::placeholders::_1, val)

// crc16 for computing redis cluster slot
static const uint16_t crc16Table[256] =
{
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0
};

uint16_t CRC16(const char *pszData, int nLen)
{
    int nCounter;
    uint16_t nCrc = 0;
    for (nCounter = 0; nCounter < nLen; nCounter++)
        nCrc = (nCrc << 8) ^ crc16Table[((nCrc >> 8) ^ * pszData++) & 0x00FF];
    return nCrc;
}

uint32_t CRedisClient::HASH_SLOT(const std::string &strKey)
{
    const char *pszKey = strKey.data();
    size_t nKeyLen = strKey.size();
    size_t nStart, nEnd; /* start-end indexes of { and  } */

    /* Search the first occurrence of '{'. */
    for (nStart = 0; nStart < nKeyLen; nStart++)
        if (pszKey[nStart] == '{')
            break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (nStart == nKeyLen)
        return CRC16(pszKey, nKeyLen) & 16383;

    /* '{' found? Check if we have the corresponding '}'. */
    for (nEnd = nStart + 1; nEnd < nKeyLen; nEnd++)
        if (pszKey[nEnd] == '}')
            break;

    /* No '}' or nothing between {} ? Hash the whole key. */
    if (nEnd == nKeyLen || nEnd == nStart + 1)
        return CRC16(pszKey, nKeyLen) & 16383;

    /* If we are here there is both a { and a  } on its right. Hash
     * what is in the middle between { and  }. */
    return CRC16(pszKey + nStart + 1, nEnd - nStart - 1) & 16383;
}

static inline std::ostream & operator<<(std::ostream &os, const std::pair<int, redisReply *> &pairReply)
{
    int nLevel = pairReply.first;
    redisReply *pReply = pairReply.second;

    for (int i = 0; i < nLevel; ++i)
        os << "  ";

    if (!pReply)
        os << "nullptr" << std::endl;
    else if (pReply->type == REDIS_REPLY_INTEGER)
        os << "integer: " << pReply->integer << std::endl;
    else if (pReply->type == REDIS_REPLY_STRING)
        os << "string: " << pReply->str << std::endl;
    else if (pReply->type == REDIS_REPLY_ARRAY)
    {
        os <<  "array: " << std::endl;
        for (unsigned int j = 0; j < pReply->elements; ++j)
            os << std::make_pair(nLevel + 1, pReply->element[j]);
    }
    else if (pReply->type == REDIS_REPLY_STATUS)
        os << "status: " << pReply->str << std::endl;
    else if (pReply->type == REDIS_REPLY_ERROR)
        os << "error: " << pReply->str << std::endl;
    else if (pReply->type == REDIS_REPLY_NIL)
        os << "nil" << std::endl;
    else
        os << "unknown" << std::endl;
    return os;
}

static inline std::ostream & operator<<(std::ostream &os, redisReply *pReply)
{
    return os << std::make_pair(0, pReply);
}

template <typename T>
std::string ConvertToString(T t)
{
    std::stringstream sstream;
    sstream << t;
    return sstream.str();
}

// for finding matched slot/server with binary search
bool operator< (const SlotRegion &lReg, const SlotRegion &rReg)
{
    return lReg.nStartSlot < rReg.nStartSlot;
}

//class CompSlot
//{
//public:
//    bool operator()(const SlotRegion &slotReg, int nSlot) { return slotReg.nEndSlot < nSlot; }
//    bool operator()(int nSlot, const SlotRegion &slotReg) { return nSlot < slotReg.nStartSlot; }
//};

class IntResConv
{
public:
    IntResConv(int nConvRet = RC_OBJ_NOT_EXIST, long nVal = 0) : m_nConvRet(nConvRet), m_nVal(nVal) {}
    int operator()(int nRet, redisReply *pReply)
    {
        if (nRet == RC_SUCCESS && pReply->integer == m_nVal)
            return m_nConvRet;
        return nRet;
    }

private:
    int m_nConvRet;
    long m_nVal;
};

class StuResConv
{
public:
    StuResConv() : m_strVal("OK") {}
    StuResConv(const std::string &strVal) : m_strVal(strVal) {}
    int operator()(int nRet, redisReply *pReply)
    {
        //if (nRet == RC_SUCCESS && m_strVal != pReply->str)
		if (nRet == RC_SUCCESS && m_strVal != pReply->str && std::string("QUEUED") != pReply->str)
			return RC_REPLY_ERR;
        return nRet;
    }

private:
    std::string m_strVal;
};

class NilResConv
{
public:
    NilResConv();
    int operator()(int nRet, redisReply *pReply)
    {
        if (nRet == RC_SUCCESS && pReply->type == REDIS_REPLY_NIL)
            return RC_OBJ_NOT_EXIST;
        return nRet;
    }
};

class ExistErrConv
{
public:
    ExistErrConv() : m_strErr("Target key name is busy") {}
    int operator()(int nRet, redisReply *pReply)
    {
        if (nRet == RC_REPLY_ERR && m_strErr == pReply->str)
            return RC_OBJ_EXIST;
        return nRet;
    }

private:
    std::string m_strErr;
};

static inline int FetchInteger(redisReply *pReply, long *pnVal)
{
    if (pReply->type == REDIS_REPLY_INTEGER)
    {
        if (pnVal)
            *pnVal = pReply->integer;
        return RC_SUCCESS;
    }
    else if (pReply->type == REDIS_REPLY_NIL)
        return RC_OBJ_NOT_EXIST;
    else
        return RC_REPLY_ERR;
}

static inline int FetchString(redisReply *pReply, std::string *pstrVal)
{
    if (pReply->type == REDIS_REPLY_STRING || pReply->type == REDIS_REPLY_STATUS)
    {
        if (pstrVal)
            pstrVal->assign(pReply->str, pReply->len);
        return RC_SUCCESS;
    }
    else if (pReply->type == REDIS_REPLY_NIL)
    {
        if (pstrVal)
            pstrVal->clear();
        return RC_SUCCESS;
    }
    else
        return RC_REPLY_ERR;
}

static inline int FetchIntegerArray(redisReply *pReply, std::vector<long> *pvecLongVal)
{
    if (pReply->type == REDIS_REPLY_INTEGER)
    {
        int nRet = RC_SUCCESS;
        if (!pvecLongVal)
            return nRet;

        long nVal;
        pvecLongVal->clear();
        for (size_t i = 0; i < pReply->elements; ++i)
        {
            int nSubRet = FetchInteger(pReply->element[i], &nVal);
            if (nSubRet == RC_SUCCESS)
                pvecLongVal->push_back(nVal);
            else
                nRet = RC_PART_SUCCESS;
        }
        return nRet;
    }
    else
        return RC_REPLY_ERR;
}

static inline int FetchStringArray(redisReply *pReply, std::vector<std::string> *pvecStrVal)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        int nRet = RC_SUCCESS;
        if (!pvecStrVal)
            return nRet;

        std::string strVal;
        pvecStrVal->clear();
        for (size_t i = 0; i < pReply->elements; ++i)
        {
            int nSubRet = FetchString(pReply->element[i], &strVal);
            if (nSubRet == RC_SUCCESS)
                pvecStrVal->push_back(strVal);
            else
                nRet = RC_PART_SUCCESS;
        }
        return nRet;
    }
    else if (pReply->type == REDIS_REPLY_NIL)
    {
        if (pvecStrVal)
            pvecStrVal->clear();
        return RC_SUCCESS;
    }
    else
        return RC_REPLY_ERR;
}

static inline int FetchMap(redisReply *pReply, std::map<std::string, std::string> *pmapFv)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        int nRet = RC_SUCCESS;
        if (!pmapFv)
            return nRet;

        if ((pReply->elements % 2) != 0)
            return RC_REPLY_ERR;

        std::string strFld;
        std::string strVal;
        pmapFv->clear();
        for (size_t i = 0; i < pReply->elements; )
        {
            int nSubRet = FetchString(pReply->element[i++], &strFld);
            if (nSubRet == RC_SUCCESS)
                nSubRet = FetchString(pReply->element[i++], &strVal);

            if (nSubRet == RC_SUCCESS)
                pmapFv->insert(std::make_pair(strFld, strVal));
            else
                return nSubRet;
        }
        return nRet;
    }
    else
        return RC_REPLY_ERR;
}

static inline int FetchTime(redisReply *pReply, struct timeval *ptmVal)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        if (pReply->elements != 2 || pReply->element[0]->type != REDIS_REPLY_STRING ||
            pReply->element[1]->type != REDIS_REPLY_STRING)
            return RC_REPLY_ERR;

        if (ptmVal)
        {
            ptmVal->tv_sec = atol(pReply->element[0]->str);
            ptmVal->tv_usec = atol(pReply->element[1]->str);
        }
        return RC_SUCCESS;
    }
    else
        return RC_REPLY_ERR;
}

static inline int FetchSlot(redisReply *pReply, std::vector<SlotRegion> *pvecSlot)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        if (!pvecSlot)
            return RC_SUCCESS;

        SlotRegion slotReg;
        pvecSlot->clear();
        for (size_t i = 0; i < pReply->elements; ++i)
        {
            redisReply *pSubReply = pReply->element[i];
            if (pSubReply->type != REDIS_REPLY_ARRAY || pSubReply->elements < 3)
                return RC_REPLY_ERR;

            slotReg.nStartSlot = pSubReply->element[0]->integer;
            slotReg.nEndSlot = pSubReply->element[1]->integer;
            slotReg.pRedisServ = nullptr;
            slotReg.strHost = pSubReply->element[2]->element[0]->str;
            slotReg.nPort = pSubReply->element[2]->element[1]->integer;
            pvecSlot->push_back(slotReg);
        }
        return RC_SUCCESS;
    }
    else
        return RC_REPLY_ERR;
}

static inline int FetchMulti(redisReply *pReply, OUT RedisResult* result)
{
	if (nullptr == result)
	{
		return RC_REPLY_ERR;
	}
	result->Clear();

	//result->type = static_cast<RedisResult::Type>(pReply->type);
	std::string strVal;
	long lVal;
	switch (pReply->type)
	{
	case RC_REPLY_ERR:
		result->type = RedisResult::Type::NIL;
		return RC_REPLY_ERR;
		break;
	case REDIS_REPLY_STATUS:
	case REDIS_REPLY_STRING:
		result->type = RedisResult::Type::STRING;
		if (RC_SUCCESS == FetchString(pReply, &strVal))
		{
			sprintf(result->m_strVal, strVal.c_str());
			return RC_SUCCESS;
		}
		else
		{
			return RC_REPLY_ERR;
		}
		break;
	case REDIS_REPLY_INTEGER:
		result->type = RedisResult::Type::INTEGER;
		if (RC_SUCCESS == FetchInteger(pReply, &lVal))
		{
			result->m_llVal = lVal;
			return RC_SUCCESS;
		}
		else
		{
			return RC_REPLY_ERR;
		}

		break;
	case REDIS_REPLY_ARRAY:
	{
		result->type = RedisResult::Type::ARRAY;
		for (int i = 0; i < pReply->elements; i++)
		{
			RedisResult arr;
			auto data = pReply->element[i];
			if (RC_SUCCESS == FetchMulti(data, &arr))
			{
				result->m_arrayVal.push_back(arr);
			}
		}
		return RC_SUCCESS;
	}
	break;
	case REDIS_REPLY_NIL:
		result->type = RedisResult::Type::NIL;
		// NULL이 리턴된 경우이다. 전달되는 데이터는 없다.
		return RC_SUCCESS;
	default:
		break;
	}
	return RC_SUCCESS;
}

// CRedisCommand methods
CRedisCommand::CRedisCommand(const std::string &strCmd, bool bShareMem)
    : m_strCmd(strCmd), m_bShareMem(bShareMem), m_nArgs(0), m_nIdx(0), m_pszArgs(nullptr),
      m_pnArgsLen(nullptr), m_pReply(nullptr), m_nSlot(-1), m_funcConv(FUNC_DEF_CONV)
{
}

void CRedisCommand::ClearArgs()
{
    if (m_pszArgs)
    {
        if (!m_bShareMem)
        {
            for (int i = 0; i < m_nArgs; ++i)
                delete [] m_pszArgs[i];
        }
        delete [] m_pszArgs;
    }
    if (m_pnArgsLen)
        delete [] m_pnArgsLen;
    if (m_pReply)
        freeReplyObject(m_pReply);
    m_pszArgs = nullptr;
    m_pnArgsLen = nullptr;
    m_pReply = nullptr;
    m_nArgs = 0;
    m_nIdx = 0;
}

std::string CRedisCommand::FetchErrMsg() const
{
    std::string strErrMsg;
    if (m_pReply && m_pReply->type == REDIS_REPLY_ERROR)
        strErrMsg.assign(m_pReply->str, m_pReply->len);
    return strErrMsg;
}

bool CRedisCommand::IsMovedErr() const
{
    return FetchErrMsg().substr(0, 5) == "MOVED";
}

void CRedisCommand::DumpArgs() const
{
    std::cout << "total " << m_nArgs << " args" << std::endl;
    for (int i = 0; i < m_nArgs; ++i)
        std::cout << i + 1 << " : " << m_pszArgs[i] << std::endl;
}

void CRedisCommand::DumpReply() const
{
    if (!m_pReply)
        std::cout << "no reply" << std::endl;
    else
        std::cout << m_pReply;
}

void CRedisCommand::SetArgs()
{
    InitMemory(1);
}

void CRedisCommand::SetArgs(const std::string &strArg)
{
    InitMemory(2);
    AppendValue(strArg);
}

void CRedisCommand::SetArgs(const std::vector<std::string> &vecArg)
{
    InitMemory(vecArg.size() + 1);
    for (auto &strArg : vecArg)
        AppendValue(strArg);
}

void CRedisCommand::SetArgs(const std::string &strArg1, const std::string &strArg2)
{
    InitMemory(3);
    AppendValue(strArg1);
    AppendValue(strArg2);
}

void CRedisCommand::SetArgs(const std::string &strArg1, const std::vector<std::string> &vecArg2)
{
    InitMemory(vecArg2.size() + 2);
    AppendValue(strArg1);
    for (auto &strArg : vecArg2)
        AppendValue(strArg);
}

void CRedisCommand::SetArgs(const std::string &strArg1, const std::set<std::string> &setArg2)
{
    InitMemory(setArg2.size() + 2);
    AppendValue(strArg1);
    for (auto &strArg : setArg2)
        AppendValue(strArg);
}

void CRedisCommand::SetArgs(const std::vector<std::string> &vecArg1, const std::string &strArg2)
{
    if (vecArg1.empty())
        return;

    InitMemory(vecArg1.size() + 2);
    for (auto &strArg : vecArg1)
        AppendValue(strArg);
    AppendValue(strArg2);
}

void CRedisCommand::SetArgs(const std::vector<std::string> &vecArg1, const std::vector<std::string> &vecArg2)
{
    if (vecArg1.size() != vecArg2.size())
        return;

    InitMemory(vecArg1.size() * 2 + 1);
    for (size_t i = 0; i < vecArg1.size(); ++i)
    {
        AppendValue(vecArg1[i]);
        AppendValue(vecArg2[i]);
    }
}

void CRedisCommand::SetArgs(const std::map<std::string, std::string> &mapArg)
{
    InitMemory(mapArg.size() * 2 + 1);
    for (auto &kvPair : mapArg)
    {
        AppendValue(kvPair.first);
        AppendValue(kvPair.second);
    }
}

void CRedisCommand::SetArgs(const std::string &strArg1, const std::map<std::string, std::string> &mapArg2)
{
    InitMemory(mapArg2.size() * 2 + 2);
    AppendValue(strArg1);
    for (auto &kvPair : mapArg2)
    {
        AppendValue(kvPair.first);
        AppendValue(kvPair.second);
    }
}

void CRedisCommand::SetArgs(const std::string &strArg1, const std::string &strArg2, const std::string &strArg3)
{
    InitMemory(4);
    AppendValue(strArg1);
    AppendValue(strArg2);
    AppendValue(strArg3);
}

void CRedisCommand::SetArgs(const std::string &strArg1, const std::string &strArg2, const std::vector<std::string> &vecArg3)
{
    InitMemory(vecArg3.size() + 3);
    AppendValue(strArg1);
    AppendValue(strArg2);
    for (auto &strArg : vecArg3)
        AppendValue(strArg);
}

void CRedisCommand::SetArgs(const std::string &strArg1, const std::vector<std::string> &vecArg2, const std::vector<std::string> &vecArg3)
{
    InitMemory(vecArg2.size() * 2 + 2);
    AppendValue(strArg1);
    for (size_t i = 0; i < vecArg2.size(); ++i)
    {
        AppendValue(vecArg2[i]);
        AppendValue(vecArg3[i]);
    }
}

void CRedisCommand::SetArgs(const std::string &strArg1, const std::string &strArg2, const std::string &strArg3, const std::string &strArg4)
{
    InitMemory(5);
    AppendValue(strArg1);
    AppendValue(strArg2);
    AppendValue(strArg3);
    AppendValue(strArg4);
}

void CRedisCommand::InitMemory(int nArgs)
{
    ClearArgs();

    m_nArgs = nArgs;
    m_pszArgs = new char *[m_nArgs];
    m_pnArgsLen = new size_t[m_nArgs];
    AppendValue(m_strCmd);
}

void CRedisCommand::AppendValue(const std::string &strVal)
{
    if (m_nIdx >= m_nArgs)
        return;

    m_pnArgsLen[m_nIdx] = strVal.size();
    if (m_bShareMem)
        m_pszArgs[m_nIdx] = (char *)strVal.data();
    else
    {
        m_pszArgs[m_nIdx] = new char[strVal.size()];
        memcpy(m_pszArgs[m_nIdx], strVal.data(), strVal.size());
    }
    ++m_nIdx;
}

int CRedisCommand::CmdRequest(redisContext *pContext)
{
    //if (m_nArgs <= 0)
    //    return RC_PARAM_ERR;

	if (!pContext)
	{
		return RC_RQST_ERR;
	}

    if (m_pReply)
    {
        freeReplyObject(m_pReply);
        m_pReply = nullptr;
    }

	m_pReply = static_cast<redisReply *>(redisCommand(pContext, m_strCmd.c_str()));
//    m_pReply = static_cast<redisReply *>(redisCommandArgv(pContext, m_nArgs, (const char **)m_pszArgs, (const size_t *)m_pnArgsLen));
    return m_pReply ? RC_SUCCESS : RC_RQST_ERR;
}

int CRedisCommand::CmdAppend(redisContext *pContext)
{
    //if (m_nArgs <= 0)
    //    return RC_PARAM_ERR;

    if (!pContext)
        return RC_RQST_ERR;

	m_pReply = static_cast<redisReply*>(redisCommand(pContext, m_strCmd.c_str()));
	return m_pReply ? RC_SUCCESS : RC_RQST_ERR;
    //int nRet = redisAppendCommandArgv(pContext, m_nArgs, (const char **)m_pszArgs, (const size_t *)m_pnArgsLen);
    //return nRet == REDIS_OK ? RC_SUCCESS : RC_RQST_ERR;
}

int CRedisCommand::CmdReply(redisContext *pContext)
{
    if (m_pReply)
    {
        freeReplyObject(m_pReply);
        m_pReply = nullptr;
    }

    return redisGetReply(pContext, (void **)&m_pReply) == REDIS_OK ? RC_SUCCESS : RC_RQST_ERR;
}

int CRedisCommand::FetchResult(const TFuncFetch &funcFetch)
{
    return m_funcConv(funcFetch(m_pReply), m_pReply);
}

// CRedisConnection methods
CRedisConnection::CRedisConnection(CRedisServer *pRedisServ) : m_pContext(nullptr), m_nUseTime(0), m_pRedisServ(pRedisServ)
{
    Reconnect();
}
CRedisConnection::~CRedisConnection()
{
	if (m_pContext)
        redisFree(m_pContext);
//	delete m_pRedisServ;
//	m_pRedisServ = nullptr;
}
int CRedisConnection::ConnRequest(CRedisCommand *pRedisCmd)
{
	time_t tmNow = time(nullptr);
	if (!m_pContext || tmNow - m_nUseTime >= m_pRedisServ->m_nSerTimeout)
	{
		if (!Reconnect())
		{
			return RC_RQST_ERR;
		}			
	}

    int nRet = pRedisCmd->CmdRequest(m_pContext);
    if (nRet == RC_RQST_ERR)
    {
        if (tmNow - m_nUseTime < m_pRedisServ->m_nSerTimeout)
            return nRet;
        else if (!Reconnect())
            return RC_RQST_ERR;
        else
            nRet = pRedisCmd->CmdRequest(m_pContext);
    }

    if (nRet != RC_RQST_ERR)
        m_nUseTime = tmNow;
    return nRet;
}

int CRedisConnection::ConnRequest(std::vector<CRedisCommand *> &vecRedisCmd)
{
    time_t tmNow = time(nullptr);
    if (!m_pContext || tmNow - m_nUseTime >= m_pRedisServ->m_nSerTimeout)
    {
        if (!Reconnect())
            return RC_RQST_ERR;
    }

    int nRet = RC_SUCCESS;
    //for (size_t i = 0; i < vecRedisCmd.size() && nRet == RC_SUCCESS; ++i)
    //    nRet = vecRedisCmd[i]->CmdAppend(m_pContext);
    for (size_t i = 0; i < vecRedisCmd.size() && nRet == RC_SUCCESS; ++i)
        nRet = vecRedisCmd[i]->CmdReply(m_pContext);
    return nRet;
}

bool CRedisConnection::ConnectToRedis(const std::string &strHost, int nPort, int nTimeout)
{
	if (m_pContext)
	{
		redisFree(m_pContext);
		m_pContext = nullptr;
	}

    struct timeval tmTimeout = {static_cast<long>(nTimeout), 0};
    m_pContext = redisConnectWithTimeout(strHost.c_str(), nPort, tmTimeout);
    if (!m_pContext || m_pContext->err)
    {
        if (m_pContext)
        {
            redisFree(m_pContext);
            m_pContext = nullptr;
        }
        return false;
    }

    //redisSetTimeout(m_pContext, tmTimeout);
    m_nUseTime = time(nullptr);
    return true;
}

bool CRedisConnection::Reconnect()
{
	if (!m_pRedisServ->m_strHost.empty() &&
		ConnectToRedis(m_pRedisServ->m_strHost, m_pRedisServ->m_nPort, m_pRedisServ->m_nCliTimeout))
		return true;

	if (0 < m_pRedisServ->m_vecHosts.size())
	{
		for (auto &hostPair : m_pRedisServ->m_vecHosts)
		{
			if (ConnectToRedis(hostPair.first, hostPair.second, m_pRedisServ->m_nCliTimeout))
			{
				m_pRedisServ->m_strHost = hostPair.first;
				m_pRedisServ->m_nPort = hostPair.second;
				return true;
			}
		}
	}

    m_pRedisServ->m_strHost.clear();
    return false;
}

// CRedisServer methods
CRedisServer::CRedisServer(const std::string &strHost, int nPort, int nClientTimeout, int nServerTimeout, int nConnNum)
    : m_strHost(strHost), m_nPort(nPort), m_nCliTimeout(nClientTimeout), m_nSerTimeout(nServerTimeout), m_nConnNum(nConnNum)
{
	SetSlave(strHost, nPort);
    Initialize();
}

CRedisServer::~CRedisServer()
{
    CleanConn();
}

void CRedisServer::CleanConn()
{
    m_mutexConn.lock();
    while (!m_queIdleConn.empty())
    {
        delete m_queIdleConn.front();
        m_queIdleConn.pop();
    }

	//m_nPort = 0;
	//m_nCliTimeout = 0;
	//m_nSerTimeout = 0;
	m_mutexConn.unlock();
}

void CRedisServer::SetSlave(const std::string &strHost, int nPort)
{
    m_vecHosts.push_back(std::make_pair(strHost, nPort));
}

CRedisConnection * CRedisServer::FetchConnection()
{
	CRedisConnection *pRedisConn = nullptr;
	
	m_mutexConn.lock();
	if (!m_queIdleConn.empty())
	{
		pRedisConn = m_queIdleConn.front();
		m_queIdleConn.pop();
	}

	m_mutexConn.unlock();
	return pRedisConn;
}

void CRedisServer::ReturnConnection(CRedisConnection *pRedisConn)
{
    m_mutexConn.lock();
    m_queIdleConn.push(pRedisConn);
    m_mutexConn.unlock();
}

bool CRedisServer::Initialize()
{
	CleanConn();

	for (int i = 0; i < m_nConnNum; ++i)
	{
		CRedisConnection *pRedisConn = new CRedisConnection(this);
		if (pRedisConn->IsValid())
			m_queIdleConn.push(pRedisConn);
	}

    if (!m_queIdleConn.empty())
    {
        std::vector<std::string> vecTimeout;
        //CRedisCommand redisCmd("config");
        //redisCmd.SetArgs("get", "timeout");
		//CRedisCommand redisCmd("config get timeout");
		//if (ServRequest(&redisCmd) == RC_SUCCESS &&
		//	redisCmd.FetchResult(BIND_VSTR(&vecTimeout)) == RC_SUCCESS &&
		//	vecTimeout.size() == 2)
		//{
		//	m_nSerTimeout = atoi(vecTimeout[1].c_str());
		//}
    }
    return !m_queIdleConn.empty();
}

int CRedisServer::ServRequest(CRedisCommand *pRedisCmd)
{
    CRedisConnection *pRedisConn = nullptr;
    int nTry = RQST_RETRY_TIMES;
    while (nTry--)
    {
        if ((pRedisConn = FetchConnection()))
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (!pRedisConn)
        return RC_NO_RESOURCE;

    int nRet = pRedisConn->ConnRequest(pRedisCmd);
    ReturnConnection(pRedisConn);
    return nRet;
}

int CRedisServer::ServRequest(CRedisConnection* connection, CRedisCommand *pRedisCmd)
{
	int nRet = connection->ConnRequest(pRedisCmd);
	return nRet;
}


// CRedisClient methods
CRedisClient::CRedisClient()
	: m_nPort(-1), m_nClientTimeout(-1), m_nServerTimeout(-1), m_nConnNum(-1), m_bCluster(false),
      m_bValid(true), m_bExit(false), m_pThread(nullptr)
{
#if defined(linux) || defined(__linux) || defined(__linux__)
    pthread_rwlockattr_init(&m_rwAttr);
    pthread_rwlockattr_setkind_np(&m_rwAttr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&m_rwLock, &m_rwAttr);
#else
	//pthread_rwlock_init(&m_rwLock, nullptr);
	InitializeSRWLock(&m_rwLock);
#endif
}

CRedisClient::~CRedisClient()
{
	m_bValid = false;
	m_bExit = true;
	{
		CSafeLock safeLock(&m_rwLock);
		safeLock.WriteLock();
		m_condAny.notify_all();
		safeLock.WriteUnlock();
	}
	if (m_pThread)
	{
		m_pThread->join();
		delete m_pThread;
		m_pThread = nullptr;
	}

	for (auto server : m_oldServerInfo)
	{
		delete server;
	}
	m_oldServerInfo.clear();
	CleanServer();
#if defined(linux) || defined(__linux) || defined(__linux__)
	pthread_rwlockattr_destroy(&m_rwAttr);
#endif
	//pthread_rwlock_destroy(&m_rwLock);
}

bool CRedisClient::Initialize(const std::string &strHost, int nPort, int nClientTimeout, int nServerTimeout, int nConnNum)
{
	//client_log_trace("CRedisClient::Initialize [host:", strHost, "][port:", nPort, "]");
	std::string::size_type nPos = strHost.find(':');
	m_strHost = (nPos == std::string::npos) ? strHost : strHost.substr(0, nPos);
	m_nPort = (nPos == std::string::npos) ? nPort : atoi(strHost.substr(nPos + 1).c_str());
	m_nClientTimeout = nClientTimeout;
	m_nServerTimeout = nServerTimeout;
	m_nConnNum = nConnNum;
	if (m_strHost.empty() || m_nPort <= 0 || m_nClientTimeout <= 0 || m_nServerTimeout <= 0 || m_nConnNum <= 0)
		return false;

    CRedisServer *pRedisServ = new CRedisServer(m_strHost, m_nPort, m_nClientTimeout, m_nServerTimeout, m_nConnNum);
    if (!pRedisServ->IsValid())
        return false;

	std::string strInfo;
	std::map<std::string, std::string> mapInfo;
	CRedisCommand redisCmd("info");
	//redisCmd.SetArgs();
	if (pRedisServ->ServRequest(&redisCmd) != RC_SUCCESS ||
		redisCmd.FetchResult(BIND_STR(&strInfo)) != RC_SUCCESS ||
		!ConvertToMapInfo(strInfo, mapInfo))
		return false;

	auto it = mapInfo.find("cluster_enabled");
	if (it == mapInfo.end())
		return false;
	else
		m_bCluster = (bool)atoi(it->second.c_str());

	auto server_vec = new std::vector<CRedisServer*>;
	server_vec->push_back(pRedisServ);	
	m_vecRedisServ.store(server_vec);

	m_bValid = (m_bCluster ? LoadClusterSlots() : LoadSlaveInfo(mapInfo)) && 
		(m_pThread = new std::thread(std::bind(&CRedisClient::operator(), this))) != nullptr;
	return m_bValid;
}

void CRedisClient::operator()()
{
	while (!m_bExit)
	{
		{
			CSafeLock safeLock(&m_rwLock);
			safeLock.WriteLock();

			if (m_bValid)
			{
				m_condAny.wait(safeLock);
			}
			
			//client_log_info("CRedisClient::operator()()");
			//m_bValid = m_bCluster ? LoadClusterSlots() : m_vecRedisServ[0]->Initialize();
			if (true == m_bCluster)
			{
				m_bValid = LoadClusterSlots();
			}
			else
			{
				std::vector<CRedisServer*>* server = m_vecRedisServ.load();
				server->at(0)->Initialize();
								
			}
			safeLock.WriteUnlock();
		}

		if (!m_bValid)
			std::this_thread::sleep_for(std::chrono::seconds(1));
	}
}

/* interfaces for generic */
//int CRedisClient::Del(const std::string &strKey, long *pnVal)
//{
//	std::string command = "del " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("del", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}

int CRedisClient::Del(CRedisConnection* connection, const std::string &strKey, OUT RedisResult* result)
{
	std::string command = "del " + strKey;
	return ExecuteImplPool(connection, command, HASH_SLOT(strKey), BIND_MULTI(result));
	//return ExecuteImpl("del", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

//int CRedisClient::Dump(const std::string &strKey, std::string *pstrVal)
//{
//	std::string command = "dump " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(pstrVal));
//    //return ExecuteImpl("dump", strKey, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
//}
//
//int CRedisClient::Exists(const std::string &strKey, long *pnVal)
//{
//	std::string command = "exists " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("exists", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Expire(const std::string &strKey, long nSec, long *pnVal)
//{
//	std::string command = "expire " + strKey + " " + std::to_string(nSec);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("expire", strKey, ConvertToString(nSec), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}

int CRedisClient::Expire(CRedisConnection* connection, const std::string &strKey, long nSec, long *pnVal)
{
	std::string command = "expire " + strKey + " " + std::to_string(nSec);
	return ExecuteImplPool(connection, command, HASH_SLOT(strKey), BIND_STR(nullptr), StuResConv());
	//return ExecuteImpl("expire", strKey, ConvertToString(nSec), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}
//
//int CRedisClient::Expireat(const std::string &strKey, long nTime, long *pnVal)
//{
//	std::string command = "expireat " + strKey + " " + std::to_string(nTime);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("expireat", strKey, ConvertToString(nTime), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Keys(const std::string &strPattern, std::vector<std::string> *pvecVal)
//{
//    CRedisCommand redisCmd("keys");
//    redisCmd.SetArgs(strPattern);
//
//    int nRet = RC_SUCCESS;
//    std::vector<std::string> vecVal;
//	
//    //for (auto pRedisServ : m_vecRedisServ)
//	auto server = m_vecRedisServ.load();
//	for (auto itr = server->begin(); itr != server->end(); ++itr)
//    {
//		CRedisServer* pRedisServ = *itr;
//        if ((nRet = pRedisServ->ServRequest(&redisCmd)) != RC_SUCCESS ||
//            (nRet = redisCmd.FetchResult(BIND_VSTR(&vecVal))) != RC_SUCCESS)
//        {
//            if (pvecVal)
//                pvecVal->clear();
//            return nRet;
//        }
//        else
//        {
//            if (pvecVal)
//                std::copy(vecVal.begin(), vecVal.end(), std::back_inserter(*pvecVal));
//        }
//    }
//    return nRet;
//}
//
//int CRedisClient::Persist(const std::string &strKey, long *pnVal)
//{
//	std::string command = "persist " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("persist", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Pexpire(const std::string &strKey, long nMilliSec, long *pnVal)
//{
//	std::string command = "pexpire " + strKey + " " + std::to_string(nMilliSec);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("pexpire", strKey, ConvertToString(nMilliSec), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Pexpireat(const std::string &strKey, long nMilliTime, long *pnVal)
//{
//	std::string command = "pexpireat " + strKey + " " + std::to_string(nMilliTime);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("pexpireat", strKey, ConvertToString(nMilliTime), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Pttl(const std::string &strKey, long *pnVal)
//{
//	std::string command = "pttl " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("pttl", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Randomkey(std::string *pstrVal)
//{
//    return ExecuteImpl("randomkey", -1, BIND_STR(pstrVal));
//}
//
//int CRedisClient::Rename(const std::string &strKey, const std::string &strNewKey)
//{
//    if (InSameNode(strKey, strNewKey))
//	{
//		std::string command = "rename " + strKey + " " + strNewKey;
//		return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(nullptr), StuResConv());
//		//return ExecuteImpl("rename", strKey, strNewKey, HASH_SLOT(strKey), nullptr, BIND_STR(nullptr), StuResConv());
//	}
//    else
//    {
//        std::string strVal;
//        long nTtl;
//        int nRet;
//        if ((nRet = Dump(strKey, &strVal)) == RC_SUCCESS && !strVal.empty() &&
//            (nRet = Pttl(strKey, &nTtl)) == RC_SUCCESS && nTtl != -2)
//        {
//            if (nTtl == -1)
//                nTtl = 0;
//            if ((nRet = Del(strNewKey)) >= RC_SUCCESS &&
//                (nRet = Restore(strNewKey, nTtl, strVal)) == RC_SUCCESS &&
//                (nRet = Del(strKey)) >= RC_SUCCESS)
//                return RC_SUCCESS;
//        }
//
//        Del(strNewKey);
//        return nRet == RC_SUCCESS ? RC_REPLY_ERR : nRet;
//    }
//}
//
//int CRedisClient::Renamenx(const std::string &strKey, const std::string &strNewKey)
//{
//	if (InSameNode(strKey, strNewKey))
//	{
//		std::string command = "renamenx " + strKey + " " + strNewKey;
//		return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(nullptr), StuResConv());
//		//return ExecuteImpl("renamenx", strKey, strNewKey, HASH_SLOT(strKey), nullptr, BIND_STR(nullptr), StuResConv());
//	}
//    else
//    {
//        int nRet;
//        long nVal;
//        if ((nRet = Exists(strNewKey, &nVal)) != RC_SUCCESS)
//            return nRet;
//        else if (nVal == 1)
//            return RC_REPLY_ERR;
//
//        std::string strVal;
//        long nTtl;
//        if ((nRet = Dump(strKey, &strVal)) == RC_SUCCESS &&
//            (nRet = Pttl(strKey, &nTtl)) == RC_SUCCESS)
//        {
//            if (nTtl == -1)
//                nTtl = 0;
//            if ((nRet = Restore(strNewKey, nTtl, strVal)) == RC_SUCCESS &&
//                (nRet = Del(strKey)) >= RC_SUCCESS)
//                return RC_SUCCESS;
//        }
//
//        Del(strNewKey);
//        return nRet == RC_SUCCESS ? RC_REPLY_ERR : nRet;
//    }
//}
//
//int CRedisClient::Restore(const std::string &strKey, long nTtl, const std::string &strVal)
//{
//	std::string command = "restore " + strKey + " " + std::to_string(nTtl) + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(nullptr), StuResConv());
//    //return ExecuteImpl("restore", strKey, ConvertToString(nTtl), strVal, HASH_SLOT(strKey), ppLine, BIND_STR(nullptr), StuResConv());
//}
//
//int CRedisClient::Scan(long *pnCursor, const std::string &strPattern, long nCount, std::vector<std::string> *pvecVal)
//{
//    return RC_SUCCESS;
//}
//
//int CRedisClient::Ttl(const std::string &strKey, long *pnVal)
//{
//	std::string command = "ttl " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("ttl", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Type(const std::string &strKey, std::string *pstrVal)
//{
//	std::string command = "type " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(pstrVal));
//    //return ExecuteImpl("type", strKey, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
//}
//
///* interfaces for string */
//int CRedisClient::Append(const std::string &strKey, const std::string &strVal, long *pnVal)
//{
//	std::string command = "append " + strKey + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("append", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Bitcount(const std::string &strKey, long *pnVal)
//{
//	std::string command = "bitcount " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("bitcount", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Bitcount(const std::string &strKey, long nStart, long nEnd, long *pnVal)
//{
//	std::string command = "bitcount " + strKey + " " + std::to_string(nStart) + " " + std::to_string(nEnd);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("bitcount", strKey, ConvertToString(nStart), ConvertToString(nEnd), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Bitop(const std::string &strDestKey, const std::string &strOp, const std::vector<std::string> &vecKey, long *pnVal)
//{
//	if (m_bCluster)
//		return RC_NOT_SUPPORT;
//
//	std::string arg;
//	for (auto elm : vecKey)
//	{
//		arg += " " + elm;
//	}
//
//	std::string command = "bitop " + strOp + " " + strDestKey + arg;
//    //return ExecuteImpl("bitop", strOp, strDestKey, vecKey, HASH_SLOT(strDestKey), ppLine, BIND_INT(pnVal));
//	return ExecuteImpl(command, HASH_SLOT(strDestKey), BIND_INT(pnVal));
//}
//
//int CRedisClient::Bitpos(const std::string &strKey, long nBitVal, long *pnVal)
//{
//	std::string command = "bitpos " + strKey + " " + std::to_string(nBitVal);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("bitpos", strKey, ConvertToString(nBitVal), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}

//int CRedisClient::Bitpos(const std::string &strKey, long nBitVal, long nStart, long nEnd, long *pnVal)
//{
//	std::string command = "bitpos " + strKey + " " + std::to_string(nBitVal) + " " + std::to_string(nStart) + " " + std::to_string(nEnd);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("bitpos", strKey, ConvertToString(nBitVal), ConvertToString(nStart), ConvertToString(nEnd), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Decr(const std::string &strKey, long *pnVal)
//{
//	std::string command = "decr " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("decr", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Decrby(const std::string &strKey, long nDecr, long *pnVal)
//{
//	std::string command = "decrby " + strKey + " " + std::to_string(nDecr);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("decrby", strKey, ConvertToString(nDecr), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Get(const std::string &strKey, std::string *pstrVal)
//{
//	std::string command = "get " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(pstrVal));
//    //return ExecuteImpl("get", strKey, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
//}

int CRedisClient::Get(CRedisConnection* connection, const std::string &strKey, std::string *pstrVal)
{
	std::string command = "get " + strKey;
	//return ExecuteImpl("set", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_STR(nullptr), StuResConv());
	return ExecuteImplPool(connection, command, HASH_SLOT(strKey), BIND_STR(pstrVal));
}

//int CRedisClient::Getbit(const std::string &strKey, long nOffset, long *pnVal)
//{
//	std::string command = "getbit " + strKey + " " + std::to_string(nOffset);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("getbit", strKey, ConvertToString(nOffset), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Getrange(const std::string &strKey, long nStart, long nStop, std::string *pstrVal)
//{
//	std::string command = "getrange " + strKey + " " + std::to_string(nStart) + " " + std::to_string(nStop);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(pstrVal));
//	//return ExecuteImpl("getrange", strKey, ConvertToString(nStart), ConvertToString(nStop), HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
//}
//
//int CRedisClient::Getset(const std::string &strKey, std::string *pstrVal)
//{
//	std::string command = "getset " + strKey + " " + *pstrVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(pstrVal));
//	//return ExecuteImpl("getset", strKey, *pstrVal, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
//}
//
//int CRedisClient::Incr(const std::string &strKey, long *pnVal)
//{
//	std::string command = "incr " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("incr", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Incrby(const std::string &strKey, long nIncr, long *pnVal)
//{
//	std::string command = "incrby " + strKey + " " + std::to_string(nIncr);
//	return	ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("incrby", strKey, ConvertToString(nIncr), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Incrbyfloat(const std::string &strKey, double dIncr, double *pdVal)
//{
//	std::string strVal;
//	std::string command = "incrbyfloat " + strKey + " " + std::to_string(dIncr);
//	int nRet = ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(&strVal));
//	//int nRet = ExecuteImpl("incrbyfloat", strKey, ConvertToString(dIncr), HASH_SLOT(strKey), ppLine, BIND_STR(&strVal));
//	if (nRet == RC_SUCCESS)
//		*pdVal = atof(strVal.c_str());
//	return nRet;
//}
//
//int CRedisClient::Mget(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal)
//{
//	if (vecKey.empty())
//		return RC_SUCCESS;
//
//	if (!m_bCluster)
//	{
//		std::string arg;
//		for (auto elm : vecKey)
//		{
//			arg += " " + elm;
//		}
//		std::string command = "mget " + arg;
//		return ExecuteImpl(command, HASH_SLOT(vecKey[0]), BIND_VSTR(pvecVal));
//		//return ExecuteImpl("mget", vecKey, HASH_SLOT(vecKey[0]), nullptr, BIND_VSTR(pvecVal));
//	}
//    else
//    {
//		return RC_NOT_SUPPORT;
//    }
//}
//
//int CRedisClient::Mset(const std::vector<std::string> &vecKey, const std::vector<std::string> &vecVal)
//{
//    if (vecKey.empty())
//        return RC_SUCCESS;
//
//	if (!m_bCluster)
//	{
//		if (vecKey.size() != vecVal.size())
//		{
//			return RC_PARAM_ERR;
//		}
//
//		int index = 0;
//		std::string args;
//		for (auto elm : vecKey)
//		{
//			args += vecKey[index] + " " + vecVal[index] + " ";
//			index++;
//		}
//		std::string command = "mset " + args;
//		return ExecuteImpl(command, HASH_SLOT(vecKey[0]), BIND_STR(nullptr), StuResConv());
//		//return ExecuteImpl("mset", vecKey, vecVal, HASH_SLOT(vecKey[0]), nullptr, BIND_STR(nullptr), StuResConv());
//		//return ExecuteImpl("mset", vecKey, vecVal, HASH_SLOT(vecKey[0]), nullptr, BIND_STR(nullptr), StuResConv());
//
//	}
//    else
//    {
//		return RC_NOT_SUPPORT;
//    }
//    return 0;
//}
//
//int CRedisClient::Psetex(const std::string &strKey, long nMilliSec, const std::string &strVal)
//{
//	std::string command = "psetex " + strKey + " " + std::to_string(nMilliSec) + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(nullptr), StuResConv());
//    //return ExecuteImpl("psetex", strKey, ConvertToString(nMilliSec), strVal, HASH_SLOT(strKey), ppLine, BIND_STR(nullptr), StuResConv());
//}
//
//int CRedisClient::Set(const std::string &strKey, const std::string &strVal, unsigned int expired)
//{
//	std::string command = "set " + strKey + " " + strVal;
//	if (0 < expired)
//	{
//		command = "set " + strKey + " " + strVal + " PX " + std::to_string(expired);
//	}
//	//return ExecuteImpl("set", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_STR(nullptr), StuResConv());
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(nullptr), StuResConv());
//}

int CRedisClient::Set(CRedisConnection* connection, const std::string &strKey, const std::string &strVal, unsigned int expired)
{
	std::string command = "set " + strKey + " " + strVal;
	if (0 < expired)
	{
		command = "set " + strKey + " " + strVal + " PX " + std::to_string(expired);
	}
	//return ExecuteImpl("set", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_STR(nullptr), StuResConv());
	return ExecuteImplPool(connection, command, HASH_SLOT(strKey), BIND_STR(nullptr), StuResConv());
}

//int CRedisClient::Setbit(const std::string &strKey, long nOffset, bool bVal)
//{
//	std::string command = "setbit " + strKey + " " + std::to_string(nOffset) + " " + std::to_string((long)bVal);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(nullptr));
//	//return ExecuteImpl("setbit", strKey, ConvertToString(nOffset), ConvertToString((long)bVal), HASH_SLOT(strKey), ppLine, BIND_INT(nullptr));
//}
//
//int CRedisClient::Setex(const std::string &strKey, long nSec, const std::string &strVal)
//{
//	std::string command = "setex " + strKey + " " + std::to_string(nSec) + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(nullptr), StuResConv());
//    //return ExecuteImpl("setex", strKey, ConvertToString(nSec), strVal, HASH_SLOT(strKey), ppLine, BIND_STR(nullptr), StuResConv());
//}

int CRedisClient::Setex(CRedisConnection* connection, const std::string &strKey, long nSec, const std::string &strVal)
{
	std::string command = "setex " + strKey + " " + std::to_string(nSec) + " " + strVal;
	return ExecuteImplPool(connection, command, HASH_SLOT(strKey), BIND_STR(nullptr), StuResConv());
	//return ExecuteImpl("setex", strKey, ConvertToString(nSec), strVal, HASH_SLOT(strKey), ppLine, BIND_STR(nullptr), StuResConv());
}

//int CRedisClient::Setnx(const std::string &strKey, const std::string &strVal)
//{
//	std::string command = "setnx " + strKey + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(nullptr), IntResConv(RC_SUCCESS));
//    //return ExecuteImpl("setnx", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(nullptr), IntResConv(RC_OBJ_EXIST));
//}

int CRedisClient::Setnx(CRedisConnection* connection, const std::string &strKey, const std::string &strVal)
{
	std::string command = "setnx " + strKey + " " + strVal;
	return ExecuteImplPool(connection, command, HASH_SLOT(strKey), BIND_STR(nullptr), StuResConv());
	//return ExecuteImpl("setnx", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(nullptr), IntResConv(RC_OBJ_EXIST));
}

//int CRedisClient::Setrange(const std::string &strKey, long nOffset, const std::string &strVal, long *pnVal)
//{
//	std::string command = "setrange " + strKey + " " + std::to_string(nOffset) + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("setrange", strKey, ConvertToString(nOffset), strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Strlen(const std::string &strKey, long *pnVal)
//{
//	std::string command = "strlen " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("strlen", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Blpop(const std::string &strKey, long nTimeout, std::vector<std::string> *pvecVal)
//{
//	std::string command = "blpop " + strKey + " " + std::to_string(nTimeout);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_VSTR(pvecVal));
//	//return ExecuteImpl("blpop", strKey, ConvertToString(nTimeout), HASH_SLOT(strKey), nullptr, BIND_VSTR(pvecVal));
//}
//
//int CRedisClient::Blpop(const std::vector<std::string> &vecKey, long nTimeout, std::vector<std::string> *pvecVal)
//{
//    if (m_bCluster)
//        return RC_NOT_SUPPORT;
//	else
//	{
//		std::string arg;
//		for (auto elm : vecKey)
//		{
//			arg += " " + elm;
//		}
//
//		std::string command = "blpop " + arg + " " + std::to_string(nTimeout);
//		return vecKey.empty() ? RC_PARAM_ERR : ExecuteImpl(command, HASH_SLOT(vecKey[0]), BIND_VSTR(pvecVal));
//		//return vecKey.empty() ? RC_PARAM_ERR : ExecuteImpl("blpop", vecKey, ConvertToString(nTimeout), HASH_SLOT(vecKey[0]), nullptr, BIND_VSTR(pvecVal));
//	}
//
//}
//
//int CRedisClient::Brpop(const std::string &strKey, long nTimeout, std::vector<std::string> *pvecVal)
//{
//	std::string command = "brpop " + strKey + " " + std::to_string(nTimeout);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_VSTR(pvecVal));
//    //return ExecuteImpl("brpop", strKey, ConvertToString(nTimeout), HASH_SLOT(strKey), nullptr, BIND_VSTR(pvecVal));
//}
//
//int CRedisClient::Brpop(const std::vector<std::string> &vecKey, long nTimeout, std::vector<std::string> *pvecVal)
//{
//	if (m_bCluster)
//		return RC_NOT_SUPPORT;
//	else
//	{
//		std::string arg;
//		for (auto elm : vecKey)
//		{
//			arg += " " + elm;
//		}
//		
//		std::string command = "brpop " + arg + " " + std::to_string(nTimeout);
//		return vecKey.empty() ? RC_PARAM_ERR : ExecuteImpl(command, HASH_SLOT(vecKey[0]), BIND_VSTR(pvecVal));
//		//return vecKey.empty() ? RC_PARAM_ERR : ExecuteImpl("brpop", vecKey, ConvertToString(nTimeout), HASH_SLOT(vecKey[0]), nullptr, BIND_VSTR(pvecVal));
//
//	}
//}
//
//int CRedisClient::Lindex(const std::string &strKey, long nIndex, std::string *pstrVal)
//{
//	std::string command = "lindex "+ strKey + " " + std::to_string(nIndex);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(pstrVal));
//	//return ExecuteImpl("lindex", strKey, ConvertToString(nIndex), HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
//}
//
//int CRedisClient::Linsert(const std::string &strKey, const std::string &strPos, const std::string &strPivot, const std::string &strVal, long *pnVal)
//{
//	std::string command = "linsert " + strKey + " " + strPos + " " + strPos + " " + strPivot;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("linsert", strKey, strPos, strPivot, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Llen(const std::string &strKey, long *pnVal)
//{
//	std::string command = "llen " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("llen", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Lpop(const std::string &strKey, std::string *pstrVal)
//{
//	std::string command = "lpop " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(pstrVal));
//	//return ExecuteImpl("lpop", strKey, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
//}
//
//int CRedisClient::Lpush(const std::string &strKey, const std::string &strVal, long *pnVal)
//{
//	std::string command = "lpush " + strKey + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("lpush", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}

//int CRedisClient::Lpush(const std::string &strKey, const std::vector<std::string> &vecVal, Pipeline ppLine)
//{
//    return ExecuteImpl("lpush", BIND_INT(nullptr), strKey, vecVal, HASH_SLOT(strKey), ppLine);
//}

//int CRedisClient::Lpushx(const std::string &strKey, const std::string &strVal, long *pnVal)
//{
//	std::string command = "lpushx " + strKey + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal), IntResConv(RC_REPLY_ERR));
//	//return ExecuteImpl("lpushx", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal), IntResConv(RC_REPLY_ERR));
//}
//
//int CRedisClient::Lrange(const std::string &strKey, long nStart, long nStop, std::vector<std::string> *pvecVal)
//{
//	std::string command = "lrange " + strKey + " " + std::to_string(nStart) + " " + std::to_string(nStop);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_VSTR(pvecVal));
//    //return ExecuteImpl("lrange", strKey, ConvertToString(nStart), ConvertToString(nStop), HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
//}
//
//int CRedisClient::Lrem(const std::string &strKey, long nCount, const std::string &strVal, long *pnVal)
//{
//	std::string command = "lrem " + strKey + " " + std::to_string(nCount) + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("lrem", strKey, ConvertToString(nCount), strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Lset(const std::string &strKey, long nIndex, const std::string &strVal)
//{
//	std::string command = "lset " + strKey + " " + std::to_string(nIndex) + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(nullptr), StuResConv());
//	//return ExecuteImpl("lset", strKey, ConvertToString(nIndex), strVal, HASH_SLOT(strKey), ppLine, BIND_STR(nullptr), StuResConv());
//}
//
//int CRedisClient::Ltrim(const std::string &strKey, long nStart, long nStop)
//{
//	std::string command = "ltrim "+ strKey + " " + std::to_string(nStart) + " " + std::to_string(nStop);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(nullptr), StuResConv());
//    //return ExecuteImpl("ltrim", strKey, ConvertToString(nStart), ConvertToString(nStop), HASH_SLOT(strKey), ppLine, BIND_STR(nullptr), StuResConv());
//}
//
//int CRedisClient::Rpop(const std::string &strKey, std::string *pstrVal)
//{
//	std::string command = "rpop " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(pstrVal));
//	//return ExecuteImpl("rpop", strKey, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
//}
//
//int CRedisClient::Rpush(const std::string &strKey, const std::string &strVal, long *pnVal)
//{
//	std::string command = "rpush " + strKey + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("rpush", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}

//int CRedisClient::Rpush(const std::string &strKey, const std::vector<std::string> &vecVal, Pipeline ppLine)
//{
//    return ExecuteImpl("rpush", BIND_INT(nullptr), strKey, vecVal, HASH_SLOT(strKey), ppLine);
//}

//int CRedisClient::Rpushx(const std::string &strKey, const std::string &strVal, long *pnVal)
//{
//	std::string command = "rpushx " + strKey + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal), IntResConv(RC_REPLY_ERR));
//	//return ExecuteImpl("rpushx", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal), IntResConv(RC_REPLY_ERR));
//}
//
///* interfaces for set */
//int CRedisClient::Sadd(const std::string &strKey, const std::string &strVal, long *pnVal)
//{
//	std::string command = "sadd " + strKey + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    /*return ExecuteImpl("sadd", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));*/
//}
//
//int CRedisClient::Scard(const std::string &strKey, long *pnVal)
//{
//	std::string command = "scard " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("scard", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
////int CRedisClient::Sdiff(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
////int CRedisClient::Sinter(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
//int CRedisClient::Sismember(const std::string &strKey, const std::string &strVal, long *pnVal)
//{
//	std::string command = "sismember " + strKey + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("sismember", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Smembers(const std::string &strKey, std::vector<std::string> *pvecVal)
//{
//	std::string command = "smembers " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_VSTR(pvecVal));
//    //return ExecuteImpl("smembers", strKey, HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
//}
//
//int CRedisClient::Spop(const std::string &strKey, std::string *pstrVal)
//{
//	std::string command = "spop " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(pstrVal));
//	//return ExecuteImpl("spop", strKey, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
//}
//
////int CRedisClient::Srandmember(const std::string &strKey, long nCount, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
//int CRedisClient::Srem(const std::string &strKey, const std::string &strVal, long *pnVal)
//{
//	std::string command = "srem " + strKey + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("srem", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Srem(const std::string &strKey, const std::vector<std::string> &vecVal, long *pnVal)
//{
//	std::string arg;
//	for (auto elm : vecVal)
//	{
//		arg += " " + elm;
//	}
//	std::string command = "srem " + strKey + " " + arg;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("srem", strKey, vecVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
////int CRedisClient::Sunion(const std::vector<std::string> &vecKey, std::vector<std::string> *pvecVal, Pipeline ppLine = nullptr);
//
///* interfaces for hash */
//int CRedisClient::Hdel(const std::string &strKey, const std::string &strField, long *pnVal)
//{
//	std::string command = "hdel " + strKey + " " + strField;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("hdel", strKey, strField, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Hexists(const std::string &strKey, const std::string &strField, long *pnVal)
//{
//	std::string command = "hexists " + strKey + " " + strField;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("hexists", strKey, strField, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Hget(const std::string &strKey, const std::string &strField, std::string *pstrVal)
//{
//	std::string command = "hget " + strKey + " " + strField;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(pstrVal));
//    //return ExecuteImpl("hget", strKey, strField, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
//}
//
//int CRedisClient::Hgetall(const std::string &strKey, std::map<std::string, std::string> *pmapFv)
//{
//	std::string command = "hgetall " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_MAP(pmapFv));
//    //return ExecuteImpl("hgetall", strKey, HASH_SLOT(strKey), ppLine, BIND_MAP(pmapFv));
//}
//
//int CRedisClient::Hincrby(const std::string &strKey, const std::string &strField, long nIncr, long *pnVal)
//{
//	std::string command = "hincrby " + strKey + " " + strField + " " + std::to_string(nIncr);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("hincrby", strKey, strField, ConvertToString(nIncr), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Hincrbyfloat(const std::string &strKey, const std::string &strField, double dIncr, double *pdVal)
//{
//    std::string strVal;
//	std::string command = "hincrbyfloat " + strKey + " " + strField + " " + std::to_string(dIncr);
//	int nRet = ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(&strVal));
//    //int nRet = ExecuteImpl("hincrbyfloat", strKey, strField, ConvertToString(dIncr), HASH_SLOT(strKey), ppLine, BIND_STR(&strVal));
//    if (nRet == RC_SUCCESS)
//        *pdVal = atof(strVal.c_str());
//    return nRet;
//}
//
//int CRedisClient::Hkeys(const std::string &strKey, std::vector<std::string> *pvecVal)
//{
//	std::string command = "hkeys " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_VSTR(pvecVal));
//    //return ExecuteImpl("hkeys", strKey, HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
//}
//
//int CRedisClient::Hlen(const std::string &strKey, long *pnVal)
//{
//	std::string command = "hlen " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("hlen", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Hmget(const std::string &strKey, const std::vector<std::string> &vecField, std::vector<std::string> *pvecVal)
//{
//	std::string args;
//	for (auto itr : vecField)
//	{
//		args += itr + " ";
//	}
//	std::string command = "hmget" + strKey + args;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_VSTR(pvecVal));
//    //return ExecuteImpl("hmget", strKey, vecField, HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
//}
//
//int CRedisClient::Hmget(const std::string &strKey, const std::vector<std::string> &vecField, std::map<std::string, std::string> *pmapVal)
//{
//    std::vector<std::string> vecVal;
//	std::string args;
//	for (auto itr : vecField)
//	{
//		args += itr + " ";
//	}
//	std::string command = "hmget " + strKey + " " + args;
//	int nRet = ExecuteImpl(command, HASH_SLOT(strKey), BIND_VSTR(&vecVal));
//    //int nRet = ExecuteImpl("hmget", strKey, vecField, HASH_SLOT(strKey), nullptr, BIND_VSTR(&vecVal));
//    if (nRet == RC_SUCCESS)
//    {
//        if (!vecVal.empty() && vecField.size() != vecVal.size())
//            nRet = RC_RQST_ERR;
//        else if (pmapVal)
//        {
//            pmapVal->clear();
//            if (!vecVal.empty())
//            {
//                auto it1 = vecField.begin();
//                auto it2 = vecVal.begin();
//                while (it1 != vecField.end())
//                {
//                    if (!((*it2).empty()))
//                        pmapVal->insert(std::make_pair(*it1, *it2));
//                    ++it1;
//                    ++it2;
//                }
//            }
//        }
//    }
//    return nRet;
//}

//int CRedisClient::Hmget(const std::string &strKey, const std::set<std::string> &setField, std::map<std::string, std::string> *pmapVal)
//{
//    std::vector<std::string> vecVal;
//    int nRet = ExecuteImpl("hmget", strKey, setField, HASH_SLOT(strKey), nullptr, BIND_VSTR(&vecVal));
//    if (nRet == RC_SUCCESS)
//    {
//        if (vecVal.size() != setField.size())
//            nRet =  RC_RQST_ERR;
//        else if (pmapVal)
//        {
//            pmapVal->clear();
//            auto it1 = setField.begin();
//            auto it2 = vecVal.begin();
//            while (it1 != setField.end())
//            {
//                if (!((*it2).empty()))
//                    pmapVal->insert(std::make_pair(*it1, *it2));
//                ++it1;
//                ++it2;
//            }
//        }
//    }
//    return nRet;
//}
//
//int CRedisClient::Hmset(const std::string &strKey, const std::vector<std::string> &vecField, const std::vector<std::string> &vecVal)
//{
//	if (vecField.size() != vecVal.size())
//	{
//		return -1;
//	}
//
//	int index = 0;
//	std::string args;
//	for (auto itr : vecField)
//	{
//		args = args + vecField[index] + " " + vecVal[index] + " ";
//		index++;
//	}
//	std::string command = "hmset "+ strKey + " " + args;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(nullptr), StuResConv());
//    //return ExecuteImpl("hmset", strKey, vecField, vecVal, HASH_SLOT(strKey), ppLine, BIND_STR(nullptr), StuResConv());
//}
//
//int CRedisClient::Hmset(const std::string &strKey, const std::map<std::string, std::string> &mapFv)
//{
//	std::string args;
//	for (auto itr : mapFv)
//	{
//		args = args + " " + itr.first + " " + itr.second + " ";
//	}
//	std::string command = "hmset " + strKey + " " + args;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(nullptr), StuResConv());
//    //return ExecuteImpl("hmset", strKey, mapFv, HASH_SLOT(strKey), ppLine, BIND_STR(nullptr), StuResConv());
//}
//
//int CRedisClient::Hset(const std::string &strKey, const std::string &strField, const std::string &strVal)
//{
//	std::string command = "hset " + strKey + " " + strField + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(nullptr));
//    //return ExecuteImpl("hset", strKey, strField, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(nullptr));
//}
//
//int CRedisClient::Hsetnx(const std::string &strKey, const std::string &strField, const std::string &strVal)
//{
//	std::string command = "hsetnx " + strKey + " " + strField + " " + strVal;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(nullptr), IntResConv(RC_REPLY_ERR));
//    //return ExecuteImpl("hsetnx", strKey, strField, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(nullptr), IntResConv(RC_REPLY_ERR));
//}
//
//int CRedisClient::Hvals(const std::string &strKey, std::vector<std::string> *pvecVal)
//{
//	std::string command = "hvals " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_VSTR(pvecVal));
//    //return ExecuteImpl("hvals", strKey, HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
//}
//
///* interfaces for sorted set */
//int CRedisClient::Zadd(const std::string &strKey, double dScore, const std::string &strElem, long *pnVal)
//{
//	std::string command = "zadd " + strKey + " " + std::to_string(dScore) + " " + strElem;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("zadd", strKey, ConvertToString(dScore), strElem, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Zcard(const std::string &strKey, long *pnVal)
//{
//	std::string command = "zcard " + strKey;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("zcard", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Zcount(const std::string &strKey, double dMin, double dMax, long *pnVal)
//{
//	std::string command = "zcount " + strKey + std::to_string(dMin) + " " + std::to_string(dMax);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("zcount", strKey, ConvertToString(dMin), ConvertToString(dMax), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Zincrby(const std::string &strKey, double dIncr, const std::string &strElem, double *pdVal)
//{
//	std::string strVal;
//	std::string command = "zincrby " + strKey + " " + std::to_string(dIncr) + " " + strElem;
//	//int nRet = ExecuteImpl("zincrby", strKey, ConvertToString(dIncr), strElem, HASH_SLOT(strKey), ppLine, BIND_STR(&strVal));
//	int nRet = ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(&strVal));
//	if (nRet == RC_SUCCESS)
//		*pdVal = atof(strVal.c_str());
//	return nRet;
//}
//
//int CRedisClient::Zlexcount(const std::string &strKey, const std::string &strMin, const std::string &strMax, long *pnVal)
//{
//	std::string command = "zlexcount " + strKey + " " + strMin + " " + strMax;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("zlexcount", strKey, strMin, strMax, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Zrange(const std::string &strKey, long nStart, long nStop, std::vector<std::string> *pvecVal)
//{
//	std::string command = "zrange " + strKey + " " + std::to_string(nStart) + " " + std::to_string(nStop);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_VSTR(pvecVal));
//    //return ExecuteImpl("zrange", strKey, ConvertToString(nStart), ConvertToString(nStop), HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
//}
//
//int CRedisClient::Zrangewithscore(const std::string &strKey, long nStart, long nStop, std::map<std::string, std::string> *pmapVal)
//{
//	std::string command = "zrange " + strKey + std::to_string(nStart) + " " + std::to_string(nStop) + " " + std::string("WITHSCORES");
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_MAP(pmapVal));
//    //return ExecuteImpl("zrange", strKey, ConvertToString(nStart), ConvertToString(nStop), std::string("WITHSCORES"), HASH_SLOT(strKey), ppLine, BIND_MAP(pmapVal));
//}
//
//int CRedisClient::Zrangebylex(const std::string &strKey, const std::string &strMin, const std::string &strMax, std::vector<std::string> *pvecVal)
//{
//	std::string command = "zrangebylex " + strKey + " " + strMin + " " + strMax;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_VSTR(pvecVal));
//    //return ExecuteImpl("zrangebylex", strKey, strMin, strMax, HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
//}
//
//int CRedisClient::Zrangebyscore(const std::string &strKey, double dMin, double dMax, std::vector<std::string> *pvecVal)
//{
//	std::string command = "zrangebyscore " + strKey + " " + std::to_string(dMin) + " " + std::to_string(dMax);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_VSTR(pvecVal));
//    //return ExecuteImpl("zrangebyscore", strKey, ConvertToString(dMin), ConvertToString(dMax), HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
//}
//
//int CRedisClient::Zrangebyscore(const std::string &strKey, double dMin, double dMax, std::map<std::string, double> *pmapVal)
//{
//    std::map<std::string, std::string> mapVal;
//	std::string command = "zrangebyscore " + strKey + " " + std::to_string(dMin) + " " + std::to_string(dMax) + " " + std::string("WITHSCORES");
//	int nRet = ExecuteImpl(command, HASH_SLOT(strKey), BIND_MAP(&mapVal));
//    //int nRet = ExecuteImpl("zrangebyscore", strKey, ConvertToString(dMin), ConvertToString(dMax), std::string("WITHSCORES"), HASH_SLOT(strKey), ppLine, BIND_MAP(&mapVal));
//    if (nRet == RC_SUCCESS && pmapVal)
//    {
//        pmapVal->clear();
//		for (auto &memPair : mapVal)
//		{
//			pmapVal->insert(std::make_pair(memPair.first, atof(memPair.second.c_str())));
//		}
//    }
//    return nRet;
//}
//
//int CRedisClient::Zrank(const std::string &strKey, const std::string &strElem, long *pnVal)
//{
//	std::string command = "zrank " + strKey + " " + strElem;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("zrank", strKey, strElem, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Zrem(const std::string &strKey, const std::string &strElem, long *pnVal)
//{
//	std::string command = "zrem " + strKey + " " + strElem;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("zrem", strKey, strElem, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Zrem(const std::string &strKey, const std::vector<std::string> &vecElem, long *pnVal)
//{
//	std::string arg;
//	for (auto elm : vecElem)
//	{
//		arg = arg + " " + elm;
//	}
//	std::string command = "zrem" + strKey + " " + arg;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("zrem", strKey, vecElem, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Zremrangebylex(const std::string &strKey, const std::string &strMin, const std::string &strMax, long *pnVal)
//{
//	std::string command = "zremrangebylex " + strKey + " " + strMin + " " + strMax;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("zremrangebylex", strKey, strMin, strMax, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Zremrangebyrank(const std::string &strKey, long nStart, long nStop, long *pnVal)
//{
//	std::string command = "zremrangebyrank " + strKey + " " + std::to_string(nStart) + " " + std::to_string(nStop);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("zremrangebyrank", strKey, ConvertToString(nStart), ConvertToString(nStop), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//
//int CRedisClient::Zremrangebyscore(const std::string &strKey, double dMin, double dMax, long *pnVal)
//{
//	std::string command = "zremrangebyscore " + strKey + " " + std::to_string(dMin) + " " + std::to_string(dMax);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//    //return ExecuteImpl("zremrangebyscore", strKey, ConvertToString(dMin), ConvertToString(dMax), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Zrevrange(const std::string &strKey, long nStart, long nStop, std::vector<std::string> *pvecVal)
//{
//	std::string command = "zrevrange " + strKey + " " + std::to_string(nStart) + " " + std::to_string(nStop);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_VSTR(pvecVal));
//	//return ExecuteImpl("zrevrange", strKey, ConvertToString(nStart), ConvertToString(nStop), HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
//}
//
//int CRedisClient::Zrevrangebyscore(const std::string &strKey, double dMax, double dMin, std::vector<std::string> *pvecVal)
//{
//	std::string command = "zrevrangebyscore " + strKey + " " + std::to_string(dMax) + " " + std::to_string(dMin);
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_VSTR(pvecVal));
//	//return ExecuteImpl("zrevrangebyscore", strKey, ConvertToString(dMax), ConvertToString(dMin), HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
//}
//
//int CRedisClient::Zrevrangebyscore(const std::string &strKey, double dMax, double dMin, std::map<std::string, double> *pmapVal)
//{
//	std::map<std::string, std::string> mapVal;
//	std::string command = "zrevrangebyscore " + strKey + " " + std::to_string(dMax) + " " + std::to_string(dMin) + " " + std::string("WITHSCORES");
//	int nRet = ExecuteImpl(command, HASH_SLOT(strKey), BIND_MAP(&mapVal));
//	//int nRet = ExecuteImpl("zrevrangebyscore", strKeyZrevrank(, ConvertToString(dMax), ConvertToString(dMin), std::string("WITHSCORES"), HASH_SLOT(strKey), ppLine, BIND_MAP(&mapVal));
//	if (nRet == RC_SUCCESS && pmapVal)
//	{
//		pmapVal->clear();
//		for (auto &memPair : mapVal)
//			pmapVal->insert(std::make_pair(memPair.first, atof(memPair.second.c_str())));
//	}
//	return nRet;
//}
//
//int CRedisClient::Zrevrank(const std::string &strKey, const std::string &strElem, long *pnVal)
//{
//	std::string command = "zrevrank " + strKey + " " + strElem;
//	return ExecuteImpl(command, HASH_SLOT(strKey), BIND_INT(pnVal));
//	//return ExecuteImpl("zrevrank", strKey, strElem, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
//}
//
//int CRedisClient::Zscore(const std::string &strKey, const std::string &strElem, double *pdVal)
//{
//    std::string strVal;
//	std::string command = "zscore " + strKey + " " + strElem;
//	int nRet = ExecuteImpl(command, HASH_SLOT(strKey), BIND_STR(&strVal));
//    //int nRet = ExecuteImpl("zscore", strKey, strElem, HASH_SLOT(strKey), ppLine, BIND_STR(&strVal));
//    if (nRet == RC_SUCCESS)
//    {
//        if (strVal.empty())
//            nRet = RC_OBJ_NOT_EXIST;
//        else if (pdVal)
//            *pdVal = atof(strVal.c_str());
//    }
//    return nRet;
//}
//
//int CRedisClient::Time(timeval *ptmVal)
//{
//    return ExecuteImpl("time", -1, BIND_TIME(ptmVal));
//}

int CRedisClient::ExecuteImpl(const std::string &strCmd, int nSlot, TFuncFetch funcFetch, TFuncConvert funcConv)
{
    CRedisCommand *pRedisCmd = new CRedisCommand(strCmd);
//    pRedisCmd->SetArgs();
    pRedisCmd->SetSlot(nSlot);
    pRedisCmd->SetConvFunc(funcConv);
    int nRet = Execute(pRedisCmd);
	if (nRet == RC_SUCCESS)
	{
		//std::cout << "CRedisClient::ExecuteImpl [command:" << strCmd.c_str() << "][slot:" << std::to_string(nSlot) << "]" << std::endl;
		nRet = pRedisCmd->FetchResult(funcFetch);
	}
    delete pRedisCmd;
    return nRet;
}

int CRedisClient::ExecuteImplPool(CRedisConnection* connection, const std::string &strCmd, int nSlot, TFuncFetch funcFetch, TFuncConvert funcConv)
{
	CRedisCommand *pRedisCmd = new CRedisCommand(strCmd);
	//    pRedisCmd->SetArgs();
	pRedisCmd->SetSlot(nSlot);
	pRedisCmd->SetConvFunc(funcConv);
	int nRet = ExecutePool(connection, pRedisCmd);
	if (nRet != RC_SUCCESS)
	{
		//client_log_error("CRedisClient::ExecuteImpl execute failed");
	}
	if (nRet == RC_SUCCESS)
	{
		//std::cout << "CRedisClient::ExecuteImpl [command:" << strCmd.c_str() << "][slot:" << std::to_string(nSlot) << "]" << std::endl;
		nRet = pRedisCmd->FetchResult(funcFetch);
	}
	delete pRedisCmd;
	return nRet;
}

// private methods
bool CRedisClient::LoadSlaveInfo(const std::map<std::string, std::string> &mapInfo)
{
    auto it = mapInfo.find("connected_slaves");
    if (it == mapInfo.end())
        return true;

    std::stringstream ss;
    std::string strItem;
    int nSlave = atoi(it->second.c_str());
    for (int i = 0; i < nSlave; ++i)
    {
        ss.str();
        ss << "slave" << i;
        it = mapInfo.find(ss.str());
        if (it == mapInfo.end())
            continue;

        std::string strHost;
        int nPort = -1;
        ss.str(it->second);
        while (ss >> strItem)
        {
            if (strItem.substr(0, 3) == "ip=")
                strHost = strItem.substr(3);
            else if (strItem.substr(0, 5) == "port=")
                nPort = atoi(strItem.substr(5).c_str());
        }
        //if (!strHost.empty() && nPort != -1)
        //    m_vecRedisServ[0]->SetSlave(strHost, nPort);
		if (!strHost.empty() && nPort != -1)
		{
			//m_vecRedisServ[0]->SetSlave(strHost, nPort);
			std::vector<CRedisServer*>* server = m_vecRedisServ.load();
			server->at(0)->SetSlave(strHost, nPort);
		}

    }
    return true;
}

bool CRedisClient::LoadClusterSlots()
{
	auto server = m_vecRedisServ.load();

	//client_log_trace("CRedisClient::LoadClusterSlots [size:", static_cast<int>(server->size()), "]");
    std::vector<CRedisServer *>* vecRedisServ = new std::vector<CRedisServer*>;
    std::vector<SlotRegion> vecSlot;
    CRedisCommand redisCmd("cluster slots");
	//redisCmd.SetArgs("slots");

	
    for (size_t i = 0; i < server->size(); ++i)
    {
        CRedisServer *pRedisServ = server->at(i);
        if (!pRedisServ->IsValid())
        {
			//for (auto pRedisServ : vecRedisServ)
			for (auto itr = vecRedisServ->begin(); itr != vecRedisServ->end(); ++itr)
			{
				CRedisServer* pRedisServ = *itr;
				//client_log_info("CRedisClient::LoadClusterSlots server not valid [host:", pRedisServ->m_strHost, "][port:", pRedisServ->m_nPort, "]");
				delete pRedisServ;
				pRedisServ = nullptr;
			}
			//client_log_error("CRedisClient::LoadClusterSlots vecRedisServ not valid server");
            return false;
        }

        CRedisServer *pSlotServ = nullptr;
		if (pRedisServ->ServRequest(&redisCmd) == RC_SUCCESS &&
			redisCmd.FetchResult(BIND_SLOT(&vecSlot)) == RC_SUCCESS)
		{
			//client_log_info("LoadClusterSlots cluster slot [size:", vecSlot.size(), "]");
			if (vecSlot.size() == 0)
			{
				//client_log_error("LoadClusterSlots size is 0");
				return false;
			}
			for (auto &slotReg : vecSlot)
			{
				if (!(pSlotServ = FindServer(vecRedisServ, slotReg.strHost, slotReg.nPort)))
				{
					pSlotServ = new CRedisServer(slotReg.strHost, slotReg.nPort, m_nClientTimeout, m_nServerTimeout, m_nConnNum);
					if (!pSlotServ->IsValid())
					{
						//for (auto pRedisServ : vecRedisServ)
						//	delete pRedisS erv;
						for (auto itr = vecRedisServ->begin(); itr != vecRedisServ->end(); ++itr)
						{
							CRedisServer* pRedisServ = *itr;
							//delete pRedisServ;
							//vecRedisServ->erase(itr);
						}
						//client_log_error("CRedisClient::LoadClusterSlots FindSerrver not valid server");
						return false;
					}
					else
					{

					}
					vecRedisServ->push_back(pSlotServ);
				}
				slotReg.pRedisServ = pSlotServ;
			}

			{
				//std::mutex mutex;
				//mutex.lock();
				CleanServer();
				std::sort(vecSlot.begin(), vecSlot.end());

				//m_vecRedisServ = vecRedisServ;
				m_oldServerInfo.push_back(server);
				m_vecSlot = vecSlot;
				m_vecRedisServ.store(vecRedisServ);
				//mutex.unlock();
			}

			//for (auto serv : m_vecRedisServ)
			//auto server = m_vecRedisServ.load();
			//for (std::vector<CRedisServer*>::iterator itr = server->begin(); itr != server->end(); ++itr)
			//{
			//	CRedisServer* serv = *itr;
			//	client_log_info("LoadClusterSlots [address:", (long long)(&m_vecRedisServ), "][host:", serv->m_strHost, "][port:", serv->m_nPort, "][timeout:", serv->m_nCliTimeout, "/", serv->m_nSerTimeout, "]");
			//}
			//for (auto vec : m_vecSlot)
			//{

			//	client_log_info("LoadClusterSlots [address:", (long long)(&m_vecSlot), "][host:", vec.strHost, "][port:", vec.nPort, "][slot:", vec.nStartSlot, "->", vec.nEndSlot, "]");
			//}
			return true;
        }
    }
	//client_log_error("CRedisClient::LoadClusterSlots cout is 0");
    return false;
}

bool CRedisClient::WaitForRefresh()
{
	{
		CSafeLock safeLock(&m_rwLock);
		if (safeLock.TryReadLock())
		{
			m_condAny.notify_all();
		}
		safeLock.ReadUnlock();
	}


    int nRetry = WAIT_RETRY_TIMES;
    while (!m_bValid && nRetry-- > 0)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    return m_bValid;
}

void CRedisClient::CleanServer()
{
	
    m_vecSlot.clear();
	if (false == m_vecRedisServ.is_lock_free())
	{
		auto server = m_vecRedisServ.load();
		//for (auto pRedisServ : m_vecRedisServ)
		//for (auto itr = server->begin(); itr != server->end(); ++itr)
		//{
		//	CRedisServer* pRedisServ = *itr;
		//	delete pRedisServ;
		//	pRedisServ = nullptr;
		//}
		//m_vecRedisServ.clear();
		server->clear();
		m_vecRedisServ.store(server);
	}
}

int CRedisClient::Execute(CRedisCommand *pRedisCmd)
{
    if (!m_bValid)
        return RC_RQST_ERR;

    int nRet = SimpleExecute(pRedisCmd);
    if (!m_bCluster)
    {
        if (nRet == RC_RQST_ERR && WaitForRefresh())
            return SimpleExecute(pRedisCmd);
    }
    else
    {
        if ((nRet == RC_REPLY_ERR && pRedisCmd->IsMovedErr()) || nRet == RC_RQST_ERR)
        {
            if (WaitForRefresh())
                return SimpleExecute(pRedisCmd);
        }
    }
    return nRet;
}

int CRedisClient::ExecutePool(CRedisConnection* connection, CRedisCommand *pRedisCmd)
{
	if (!m_bValid)
	{
		//client_log_error("CRedisClient::Execute not valid");
		return RC_RQST_ERR;
	}

	int nRet = SimpleExecute(connection, pRedisCmd);
	if (nRet != RC_SUCCESS)
	{
		//client_log_error("CRedisClient::Execute SimpleExecute failed");
	}
	if (!m_bCluster)
	{
		if (nRet == RC_RQST_ERR && WaitForRefresh())
			return SimpleExecute(connection, pRedisCmd);
	}
	else
	{
		if ((nRet == RC_REPLY_ERR && pRedisCmd->IsMovedErr()) || nRet == RC_RQST_ERR)
		{
			if (WaitForRefresh())
				return SimpleExecute(connection, pRedisCmd);
		}
	}
	return nRet;
}

int CRedisClient::SimpleExecute(CRedisCommand *pRedisCmd)
{
	CSafeLock safeLock(&m_rwLock);
	if (!safeLock.ReadLock() || !m_bValid)
	{
		safeLock.ReadUnlock();
		return RC_RQST_ERR;
	}

	CRedisServer *pRedisServ = GetMatchedServer(pRedisCmd);
	safeLock.ReadUnlock();
    return pRedisServ ? pRedisServ->ServRequest(pRedisCmd) : RC_RQST_ERR;
}

int CRedisClient::SimpleExecute(CRedisConnection* connection, CRedisCommand *pRedisCmd)
{
	CSafeLock safeLock(&m_rwLock);
	if (!safeLock.ReadLock() || !m_bValid)
	{
		safeLock.ReadUnlock();
		//client_log_error("CRedisClient::SimpleExecute not valid or read lock");
		return RC_RQST_ERR;
	}

	CRedisServer *pRedisServ = GetMatchedServer(pRedisCmd);
	safeLock.ReadUnlock();
	if (!pRedisServ)
	{
		//client_log_error("CRedisClient::SimpleExecute pRedisServ not valid");
	}
	return pRedisServ ? pRedisServ->ServRequest(connection, pRedisCmd) : RC_RQST_ERR;
}


bool CRedisClient::ConvertToMapInfo(const std::string &strVal, std::map<std::string, std::string> &mapVal)
{
    std::stringstream ss(strVal);
    std::string strLine;
    while (std::getline(ss, strLine))
    {
        if (strLine.empty() || strLine[0] == '\r' || strLine[0] == '#')
            continue;

        std::string::size_type nPos = strLine.find(':');
        if (nPos == std::string::npos)
            return false;
        mapVal.insert(std::make_pair(strLine.substr(0, nPos), strLine.substr(nPos + 1)));
    }
    return true;
}

CRedisServer * CRedisClient::GetMatchedServer(const CRedisCommand *pRedisCmd) const
{
	if (!m_bCluster)
	{
		//return m_vecRedisServ.empty() ? nullptr : m_vecRedisServ[0];
		auto server = m_vecRedisServ.load();
		return server->empty() ? nullptr : server->at(0);
	}
    else if (pRedisCmd->GetSlot() != -1)
        return FindServer(pRedisCmd->GetSlot());
    else
    {
		auto server = m_vecRedisServ.load();
        //for (auto &pRedisServ : m_vecRedisServ)
		for (auto itr = server->begin(); itr != server->end(); ++itr)
        {
			CRedisServer* pRedisServ = *itr;
            if (pRedisServ->IsValid())
                return pRedisServ;
        }
        return nullptr;
    }
}

CRedisServer * CRedisClient::FindServer(int nSlot) const
{
	//auto pairIter = std::equal_range(m_vecSlot.begin(), m_vecSlot.end(), nSlot, CompSlot());
	//if (pairIter.first != m_vecSlot.end() && pairIter.first != pairIter.second)
	//	return pairIter.first->pRedisServ;
	//else
	//	return nullptr;
	for (auto elm : m_vecSlot)
	{
		if (elm.nStartSlot <= nSlot && nSlot < elm.nEndSlot)
		{
			return elm.pRedisServ;
		}
	}
	return nullptr;
}

CRedisServer * CRedisClient::FindServer(const std::vector<CRedisServer *> *vecRedisServ, const std::string &strHost, int nPort)
{
    //for (auto &pRedisServ : vecRedisServ)
	for (auto itr = vecRedisServ->begin(); itr != vecRedisServ->end(); ++itr)
    {
		CRedisServer* pRedisServ = *itr;
        if (strHost == pRedisServ->GetHost() && nPort == pRedisServ->GetPort())
            return pRedisServ;
    }
    return nullptr;
}

bool CRedisClient::InSameNode(const std::string &strKey1, const std::string &strKey2)
{
    return m_bCluster ? FindServer(HASH_SLOT(strKey1)) == FindServer(HASH_SLOT(strKey2)) : true;
}

int CRedisClient::Watch(CRedisConnection* connection, const std::string &strKey)
{
	std::string command = "watch " + strKey;
	return ExecuteImplPool(connection, command, HASH_SLOT(strKey), BIND_STR(nullptr));
}

int CRedisClient::Multi(CRedisConnection* connection, const std::string &strKey)
{
	std::string command = "multi ";
	return ExecuteImplPool(connection, command, HASH_SLOT(strKey), BIND_STR(nullptr), StuResConv());
}

int CRedisClient::Exec(CRedisConnection* connection, const std::string &strKey, OUT RedisResult* result)
{
	std::string command = "exec";
	return ExecuteImplPool(connection, command, HASH_SLOT(strKey), BIND_MULTI(result));
}

int CRedisClient::Unwatch(CRedisConnection* connection, const std::string &strKey)
{
	std::string command = "unwatch ";
	return ExecuteImplPool(connection, command, HASH_SLOT(strKey), BIND_STR(nullptr));
}

int CRedisClient::Discard(CRedisConnection* connection, const std::string &strKey)
{
	std::string command = "discard ";
	return ExecuteImplPool(connection, command, HASH_SLOT(strKey), BIND_STR(nullptr));
}

CRedisConnection* CRedisClient::AttachConnection(int slot)
{
	auto server = FindServer(slot);
	if (nullptr == server)
	{
		//client_log_error("CRedisClient::AttachConnection [slot:", slot, "]");
		return nullptr;
	}
	//return server->FetchConnection();
	auto ret = server->FetchConnection();
	if (nullptr == ret)
	{
		//client_log_error("CRedisClient::AttachConnection fetch failed [slot:", slot, "]");
		return nullptr;
	}
	return ret;
}

void CRedisClient::DetachConnection(int slot, CRedisConnection* connection)
{
	auto server = FindServer(slot);
	if (nullptr == server)
	{
		return;
	}
	server->ReturnConnection(connection);
}