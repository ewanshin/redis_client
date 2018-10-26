/*
 * hiredispp.h
 * This is based on the original hiredispp.h by petrohi @https://github.com/petrohi/hiredispp
 */

//#define DO_UNICODE_WITH_BOOST

#ifndef HIREDISPP_H
#define HIREDISPP_H

#include <string>
#include <vector>
#include <map>
#include <stdexcept>
#include <sstream>
#include <stdint.h>  // int64_t

#include "hiredis.h"

namespace hiredispp
{
    template<typename CharT>
    class RedisEncoding
    {
    public:
        typedef std::basic_string<CharT> RedisString;

        static void decode(const std::string& data, RedisString& string);
        static void decode(const char* data, size_t size, RedisString& string);

        static void encode(const RedisString& string, std::string& data);
        static void encode(const RedisString& string, std::ostream& data);
    };

    class RedisException : public std::exception
    {
        std::string _what;

    public:
        RedisException(const char* cstr) : _what(cstr) { }
        RedisException(const std::string& what) : _what(what) { }

        virtual ~RedisException() throw() { }

        virtual const char* what() const throw() { return _what.c_str(); }
    };

    class RedisElementBase
    {
        redisReply* _r;

    public:
        RedisElementBase(redisReply* r) : _r(r) { }
        RedisElementBase(const RedisElementBase& from) : _r(from._r) {}
        RedisElementBase& operator=(const RedisElementBase& from)
        {
            _r = from._r;
            return *this;
        }
        virtual ~RedisElementBase() { _r = 0; }

        redisReply* get() const { return _r; }
    };

    template<typename CharT>
    class RedisConst
    {
    public:
        static const std::basic_string<CharT> Nil;
        static const std::basic_string<CharT> InfoSeparator;
        static const std::basic_string<CharT> InfoCrLf;
    };

    template<typename CharT>
    class RedisValue
    {
        int type;
        long long number;
        std::basic_string<CharT> str;

    public:
        RedisValue(const redisReply &r) : type (r.type), number(r.integer)
        { 
            if( r.str && r.len > 0 )
            {
                RedisEncoding<CharT>::decode(r.str, r.len, str);
            }
        }
        virtual ~RedisValue() {}

        bool isError() const { return (type == REDIS_REPLY_ERROR); }

        void checkError() const
        {
            if (isError())
            {
                std::ostringstream stringStream;
                stringStream << "Redis reply Error: " << str;
                throw RedisException(stringStream.str());
            }
        }

        bool isNil() const { return (type == REDIS_REPLY_NIL); }

        bool isArray() const { return (type == REDIS_REPLY_ARRAY); }

        std::basic_string<CharT> getErrorMessage() const
        {
            if (isError())  {  return str;  }
            return std::basic_string<CharT>();
        }

        std::basic_string<CharT> getStatus() const
        {
            checkError();
            if (type != REDIS_REPLY_STATUS)
            {
                std::ostringstream stringStream;
                stringStream << "Invalid reply type; " << type << " != " << REDIS_REPLY_STATUS << ". str='" << str << "', num=" << number;
                throw std::runtime_error(stringStream.str());
            }
            return str;
        }

        operator std::basic_string<CharT>() const
        {
            checkError();
            if (type != REDIS_REPLY_STRING && type != REDIS_REPLY_NIL)
            {
                std::ostringstream stringStream;
                stringStream << "Invalid reply type; " << type << " != " << REDIS_REPLY_STATUS << " or " << REDIS_REPLY_NIL << ". str='" << str << "', num=" << number;
                throw std::runtime_error(stringStream.str());
            }
            if (isNil())
            {
                return RedisConst<CharT>::Nil;
            }
            return str;
        }

        operator int64_t() const
        {
            checkError();
            if (type != REDIS_REPLY_INTEGER)
            {
                std::ostringstream stringStream;
                stringStream << "Invalid reply type; " << type << " != " << REDIS_REPLY_INTEGER << ". str='" << str << "', num=" << number;
                throw std::runtime_error(stringStream.str());
            }
            return number;
        }

        // not including these that include optional for now because we don't want the boost dependency
        //#include <boost/optional.hpp>
        //operator boost::optional<int64_t>() const
        //{
        //    checkError();
        //    if (type != REDIS_REPLY_INTEGER && type != REDIS_REPLY_NIL)
        //    {
        //        throw std::runtime_error("Invalid reply type");
        //    }
        //    if (isNil())
        //    {
        //        return boost::optional<int64_t>();
        //    }
        //    return boost::optional<int64_t>(number);
        //}
    };

    template<typename CharT>
    class RedisResult : public RedisValue<CharT>
    {
        std::vector<RedisValue<CharT>> elements;

    public:
        RedisResult<CharT>(redisReply &r) : RedisValue<CharT>(r)
        {
            if( RedisValue<CharT>::isArray() )
            {
                for( int i = 0; i < (int)r.elements; ++i )
                {
                    if( NULL != r.element[i] )
                        elements.push_back(RedisValue<CharT>(*(r.element[i])));
                }
            }
        }

        virtual ~RedisResult() {}

        size_t size() const
        {
            RedisValue<CharT>::checkError();
            if (!RedisValue<CharT>::isArray())
            {
                throw std::runtime_error("Invalid reply type - result type is not an array.");
            }
            return elements.size();
        }

        RedisValue<CharT> operator[](size_t i) const
        {
            RedisValue<CharT>::checkError();
            if (!RedisValue<CharT>::isArray())
            {
                throw std::runtime_error("Invalid reply type - result type is not an array.");
            }
            if (i >= elements.size())
            {
                std::ostringstream stringStream;
                stringStream << "Out of range - array has size " << elements.size();
                throw std::runtime_error(stringStream.str());
            }
            return elements[i];
        }
    };

    template<typename CharT>
    class RedisCommandBase
    {
        std::vector<std::string> _parts;

        void addPart(const std::basic_string<CharT>& s)
        {
            std::string data;
            RedisEncoding<CharT>::encode(s, data);
            _parts.push_back(data);
        }

        void addPart(const char* s)
        {
            std::string data(s, s + ::strlen(s));
            _parts.push_back(data);
        }

    public:
        RedisCommandBase() {}
        RedisCommandBase(const char* s) 
            { addPart(s); }
        RedisCommandBase(const std::basic_string<CharT>& s) 
            { addPart(s); }
        RedisCommandBase(const std::vector<std::string>& parts)
            : _parts(parts) { }

        RedisCommandBase(const RedisCommandBase<CharT>& from)
            : _parts(from._parts) { }

        const std::string &operator[](size_t i) const  { return _parts[i]; }

        size_t size() const { return _parts.size(); }

        RedisCommandBase<CharT>& operator=(const std::vector<std::string>& parts)
            { _parts = parts; return *this; }

        RedisCommandBase<CharT>& operator=(const RedisCommandBase<CharT>& from)
            { _parts = from._parts;  return *this; }

        RedisCommandBase<CharT>& operator<<(const std::basic_string<CharT>& s)
            { addPart(s); return *this; }

        RedisCommandBase<CharT>& operator<<(const char* s)
            { addPart(s); return *this; }

        RedisCommandBase<CharT>& operator<<(const std::vector<std::basic_string<CharT> >& ss)
        {
            _parts.reserve(_parts.size() + ss.size());

            for (size_t i = 0; i < ss.size(); ++i)
            {
                addPart(ss[i]);
            }

            return *this;
        }

        template<class T> RedisCommandBase<CharT>& operator<<(const T& v)
        {
            std::basic_ostringstream<CharT> os;
            os << v;
            addPart(os.str());
            return *this;
        }
    };

    template<typename CharT>
    class RedisBase : public RedisConst<CharT>
    {
        mutable redisContext* _context;

        std::string _host;
        int _port;
        std::string _auth;
        timeval _timeout;

        RedisBase(const RedisBase<CharT>&);
        RedisBase<CharT>& operator=(const RedisBase<CharT>&);

        void connect() const
        {
            if (_context == 0)
            {
                _context = ::redisConnectWithTimeout(_host.c_str(), _port, _timeout);
                if (_context->err)
                {
                    std::ostringstream stringStream;
                    stringStream << "Redis connect error: " << _context->errstr;
                    RedisException e(stringStream.str());
                    ::redisFree(_context);
                    _context = 0;
                    throw e;
                }
                if( !_auth.empty() )
                {
                    redisReply *reply = (redisReply *)::redisCommand(_context, "AUTH %s", _auth.c_str());
                    if( NULL == reply )
                    {
                        RedisException e("Redis authentication failed!");
                        ::redisFree(_context);
                        _context = 0;
                        throw e;
                    }
                    ::freeReplyObject(reply);
                }
            }
        }

    public:
        typedef RedisCommandBase<CharT> Command;
        typedef RedisResult<CharT> Reply;

        RedisBase(const std::string& host, int port = 6379)
            : _context(0), _host(host), _port(port), _timeout(2, 0) { } // 2 sec timeout

        RedisBase(const std::string& host, const std::string &auth, int port = 6379, int timeoutSec = 2)
            : _context(0), _host(host), _auth(auth), _port(port)  { _timeout.tv_sec = 2, _timeout.tv_usec = 0; }

        virtual ~RedisBase()
        {
            if (_context != 0)
            {
                ::redisFree(_context);
                _context = 0;
            }
        }

        const std::string& host() const { return _host; }
        int port() const { return _port; }

        Reply endCommand() const
        {
            redisReply* r;
            if (::redisGetReply(_context, reinterpret_cast<void**>(&r)) != REDIS_OK)
            {
                std::ostringstream stringStream;
                stringStream << "Redis command error: " << _context->errstr;
                RedisException e(stringStream.str());
                ::redisFree(_context);
                _context = 0;
                throw e;
            }
            Reply reply(*r);
            ::freeReplyObject(r);
            return reply;
        }

        void beginInfo() const
        {
            connect();
            ::redisAppendCommand(_context, "INFO");
        }

        std::map<std::basic_string<CharT>, std::basic_string<CharT> > info() const
        {
            beginInfo();

            std::map<std::basic_string<CharT>, std::basic_string<CharT> > info;
            std::basic_string<CharT> lines = endCommand();

            size_t i = 0;
            size_t j = lines.find(RedisConst<CharT>::InfoCrLf);

            while (i != std::basic_string<CharT>::npos)
            {
                std::basic_string<CharT> line = lines.substr(i, j == std::basic_string<CharT>::npos ? j : j - i);
                i = j == std::basic_string<CharT>::npos ? j : j + RedisConst<CharT>::InfoCrLf.size();
                j = lines.find(RedisConst<CharT>::InfoCrLf, i);

                size_t p = line.find(RedisConst<CharT>::InfoSeparator);
                
                if (p != std::basic_string<CharT>::npos &&
                    p < (line.size() - 1))
                {
                    info[line.substr(0, p)] = line.substr(p + 1, std::basic_string<CharT>::npos);
                }
            }
            return info;
        }

        void beginPing() const
        {
            connect();
            ::redisAppendCommand(_context, "PING");
        }

        std::basic_string<CharT> ping() const
        {
            beginPing();
            return endCommand().getStatus();
        }

        void beginSelect(int database) const
        {
            connect();
            ::redisAppendCommand(_context, "SELECT %d", database);
        }

        void select(int database) const
        {
            beginSelect(database);
            endCommand();
        }

        // These macros help to reduce code duplication below, they handle 1, 2, or 3 parameters cases
        // The one parameter version 1 parameter will produce the equivalent of:
        // void beginGet(const std::basic_string<CharT>& key) const
        // {
        //     beginCommand(Command("GET") << key);
        // }
        // std::basic_string<CharT> get(const std::basic_string<CharT>& key) const
        // {
        //     beginGet(key);
        //     return endCommand();
        // }

#define DEFINE_BEGIN_COMMAND1(beginFunc, cmdName, argType)\
        void beginFunc (const argType &arg) const\
        { beginCommand(Command( cmdName ) << arg ); }

#define DEFINE_COMMAND1(cmdFunc, beginFunc, cmdName, argType, returnType) \
        DEFINE_BEGIN_COMMAND1(beginFunc, cmdName, argType)\
        returnType cmdFunc (const argType &arg) const\
        { beginFunc (arg); return endCommand(); }

#define DEFINE_BEGIN_COMMAND2(beginFunc, cmdName, argType1, argType2)\
        void beginFunc (const argType1 &arg1, const argType2 &arg2) const\
        { beginCommand(Command( cmdName ) << arg1 << arg2 ); }

#define DEFINE_COMMAND2(cmdFunc, beginFunc, cmdName, argType1, argType2, returnType) \
        DEFINE_BEGIN_COMMAND2(beginFunc, cmdName, argType1, argType2)\
        returnType cmdFunc (const argType1 &arg1, const argType2 &arg2) const\
        { beginFunc (arg1, arg2); return endCommand(); }
        
#define DEFINE_COMMAND3(cmdFunc, beginFunc, cmdName, argType1, argType2, argType3, returnType) \
        void beginFunc (const argType1 &arg1, const argType2 &arg2, const argType3 &arg3) const\
        { beginCommand(Command( cmdName ) << arg1 << arg2 << arg3 ); }\
        returnType cmdFunc (const argType1 &arg1, const argType2 &arg2, const argType3 &arg3) const\
        { beginFunc (arg1, arg2, arg3); return endCommand(); }

        // get key
        DEFINE_COMMAND1(get, beginGet, "GET", std::basic_string<CharT>, std::basic_string<CharT>)
        
        // mget key1 key2
        DEFINE_COMMAND1(mget, beginMget, "MGET", std::vector<std::basic_string<CharT>>, Reply)

        // exists key
        DEFINE_BEGIN_COMMAND1(beginExists, "EXISTS", std::basic_string<CharT>)
        bool exists(const std::basic_string<CharT>& key) const
        {
            beginExists(key);
            return(((int64_t)endCommand()) != 0);
        }

        // set key value
        DEFINE_BEGIN_COMMAND2(beginSet, "SET", std::basic_string<CharT>, std::basic_string<CharT>)
        void set(std::basic_string<CharT> key, std::basic_string<CharT> value)
        {
            beginSet(key, value); endCommand();
        }

        // setnx key value
        DEFINE_COMMAND2(setnx, beginSetnx, "SETNX", std::basic_string<CharT>, std::basic_string<CharT>, int64_t)

        // incr key
        DEFINE_COMMAND1(incr, beginIncr, "INCR", std::basic_string<CharT>, int64_t)

        // keys pattern
        DEFINE_COMMAND1(keys, beginKeys, "KEYS", std::basic_string<CharT>, Reply)

        // del key[s]
        DEFINE_COMMAND1(del, beginDel, "DEL", std::basic_string<CharT>, int64_t)
        DEFINE_COMMAND1(del, beginDel, "DEL", std::vector<std::basic_string<CharT>>, int64_t)

        // lpush key value
        DEFINE_BEGIN_COMMAND2(beginLpush, "LPUSH", std::basic_string<CharT>, std::basic_string<CharT>)
        void lpush(std::basic_string<CharT> key, std::basic_string<CharT> value)
        {
            beginLpush(key, value); endCommand();
        }
        // lpop key
        DEFINE_COMMAND1(lpop, beginLpop, "LPOP", std::basic_string<CharT>, std::basic_string<CharT>)
        // rpush key value
        DEFINE_BEGIN_COMMAND2(beginRpush, "RPUSH", std::basic_string<CharT>, std::basic_string<CharT>)
        void rpush(std::basic_string<CharT> key, std::basic_string<CharT> value)
        {
            beginRpush(key, value); endCommand();
        }
        // rpop key
        DEFINE_COMMAND1(rpop, beginRpop, "RPOP", std::basic_string<CharT>, std::basic_string<CharT>)

        // lindex key index
        DEFINE_COMMAND2(lindex, beginLindex, "LINDEX", std::basic_string<CharT>, int64_t, std::basic_string<CharT>)
        // lrange key start end
        DEFINE_COMMAND3(lrange, beginLrange, "LRANGE", std::basic_string<CharT>, int64_t, int64_t, Reply)
        // llen key
        DEFINE_COMMAND1(llen, beginLlen, "LLEN", std::basic_string<CharT>, int64_t)

        // hget key field
        DEFINE_COMMAND2(hget, beginHget, "HGET", std::basic_string<CharT>, std::basic_string<CharT>, std::basic_string<CharT>)
        // hdel key field
        DEFINE_COMMAND2(hdel, beginHdel, "HDEL", std::basic_string<CharT>, std::basic_string<CharT>, int64_t)
        // hset key field value
        DEFINE_COMMAND3(hset, beginHset, "HSET", std::basic_string<CharT>, std::basic_string<CharT>, std::basic_string<CharT>, int64_t)
        // hsetnx key field value
        DEFINE_COMMAND3(hsetnx, beginHsetnx, "HSETNX", std::basic_string<CharT>, std::basic_string<CharT>, std::basic_string<CharT>, int64_t)
        // hincrby key field value
        DEFINE_COMMAND3(hincrby, beingHincrby, "HINCRBY", std::basic_string<CharT>, std::basic_string<CharT>, int64_t, int64_t)
        // hgetall key
        DEFINE_COMMAND1(hgetall, beginHgetall, "HGETALL", std::basic_string<CharT>, Reply)

        // publish channel message, returns number of clients receiving message
        DEFINE_COMMAND2(publish, beginPublish, "PUBLISH", std::basic_string<CharT>, std::basic_string<CharT>, int64_t)

        // sadd key member
        DEFINE_COMMAND2(sadd, beginSadd, "SADD", std::basic_string<CharT>, std::basic_string<CharT>, int64_t)

        // sismember key member
        DEFINE_BEGIN_COMMAND2(beginSismember, "SISMEMBER", std::basic_string<CharT>, std::basic_string<CharT>)
        bool sismember(const std::basic_string<CharT>& key, const std::basic_string<CharT>& member) const
        {
            beginSismember(key, member);
            return ((int64_t)endCommand() == 1);
        }

        // srem key member
        DEFINE_COMMAND2(srem, beginSrem, "SREM", std::basic_string<CharT>, std::basic_string<CharT>, int64_t)

        // smembers key
        DEFINE_COMMAND1(smembers, beginSmembers, "SMEMBERS", std::basic_string<CharT>, Reply)

        // sdiff key diffkey[s]
        DEFINE_COMMAND2(sdiff, beginSdiff, "SDIFF", std::basic_string<CharT>, std::basic_string<CharT>, Reply)
        DEFINE_COMMAND2(sdiff, beginSdiff, "SDIFF", std::basic_string<CharT>, std::vector<std::basic_string<CharT>>, Reply)

        // sunion keys
        DEFINE_COMMAND1(sunion, beginSunion, "SUNION", std::basic_string<CharT>, Reply)

        // sunion key0 key1
        DEFINE_COMMAND2(sunion, beginSunion, "SUNION", std::basic_string<CharT>, std::basic_string<CharT>, Reply)

        // scard key
        DEFINE_COMMAND1(scard, beginScard, "SCARD", std::basic_string<CharT>, int64_t)

        void beginShutdown() const
        {
            beginCommand(Command("SHUTDOWN"));
        }
        std::basic_string<CharT> shutdown() const
        {
            beginShutdown();
            return endCommand();
        }

        // zadd key member
        DEFINE_COMMAND3(zadd, beginZadd, "ZADD", std::basic_string<CharT>, double, std::basic_string<CharT>, int64_t)
        // zrem key member
        DEFINE_COMMAND2(zrem, beginZrem, "ZREM", std::basic_string<CharT>, std::basic_string<CharT>, int64_t)

        // not including these that include optional for now because we don't want the boost dependency
        //#include <boost/optional.hpp>
        //// zrank key member
        //DEFINE_COMMAND2(zrank, beginZrank, "ZRANK", std::basic_string<CharT>, std::basic_string<CharT>, boost::optional<int64_t>)
        //// zrevrank key member
        //DEFINE_COMMAND2(zrevrank, beginZrevrank, "ZREVRANK", std::basic_string<CharT>, std::basic_string<CharT>, boost::optional<int64_t>)

        // zrange key start end
        DEFINE_COMMAND3(zrange, beginZrange, "ZRANGE", std::basic_string<CharT>, int64_t, int64_t, Reply)
        // zrevrange key start end
        DEFINE_COMMAND3(zrevrange, beginZrevrange, "ZREVRANGE", std::basic_string<CharT>, int64_t, int64_t, Reply)

        // zrangebyscore key min max
        DEFINE_COMMAND3(zrangebyscore, beginZrangebyscore, "ZRANGEBYSCORE", std::basic_string<CharT>, std::basic_string<CharT>, std::basic_string<CharT>, Reply)
        // zrevrangebyscore key max min
        DEFINE_COMMAND3(zrevrangebyscore, beginZrevrangebyscore, "ZREVRANGEBYSCORE", std::basic_string<CharT>, std::basic_string<CharT>, std::basic_string<CharT>, Reply)

        // zcard key
        DEFINE_COMMAND1(zcard, beginZcard, "ZCARD", std::basic_string<CharT>, int64_t)

        void beginCommand(const Command& command) const
        {
            connect();
            const char **argv = new const char *[command.size()];
            size_t *argvlen = new size_t[command.size()];
            for (size_t i = 0; i < command.size(); ++i)
            {
                argv[i] = command[i].data();
                argvlen[i] = command[i].size();
            }
            ::redisAppendCommandArgv(_context, command.size(), argv, argvlen);
            delete []argv; argv = NULL;
            delete []argvlen; argvlen = NULL;
        }

        Reply doCommand(const Command& command) const
        {
            beginCommand(command);
            return endCommand();
        }

        void doPipeline(const std::vector<Command>& commands) const
        {
            for (size_t i = 0; i < commands.size(); ++i)
            {
                beginCommand(commands[i]);
            }
            for (size_t i = 0; i < commands.size(); ++i)
            {
                endCommand();
            }
        }

        void doPipeline(const std::vector<Command>& commands, std::vector<Reply>& replies) const
        {
            for (size_t i = 0; i < commands.size(); ++i)
            {
                beginCommand(commands[i]);
            }
            replies.reserve(commands.size());
            for (size_t i = 0; i < commands.size(); ++i)
            {
                replies.push_back(endCommand());
            }
        }

        DEFINE_BEGIN_COMMAND1(beginWatch, "WATCH", std::vector<std::basic_string<CharT>>)
        void watch(const std::vector<std::basic_string<CharT>> &keys) const
        {
            beginWatch(keys); endCommand();
        }

        void watch(const std::basic_string<CharT> &key) const
        {
            std::vector<std::basic_string<CharT> > keys;
            keys.push_back(key);
            watch(keys);
        }

        void beginUnwatch() const
        {
            beginCommand(Command("UNWATCH"));
        }

        void unwatch() const
        {
            beginUnwatch();
            endCommand();
        }

        Reply doTransaction(const std::vector<Command>& commands) const
        {
            beginCommand(Command("MULTI"));

            for (size_t i = 0; i < commands.size(); ++i)
            {
                beginCommand(commands[i]);
            }

            beginCommand(Command("EXEC"));
            endCommand();

            for (size_t i = 0; i < commands.size(); ++i)
            {
                endCommand();
            }

            return endCommand();
        }
    };

    typedef RedisBase<char> Redis;
#if defined( DO_UNICODE_WITH_BOOST )
    typedef RedisBase<wchar_t> wRedis;
#endif

    template<>
    inline void RedisEncoding<char>::decode(const char* data, size_t size, std::basic_string<char>& string)
    {
        string.assign(data, size);
    }

    template<>
    inline void RedisEncoding<char>::decode(const std::string& data, std::basic_string<char>& string)
    {
        string=data;
    }

    template<>
    inline void RedisEncoding<char>::encode(const std::basic_string<char>& string, std::string& data)
    {
        data.resize(0);
        data.append(string.begin(), string.end());
    }

    template<>
    inline void RedisEncoding<char>::encode(const std::basic_string<char>& string, std::ostream& data)
    {
        data << string;
    }


}

#endif // HIREDISPP_H
