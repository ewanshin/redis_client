#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "hiredispp.h"

int main(int argc, const char *argv[]) 
{
    std::string auth;
    int port = 6379;
    if(argc > 1)
        auth = argv[1];
    if(argc > 2 )
        port = atoi(argv[2]);

    for(int i = 0; i < 10; ++i )
    {
        if(0 != i) { Sleep(1500); }
        printf("\n\n------------ run: %d --------------\n", i);
        try
        {
            printf("using: localhost:%d, and %s\n", port, auth.empty() ? "NO auth" : "AUTH enabled");
            hiredispp::Redis redis("localhost", auth, port);

            printf("ping: %s\n", redis.ping().c_str());

            std::map<std::string, std::string> info = redis.info();
            printf("info:\n");
            for( std::map<std::string, std::string>::const_iterator i = info.begin(); i != info.end(); ++i )
            {
                std::map<std::string, std::string>::const_iterator i1(i);
                ++i;
                printf("  %25s: %-10s  %25s: %s\n", i1->first.c_str(), i1->second.c_str(), 
                       ((i == info.end()) ? "" : i->first.c_str()), ((i == info.end()) ? "" : i->second.c_str()));
            }
    
            redis.set("good", "food");
            printf("get: %s\n", redis.get("good").c_str());

            redis.beginDel("counter");
            redis.beginIncr("counter");
            redis.beginIncr("counter");
            redis.endCommand();
            printf("incr 1: %ld    2: %ld\n", (long)redis.endCommand(), (long)redis.endCommand());

            int64_t clients = redis.publish("fake_channel", "fake_message");
            printf("published to clients: %d\n", (int)clients);

            /* Create a list of numbers, from 0 to 9 */
            redis.del("test_list");
            for (int j = 0; j < 10; j++) {
                char buf[64];
                sprintf(buf, "%d", j);
                redis.lpush("test_list", buf);
            }

            hiredispp::Redis::Reply reply = redis.lrange("test_list", 0, -1);
            printf("test_list: ");
            for (int j = 0; j < (int)reply.size(); j++) {
                printf("%s  ", ((std::string)reply[j]).c_str());
            }
            printf("\n");
        }
        catch(hiredispp::RedisException e)
        {
            printf("!!!!!!!!Caught Redis Exception: %s\n", e.what());
        }
        catch(std::runtime_error er)
        {
            printf("!!!!!!!!Caught runtime error: %s\n", er.what());
        }
    }

    return 0;
}
