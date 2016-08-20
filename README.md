Sider
=====
A Redis clone written in Go. Supports 1.0 commands.
I wrote this to prove to myself that Go can perform nearly as well as C for general networking and disk io.
This project helped to lead to the development of [Tile38](https://github.com/tidwall/tile38) and [Redcon](https://github.com/tidwall/redcon).

**This is an experimental side project and is not intended for production.**

Why?
----
I wanted to understand all of the baseline challenges of running a Redis implementation in Go, and sometimes the best way to understand an architecture is to cleanroom it. 


Commands
--------
**Strings**  
append,bitcount,decr,decrby,get,getset,incr,incrby,mget,mset,msetnx,set,setnx

**Lists**  
lindex,llen,lpop,lpush,lrange,lrem,lset,ltrim,rpoplpush,rpop,rpush

**Sets**  
sadd,scard,smembers,sismember,sdiff,sinter,sunion,sdiffstore,sinterstore,sunionstore,spop,srandmember,srem,smove

**Connection**  
echo,ping,select

**Server**  
auth,bgrewriteaof,bgsave,config,dbsize,debug,flushdb,flushall,info,lastsave,monitor,save,shutdown

**Keys**  
del,exists,expireat,expire,keys,move,randomkey,rename,renamenx,sort,ttl,type


License
-------
BSD
