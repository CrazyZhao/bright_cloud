package com.bright.cloudconsumer.redis;

import com.alibaba.fastjson.JSON;
import com.bright.cloudconsumer.utils.GfJsonUtil;
import com.bright.cloudconsumer.utils.LogExceptionStackTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import redis.clients.jedis.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

@ConfigurationProperties(prefix = "redis.manager")
@Component
public class RedisManager {
    private static final Logger logger = LoggerFactory.getLogger(RedisManager.class);
    private int maxActive = 8;
    private int maxIdle = 8;
    private int maxWait = 10000;
    private int timeOut = 10000;
    private boolean testOnBorrow = true;
    private String host;
    private int port;
    private String auth;
    private int db = 0;

    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public void setMaxWait(int maxWait) {
        this.maxWait = maxWait;
    }

    public void setTimeOut(int timeOut) {
        this.timeOut = timeOut;
    }

    public void setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setAuth(String auth) {
        this.auth = auth;
    }

    public void setDb(int db) {
        this.db = db;
    }

    private JedisPool jedisPool = null;

    @Bean
    protected JedisPool init() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(maxActive);
        config.setMaxIdle(maxIdle);
        config.setMaxWaitMillis(maxWait);
        config.setTestOnBorrow(testOnBorrow);

        if (auth != null && !"".equals(auth)) {
            jedisPool = new JedisPool(config, host, port, timeOut, auth, db);
        } else {
            jedisPool = new JedisPool(config, host, port, timeOut);

        }
        return jedisPool;
    }

    /**
     * 释放jedis资源
     *
     * @param jedis
     */
    private void returnResource(final Jedis jedis) {
        if (jedis != null) {
            jedisPool.returnResource(jedis);
        }
    }

    public Long incr(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.incr(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：incr key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：incr key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long incr(String key, Long integer) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.incrBy(key, integer);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：incr key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：incr key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    /**
     * @return Set<String> 匹配到的结果集合
     * @Description 通配所有相关的redis key值
     * @params pattern 需要匹配的表达式
     */
    public Set<String> keys(String key, String pattern) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            return sj.keys(pattern);
        } catch (Exception e) {
            logger.error("command：keys key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }

        return null;
    }

    public Set<String> hkeys(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Set<String> v = sj.hkeys(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：hkeys key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：hkeys key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public List<String> hvals(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            List<String> v = sj.hvals(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：hvals key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：hvals key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public String get(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            String v = sj.get(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：get key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：get key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }

        return null;
    }

    public String getrange(String key, long startOffSet, long endOffSet) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            String ret = jedis.getrange(key, startOffSet, endOffSet);
            long time = System.currentTimeMillis() - start;
            if (time > 500) {
                logger.warn("ip={}, host={}, command=getrange, execute_time={}ms", host, port, time);
            }
            return ret;
        } catch (Exception e) {
            logger.error("command=getrange, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public String spop(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            String v = sj.spop(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：spop key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：sadd key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long scard(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.scard(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：scard key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：scard key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Set<String> sdiff(String... keys) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Set<String> set = jedis.sdiff(keys);
            long time = System.currentTimeMillis() - start;
            if (time > 500) {
                logger.warn("ip={}, port={}, command= sdiff, keys={}, execute_time={}ms", host, port, keys, time);
            }
            return set;
        } catch (Exception e) {
            logger.error(" command= sdiff, keys={}, error={}", keys, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long sdiffstore(String dstkey, String... keys) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long num = jedis.sdiffstore(dstkey, keys);
            long time = System.currentTimeMillis() - start;
            if (time > 500) {
                logger.warn("ip={}, port={}, dstkey={}, command=sdiffstore, execute_time={}ms", host, port, dstkey, time);
            }
            return num;
        } catch (Exception e) {
            logger.error("command=sdiffstore, dstkey={}, error={}", dstkey, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Set<String> sinter(String... keys) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Set<String> set = jedis.sinter(keys);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, port={}, command=sinter, execute_time={}", host, port, time);
            return set;
        } catch (Exception e) {
            logger.error("command=sinter, keys={}, error={}", JSON.toJSONString(keys), LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long sinterstore(String dstkey, String... keys) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long ret = jedis.sinterstore(dstkey, keys);
            long time = System.currentTimeMillis() - start;
            if (time > 500) {
                logger.warn("ip={}, port={}, command=sinterstore, execut_time={}ms", host, port, time);
            }
            return ret;
        } catch (Exception e) {
            logger.error("command=sinterstore, dstkey={}, error={}", dstkey, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Set<String> spop(String key, int count) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Set<String> v = sj.spop(key, count);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：spop key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：sadd key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public String set(String key, String value) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            String v = sj.set(key, value);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：set key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：set key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }

        return null;
    }

    public String setex(String key, int seconds, String value) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            String ret = jedis.setex(key, seconds, value);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, port={}, command=setex, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=setex, key={}, seconds={}, value={}, error={}", key, seconds, value, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long setnx(String key, String value) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long ret = jedis.setnx(key, value);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, port={}, command=setnx, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=setnx, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long setrange(String key, long offset, String value) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long ret = jedis.setrange(key, offset, value);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, host={}, command=setrange, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=setrange, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long strlen(String key) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long ret = jedis.strlen(key);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, host={}, command=strlen, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=strlen, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public void set(List<String> keys, List<String> values) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Pipeline pip = sj.pipelined();
            for (int i = 0; i < keys.size(); i++) {
                pip.set(keys.get(i), values.get(i));
            }
            pip.sync();
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：set list key:{} execution time:{}ms", this.host, this.port, GfJsonUtil.toJSONString(keys), time);
            }
        } catch (Exception e) {
            logger.error("command：set key:{} ex={}", GfJsonUtil.toJSONString(keys), LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
    }

    public void mset(String... keysvalues) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            sj.mset(keysvalues);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：mset key:{} execution time:{}ms", this.host, this.port, keysvalues, time);
            }
        } catch (Exception e) {
            logger.error("command：mset key:{} ex={}", GfJsonUtil.toJSONString(keysvalues), LogExceptionStackTrace.erroStackTrace(e), LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
    }

    public String setObject(String key, Object value) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            String objectValue = GfJsonUtil.toJSONString(value);
            String v = sj.set(key, objectValue);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：setObject key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：setObject key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }

        return null;
    }

    public void set(String key, List<String> strings) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Pipeline pip = sj.pipelined();
            for (String s : strings) {
                pip.set(key, s);
            }
            pip.sync();
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：set key:{} execution time:{}ms", this.host, this.port, key, time);
            }
        } catch (Exception e) {
            logger.error("command：Pipeline set key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
    }

    public String getSet(String key, String value) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            String v = sj.getSet(key, value);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：getSet key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：getSet key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long sadd(String key, String... members) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.sadd(key, members);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：sadd key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：sadd key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long srem(String key, String... members) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.srem(key, members);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：srem key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：srem key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Set<String> sunion(String... keys) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Set<String> ret = jedis.sunion(keys);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, port={}, command=sunion, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=sunion, keys={}, error={}", JSON.toJSONString(keys), LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long incrby(String key, long num) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long ret = jedis.incrBy(key, num);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, host={}, command=incrby, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=incrby, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Double incrbyfloat(String key, double d) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Double ret = jedis.incrByFloat(key, d);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, host={}, command=incrbyfloat, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=incrbyfloat, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long sunionstore(String dstkey, String... keys) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long ret = jedis.sunionstore(dstkey, keys);
            long time = System.currentTimeMillis();
            if (time > 500)
                logger.warn("ip={}, port={}, command=sunionstore, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=sunionstore, dstkey={}, error={}", dstkey, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long zadd(String key, Map<String, Double> map) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.zadd(key, map);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：zadd key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：zadd key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public void zadd(String key, List<Map<String, Double>> maps) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Pipeline pip = sj.pipelined();
            for (Map<String, Double> map : maps) {
                pip.zadd(key, map);
            }
            pip.sync();
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：Pipeline zadd key:{} execution time:{}ms", this.host, this.port, key, time);
            }
        } catch (Exception e) {
            logger.error("command：Pipeline zadd key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
    }

    public void sadd(String key, List<String> strings) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Pipeline pip = sj.pipelined();
            for (String s : strings) {
                pip.sadd(key, s);
            }
            pip.sync();
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：Pipeline sadd key:{} execution time:{}ms", this.host, this.port, key, time);
            }
        } catch (Exception e) {
            logger.error("command：Pipeline sadd key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
    }

    public String hget(String key, String field) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            String v = sj.hget(key, field);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：hget key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：hget key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public String hmset(String key, Map<String, String> hash) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            String v = sj.hmset(key, hash);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：hmset key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：hmset key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public void hmset(String key, List<Map<String, String>> hash) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Pipeline pip = sj.pipelined();
            for (Map<String, String> h : hash) {
                pip.hmset(key, h);
            }
            pip.sync();
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：Pipeline hmset key:{} execution time:{}ms", this.host, this.port, key, time);
            }
        } catch (Exception e) {
            logger.error("command：Pipeline hmset key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
    }

    public List<String> hmget(String key, String... fields) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            List<String> v = sj.hmget(key, fields);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：hmget key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：hmget key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long del(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.del(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：del key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：del key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long decr(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.decr(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：decr key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：decr key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long decr(String key, Long integer) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.decrBy(key, integer);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：decrBy key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：decrBy key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long append(String key, String value) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.append(key, value);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：append key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：append key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long bitcount(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.bitcount(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：bitcount key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：bitcount key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long bitpos(String key, boolean bool) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.bitpos(key, bool);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：bitpos key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：bitpos key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long expire(String key, int seconds) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.expire(key, seconds);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：expire key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：expire key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long expireAt(String key, Long unixTime) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.expireAt(key, unixTime);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：expireAt key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：expireAt key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long hdel(String key, String... fields) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.hdel(key, fields);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：hdel key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：hdel key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Boolean hexists(String key, String field) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Boolean ret = jedis.hexists(key, field);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, host={}, command=hexists, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=hexists, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long hincrBy(String key, String field, Long value) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.hincrBy(key, field, value);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：hincrBy key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：hincrBy key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Double hincrbyfloat(String key, String field, Double d) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Double ret = jedis.hincrByFloat(key, field, d);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, host={}, command=hincrbyfloat, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=hincrbyfloat, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long hlen(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.hlen(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：hlen key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：hlen key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long hset(String key, String field, String value) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.hset(key, field, value);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：hset key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：hset key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public void hset(String key, String field, List<String> strings) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Pipeline pip = sj.pipelined();
            for (String s : strings) {
                pip.hset(key, field, s);
            }
            pip.sync();
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：Pipeline hset key:{} execution time:{}ms", this.host, this.port, key, time);
            }
        } catch (Exception e) {
            logger.error("command：Pipeline hset key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
    }

    public Long hsetnx(String key, String field, String value) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.hsetnx(key, field, value);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：hsetnx key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：hsetnx key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public void hsetnx(String key, String field, List<String> strings) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Pipeline pip = sj.pipelined();
            for (String s : strings) {
                pip.hsetnx(key, field, s);
            }
            pip.sync();
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：Pipeline hsetnx key:{} execution time:{}ms", this.host, this.port, key, time);
            }
        } catch (Exception e) {
            logger.error("command：Pipeline hsetnx key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
    }

    public Long llen(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.llen(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：llen key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：llen key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long lpush(String key, String... strings) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.lpush(key, strings);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：lpush key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：lpush key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long lpushx(String key, String... string) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long ret = jedis.lpushx(key, string);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, host={}, command=lpushx, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=lpushx, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public void lpush(String key, List<String> strings) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Pipeline pip = sj.pipelined();
            for (String s : strings) {
                pip.lpush(key, s);
            }
            pip.sync();
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：Pipeline lpush key:{} execution time:{}ms", this.host, this.port, key, time);
            }
        } catch (Exception e) {
            logger.error("command：Pipeline lpush key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
    }

    public Long lpushx(String key, String string) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.lpushx(key, string);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：lpushx key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：lpushx key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public void lpushx(String key, List<String> strings) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Pipeline pip = sj.pipelined();
            for (String s : strings) {
                pip.lpushx(key, s);
            }
            pip.sync();
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：Pipeline lpushx key:{} execution time:{}ms", this.host, this.port, key, time);
            }
        } catch (Exception e) {
            logger.error("command：Pipeline lpushx key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
    }

    public String lindex(String key, int index) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            String v = sj.lindex(key, index);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：lindex key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：lindex key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public String lpop(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            String v = sj.lpop(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：lpop key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：lpop key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public List<String> lpop(String key, int start, int end) {
        Long startTime = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            List<String> v = sj.lrange(key, start, end);
            Long endTime = System.currentTimeMillis();
            Long time = endTime - startTime;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：lpop key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：lpop key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long lrem(String key, int count, String value) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.lrem(key, count, value);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：lrem key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：lrem key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public String lset(String key, long index, String value) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            String ret = jedis.lset(key, index, value);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, host={}, command=lset, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=lset, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public String ltrim(String key, long start, long end) {
        long startTime = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            String ret = jedis.ltrim(key, start, end);
            long time = System.currentTimeMillis() - startTime;
            if (time > 500)
                logger.warn("ip={}, host={}, command=ltrim, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=ltrim, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long move(String key, int dbIndex) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.move(key, dbIndex);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：move key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：move key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long persist(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.persist(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：persist key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：persist key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long persist(String key, Long milliseconds) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.pexpire(key, milliseconds);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：persist key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：persist key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long pexpireAt(String key, Long milliseconds) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.pexpireAt(key, milliseconds);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：pexpireAt key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：pexpireAt key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long pfadd(String key, String... elements) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.pfadd(key, elements);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：pfadd key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：pfadd key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long pttl(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.pttl(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：pttl key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：pttl key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
            return null;
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
    }

    public Long pfcount(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.pfcount(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：pfcount key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：pfcount key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long rpush(String key, String... strings) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.rpush(key, strings);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：rpush key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：rpush key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public void rpush(String key, List<String> strings) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Pipeline pip = sj.pipelined();
            for (String s : strings) {
                pip.rpush(key, s);
            }
            pip.sync();
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：Pipeline rpush key:{} execution time:{}ms", this.host, this.port, key, time);
            }
        } catch (Exception e) {
            logger.error("command：Pipeline rpush key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
    }

    public void rpushx(String key, List<String> strings) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Pipeline pip = sj.pipelined();
            for (String s : strings) {
                pip.rpushx(key, s);
            }
            pip.sync();
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：Pipeline rpushx key:{} execution time:{}ms", this.host, this.port, key, time);
            }
        } catch (Exception e) {
            logger.error("command：Pipeline rpushx key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
    }

    public Long rpushx(String key, String... strings) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.rpushx(key, strings);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：rpushx key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：rpushx key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public String rpop(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            String v = sj.rpop(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：rpop key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：rpop key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Double zscore(String key, String member) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Double v = sj.zscore(key, member);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：zscore key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：zscore key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long zcard(String key) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long len = jedis.zcard(key);
            long time = System.currentTimeMillis() - start;
            if (time > 500) {
                logger.warn("ip:{}, port:{}, command: zcard, key:{}, excute Time: {}ms", this.host, this.port, key, time);
            }
            return len;
        } catch (Exception e) {
            logger.error(" command: zcard, key: {}, ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long zcount(String key, Double min, Double max) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long val = jedis.zcount(key, min, max);
            long time = System.currentTimeMillis() - start;
            if (time > 500) {
                logger.warn("ip={}, port={}, command= zcount, key={}, excute_time= {}ms", this.host, this.port, key, time);
            }
            return val;
        } catch (Exception e) {
            logger.error("command= zcount, key={}, min={}, max={}, error={}", key, min, max, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Double zincrby(String key, double score, String member) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Double newScore = jedis.zincrby(key, score, member);
            long time = System.currentTimeMillis() - start;
            if (time > 500) {
                logger.warn("ip={}, port={}, command= zincrby, key={}, score={}, member={}, excute_time={}ms", this.host, this.port, key, score, member, time);
            }
            return newScore;
        } catch (Exception e) {
            logger.error("command= zincrby, key={}, score={}, member={}, error={}", key, score, member, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long zinterstore(String dstkey, String... sets) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long ret = jedis.zinterstore(dstkey, sets);
            long time = System.currentTimeMillis() - start;
            if (time > 500) {
                logger.warn("ip={}, port={}, comman=zinterstore, dstkey={}, sets={}", host, port, dstkey, JSON.toJSONString(sets));
            }
            return ret;
        } catch (Exception e) {
            logger.error("command=zinterstore, dstkey={}, error={}", dstkey, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long zlexcount(String key, String min, String max) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long ret = jedis.zlexcount(key, min, max);
            long time = System.currentTimeMillis() - start;
            if (time > 500) {
                logger.warn("ip={}, port={}, command=zlexcount, key={}, min={}, max={}", host, port, key, min, max);
            }
            return ret;
        } catch (Exception e) {
            logger.error(" command= zlexcount, key={}, min={}, max={}, error={}", key, min, max, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long zrank(String key, String member) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long rank = jedis.zrank(key, member);
            long time = System.currentTimeMillis() - start;
            if (time > 500) {
                logger.warn("ip={}, port={}, command=zrank, key={}, member={}, excute_time={}ms", host, port, key, member, time);
            }
            return rank;
        } catch (Exception e) {
            logger.error("command=zrank, key={}, member={}, error={}", key, member, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Set<String> smembers(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Set<String> v = sj.smembers(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：smembers key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：smembers key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long smove(String srckey, String dstkey, String member) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long ret = jedis.smove(srckey, dstkey, member);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, port={}, command=smove, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=smove, srckey={}, dstkey={}, member={}, error={}", srckey, dstkey, member, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Boolean sismember(String key, String member) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Boolean v = sj.sismember(key, member);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：sismember key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：sismember key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Boolean exists(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Boolean v = sj.exists(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：exists key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：exists key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public String echo(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            String v = sj.echo(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：echo key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：echo key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Set<String> zrange(String key, Long start, Long end) {
        Long startTime = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Set<String> v = sj.zrange(key, start, end);
            Long endTime = System.currentTimeMillis();
            Long time = endTime - startTime;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：zrange key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：zrange key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long zunionstore(String key, String... sets) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long ret = jedis.zunionstore(key, sets);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, port={}, command=zunionstore, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=zunionstore, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Set<String> zrangebylex(String key, String min, String max) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Set<String> set = jedis.zrangeByLex(key, min, max);
            long time = System.currentTimeMillis() - start;
            if (time > 500) {
                logger.warn("ip={}, port={}, command=zrangebylex, execute_time={}ms", host, port, time);
            }
            return set;
        } catch (Exception e) {
            logger.error("command=zrangebylex, key={}, min={}, max={}, error={}", key, min, max, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Set<String> zrangebyscore(String key, String min, String max) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Set<String> set = jedis.zrangeByScore(key, min, max);
            long time = System.currentTimeMillis() - start;
            if (time > 500) {
                logger.warn("ip={}, host={}, command=zrangebyscore, execute_time={}", host, port, time);
            }
            return set;
        } catch (Exception e) {
            logger.error("command= zrangebyscore, key={}, min={}, max={}, error={}", key, min, max, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long zrem(String key, String... members) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.zrem(key, members);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：zrem key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：zrem key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long zremrangebylex(String key, String min, String max) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long ret = jedis.zremrangeByLex(key, min, max);
            long time = System.currentTimeMillis() - start;
            if (time > 500) {
                logger.warn("ip={}, port={}, command=zremrangebylex, execute_time={}", host, port, time);
            }
            return ret;
        } catch (Exception e) {
            logger.error("command= zremrangebylex, key={}, min={}, max={}, error={}", key, min, max, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long zremrangebyrank(String key, long start, long end) {
        long startTime = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long ret = jedis.zremrangeByRank(key, start, end);
            long time = System.currentTimeMillis() - startTime;
            if (time > 500) {
                logger.warn("ip={}, port={}, command= zremrangebyrank, execute_time={}ms", host, port, time);
            }
            return ret;
        } catch (Exception e) {
            logger.error("command= zremrangebyrank, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long zremrangebyscore(String key, String start, String end) {
        long startTime = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long ret = jedis.zremrangeByScore(key, start, end);
            long time = System.currentTimeMillis() - startTime;
            if (time > 500) {
                logger.warn("ip={}, port={}, command=zremrangebycore, execute_time={}ms", host, port, time);
            }
            return ret;
        } catch (Exception e) {
            logger.error("command=zremrangebyscore, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }


    public Set<String> zrange(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Set<String> v = sj.zrange(key, 0, -1);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：zrange key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：zrange key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Set<String> zrevrange(String key, Long start, Long end) {
        Long startTime = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Set<String> v = sj.zrevrange(key, start, end);
            Long endTime = System.currentTimeMillis();
            Long time = endTime - startTime;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：zrevrange key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：zrevrange key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Set<String> zrevrangebylex(String key, String min, String max) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Set<String> set = jedis.zrevrangeByLex(key, max, min);
            long time = System.currentTimeMillis() - start;
            if (time > 500) {
                logger.warn("ip={}, port={}, command=zrevrangebylex, execute_time={}ms", host, port, time);
            }
            return set;
        } catch (Exception e) {
            logger.error("command=zrevrangebylex, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Set<String> zrevrangbyscore(String key, String min, String max) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Set<String> set = jedis.zrevrangeByScore(key, max, min);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, port={}, command=zrevrangebyscore, execute_time={}ms", host, port, time);
            return set;
        } catch (Exception e) {
            logger.error("command=zrevrangebyscore, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Long zrevrank(String key, String member) {
        long start = System.currentTimeMillis();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Long ret = jedis.zrevrank(key, member);
            long time = System.currentTimeMillis() - start;
            if (time > 500)
                logger.warn("ip={}, port={}, command=zrevrank, execute_time={}ms", host, port, time);
            return ret;
        } catch (Exception e) {
            logger.error("command=zrevrank, key={}, error={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            returnResource(jedis);
        }
        return null;
    }

    public Set<String> zrevrange(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Set<String> v = sj.zrevrange(key, 0, -1);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：zrevrange key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：zrevrange key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Map<String, String> hgetAll(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Map<String, String> v = sj.hgetAll(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：hgetAll key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：hgetAll key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public List<String> brpop(String arg) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            List<String> v = sj.brpop(arg);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：brpop key:{} execution time:{}ms", this.host, this.port, arg, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：brpop key:{} ex={}", arg, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public List<String> sort(String key) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            List<String> v = sj.sort(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：sort key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：sort key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public List<String> srandmember(String key, int count) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            List<String> v = sj.srandmember(key, count);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：srandmember key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：srandmember key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public Long pttl(String key, int count) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long v = sj.pttl(key);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：pttl key:{} execution time:{}ms", this.host, this.port, key, time);
            }
            return v;
        } catch (Exception e) {
            logger.error("command：pttl key:{} ex={}", key, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }

    public void subscribe(JedisPubSub pubSub, String... channels) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            sj.subscribe(pubSub, channels);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：subscribe key:{} execution time:{}ms", this.host, this.port, channels, time);
            }
        } catch (Exception e) {
            logger.error("command：subscribe channels:{} ex={}", channels, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
    }

    public Long publish(String channel, String msg) {
        Long start = System.currentTimeMillis();
        Jedis sj = null;
        try {
            sj = jedisPool.getResource();
            Long re = sj.publish(channel, msg);
            Long end = System.currentTimeMillis();
            Long time = end - start;
            if (time > 500) {
                logger.warn("ip:{} port:{} command：publish key:{} execution time:{}ms", this.host, this.port, channel, time);
            }
            return re;
        } catch (Exception e) {
            logger.error("command：publish channels:{} ex={}", channel, LogExceptionStackTrace.erroStackTrace(e));
        } finally {
            if (sj != null) {
                returnResource(sj);
            }
        }
        return null;
    }
}