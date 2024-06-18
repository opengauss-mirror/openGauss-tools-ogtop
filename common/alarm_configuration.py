#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'lpc'
alarm_parameter = {
    "database": {
        "active session/session": {"symbol": ">", "numerical": 30},
        "idleintran": {"symbol": ">", "numerical": 10},
        "dynamic_used_memory/max_dynamic_memory": {"symbol": ">", "numerical": 90},
        "busy_time/all_time": {"symbol": ">", "numerical": 70},
        "wal_status": {"symbol": "!=", "numerical": "Normal"},
        "DATA_IO_TIME/DB_TIME": {"symbol": ">", "numerical": 50},
        "hitratio": {"symbol": "<", "numerical": 90},
        "deadlocks": {"symbol": ">", "numerical": 0},
    },
    "session": {
        "active session/session": {"symbol": ">", "numerical": 30},
        "n_rollback/n_sql": {"symbol": ">", "numerical": 10},
        "n_shared_blocks_read/(n_shared_blocks_read+n_blocks_read_time)": {"symbol": "<", "numerical": 50},
    },
    "session_detail": {
        "block_sessionid": {"symbol": "!=", "numerical": '-'},
    },
    "asp": {
        "duration_ms": {"symbol": ">", "numerical": 5000},
    },
    "table": {
        "last_autovacuum": {"symbol": ">", "numerical": "超过一周"},
        "n_dead_tup/(n_live_tup+n_dead_tup)": {"symbol": ">", "numerical": 0.8}
    },
    "memory": {
        "process_memory_used_percent": {"symbol": ">", "numerical": 0.9},
        "dynamic_memory_used_percent": {"symbol": ">", "numerical": 0.9},
        "backend_memory_used_percent": {"symbol": ">", "numerical": 0.9},
        "shared_memory_used_percent": {"symbol": ">", "numerical": 0.9},
        "cstore_memory_used_percent": {"symbol": ">", "numerical": 0.9},
        "sctpcomm_memory_used_percent": {"symbol": ">", "numerical": 0.9},
        "gpu_dynamic_memory_used_percent": {"symbol": ">", "numerical": 0.9},
    },
    "sharemem": {
        "usedsize/totalsize": {"symbol": ">", "numerical": 95},
    },
    "dynamicsql": {
        "avg_db_time": {"symbol": ">", "numerical": 5},
    },
    "lock": {
        "granted": {"symbol": "==", "numerical": 0.0},
    },
    "wait_event": {
        "avg_wait_time": {"symbol": ">", "numerical": 20},
    },
    "replication": {
        "peer_state": {"symbol": "!=", "numerical": "Normal"},
        "receive_gap": {"symbol": ">", "numerical": 1024},
        "replay_gap": {"symbol": ">", "numerical": 1048576},
        "active": {"symbol": "==", "numerical": 0.0},
        "diff_lsn": {"symbol": ">", "numerical": 1073741824},
    },
    "slow_sql": {
        "duration_ms": {"symbol": ">", "numerical": 5000},
    }
}
"""
database页面：
active的session数：active session > 30
idle_in_trastion的session数：idleintran > 10
动态内存使用率：dynamic_used_memory/max_dynamic_memory > 90%
服务器的繁忙程度：busy_time/all_time > 70%
集群状态：wal_status != Normal
数据io时间在数据库时间的占比：DATA_IO_TIME/DB_TIME > 50%
缓存命中率：hitratio < 90%
死锁：deadlocks > 0
session页面：
活动会话占总会话数的比例：active session > 30
Session的回滚数量：n_rollback/n_sql > 10%
查询命中率：n_shared_blocks_read/(n_shared_blocks_read+n_local_blocks_read) < 50%
session_detail页面：
查询当前session是否被阻塞：block_sessionid is not null
Asp 页面：
SQL的执行时间：duration_ms> 5s
table页面：
最后一次清理该表的时间：last_autovacuum 超过一周
table_performance页面：
死元组所占的百分比：n_dead_tup/(n_live_tup+n_dead_tup) > 80%
memory页面：
process_memory_used_percent > 90%
dynamic_memory_used_percent > 90%
backend_memory_used_percent > 90%
shared_memory_used_percent > 90%
cstore_memory_used_percent > 90%
sctpcomm_memory_used_percent > 90%
gpu_dynamic_memory_used_percent > 90%
sharemem 页面：
usedsize/totalsize > 95%
Dynamicsql 页面：
平均执行时间：avg_db_time > 5s
Lock 页面：
等待锁：granted = f
wait_event 页面：
平均等待时间：avg_wait_time(ms) > 20
Replication 页面：
peer_state != Normal
receive_gap(kb) > 100mb
replay_gap > 1GB
复制槽是否活跃：active = f
diff_lsn > 1gb
slow_sql 页面
执行时间：duration > 5s
"""