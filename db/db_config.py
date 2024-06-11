# -*- coding: utf-8 -*-
"""
Query the database sql configuration of the data
"""
standby_sql = """select count(*) from pg_stat_replication;"""
######################################  database page query sql  ######################################
database_sql = """
select pg_postmaster_start_time() as start_time,
NUM_CPUS,NUM_CPU_CORES,NUM_CPU_SOCKETS,PHYSICAL_MEMORY_BYTES,IDLE_TIME,BUSY_TIME,USER_TIME,SYS_TIME,IOWAIT_TIME,NICE_TIME,VM_PAGE_IN_BYTES,VM_PAGE_OUT_BYTES,LOAD,
pg_stat_get_stream_replications.db_state,pg_stat_get_stream_replications.local_role, DATA_IO_TIME,DB_TIME,CPU_TIME,EXECUTION_TIME,GLOBAL_DOUBLE_WRITE_STATUS.total_writes,dbe_perf.BGWRITER_STAT.checkpoints_timed+dbe_perf.BGWRITER_STAT.checkpoints_req as checkpoints,XACT.blks_hit/(XACT.blks_hit+XACT.blks_read) as hitratio,XACT.xact_commit,XACT.xact_rollback,XACT.blks_read,XACT.blks_hit,XACT.tup_fetched,XACT.tup_inserted,XACT.tup_updated,XACT.tup_deleted,XACT.blk_read_time
,XACT.blk_write_time,XACT.conflicts,XACT.temp_files,XACT.temp_bytes,XACT.deadlocks,WORKLOAD_SQL_COUNT.select_count,WORKLOAD_SQL_COUNT.update_count,WORKLOAD_SQL_COUNT.insert_count,WORKLOAD_SQL_COUNT.delete_count,WORKLOAD_SQL_COUNT.ddl_count,WORKLOAD_SQL_COUNT.dml_count,WORKLOAD_SQL_COUNT.dcl_count,WORKLOAD_SQL_COUNT.ddl_count+WORKLOAD_SQL_COUNT.dml_count+dcl_count as totalcount,ProcessMem,max_dynamic_memory,DynamicMem,SharedMem,Actsess,Sessions,idleintran,Waiting,dbe_perf.FILE_REDO_IOSTAT.writetim

from dbe_perf.FILE_REDO_IOSTAT,
(select xact_commit,xact_rollback,blks_read,blks_hit,tup_fetched,tup_inserted,tup_updated,tup_deleted,blk_read_time,blk_write_time,conflicts,temp_files,temp_bytes,deadlocks from pg_stat_database where datname=%s) XACT ,
(select * from dbe_perf.WORKLOAD_SQL_COUNT where workload='default_pool' ) as WORKLOAD_SQL_COUNT,
dbe_perf.BGWRITER_STAT,

dbe_perf.GLOBAL_DOUBLE_WRITE_STATUS,

(select * from pg_stat_get_stream_replications() ) as pg_stat_get_stream_replications  ,
(select count(1) as Sessions,
sum (case state when 'active' then 1 else 0 end) as Actsess,
sum (case state when 'idle in transaction' then 1 else 0 end) as idleintran,
sum (case waiting when 't' then 1 else 0 end) as Waiting
 from dbe_perf.SESSION_STAT_ACTIVITY) as SESSION_STAT_ACTIVITY,

(select sum (case stat_name when 'DB_TIME' then value else 0 end) as DB_TIME,
sum (case stat_name when 'CPU_TIME' then value else 0 end) as CPU_TIME,
sum (case stat_name when 'EXECUTION_TIME' then value else 0 end) as EXECUTION_TIME,
sum (case stat_name when 'REWRITE_TIME' then value else 0 end) as REWRITE_TIME,
sum (case stat_name when 'DATA_IO_TIME' then value else 0 end) as DATA_IO_TIME
from dbe_perf.instance_time ) as instance_time,

(select sum (case memorytype when 'process_used_memory' then memorymbytes else 0 end) as ProcessMem,
sum (case memorytype when 'dynamic_used_memory' then memorymbytes else 0 end) as DynamicMem,
sum (case memorytype when 'shared_used_memory' then memorymbytes else 0 end) as SharedMem,
sum (case memorytype when 'max_dynamic_memory' then memorymbytes else 0 end) as max_dynamic_memory
from GS_TOTAL_MEMORY_DETAIL ) as GS_TOTAL_MEMORY_DETAIL,

(select sum (case name when 'NUM_CPUS' then value else 0 end) as NUM_CPUS,
sum (case name when 'NUM_CPU_CORES' then value else 0 end) as NUM_CPU_CORES,
sum (case name when 'NUM_CPU_SOCKETS' then value else 0 end) as NUM_CPU_SOCKETS,
sum (case name when 'PHYSICAL_MEMORY_BYTES' then value else 0 end) as PHYSICAL_MEMORY_BYTES,
sum (case name when 'IDLE_TIME' then value else 0 end) as IDLE_TIME,
sum (case name when 'BUSY_TIME' then value else 0 end) as BUSY_TIME,
sum (case name when 'USER_TIME' then value else 0 end) as USER_TIME,
sum (case name when 'SYS_TIME' then value else 0 end) as SYS_TIME,
sum (case name when 'IOWAIT_TIME' then value else 0 end) as IOWAIT_TIME,
sum (case name when 'NICE_TIME' then value else 0 end) as NICE_TIME,
sum (case name when 'VM_PAGE_IN_BYTES' then value else 0 end) as VM_PAGE_IN_BYTES,
sum (case name when 'VM_PAGE_OUT_BYTES' then value else 0 end) as VM_PAGE_OUT_BYTES,
sum (case name when 'LOAD' then value else 0 end) as LOAD
from dbe_perf.os_runtime) as os_runtime ;
"""
# memory related data is not include
database_spare_sql = """select pg_postmaster_start_time() as start_time,
NUM_CPUS,NUM_CPU_CORES,NUM_CPU_SOCKETS,PHYSICAL_MEMORY_BYTES,IDLE_TIME,BUSY_TIME,USER_TIME,SYS_TIME,IOWAIT_TIME,NICE_TIME,VM_PAGE_IN_BYTES,VM_PAGE_OUT_BYTES,LOAD,
pg_stat_get_stream_replications.db_state,pg_stat_get_stream_replications.local_role, DATA_IO_TIME,DB_TIME,CPU_TIME,EXECUTION_TIME,GLOBAL_DOUBLE_WRITE_STATUS.total_writes,dbe_perf.BGWRITER_STAT.checkpoints_timed+dbe_perf.BGWRITER_STAT.checkpoints_req as checkpoints,XACT.blks_hit/(XACT.blks_hit+XACT.blks_read) as hitratio,XACT.xact_commit,XACT.xact_rollback,XACT.blks_read,XACT.blks_hit,XACT.tup_fetched,XACT.tup_inserted,XACT.tup_updated,XACT.tup_deleted,XACT.blk_read_time,XACT.blk_write_time,XACT.conflicts,XACT.temp_files,XACT.temp_bytes,XACT.deadlocks,WORKLOAD_SQL_COUNT.select_count,WORKLOAD_SQL_COUNT.update_count,WORKLOAD_SQL_COUNT.insert_count,WORKLOAD_SQL_COUNT.delete_count,WORKLOAD_SQL_COUNT.ddl_count,WORKLOAD_SQL_COUNT.dml_count,WORKLOAD_SQL_COUNT.dcl_count,WORKLOAD_SQL_COUNT.ddl_count+WORKLOAD_SQL_COUNT.dml_count+dcl_count as totalcount,Actsess,Sessions,idleintran,Waiting,  dbe_perf.FILE_REDO_IOSTAT.writetim

from dbe_perf.FILE_REDO_IOSTAT,
(select xact_commit,xact_rollback,blks_read,blks_hit,tup_fetched,tup_inserted,tup_updated,tup_deleted,blk_read_time,blk_write_time,conflicts,temp_files,temp_bytes,deadlocks from pg_stat_database where datname=%s) XACT ,
(select * from dbe_perf.WORKLOAD_SQL_COUNT where workload='default_pool' ) as WORKLOAD_SQL_COUNT,
dbe_perf.BGWRITER_STAT,

dbe_perf.GLOBAL_DOUBLE_WRITE_STATUS,

(select * from pg_stat_get_stream_replications() ) as pg_stat_get_stream_replications  ,
(select count(1) as Sessions,
sum (case state when 'active' then 1 else 0 end) as Actsess,
sum (case state when 'idle in transaction' then 1 else 0 end) as idleintran,
sum (case waiting when 't' then 1 else 0 end) as Waiting
 from dbe_perf.SESSION_STAT_ACTIVITY) as SESSION_STAT_ACTIVITY,

(select sum (case stat_name when 'DB_TIME' then value else 0 end) as DB_TIME,
sum (case stat_name when 'CPU_TIME' then value else 0 end) as CPU_TIME,
sum (case stat_name when 'EXECUTION_TIME' then value else 0 end) as EXECUTION_TIME,
sum (case stat_name when 'REWRITE_TIME' then value else 0 end) as REWRITE_TIME,
sum (case stat_name when 'DATA_IO_TIME' then value else 0 end) as DATA_IO_TIME
from dbe_perf.instance_time ) as instance_time,

(select sum (case name when 'NUM_CPUS' then value else 0 end) as NUM_CPUS,
sum (case name when 'NUM_CPU_CORES' then value else 0 end) as NUM_CPU_CORES,
sum (case name when 'NUM_CPU_SOCKETS' then value else 0 end) as NUM_CPU_SOCKETS,
sum (case name when 'PHYSICAL_MEMORY_BYTES' then value else 0 end) as PHYSICAL_MEMORY_BYTES,
sum (case name when 'IDLE_TIME' then value else 0 end) as IDLE_TIME,
sum (case name when 'BUSY_TIME' then value else 0 end) as BUSY_TIME,
sum (case name when 'USER_TIME' then value else 0 end) as USER_TIME,
sum (case name when 'SYS_TIME' then value else 0 end) as SYS_TIME,
sum (case name when 'IOWAIT_TIME' then value else 0 end) as IOWAIT_TIME,
sum (case name when 'NICE_TIME' then value else 0 end) as NICE_TIME,
sum (case name when 'VM_PAGE_IN_BYTES' then value else 0 end) as VM_PAGE_IN_BYTES,
sum (case name when 'VM_PAGE_OUT_BYTES' then value else 0 end) as VM_PAGE_OUT_BYTES,
sum (case name when 'LOAD' then value else 0 end) as LOAD
from dbe_perf.os_runtime) as os_runtime ;"""
time = "select localtimestamp;"
######################################  session  page query sql  ######################################
session_sql = '''select ACTIVITY.datid,
       ACTIVITY.datname,
       ACTIVITY.pid,
       ACTIVITY.usesysid,
       ACTIVITY.usename,
       ACTIVITY.application_name,
       ACTIVITY.client_addr,
       ACTIVITY.client_hostname,
       ACTIVITY.client_port,
       ACTIVITY.backend_start,
       TIMESTAMP_DIFF('MICROSECOND',ACTIVITY.xact_start,clock_timestamp())/1000 as xact_start_time,
      TIMESTAMP_DIFF('MICROSECOND',ACTIVITY.query_start,clock_timestamp())/1000 as query_start_time,
       ACTIVITY.state_change,
       ACTIVITY.waiting,
       ACTIVITY.enqueue,
       ACTIVITY.state,
       ACTIVITY.resource_pool,
       ACTIVITY.query_id,
       ACTIVITY.query,
       SESSION_STAT.pid,
       SESSION_STAT.n_commit,
       SESSION_STAT.n_rollback,
       SESSION_STAT.n_sql,
       SESSION_STAT.n_table_scan,
       SESSION_STAT.n_blocks_fetched,
       SESSION_STAT.
           n_physical_read_operation,
       SESSION_STAT.n_shared_blocks_dirtied,
       SESSION_STAT.n_local_blocks_dirtied,
       SESSION_STAT.
           n_shared_blocks_read,
       SESSION_STAT.n_local_blocks_read,
       SESSION_STAT.n_blocks_read_time,
       SESSION_STAT.n_blocks_write_time,
       SESSION_STAT.
           n_sort_in_memory,
       SESSION_STAT.n_sort_in_disk,
       SESSION_STAT.n_cu_mem_hit,
       SESSION_STAT.n_cu_hdd_sync_read,
       SESSION_STAT.n_cu_hdd_asyn_read,
       SESSION_TIME.pid,
       SESSION_TIME.DB_TIME,
       SESSION_TIME.CPU_TIME,
       SESSION_TIME.EXECUTION_TIME,
       SESSION_TIME.PARSE_TIME,
       SESSION_TIME.PLAN_TIME,
       SESSION_TIME.REWRITE_TIME,
       SESSION_TIME.PL_EXECUTION_TIME,
       SESSION_TIME.PL_COMPILATION_TIME,
       SESSION_TIME.NET_SEND_TIME,
       SESSION_TIME.DATA_IO_TIME,
       wait.pid,
       wait.wait_status,
       wait.wait_event,
       localtimestamp as snaptime
from dbe_perf.SESSION_STAT_ACTIVITY ACTIVITY
       left join (select split_part(sessid, '.', 2)                                                as pid,
                         sum(CASE statname when 'n_commit' then value else 0 end)                  as n_commit,
                         sum(CASE statname when 'n_rollback' then value else 0 end)                as n_rollback,
                         sum(CASE statname when 'n_sql' then value else 0 end)                     as n_sql,
                         sum(CASE statname when 'n_table_scan' then value else 0 end)              as n_table_scan,
                         sum(CASE statname when 'n_blocks_fetched' then value else 0 end)          as n_blocks_fetched,
                         sum(
                           CASE statname when 'n_physical_read_operation' then value else 0 end)   as n_physical_read_operation,
                         sum(CASE statname when 'n_shared_blocks_dirtied' then value else 0 end)   as n_shared_blocks_dirtied,
                         sum(CASE statname when 'n_local_blocks_dirtied' then value else 0 end)    as n_local_blocks_dirtied,
                         sum(CASE statname when 'n_shared_blocks_read' then value else 0 end)      as n_shared_blocks_read,
                         sum(CASE statname when 'n_local_blocks_read' then value else 0 end)       as n_local_blocks_read,
                         sum(CASE statname when 'n_blocks_read_time' then value else 0 end)        as n_blocks_read_time,
                         sum(CASE statname when 'n_blocks_write_time' then value else 0 end)       as n_blocks_write_time,
                         sum(CASE statname when 'n_sort_in_memory' then value else 0 end)          as n_sort_in_memory,
                         sum(CASE statname when 'n_sort_in_disk' then value else 0 end)            as n_sort_in_disk,
                         sum(CASE statname when 'n_cu_mem_hit' then value else 0 end)              as n_cu_mem_hit,
                         sum(CASE statname when 'n_cu_hdd_sync_read' then value else 0 end)        as n_cu_hdd_sync_read,
                         sum(CASE statname when 'n_cu_hdd_asyn_read' then value else 0 end)        as n_cu_hdd_asyn_read
                  from dbe_perf.SESSION_STAT
                  group by sessid
                  order by sessid) AS SESSION_STAT on SESSION_STAT.pid = ACTIVITY.pid
       left join (select split_part(sessid, '.', 2)                                           as pid,
                         sum(case stat_name when 'DB_TIME' then value else 0 end)             as DB_TIME,
                         sum(case stat_name when 'CPU_TIME' then value else 0 end)            as CPU_TIME,
                         sum(case stat_name when 'EXECUTION_TIME' then value else 0 end)      as EXECUTION_TIME,
                         sum(case stat_name when 'PARSE_TIME' then value else 0 end)          as PARSE_TIME,
                         sum(case stat_name when 'PLAN_TIME' then value else 0 end)           as PLAN_TIME,
                         sum(case stat_name when 'REWRITE_TIME' then value else 0 end)        as REWRITE_TIME,
                         sum(case stat_name when 'PL_EXECUTION_TIME' then value else 0 end)   as PL_EXECUTION_TIME,
                         sum(case stat_name when 'PL_COMPILATION_TIME' then value else 0 end) as PL_COMPILATION_TIME,
                         sum(case stat_name when 'NET_SEND_TIME' then value else 0 end)       as NET_SEND_TIME,
                         sum(case stat_name when 'DATA_IO_TIME' then value else 0 end)        as DATA_IO_TIME
                  from dbe_perf.SESSION_TIME
                  group by sessid
                  order by sessid) AS SESSION_TIME on SESSION_TIME.pid = ACTIVITY.pid
       left join (select sessionid as pid, wait_status, wait_event from pg_thread_wait_status) wait
         on wait.pid = ACTIVITY.pid;
'''
session_detail_sql = """select query_id,
       block_sessionid,
       backend_start,
       query_start,
       datid,
       datname,
       usesysid,
       usename,
       application_name,
       client_addr,
       client_hostname,
       client_port,
       xact_start                as xact_start_time,
       state_change              as state_change_time,
       waiting,
       case
         when total_cpu_time is null then 0
         else total_cpu_time end as total_cpu_time,
       sum(db_time)              as db_time,
       sum(cpu_time)             as cpu_time,
       sum(execution_time)       as execution_time,
       sum(parse_time)           as parse_time,
       sum(plan_time)            as plan_time,
       sum(rewrite_time)         as rewrite_time,
       sum(pl_execution_time)    as pl_execution_time,
       sum(pl_compilation_time)  as pl_compilation_time,
       sum(net_send_time)        as net_send_time,
       sum(data_io_time)         as data_io_time,
       init_mem,
       used_mem,
       peak_mem,
       wait_status,
       locktag,
       lockmode,
       query,
       pid,
       n_commit,
       n_rollback,
       n_sql,
       n_table_scan,
       n_blocks_fetched,
       n_physical_read_operation,
       n_shared_blocks_dirtied,
       n_local_blocks_dirtied,
       n_shared_blocks_read,
       n_local_blocks_read,
       n_blocks_read_time,
       n_blocks_write_time,
       n_sort_in_memory,
       n_sort_in_disk,
       n_cu_mem_hit,
       n_cu_hdd_sync_read,
       n_cu_hdd_asyn_read
from (select session_stat_activity.pid,
             thread_wait_status.sessionid,
             session_memory.sessid,
             n_commit,
             n_rollback,
             n_sql,
             n_table_scan,
             n_blocks_fetched,
             n_physical_read_operation,
             n_shared_blocks_dirtied,
             n_local_blocks_dirtied,
             n_shared_blocks_read,
             n_local_blocks_read,
             n_blocks_read_time,
             n_blocks_write_time,
             n_sort_in_memory,
             n_sort_in_disk,
             n_cu_mem_hit,
             n_cu_hdd_sync_read,
             n_cu_hdd_asyn_read,
             case
               when session_time.stat_name = 'DB_TIME' then session_time.value
               else 0 end as db_time,
             case
               when session_time.stat_name = 'CPU_TIME' then session_time.value
               else 0 end as cpu_time,
             case
               when session_time.stat_name = 'EXECUTION_TIME' then session_time.value
               else 0 end as execution_time,
             case
               when session_time.stat_name = 'PARSE_TIME' then session_time.value
               else 0 end as parse_time,
             case
               when session_time.stat_name = 'PLAN_TIME' then session_time.value
               else 0 end as plan_time,
             case
               when session_time.stat_name = 'REWRITE_TIME' then session_time.value
               else 0 end as rewrite_time,
             case
               when session_time.stat_name = 'PL_EXECUTION_TIME' then session_time.value
               else 0 end as pl_execution_time,
             case
               when session_time.stat_name = 'PL_COMPILATION_TIME' then session_time.value
               else 0 end as pl_compilation_time,
             case
               when session_time.stat_name = 'NET_SEND_TIME' then session_time.value
               else 0 end as net_send_time,
             case
               when session_time.stat_name = 'DATA_IO_TIME' then session_time.value
               else 0 end as data_io_time,
             session_memory.init_mem,
             session_memory.used_mem,
             session_memory.peak_mem,
             thread_wait_status.locktag,
             thread_wait_status.wait_status,
             thread_wait_status.lockmode,
             thread_wait_status.block_sessionid,
             session_stat_activity.usesysid,
             session_stat_activity.usename,
             session_stat_activity.application_name,
             session_stat_activity.client_addr,
             session_stat_activity.client_hostname,
             session_stat_activity.client_port,
             session_stat_activity.datid,
             session_stat_activity.datname,
             session_stat_activity.xact_start,
             session_stat_activity.state_change,
             session_stat_activity.waiting,
             session_stat_activity.query_start,
             session_stat_activity.backend_start,
             session_stat_activity.query_id,
             session_stat_activity.query,
             session_cpu_runtime.total_cpu_time
      from (select sessid,
                   sum(n_commit)                  as n_commit,
                   sum(n_rollback)                as n_rollback,
                   sum(n_sql)                     as n_sql,
                   sum(n_table_scan)              as n_table_scan,
                   sum(n_blocks_fetched)          as n_blocks_fetched,
                   sum(n_physical_read_operation) as n_physical_read_operation,
                   sum(n_shared_blocks_dirtied)   as n_shared_blocks_dirtied,
                   sum(n_local_blocks_dirtied)    as n_local_blocks_dirtied,
                   sum(n_shared_blocks_read)      as n_shared_blocks_read,
                   sum(n_local_blocks_read)       as n_local_blocks_read,
                   sum(n_blocks_read_time)        as n_blocks_read_time,
                   sum(n_blocks_write_time)       as n_blocks_write_time,
                   sum(n_sort_in_memory)          as n_sort_in_memory,
                   sum(n_sort_in_disk)            as n_sort_in_disk,
                   sum(n_cu_mem_hit)              as n_cu_mem_hit,
                   sum(n_cu_hdd_sync_read)        as n_cu_hdd_sync_read,
                   sum(n_cu_hdd_asyn_read)        as n_cu_hdd_asyn_read
            from (select sessid,
                         statname,
                         case
                           when statname = 'n_commit' then value
                           else 0 end as n_commit,
                         case
                           when statname = 'n_rollback' then value
                           else 0 end as n_rollback,
                         case
                           when statname = 'n_sql' then value
                           else 0 end as n_sql,
                         case
                           when statname = 'n_table_scan' then value
                           else 0 end as n_table_scan,
                         case
                           when statname = 'n_blocks_fetched' then value
                           else 0 end as n_blocks_fetched,
                         case
                           when statname = 'n_physical_read_operation' then value
                           else 0 end as n_physical_read_operation,
                         case
                           when statname = 'n_shared_blocks_dirtied' then value
                           else 0 end as n_shared_blocks_dirtied,
                         case
                           when statname = 'n_local_blocks_dirtied' then value
                           else 0 end as n_local_blocks_dirtied,
                         case
                           when statname = 'n_shared_blocks_read' then value
                           else 0 end as n_shared_blocks_read,
                         case
                           when statname = 'n_local_blocks_read' then value
                           else 0 end as n_local_blocks_read,
                         case
                           when statname = 'n_blocks_read_time' then value
                           else 0 end as n_blocks_read_time,
                         case
                           when statname = 'n_blocks_write_time' then value
                           else 0 end as n_blocks_write_time,
                         case
                           when statname = 'n_sort_in_memory' then value
                           else 0 end as n_sort_in_memory,
                         case
                           when statname = 'n_sort_in_disk' then value
                           else 0 end as n_sort_in_disk,
                         case
                           when statname = 'n_cu_mem_hit' then value
                           else 0 end as n_cu_mem_hit,
                         case
                           when statname = 'n_cu_hdd_sync_read' then value
                           else 0 end as n_cu_hdd_sync_read,
                         case
                           when statname = 'n_cu_hdd_asyn_read' then value
                           else 0 end as n_cu_hdd_asyn_read
                  from dbe_perf.SESSION_STAT
                  where sessid like %s)
            group by sessid) starte full
             outer join (select sessid, value, stat_name
                         from dbe_perf.SESSION_TIME
                         where sessid like %s) session_time on 1 = 1 full
             outer join (select sessid, init_mem, used_mem, peak_mem
                         from dbe_perf.SESSION_MEMORY
                         where sessid like %s) session_memory on 1 = 1 full
             outer join (select sessionid, locktag, wait_status, lockmode, block_sessionid
                         from dbe_perf.THREAD_WAIT_STATUS
                         where sessionid like %s) thread_wait_status on 1 = 1 full
             outer join (select pid,
                                usesysid,
                                usename,
                                application_name,
                                client_addr,
                                client_hostname,
                                client_port,
                                datid,
                                datname,
                                xact_start,
                                state_change,
                                waiting,
                                query_start,
                                backend_start,
                                query_id,
                                query
                         from dbe_perf.SESSION_STAT_ACTIVITY
                         where pid like %s) session_stat_activity on 1 = 1 full
             outer join (select pid, total_cpu_time
                         from dbe_perf.SESSION_CPU_RUNTIME
                         where pid like %s) session_cpu_runtime on 1 = 1)
group by pid,
         sessionid,
         sessid,
         n_commit,
         n_rollback,
         n_sql,
         n_table_scan,
         n_blocks_fetched,
         n_physical_read_operation,
         n_shared_blocks_dirtied,
         n_local_blocks_dirtied,
         n_shared_blocks_read,
         n_local_blocks_read,
         n_blocks_read_time,
         n_blocks_write_time,
         n_sort_in_memory,
         n_sort_in_disk,
         n_cu_mem_hit,
         n_cu_hdd_sync_read,
         n_cu_hdd_asyn_read,
         init_mem,
         used_mem,
         peak_mem,
         locktag,
         wait_status,
         lockmode,
         block_sessionid,
         usesysid,
         usename,
         application_name,
         client_addr,
         client_hostname,
         client_port,
         datid,
         datname,
         xact_start,
         state_change,
         waiting,
         query_start,
         backend_start,
         query_id,
         query,
         total_cpu_time;
"""
session_detail_spare_sql = """select
query_id,
block_sessionid,
backend_start,
query_start,
datid,
datname,
usesysid,
usename,
application_name,
client_addr,
client_hostname,
client_port,
xact_start as xact_start_time,
state_change as state_change_time,
waiting,
case
            when total_cpu_time is null then 0
               else total_cpu_time end as total_cpu_time,
sum(db_time)             as db_time,
sum(cpu_time)            as cpu_time,
sum(execution_time)      as execution_time,
sum(parse_time)          as parse_time,
sum(plan_time)           as plan_time,
sum(rewrite_time)        as rewrite_time,
sum(pl_execution_time)   as pl_execution_time,
sum(pl_compilation_time) as pl_compilation_time,
sum(net_send_time)       as net_send_time,
sum(data_io_time)        as data_io_time,
wait_status,
locktag,
lockmode,
query,
pid,
       n_commit,
       n_rollback,
       n_sql,
       n_table_scan,
       n_blocks_fetched,
       n_physical_read_operation,
       n_shared_blocks_dirtied,
       n_local_blocks_dirtied,
       n_shared_blocks_read,
       n_local_blocks_read,
       n_blocks_read_time,
       n_blocks_write_time,
       n_sort_in_memory,
       n_sort_in_disk,
       n_cu_mem_hit,
       n_cu_hdd_sync_read,
       n_cu_hdd_asyn_read
from (select session_stat_activity.pid,
       thread_wait_status.sessionid,
       starte.starte_sum,
        n_commit,
             n_rollback,
             n_sql,
             n_table_scan,
             n_blocks_fetched,
             n_physical_read_operation,
             n_shared_blocks_dirtied,
             n_local_blocks_dirtied,
             n_shared_blocks_read,
             n_local_blocks_read,
             n_blocks_read_time,
             n_blocks_write_time,
             n_sort_in_memory,
             n_sort_in_disk,
             n_cu_mem_hit,
             n_cu_hdd_sync_read,
             n_cu_hdd_asyn_read,
        case
               when session_time.stat_name = 'DB_TIME' then session_time.value
               else 0 end as db_time,
             case
               when session_time.stat_name = 'CPU_TIME' then session_time.value
               else 0 end as cpu_time,
             case
               when session_time.stat_name = 'EXECUTION_TIME' then session_time.value
               else 0 end as execution_time,
             case
               when session_time.stat_name = 'PARSE_TIME' then session_time.value
               else 0 end as parse_time,
             case
               when session_time.stat_name = 'PLAN_TIME' then session_time.value
               else 0 end as plan_time,
             case
               when session_time.stat_name = 'REWRITE_TIME' then session_time.value
               else 0 end as rewrite_time,
             case
               when session_time.stat_name = 'PL_EXECUTION_TIME' then session_time.value
               else 0 end as pl_execution_time,
             case
               when session_time.stat_name = 'PL_COMPILATION_TIME' then session_time.value
               else 0 end as pl_compilation_time,
             case
               when session_time.stat_name = 'NET_SEND_TIME' then session_time.value
               else 0 end as net_send_time,
             case
               when session_time.stat_name = 'DATA_IO_TIME' then session_time.value
               else 0 end as data_io_time,
       thread_wait_status.locktag,
       thread_wait_status.wait_status,
       thread_wait_status.lockmode,
       thread_wait_status.block_sessionid,
       session_stat_activity.usesysid,
       session_stat_activity.usename,
       session_stat_activity.application_name,
       session_stat_activity.client_addr,
       session_stat_activity.client_hostname,
       session_stat_activity.client_port,
       session_stat_activity.datid,
       session_stat_activity.datname,
       session_stat_activity.xact_start,
       session_stat_activity.state_change,
       session_stat_activity.waiting,
       session_stat_activity.query_start,
       session_stat_activity.backend_start,
       session_stat_activity.query_id,
       session_stat_activity.query,
       total_cpu_time.total_cpu_time
from (select sessid,
                   sum(n_commit)                  as n_commit,
                   sum(n_rollback)                as n_rollback,
                   sum(n_sql)                     as n_sql,
                   sum(n_table_scan)              as n_table_scan,
                   sum(n_blocks_fetched)          as n_blocks_fetched,
                   sum(n_physical_read_operation) as n_physical_read_operation,
                   sum(n_shared_blocks_dirtied)   as n_shared_blocks_dirtied,
                   sum(n_local_blocks_dirtied)    as n_local_blocks_dirtied,
                   sum(n_shared_blocks_read)      as n_shared_blocks_read,
                   sum(n_local_blocks_read)       as n_local_blocks_read,
                   sum(n_blocks_read_time)        as n_blocks_read_time,
                   sum(n_blocks_write_time)       as n_blocks_write_time,
                   sum(n_sort_in_memory)          as n_sort_in_memory,
                   sum(n_sort_in_disk)            as n_sort_in_disk,
                   sum(n_cu_mem_hit)              as n_cu_mem_hit,
                   sum(n_cu_hdd_sync_read)        as n_cu_hdd_sync_read,
                   sum(n_cu_hdd_asyn_read)        as n_cu_hdd_asyn_read
            from (select sessid,
                         statname,
                         case
                           when statname = 'n_commit' then value
                           else 0 end as n_commit,
                         case
                           when statname = 'n_rollback' then value
                           else 0 end as n_rollback,
                         case
                           when statname = 'n_sql' then value
                           else 0 end as n_sql,
                         case
                           when statname = 'n_table_scan' then value
                           else 0 end as n_table_scan,
                         case
                           when statname = 'n_blocks_fetched' then value
                           else 0 end as n_blocks_fetched,
                         case
                           when statname = 'n_physical_read_operation' then value
                           else 0 end as n_physical_read_operation,
                         case
                           when statname = 'n_shared_blocks_dirtied' then value
                           else 0 end as n_shared_blocks_dirtied,
                         case
                           when statname = 'n_local_blocks_dirtied' then value
                           else 0 end as n_local_blocks_dirtied,
                         case
                           when statname = 'n_shared_blocks_read' then value
                           else 0 end as n_shared_blocks_read,
                         case
                           when statname = 'n_local_blocks_read' then value
                           else 0 end as n_local_blocks_read,
                         case
                           when statname = 'n_blocks_read_time' then value
                           else 0 end as n_blocks_read_time,
                         case
                           when statname = 'n_blocks_write_time' then value
                           else 0 end as n_blocks_write_time,
                         case
                           when statname = 'n_sort_in_memory' then value
                           else 0 end as n_sort_in_memory,
                         case
                           when statname = 'n_sort_in_disk' then value
                           else 0 end as n_sort_in_disk,
                         case
                           when statname = 'n_cu_mem_hit' then value
                           else 0 end as n_cu_mem_hit,
                         case
                           when statname = 'n_cu_hdd_sync_read' then value
                           else 0 end as n_cu_hdd_sync_read,
                         case
                           when statname = 'n_cu_hdd_asyn_read' then value
                           else 0 end as n_cu_hdd_asyn_read
                  from dbe_perf.SESSION_STAT
                  where sessid like %s)
      group by sessid) starte full
       outer join (select sessid, value, stat_name from dbe_perf.SESSION_TIME where sessid like %s) session_time
         on 1 = 1 full
       outer join (select sessionid, locktag, wait_status, lockmode, block_sessionid
                   from dbe_perf.THREAD_WAIT_STATUS
                   where sessionid like %s) thread_wait_status on 1 = 1 full
       outer join (select pid,
                          usesysid,
                          usename,
                          application_name,
                          client_addr,
                          client_hostname,
                          client_port,
                          datid,
                          datname,
                          xact_start,
                          state_change,
                          waiting,
                          query_start,
                          backend_start,
                          query_id,
                          query
                   from dbe_perf.SESSION_STAT_ACTIVITY
                   where pid like %s) session_stat_activity on 1 = 1 full
       outer join (select pid, total_cpu_time from dbe_perf.SESSION_CPU_RUNTIME where pid like %s) total_cpu_time
         on 1 = 1)group by pid,
sessionid,
n_commit,
             n_rollback,
             n_sql,
             n_table_scan,
             n_blocks_fetched,
             n_physical_read_operation,
             n_shared_blocks_dirtied,
             n_local_blocks_dirtied,
             n_shared_blocks_read,
             n_local_blocks_read,
             n_blocks_read_time,
             n_blocks_write_time,
             n_sort_in_memory,
             n_sort_in_disk,
             n_cu_mem_hit,
             n_cu_hdd_sync_read,
             n_cu_hdd_asyn_read,locktag,
wait_status,
lockmode,
block_sessionid,
usesysid,
usename,
application_name,
client_addr,
client_hostname,
client_port,
datid,
datname,
xact_start,
state_change,
waiting,
query_start,
backend_start,
query_id,
query,
total_cpu_time;"""
session_plan_sql = {
    "prepare": "PREPARE ogtop{} AS ",
    "explain": " explain EXECUTE ogtop{}('');",
    "advise": "select * from gs_index_advise('{}');"
}
session_stack = """select gs_stack(%s);"""
######################################  asp  page query sql  ######################################
asp_sql = """select p.datname,
       pg_get_userbyid(a.userid)                                               as username,
       a.sessionid,
       a.client_addr,
       TIMESTAMP_DIFF('MICROSECOND', a.query_start_time, a.sample_time) / 1000 as duration_ms,
       a.event,
       a.unique_query,
       CAST(a.sample_time AS VARCHAR ) as sample_time,
       a.unique_query_id
from gs_asp a
       left join pg_database p on p.oid = a.databaseid
where a.unique_query_id is not null
  and a.state = 'active'
  and a.sample_time >= %s  :: timestamp - interval '1 minutes'
  and a.sample_time <= %s  :: timestamp + interval '1 minutes'
order by sample_time asc;"""

######################################  table page query sql  ######################################
table_sql = """select
       relname,
       schemaname,
       current_database()            as datname,
       relid ,
       n_live_tup,
       n_dead_tup,
       last_vacuum,
       last_autovacuum,
       last_analyze,
       last_autoanalyze,
       last_data_changed,
       pg_table_size(relid)          as tabsize,
       pg_indexes_size(relid)        as idxsize,
       pg_total_relation_size(relid) as totalsize
FROM pg_stat_user_tables
order by totalsize desc;"""
table_performance_sql = """WITH RECURSIVE r as (select p.oid as poid, p.oid as relid
                     from pg_class p
                     where p.relkind in ('r', 'i', 'I', 't')
                       and not exists(select 1
                                      from (select unnest(
                                                     ARRAY [reltoastrelid,reltoastidxid,reldeltarelid,reldeltaidx,relcudescrelid,relcudescidx]) as relid
                                            from pg_class
                                            union select unnest(
                                                           ARRAY [reltoastrelid,reltoastidxid,reldeltarelid,reldeltaidx,relcudescrelid,relcudescidx]) as relid
                                                  from pg_partition) a
                                      where a.relid = p.oid)
    union select r.poid, c.relid
          from (select oid                                                                                                       as poid,
                       unnest(
                         ARRAY [reltoastrelid,reltoastidxid,reldeltarelid,reldeltaidx,relcudescrelid,relcudescidx])              as relid
                from pg_class
                where relkind in ('r', 'i', 'I', 't')
                union select oid                                                                                                       as poid,
                             unnest(
                               ARRAY [reltoastrelid,reltoastidxid,reldeltarelid,reldeltaidx,relcudescrelid,relcudescidx])              as relid
                      from pg_partition
                      where parttype <> 'r'
                union select parentid as poid, oid as relid from pg_partition where parttype <> 'r') c,
               r
          where r.relid = c.poid
            and c.relid <> 0)
select 
       current_database()       as datname,
       schemaname,
       relname,
       relid,
       seq_scan,
       seq_tup_read,
       idx_scan,
       idx_tup_fetch,
       n_tup_ins,
       n_tup_upd,
       n_tup_del,
       n_tup_hot_upd,
       heap_blks_read,
       heap_blks_hit,
       idx_blks_read,
       idx_blks_hit,
       toast_blks_read,
       toast_blks_hit,
       tidx_blks_read,
       tidx_blks_hit,
       phyrds,
       phywrts,
       phyblkrd,
       phyblkwrt,
       readtim,
       writetim
from (SELECT relid, schemaname, relname, b.*
      FROM pg_stat_user_tables a
             inner join (select r.poid,
                                sum(nvl(n_live_tup, 0))      as n_live_tup,
                                sum(nvl(n_dead_tup, 0))      as n_dead_tup,
                                sum(nvl(seq_scan, 0))        as seq_scan,
                                sum(nvl(seq_tup_read, 0))    as seq_tup_read,
                                sum(nvl(idx_scan, 0))        as idx_scan,
                                sum(nvl(idx_tup_fetch, 0))   as idx_tup_fetch,
                                sum(nvl(n_tup_ins, 0))       as n_tup_ins,
                                sum(nvl(n_tup_upd, 0))       as n_tup_upd,
                                sum(nvl(n_tup_del, 0))       as n_tup_del,
                                sum(nvl(n_tup_hot_upd, 0))   as n_tup_hot_upd,
                                sum(nvl(heap_blks_read, 0))  as heap_blks_read,
                                sum(nvl(heap_blks_hit, 0))   as heap_blks_hit,
                                sum(nvl(idx_blks_read, 0))   as idx_blks_read,
                                sum(nvl(idx_blks_hit, 0))    as idx_blks_hit,
                                sum(nvl(toast_blks_read, 0)) as toast_blks_read,
                                sum(nvl(toast_blks_hit, 0))  as toast_blks_hit,
                                sum(nvl(tidx_blks_read, 0))  as tidx_blks_read,
                                sum(nvl(tidx_blks_hit, 0))   as tidx_blks_hit,
                                sum(nvl(phyrds, 0))          as phyrds,
                                sum(nvl(phywrts, 0))         as phywrts,
                                sum(nvl(phyblkrd, 0))        as phyblkrd,
                                sum(nvl(phyblkwrt, 0))       as phyblkwrt,
                                sum(nvl(readtim, 0))         as readtim,
                                sum(nvl(writetim, 0))        as writetim
                         from r
                                inner join pg_stat_all_tables stat on r.relid = stat.relid
                                inner join pg_statio_all_tables statio on r.relid = statio.relid
                                inner join (select oid, relfilenode from pg_class
                                            union select oid, relfilenode from pg_partition) t on r.relid = t.oid
                                left join dbe_perf.file_iostat f on t.relfilenode = f.filenum
                         group by r.poid) b on a.relid = b.poid);
"""
table_index_sql = """
    WITH RECURSIVE r as (
    select p.oid as poid,
        p.oid as relid
    from pg_class p
    where p.relkind in ('r', 'i', 'I', 't')
        and not exists (
            select 1
            from (
                    select unnest(
                            ARRAY [reltoastrelid,reltoastidxid,reldeltarelid,reldeltaidx,relcudescrelid,relcudescidx]
                        ) as relid
                    from pg_class
                    union
                    select unnest(
                            ARRAY [reltoastrelid,reltoastidxid,reldeltarelid,reldeltaidx,relcudescrelid,relcudescidx]
                        ) as relid
                    from pg_partition
                ) a
            where a.relid = p.oid
        )
    union
    select r.poid,
        c.relid
    from (
            select oid as poid,
                unnest(
                    ARRAY [reltoastrelid,reltoastidxid,reldeltarelid,reldeltaidx,relcudescrelid,relcudescidx]
                ) as relid
            from pg_class
            where relkind in ('r', 'i', 'I', 't')
            union
            select oid as poid,
                unnest(
                    ARRAY [reltoastrelid,reltoastidxid,reldeltarelid,reldeltaidx,relcudescrelid,relcudescidx]
                ) as relid
            from pg_partition
            where parttype <> 'r'
            union
            select parentid as poid,
                oid as relid
            from pg_partition
            where parttype <> 'r'
        ) c,
        r
    where r.relid = c.poid
        and c.relid <> 0
)
select
    current_database() as datname,
    localtimestamp as snaptime,
    schemaname,
    relname,
    indexrelid,
    indexrelname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch,
    idx_blks_hit,
    phyrds,
    phywrts,
    phyblkrd,
    phyblkwrt,
    readtim,
    writetim
from (
        SELECT indexrelid,
            relid as table_poid,
            schemaname,
            relname,
            indexrelname,
            pg_relation_size(indexrelid) as idxsize,
            b.*
        FROM pg_stat_user_indexes a
            inner join (
                select r.poid,
                    sum(nvl(idx_scan, 0)) as idx_scan,
                    sum(nvl(idx_tup_read, 0)) as idx_tup_read,
                    sum(nvl(idx_tup_fetch, 0)) as idx_tup_fetch,
                    sum(nvl(idx_blks_read, 0)) as idx_blks_read,
                    sum(nvl(idx_blks_hit, 0)) as idx_blks_hit,
                    sum(nvl(phyrds, 0)) as phyrds,
                    sum(nvl(phywrts, 0)) as phywrts,
                    sum(nvl(phyblkrd, 0)) as phyblkrd,
                    sum(nvl(phyblkwrt, 0)) as phyblkwrt,
                    sum(nvl(readtim, 0)) as readtim,
                    sum(nvl(writetim, 0)) as writetim
                from r
                    inner join pg_stat_user_indexes stat on r.relid = stat.indexrelid
                    inner join pg_statio_user_indexes statio on r.relid = statio.indexrelid
                    inner join (
                        select oid,
                            relfilenode
                        from pg_class
                        union
                        select oid,
                            relfilenode
                        from pg_partition
                    ) t on r.relid = t.oid
                    left join dbe_perf.file_iostat f on t.relfilenode = f.filenum
                group by r.poid
            ) b on a.indexrelid = b.poid
    )
where table_poid = %s;
	"""
table_detail_sql = """
select current_database()                                     as datname,
       relid,
       schemaname,
       relname,
       pg_get_tabledef(%s) as table_detail
FROM pg_stat_user_tables
where schemaname = %s
  and relname = %s;"""
######################################  lock page query sql  ######################################
lock_sql = """
select relname,
       locktype,
       tb2.datname,
       relation,
       page,
       tuple,
       transactionid,
       virtualxid,
       objid,
       virtualtransaction,
       pid,
       mode,
       granted,
       fastpath,
       locktag
from dbe_perf.locks
       left join (select oid, relname from pg_class) pg_class on dbe_perf.locks.relation = pg_class.oid
       left join (select datname, datid from dbe_perf.stat_database) tb2 on tb2.datid = dbe_perf.locks.database;

"""
even_twait_sql = """select nodename,
       type,
       event,
       wait,
       failed_wait,
       total_wait_time,
       avg_wait_time,
       max_wait_time,
       min_wait_time,
       last_updated
from dbe_perf.WAIT_EVENTS;"""
######################################  memory page query sql  ######################################
memory_sql = """select host(inet_server_addr()) as target_ip,
    localtimestamp as snaptime,
    sum (case memorytype when 'max_process_memory' then memorymbytes else 0 end) as max_process_memory,
    sum (case memorytype when 'process_used_memory' then memorymbytes else 0 end) as process_used_memory,
    sum (case memorytype when 'max_dynamic_memory' then memorymbytes else 0 end) as max_dynamic_memory,
    sum (case memorytype when 'dynamic_used_memory' then memorymbytes else 0 end) as dynamic_used_memory,
    sum (case memorytype when 'dynamic_used_shrctx' then memorymbytes else 0 end) as dynamic_used_shrctx,
    sum (case memorytype when 'max_backend_memory' then memorymbytes else 0 end) as max_backend_memory,
    sum (case memorytype when 'backend_used_memory' then memorymbytes else 0 end) as backend_used_memory,
    sum (case memorytype when 'max_shared_memory' then memorymbytes else 0 end) as max_shared_memory,
    sum (case memorytype when 'shared_used_memory' then memorymbytes else 0 end) as shared_used_memory,
    sum (case memorytype when 'max_cstore_memory' then memorymbytes else 0 end) as max_cstore_memory,
    sum (case memorytype when 'cstore_used_memory' then memorymbytes else 0 end) as cstore_used_memory,
    sum (case memorytype when 'max_sctpcomm_memory' then memorymbytes else 0 end) as max_sctpcomm_memory,
    sum (case memorytype when 'sctpcomm_used_memory' then memorymbytes else 0 end) as sctpcomm_used_memory,
    sum (case memorytype when 'other_used_memory' then memorymbytes else 0 end) as other_used_memory,
    sum (case memorytype when 'gpu_max_dynamic_memory' then memorymbytes else 0 end) as gpu_max_dynamic_memory,
    sum (case memorytype when 'gpu_dynamic_used_memory' then memorymbytes else 0 end) as gpu_dynamic_used_memory,
    sum (case memorytype when 'pooler_conn_memory' then memorymbytes else 0 end) as pooler_conn_memory,
    sum (case memorytype when 'pooler_freeconn_memory' then memorymbytes else 0 end) as pooler_freeconn_memory,
    sum (case memorytype when 'storage_compress_memory' then memorymbytes else 0 end) as storage_compress_memory,
    sum (case memorytype when 'udf_reserved_memory' then memorymbytes else 0 end) as udf_reserved_memory
from gs_total_memory_detail;"""
sharemem_sql = '''select host(inet_server_addr()) as target_ip,
    nvl(parent,'root') as parent,
    contextname,
    level,
    sum(totalsize) as totalsize,
    sum(freesize) as freesize,
    sum(usedsize) as usedsize
from gs_shared_memory_detail
group by parent,
    contextname,
    level;'''
top_mem_used_session_sql = '''select *
from gs_session_memory
       join (select pid, query, usename, client_addr from pg_stat_activity) db2
         on split_part(gs_session_memory.sessid, '.', 2) = db2.pid
order by used_mem;'''
######################################  dynamicsql page query sql  ######################################
dynamicsql_sql = """select 
    user_name,
    unique_sql_id,
    left(query,30) as query,
    n_calls,
    total_elapse_time,
    n_returned_rows,
    n_tuples_fetched,
    n_tuples_returned,
    n_tuples_inserted,
    n_tuples_updated,
    n_tuples_deleted,
    n_blocks_fetched,
    n_blocks_hit,
    n_soft_parse,
    n_hard_parse,
    db_time,
    cpu_time,
    execution_time,
    parse_time,
    plan_time,
    rewrite_time,
    pl_execution_time,
    pl_compilation_time,
    data_io_time,
    net_send_info,
    net_recv_info,
    net_stream_send_info,
    net_stream_recv_info,
    sort_count,
    sort_time,
    sort_mem_used,
    sort_spill_count,
    sort_spill_size,
    hash_count,
    hash_time,
    hash_mem_used,
    hash_spill_count,
    hash_spill_size
from dbe_perf.statement;"""
dynamicsql_query_sql = """select query from  dbe_perf.statement where unique_sql_id =%s;"""
enable_resource_track = """show enable_resource_track;"""
index_recommendation_sql = """select 
    query,
    n_calls
from dbe_perf.statement
where unique_sql_id={} order by last_updated desc limit 1;"""
######################################  replication page query sql  ######################################
replication_sql = """select channel,
       localtimestamp                                                              as snaptime,
       local_role,
       peer_role,
       peer_state,
       sync_percent,
       sync_state,
       pg_xlog_location_diff(sender_sent_location, '0/0')                          as sender_sent_location,
       pg_xlog_location_diff(sender_write_location, '0/0')                         as sender_write_location,
       pg_xlog_location_diff(sender_flush_location, '0/0')                         as sender_flush_location,
       pg_xlog_location_diff(sender_replay_location, '0/0')                        as sender_replay_location,
       pg_xlog_location_diff(receiver_received_location, '0/0')                    as receiver_received_location,
       pg_xlog_location_diff(receiver_write_location, '0/0')                       as receiver_write_location,
       pg_xlog_location_diff(receiver_flush_location, '0/0')                       as receiver_flush_location,
       pg_xlog_location_diff(receiver_replay_location, '0/0')                      as receiver_replay_location,
       pg_xlog_location_diff(sender_flush_location, receiver_received_location)    as receive_gap,
       pg_xlog_location_diff(receiver_received_location, receiver_replay_location) as replay_gap
from pg_stat_get_wal_senders()
union
select channel,
       localtimestamp                                                              as snaptime,
       local_role,
       peer_role,
       peer_state,
       sync_percent,
       'unknown' as sync_state,
       pg_xlog_location_diff(sender_sent_location, '0/0')                          as sender_sent_location,
       pg_xlog_location_diff(sender_write_location, '0/0')                         as sender_write_location,
       pg_xlog_location_diff(sender_flush_location, '0/0')                         as sender_flush_location,
       pg_xlog_location_diff(sender_replay_location, '0/0')                        as sender_replay_location,
       pg_xlog_location_diff(receiver_received_location, '0/0')                    as receiver_received_location,
       pg_xlog_location_diff(receiver_write_location, '0/0')                       as receiver_write_location,
       pg_xlog_location_diff(receiver_flush_location, '0/0')                       as receiver_flush_location,
       pg_xlog_location_diff(receiver_replay_location, '0/0')                      as receiver_replay_location,
       pg_xlog_location_diff(sender_flush_location, receiver_received_location)    as receive_gap,
       pg_xlog_location_diff(receiver_received_location, receiver_replay_location) as replay_gap
from pg_stat_get_wal_receiver();"""

replication_slots_sql = """
SELECT slot_name,
       nvl(plugin, '-')                                                          as plugin,
       slot_type,
       nvl(database, '-')                                                        as datname,
       active,
       coalesce(catalog_xmin, '-')                                               as catalog_xmin,
       pg_xlog_location_diff(restart_lsn, '0/0' :: text)                         AS restart_lsn,
       pg_xlog_location_diff(CASE
                               WHEN pg_is_in_recovery() THEN pg_last_xlog_receive_location()
                               ELSE pg_current_xlog_location() END, restart_lsn) as diff_lsn,
       dummy_standby,
       confirmed_flush
from pg_replication_slots;
"""

drop_replication_slot_sql = """select pg_drop_replication_slot(%s) ;"""
######################################  database_list page query sql  ######################################
database_list_sql = """select * from pg_stat_database;"""

######################################  sql_patch page query sql  ######################################

sql_patch_sql = """
select patch_name,
       unique_sql_id,
       pg_get_userbyid(owner) as username,
       enable,
       status,
       abort,
       hint_string,
       hint_node,
       original_query,
       original_query_tree,
       patched_query,
       patched_query_tree,
       description
from gs_sql_patch;
"""

abort_sql_patch = "select * from dbe_sql_util.create_abort_sql_patch(%s, %s)"

drop_sql_patch = "select * from dbe_sql_util.drop_sql_patch(%s);"
######################################  slow_sql page query sql  ######################################
slow_sql = """select db_name,
       user_name,
       application_name,
       client_addr,
       unique_query_id,
       debug_query_id,
       query,
       TIMESTAMP_DIFF('MICROSECOND', start_time, finish_time) / 1000  as duration_ms,
       session_id,
       n_soft_parse,
       n_hard_parse,
       query_plan,
       n_returned_rows,
       n_tuples_fetched,
       n_tuples_returned,
       n_tuples_inserted,
       n_tuples_updated,
       n_tuples_deleted,
       n_blocks_fetched,
       n_blocks_hit,
       db_time,
       cpu_time,
       execution_time,
       parse_time,
       plan_time,
       rewrite_time,
       pl_execution_time,
       pl_compilation_time,
       data_io_time,
       net_send_info,
       net_recv_info,
       net_stream_send_info,
       net_stream_recv_info,
       lock_count,
       lock_time,
       lock_wait_count,
       lock_wait_time,
       lock_max_count,
       lwlock_count,
       lwlock_wait_count,
       lwlock_time,
       lwlock_wait_time,
       pg_catalog.statement_detail_decode(details, 'plaintext', true) as detail
from dbe_perf.statement_history  where start_time between  %s and %s 
order by start_time asc;"""

######################################  redo page query sql  ######################################
redo_stat_sql = """select * from local_redo_stat();"""
xlog_redo_statics_sql = """select * from local_xlog_redo_statics(); """
redo_time_count_sql = """select * from local_redo_time_count();"""

######################################  wdr page query sql  ######################################
monitor_authority_sql = """select rolmonitoradmin from pg_authid  where rolname =%s;"""
check_whether_wdr_takes_effect_sql = """select setting from pg_settings where name = 'enable_wdr_snapshot'; """
wdr_sql = """select * from snapshot.snapshot order by snapshot_id;"""

snap_summary_more_statement = """
select 
       a.snapshot_id,
	   a.snap_user_name,
	   a.snap_unique_sql_id,
	   a.snap_node_name,
	   a.snap_node_id,
	   a.snap_user_id,
	   a.snap_query,
	   a.snap_n_calls - b.snap_n_calls as n_calls,
	   a.snap_total_elapse_time - b.snap_total_elapse_time as total_elapse_time,
	   a.snap_n_returned_rows - b.snap_n_returned_rows as n_returned_rows,
	   a.snap_n_tuples_fetched - b.snap_n_tuples_fetched as n_tuples_fetched,
	   a.snap_n_tuples_returned - b.snap_n_tuples_returned as n_tuples_returned,
	   a.snap_n_tuples_inserted - b.snap_n_tuples_inserted as n_tuples_inserted,
	   a.snap_n_tuples_updated - b.snap_n_tuples_updated as n_tuples_updated,
	   a.snap_n_tuples_deleted - b.snap_n_tuples_deleted as n_tuples_deleted,
	   a.snap_n_blocks_fetched - b.snap_n_blocks_fetched as n_blocks_fetched,
	   a.snap_n_blocks_hit - b.snap_n_blocks_hit as n_blocks_hit,
	   a.snap_n_soft_parse - b.snap_n_soft_parse as n_soft_parse,
	   a.snap_n_hard_parse - b.snap_n_hard_parse as n_hard_parse,
	   a.snap_db_time - b.snap_db_time as db_time,
	   a.snap_cpu_time - b.snap_cpu_time as cpu_time,
	   a.snap_execution_time - b.snap_execution_time as execution_time,
	   a.snap_parse_time - b.snap_parse_time as parse_time,
	   a.snap_plan_time - b.snap_plan_time as plan_time,
	   a.snap_rewrite_time - b.snap_rewrite_time as rewrite_time,
	   a.snap_pl_execution_time - b.snap_pl_execution_time as pl_execution_time,
	   a.snap_pl_compilation_time - b.snap_pl_compilation_time as pl_compilation_time,
	   ((json_object_field((a.snap_net_send_info::json),'time')::varchar)::bigint) -((json_object_field((b.snap_net_send_info::json),'time')::varchar)::bigint) as snap_net_send_info_time,
	   ((json_object_field((a.snap_net_send_info::json),'n_calls')::varchar)::bigint) -((json_object_field((b.snap_net_send_info::json),'n_calls')::varchar)::bigint) as snap_net_send_info_n_calls,
	   ((json_object_field((a.snap_net_send_info::json),'size')::varchar)::bigint) -((json_object_field((b.snap_net_send_info::json),'size')::varchar)::bigint) as snap_net_send_info_size,
	   ((json_object_field((a.snap_net_recv_info::json),'time')::varchar)::bigint) -((json_object_field((b.snap_net_recv_info::json),'time')::varchar)::bigint) as snap_net_recv_info_time,
	   ((json_object_field((a.snap_net_recv_info::json),'n_calls')::varchar)::bigint) -((json_object_field((b.snap_net_recv_info::json),'n_calls')::varchar)::bigint) as snap_net_recv_info_n_calls,
	   ((json_object_field((a.snap_net_recv_info::json),'size')::varchar)::bigint) -((json_object_field((b.snap_net_recv_info::json),'size')::varchar)::bigint) as snap_net_recv_info_size,
	   ((json_object_field((a.snap_net_stream_send_info::json),'time')::varchar)::bigint) -((json_object_field((b.snap_net_stream_send_info::json),'time')::varchar)::bigint) as snap_net_stream_send_info_time,
	   ((json_object_field((a.snap_net_stream_send_info::json),'n_calls')::varchar)::bigint) -((json_object_field((b.snap_net_stream_send_info::json),'n_calls')::varchar)::bigint) as snap_net_stream_send_info_n_calls,
	   ((json_object_field((a.snap_net_stream_send_info::json),'size')::varchar)::bigint) -((json_object_field((b.snap_net_stream_send_info::json),'size')::varchar)::bigint) as snap_net_stream_send_info_size,
	   ((json_object_field((a.snap_net_stream_recv_info::json),'time')::varchar)::bigint) -((json_object_field((b.snap_net_stream_recv_info::json),'time')::varchar)::bigint) as snap_net_stream_recv_info_time,
	   ((json_object_field((a.snap_net_stream_recv_info::json),'n_calls')::varchar)::bigint) -((json_object_field((b.snap_net_stream_recv_info::json),'n_calls')::varchar)::bigint) as snap_net_stream_recv_info_info_n_calls,
	   ((json_object_field((a.snap_net_stream_recv_info::json),'size')::varchar)::bigint) -((json_object_field((b.snap_net_stream_recv_info::json),'size')::varchar)::bigint) as snap_net_stream_recv_info_size,
	   a.snap_last_updated,
	   a.snap_sort_count - b.snap_sort_count as sort_count,
	   a.snap_sort_time - b.snap_sort_time as sort_time,
	   a.snap_sort_mem_used - b.snap_sort_mem_used as sort_mem_used,
	   a.snap_sort_spill_count - b.snap_sort_spill_count as sort_spill_count,
	   a.snap_sort_spill_size - b.snap_sort_spill_size as sort_spill_size,
	   a.snap_hash_count - b.snap_hash_count as hash_count,
	   a.snap_hash_time - b.snap_hash_time as hash_time,
	   a.snap_hash_mem_used - b.snap_hash_mem_used as hash_mem_used,
	   a.snap_hash_spill_count - b.snap_hash_spill_count as hash_spill_count,
	   a.snap_hash_spill_size - b.snap_hash_spill_size as hash_spill_size
from (select * from snapshot.snap_summary_statement where snapshot_id=%s) a left join 
(select * from snapshot.snap_summary_statement where snapshot_id=%s) b on 
a.snap_user_name=b.snap_user_name and a.snap_unique_sql_id=b.snap_unique_sql_id where n_calls>100000;
"""
snap_summary_avg_statement = """
select 
       a.snapshot_id,
	   a.snap_user_name,
	   a.snap_unique_sql_id,
	   a.snap_node_name,
	   a.snap_node_id,
	   a.snap_user_id,
	   a.snap_query,
	   a.snap_n_calls - b.snap_n_calls as n_calls,
	   a.snap_total_elapse_time - b.snap_total_elapse_time as total_elapse_time,
	   a.snap_n_returned_rows - b.snap_n_returned_rows as n_returned_rows,
	   a.snap_n_tuples_fetched - b.snap_n_tuples_fetched as n_tuples_fetched,
	   a.snap_n_tuples_returned - b.snap_n_tuples_returned as n_tuples_returned,
	   a.snap_n_tuples_inserted - b.snap_n_tuples_inserted as n_tuples_inserted,
	   a.snap_n_tuples_updated - b.snap_n_tuples_updated as n_tuples_updated,
	   a.snap_n_tuples_deleted - b.snap_n_tuples_deleted as n_tuples_deleted,
	   a.snap_n_blocks_fetched - b.snap_n_blocks_fetched as n_blocks_fetched,
	   a.snap_n_blocks_hit - b.snap_n_blocks_hit as n_blocks_hit,
	   a.snap_n_soft_parse - b.snap_n_soft_parse as n_soft_parse,
	   a.snap_n_hard_parse - b.snap_n_hard_parse as n_hard_parse,
	   a.snap_db_time - b.snap_db_time as db_time,
	   a.snap_cpu_time - b.snap_cpu_time as cpu_time,
	   a.snap_execution_time - b.snap_execution_time as execution_time,
	   a.snap_parse_time - b.snap_parse_time as parse_time,
	   a.snap_plan_time - b.snap_plan_time as plan_time,
	   a.snap_rewrite_time - b.snap_rewrite_time as rewrite_time,
	   a.snap_pl_execution_time - b.snap_pl_execution_time as pl_execution_time,
	   a.snap_pl_compilation_time - b.snap_pl_compilation_time as pl_compilation_time,
	   ((json_object_field((a.snap_net_send_info::json),'time')::varchar)::bigint) -((json_object_field((b.snap_net_send_info::json),'time')::varchar)::bigint) as snap_net_send_info_time,
	   ((json_object_field((a.snap_net_send_info::json),'n_calls')::varchar)::bigint) -((json_object_field((b.snap_net_send_info::json),'n_calls')::varchar)::bigint) as snap_net_send_info_n_calls,
	   ((json_object_field((a.snap_net_send_info::json),'size')::varchar)::bigint) -((json_object_field((b.snap_net_send_info::json),'size')::varchar)::bigint) as snap_net_send_info_size,
	   ((json_object_field((a.snap_net_recv_info::json),'time')::varchar)::bigint) -((json_object_field((b.snap_net_recv_info::json),'time')::varchar)::bigint) as snap_net_recv_info_time,
	   ((json_object_field((a.snap_net_recv_info::json),'n_calls')::varchar)::bigint) -((json_object_field((b.snap_net_recv_info::json),'n_calls')::varchar)::bigint) as snap_net_recv_info_n_calls,
	   ((json_object_field((a.snap_net_recv_info::json),'size')::varchar)::bigint) -((json_object_field((b.snap_net_recv_info::json),'size')::varchar)::bigint) as snap_net_recv_info_size,
	   ((json_object_field((a.snap_net_stream_send_info::json),'time')::varchar)::bigint) -((json_object_field((b.snap_net_stream_send_info::json),'time')::varchar)::bigint) as snap_net_stream_send_info_time,
	   ((json_object_field((a.snap_net_stream_send_info::json),'n_calls')::varchar)::bigint) -((json_object_field((b.snap_net_stream_send_info::json),'n_calls')::varchar)::bigint) as snap_net_stream_send_info_n_calls,
	   ((json_object_field((a.snap_net_stream_send_info::json),'size')::varchar)::bigint) -((json_object_field((b.snap_net_stream_send_info::json),'size')::varchar)::bigint) as snap_net_stream_send_info_size,
	   ((json_object_field((a.snap_net_stream_recv_info::json),'time')::varchar)::bigint) -((json_object_field((b.snap_net_stream_recv_info::json),'time')::varchar)::bigint) as snap_net_stream_recv_info_time,
	   ((json_object_field((a.snap_net_stream_recv_info::json),'n_calls')::varchar)::bigint) -((json_object_field((b.snap_net_stream_recv_info::json),'n_calls')::varchar)::bigint) as snap_net_stream_recv_info_info_n_calls,
	   ((json_object_field((a.snap_net_stream_recv_info::json),'size')::varchar)::bigint) -((json_object_field((b.snap_net_stream_recv_info::json),'size')::varchar)::bigint) as snap_net_stream_recv_info_size,
	   a.snap_last_updated,
	   a.snap_sort_count - b.snap_sort_count as sort_count,
	   a.snap_sort_time - b.snap_sort_time as sort_time,
	   a.snap_sort_mem_used - b.snap_sort_mem_used as sort_mem_used,
	   a.snap_sort_spill_count - b.snap_sort_spill_count as sort_spill_count,
	   a.snap_sort_spill_size - b.snap_sort_spill_size as sort_spill_size,
	   a.snap_hash_count - b.snap_hash_count as hash_count,
	   a.snap_hash_time - b.snap_hash_time as hash_time,
	   a.snap_hash_mem_used - b.snap_hash_mem_used as hash_mem_used,
	   a.snap_hash_spill_count - b.snap_hash_spill_count as hash_spill_count,
	   a.snap_hash_spill_size - b.snap_hash_spill_size as hash_spill_size
from snapshot.snap_summary_statement a, 
	 snapshot.snap_summary_statement b 
where a.snapshot_id - 1 =b.snapshot_id 
and a.snap_user_id=b.snap_user_id 
and a.snap_unique_sql_id=b.snap_unique_sql_id  and a.snapshot_id< %s 
and a.snapshot_id> %s
and a.snap_unique_sql_id in %s;
"""
snap_summary_statement = """select snapshot_id,
       snap_node_name,
       snap_node_id,
       snap_user_name,
       snap_user_id,
       snap_unique_sql_id,
       snap_n_calls,
       snap_min_elapse_time,
       snap_max_elapse_time,
       snap_total_elapse_time,
       snap_n_returned_rows,
       snap_n_tuples_fetched,
       snap_n_tuples_returned,
       snap_n_tuples_inserted,
       snap_n_tuples_updated,
       snap_n_tuples_deleted,
       snap_n_blocks_fetched,
       snap_n_blocks_hit,
       snap_n_soft_parse,
       snap_n_hard_parse,
       snap_db_time,
       snap_cpu_time,
       snap_execution_time,
       snap_parse_time,
       snap_plan_time,
       snap_rewrite_time,
       snap_pl_execution_time,
       snap_pl_compilation_time,
       snap_data_io_time,
       ((json_object_field((snap_net_send_info::json),'time')::varchar)::bigint) as snap_net_send_info_time,
	   ((json_object_field((snap_net_send_info::json),'n_calls')::varchar)::bigint) as snap_net_send_info_n_calls,
	   ((json_object_field((snap_net_send_info::json),'size')::varchar)::bigint) as snap_net_send_info_size,
	   ((json_object_field((snap_net_recv_info::json),'time')::varchar)::bigint) as snap_net_recv_info_time,
	   ((json_object_field((snap_net_recv_info::json),'n_calls')::varchar)::bigint) as snap_net_recv_info_n_calls,
	   ((json_object_field((snap_net_recv_info::json),'size')::varchar)::bigint) as snap_net_recv_info_size,
	   ((json_object_field((snap_net_stream_send_info::json),'time')::varchar)::bigint) as snap_net_stream_send_info_time,
	   ((json_object_field((snap_net_stream_send_info::json),'n_calls')::varchar)::bigint) as snap_net_stream_send_info_n_calls,
	   ((json_object_field((snap_net_stream_send_info::json),'size')::varchar)::bigint) as snap_net_stream_send_info_size,
	   ((json_object_field((snap_net_stream_recv_info::json),'time')::varchar)::bigint) as snap_net_stream_recv_info_time,
	   ((json_object_field((snap_net_stream_recv_info::json),'n_calls')::varchar)::bigint) as snap_net_stream_recv_info_info_n_calls,
	   ((json_object_field((snap_net_stream_recv_info::json),'size')::varchar)::bigint) as snap_net_stream_recv_info_size,
	   snap_last_updated,
       snap_sort_count,
       snap_sort_time,
       snap_sort_mem_used,
       snap_sort_spill_count,
       snap_sort_spill_size,
       snap_hash_count,
       snap_hash_time,
       snap_hash_mem_used,
       snap_hash_spill_count,
       snap_hash_spill_size,
       snap_query
from snapshot.snap_summary_statement
where snapshot_id = %s
  and snap_unique_sql_id =%s;"""
