/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SERVER
#include "config_bridge.h"
#include "share/config/ob_config_helper.h"
#include "ob_server_duty_task.h"
#include "share/rc/ob_server_runtime.h"
#include "sql/engine/ob_sql_memory_manager.h"
#include "observer/omt/ob_server_runtime.h"
#include "share/ob_server_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "common/mysqlclient/ob_mysql_result.h"

using namespace oceanbase::common;

namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace sql;
namespace observer {

namespace
{
struct AIWorkerColumn
{
  int64_t retry_count_ = 0;
  int64_t max_retries_ = 0;
  ObString column_name_;
  ObString execution_expr_;
  ObString state_column_name_;
  ObString request_column_name_;
};

int append_identifier(ObSqlString &sql, const ObString &name)
{
  int ret = OB_SUCCESS;
  OZ(sql.append("`"));
  for (int64_t i = 0; OB_SUCC(ret) && i < name.length(); ++i) {
    OZ(sql.append('`' == name.ptr()[i] ? "``" : ObString(1, name.ptr() + i)));
  }
  OZ(sql.append("`"));
  return ret;
}

int append_hex_literal(ObSqlString &sql, const ObString &value)
{
  static const char HEX[] = "0123456789ABCDEF";
  int ret = OB_SUCCESS;
  OZ(sql.append("X'"));
  for (int64_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
    const unsigned char c = static_cast<unsigned char>(value.ptr()[i]);
    OZ(sql.append_fmt("%c%c", HEX[c >> 4], HEX[c & 0x0F]));
  }
  OZ(sql.append("'"));
  return ret;
}
} // namespace

ObServerDutyTask::ObServerDutyTask()
  : allocator_(ObModIds::OB_DUTY_TASK)
{
}

void ObServerDutyTask::runTimerTask()
{
  allocator_.reset_remain_one_page();
  update_runtime_settings();
}

int ObServerDutyTask::schedule(common::ObTimer &timer)
{
  return timer.schedule(*this, SCHEDULE_PERIOD, true);
}

void ObServerDutyTask::update_runtime_settings()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_ctx_memory_throttle())) {
  }
}

int ObServerDutyTask::update_ctx_memory_throttle()
{
  int ret = OB_SUCCESS;
  {
    ObCtxMemoryLimitChecker checker;
    uint64_t ctx_id = 0;
    int64_t limit = 0;
    ObMallocAllocator *alloc = ObMallocAllocator::get_instance();
    rust::String limits = config::_ctx_memory_limit();
    if (!checker.check(limits.c_str(), ctx_id, limit)) {
      // do nothing
    } else {
      if (limits.empty()) {
        ctx_id = ObCtxIds::MAX_CTX_ID;
        limit = INT64_MAX; // empty str means no limit, and not care ctx_id.
      }
      for (int i = 0; i < ObCtxIds::MAX_CTX_ID; i++) {
        if (ObCtxIds::WORK_AREA == i ||
            ObCtxIds::META_OBJ_CTX_ID == i) {
          continue;
        }
        auto allocator = alloc->get_ctx_allocator(i);
        if (OB_NOT_NULL(allocator)) {
          if (OB_FAIL(allocator->set_limit(ctx_id == i ? limit : INT64_MAX))) {
          }
        }
      }
    }
  }
  return ret;
}

//////////////////////////////////////////////////////////////////////////////////////////

int ObSqlMemoryTimerTask::schedule(common::ObTimer &timer)
{
  return timer.schedule(*this, SCHEDULE_PERIOD, true);
}

void ObSqlMemoryTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  {
    SERVER_MODULE_SCOPE {
      ObSqlMemoryManager *sql_mem_mgr = ::oceanbase::share::server_service<::oceanbase::sql::ObSqlMemoryManager>();
      if (OB_UNLIKELY(nullptr == sql_mem_mgr)) {
      } else if (OB_FAIL(sql_mem_mgr->calculate_global_bound_size())) {
      }
    }
  }
}

AIColumnWorkerTask::AIColumnWorkerTask()
  : allocator_("AIColumnWorker")
{
}

int AIColumnWorkerTask::schedule(common::ObTimer &timer)
{
  return timer.schedule(*this, SCHEDULE_PERIOD, true);
}

void AIColumnWorkerTask::runTimerTask()
{
  if (GCTX.in_bootstrap_ || GCTX.start_service_time_ <= 0) {
    return;
  }
  allocator_.reset_remain_one_page();
  int ret = process_one_group();
  if (OB_FAIL(ret) && OB_TABLE_NOT_EXIST != ret && OB_ITER_END != ret) {
    LOG_WARN("failed to process AI column task group", KR(ret));
  }
}

int AIColumnWorkerTask::process_one_group()
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  ObString request_id;
  ObString rowkey_predicate;
  ObSqlString sql;
  ObMySQLProxy *proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(proxy)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT table_id, request_id, rowkey_predicate FROM %s.%s "
      "WHERE eligible_ts <= NOW(6) AND state IN (0, 1, 3) ORDER BY task_id LIMIT 1",
      OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TASK_TNAME))) {
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      ObString tmp_request;
      ObString tmp_rowkey;
      if (OB_FAIL(proxy->read(res, sql.ptr()))) {
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, uint64_t);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "request_id", tmp_request);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "rowkey_predicate", tmp_rowkey);
        OZ(ob_write_string(allocator_, tmp_request, request_id));
        OZ(ob_write_string(allocator_, tmp_rowkey, rowkey_predicate));
      }
    }
  }

  if (OB_SUCC(ret) && OB_INVALID_ID != table_id) {
    int64_t affected_rows = 0;
    sql.reset();
    OZ(sql.append_fmt("UPDATE %s.%s SET state=1, "
                      "eligible_ts=DATE_ADD(NOW(6), INTERVAL 5 SECOND) "
                      "WHERE table_id=%lu AND request_id=",
                      OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TASK_TNAME, table_id));
    OZ(append_hex_literal(sql, request_id));
    OZ(sql.append(" AND rowkey_predicate="));
    OZ(append_hex_literal(sql, rowkey_predicate));
    OZ(sql.append(" AND eligible_ts <= NOW(6) AND state IN (0, 1, 3)"));
    OZ(proxy->write(sql.ptr(), affected_rows));
    if (OB_SUCC(ret) && 0 == affected_rows) {
      table_id = OB_INVALID_ID;
    }
  }

  ObString database_name;
  ObString table_name;
  ObSEArray<AIWorkerColumn, 4> columns;
  if (OB_SUCC(ret) && OB_INVALID_ID != table_id) {
    sql.reset();
    OZ(sql.append_fmt(
        "SELECT task.retry_count, task.max_retries, d.database_name, t.table_name, "
        "a.column_name, a.execution_expr, sc.column_name AS state_column_name, "
        "rc.column_name AS request_column_name FROM %s.%s task "
        "JOIN %s.%s a ON a.table_id=task.table_id AND a.column_id=task.column_id "
        "AND a.definition_version=task.definition_version "
        "JOIN %s.%s t ON t.table_id=task.table_id "
        "JOIN %s.%s d ON d.database_id=t.database_id "
        "JOIN %s.%s sc ON sc.table_id=a.table_id AND sc.column_id=a.state_column_id "
        "JOIN %s.%s rc ON rc.table_id=a.table_id AND rc.column_id=a.request_column_id "
        "WHERE task.table_id=%lu AND task.request_id=",
        OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TASK_TNAME,
        OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TNAME,
        OB_SYS_DATABASE_NAME, OB_ALL_TABLE_TNAME,
        OB_SYS_DATABASE_NAME, OB_ALL_DATABASE_TNAME,
        OB_SYS_DATABASE_NAME, OB_ALL_COLUMN_TNAME,
        OB_SYS_DATABASE_NAME, OB_ALL_COLUMN_TNAME, table_id));
    OZ(append_hex_literal(sql, request_id));
    OZ(sql.append(" AND task.rowkey_predicate="));
    OZ(append_hex_literal(sql, rowkey_predicate));
    OZ(sql.append(" ORDER BY task.task_id"));
  }
  if (OB_SUCC(ret) && OB_INVALID_ID != table_id) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy->read(res, sql.ptr()))) {
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      }
      while (OB_SUCC(ret)) {
        ObString tmp_database;
        ObString tmp_table;
        ObString tmp_column;
        ObString tmp_expr;
        ObString tmp_state;
        ObString tmp_request_column;
        AIWorkerColumn column;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
          break;
        }
        EXTRACT_INT_FIELD_MYSQL(*result, "retry_count", column.retry_count_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "max_retries", column.max_retries_, int64_t);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "database_name", tmp_database);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "table_name", tmp_table);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "column_name", tmp_column);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "execution_expr", tmp_expr);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "state_column_name", tmp_state);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "request_column_name", tmp_request_column);
        if (columns.empty()) {
          OZ(ob_write_string(allocator_, tmp_database, database_name));
          OZ(ob_write_string(allocator_, tmp_table, table_name));
        }
        OZ(ob_write_string(allocator_, tmp_column, column.column_name_));
        OZ(ob_write_string(allocator_, tmp_expr, column.execution_expr_));
        OZ(ob_write_string(allocator_, tmp_state, column.state_column_name_));
        OZ(ob_write_string(allocator_, tmp_request_column, column.request_column_name_));
        OZ(columns.push_back(column));
      }
    }
  }

  if (OB_SUCC(ret) && OB_INVALID_ID != table_id && columns.empty()) {
    int64_t affected_rows = 0;
    sql.reset();
    OZ(sql.append_fmt("DELETE FROM %s.%s WHERE table_id=%lu AND request_id=",
                      OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TASK_TNAME, table_id));
    OZ(append_hex_literal(sql, request_id));
    OZ(sql.append(" AND rowkey_predicate="));
    OZ(append_hex_literal(sql, rowkey_predicate));
    OZ(proxy->write(sql.ptr(), affected_rows));
  }

  if (OB_SUCC(ret) && !columns.empty()) {
    sql.reset();
    OZ(sql.append("UPDATE "));
    OZ(append_identifier(sql, database_name));
    OZ(sql.append("."));
    OZ(append_identifier(sql, table_name));
    OZ(sql.append(" SET "));
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      const AIWorkerColumn &column = columns.at(i);
      if (i > 0) {
        OZ(sql.append(", "));
      }
      OZ(append_identifier(sql, column.column_name_));
      OZ(sql.append(" = ("));
      OZ(sql.append(column.execution_expr_));
      OZ(sql.append(")"));
    }
    OZ(sql.append(" WHERE "));
    OZ(sql.append(rowkey_predicate));
    OZ(sql.append_fmt(" AND EXISTS (SELECT 1 FROM %s.%s task WHERE task.table_id=%lu AND task.request_id=",
                      OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TASK_TNAME, table_id));
    OZ(append_hex_literal(sql, request_id));
    OZ(sql.append(" AND task.rowkey_predicate="));
    OZ(append_hex_literal(sql, rowkey_predicate));
    OZ(sql.append(")"));
  }

  int worker_ret = ret;
  if (OB_SUCC(ret) && !columns.empty()) {
    ObMySQLTransaction trans;
    int64_t affected_rows = 0;
    if (OB_FAIL(trans.start(proxy))) {
    } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
    } else {
      sql.reset();
      OZ(sql.append_fmt("DELETE FROM %s.%s WHERE table_id=%lu AND request_id=",
                        OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TASK_TNAME, table_id));
      OZ(append_hex_literal(sql, request_id));
      OZ(sql.append(" AND rowkey_predicate="));
      OZ(append_hex_literal(sql, rowkey_predicate));
      OZ(trans.write(sql.ptr(), affected_rows));
    }
    worker_ret = ret;
    if (trans.is_started()) {
      int end_ret = trans.end(OB_SUCC(ret));
      if (OB_SUCC(ret) && OB_SUCCESS != end_ret) {
        ret = end_ret;
        worker_ret = end_ret;
      }
    }
  }

  if (OB_FAIL(worker_ret) && !columns.empty()) {
    ret = OB_SUCCESS;
    bool retryable = false;
    for (int64_t i = 0; i < columns.count(); ++i) {
      retryable = retryable || columns.at(i).retry_count_ < columns.at(i).max_retries_;
    }
    if (retryable) {
      sql.reset();
      OZ(sql.append_fmt("UPDATE %s.%s SET retry_count=retry_count+1, state=3, "
                        "eligible_ts=DATE_ADD(NOW(6), INTERVAL 1 SECOND) WHERE table_id=%lu AND request_id=",
                        OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TASK_TNAME, table_id));
      OZ(append_hex_literal(sql, request_id));
      OZ(sql.append(" AND rowkey_predicate="));
      OZ(append_hex_literal(sql, rowkey_predicate));
      int64_t affected_rows = 0;
      OZ(proxy->write(sql.ptr(), affected_rows));
    } else {
      ObMySQLTransaction trans;
      int64_t affected_rows = 0;
      OZ(trans.start(proxy));
      sql.reset();
      OZ(sql.append_fmt("DELETE FROM %s.%s WHERE table_id=%lu AND request_id=",
                        OB_SYS_DATABASE_NAME, OB_ALL_AI_COLUMN_TASK_TNAME, table_id));
      OZ(append_hex_literal(sql, request_id));
      OZ(sql.append(" AND rowkey_predicate="));
      OZ(append_hex_literal(sql, rowkey_predicate));
      OZ(trans.write(sql.ptr(), affected_rows));
      if (trans.is_started()) {
        int end_ret = trans.end(OB_SUCC(ret));
        if (OB_SUCC(ret) && OB_SUCCESS != end_ret) {
          ret = end_ret;
        }
      }
    }
  }
  return ret;
}

}  // observer
}  // oceanbase
