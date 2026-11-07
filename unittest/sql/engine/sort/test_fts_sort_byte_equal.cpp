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

#include <gtest/gtest.h>
#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include "lib/worker.h"
#include "lib/allocator/page_arena.h"
#include "lib/charset/ob_charset.h"
#include "share/datum/ob_datum_funcs.h"
#include "share/schema/ob_table_schema.h"
// Preload dependencies before test-only access overrides; production headers are unchanged.
#include "lib/container/ob_heap.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "share/ob_rpc_struct.h"
#include "sql/engine/basic/chunk_store/ob_compact_store.h"
#include "sql/engine/sort/ob_pd_topn_sort_filter.h"
#include "sql/engine/px/p2p_datahub/ob_pushdown_topn_filter_msg.h"
#define protected public
#include "sql/engine/sort/ob_sort_op_impl.h"
#undef protected
#define private public
#include "sql/engine/sort/ob_sort_op.h"
#undef private
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/sort/ob_sort_op_impl.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/resolver/dml/ob_hint.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
namespace sql {
namespace {
using namespace common;
using namespace share::schema;
using StoredRow = ObChunkDatumStore::StoredRow;
#define RETURN_IF_ERROR(expr) do { const int rc = (expr); if (OB_SUCCESS != rc) return rc; } while (false)
struct TestEnvironment {
  ObArenaAllocator arena;
  ObSQLSessionInfo session;
  ObExecContext exec;
  ObEvalCtx eval;
  TestEnvironment() : arena("FtsSortTest"), session(), exec(arena), eval(exec, &arena) {}
  int init()
  {
    RETURN_IF_ERROR(session.test_init(0, 17, &arena));
    exec.set_my_session(&session);
    RETURN_IF_ERROR(exec.create_physical_plan_ctx());
    exec.get_physical_plan_ctx()->set_timeout_timestamp(0);
    eval.max_batch_size_ = 16;
    return OB_SUCCESS;
  }
};
class SortGatePlan : public ObLogPlan {
public:
  SortGatePlan(ObOptimizerContext &ctx, const ObDMLStmt *stmt) : ObLogPlan(ctx, stmt) {}
  int generate_normal_raw_plan() override { return OB_NOT_SUPPORTED; }
};
// Real DDL scan transition: save column IDs before replacing generated expressions.
struct GateFixture {
  static const uint64_t DATA_ID = 10001, INDEX_ID = 10002, WORD_ID = 16, DOC_ID = 17;
  TestEnvironment &env;
  ObAddr address;
  ObGlobalHint hint;
  ObRawExprFactory factory;
  ObInsertStmt stmt;
  ObOptimizerContext context;
  SortGatePlan plan;
  ObLogTableScan scan;
  ObLogSort sort;
  ObLogSubPlanScan subplan;
  ObTableSchema data_schema, index_schema;
  TableItem table;
  ObColumnRefRawExpr word_column, doc_column, outer_column;
  ObSysFunRawExpr word_expression;
  GateFixture(TestEnvironment &e)
    : env(e), factory(e.arena), context(&e.session, &e.exec, nullptr, nullptr,
      e.arena, nullptr, address, hint, factory, &stmt, false), plan(context, &stmt),
      scan(plan), sort(plan), subplan(plan), data_schema(&e.arena), index_schema(&e.arena) {}
  int init()
  {
    context.set_is_online_ddl(true);
    env.session.set_stmt_type(stmt::T_INSERT);
    table.table_id_ = DATA_ID; table.ref_id_ = DATA_ID; table.ddl_table_id_ = INDEX_ID;
    RETURN_IF_ERROR(stmt.get_table_items().push_back(&table));
    data_schema.set_table_id(DATA_ID); data_schema.set_table_type(USER_TABLE);
    index_schema.set_table_id(INDEX_ID);
    index_schema.set_table_type(USER_INDEX); index_schema.set_index_type(INDEX_TYPE_FTS_INDEX_LOCAL);
    index_schema.set_data_table_id(DATA_ID); index_schema.set_rowkey_column_num(2);
    for (int i = 0; i < 2; ++i) {
      ObColumnSchemaV2 column(&env.arena);
      column.set_table_id(DATA_ID); column.set_column_id(i == 0 ? WORD_ID : DOC_ID);
      column.set_schema_version(1); column.set_data_type(ObVarcharType); column.set_data_length(i == 0 ? 84 : 16);
      column.set_collation_type(i == 0 ? CS_TYPE_UTF8MB4_GENERAL_CI : CS_TYPE_BINARY);
      column.set_charset_type(i == 0 ? CHARSET_UTF8MB4 : CHARSET_BINARY);
      column.set_prev_column_id(UINT64_MAX); column.set_next_column_id(UINT64_MAX);
      RETURN_IF_ERROR(column.set_column_name(i == 0 ? "generated_word" : "generated_docid"));
      if (i == 0) column.add_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG | VIRTUAL_GENERATED_COLUMN_FLAG);
      RETURN_IF_ERROR(data_schema.add_column(column));
      column.set_table_id(INDEX_ID); column.set_rowkey_position(i + 1);
      RETURN_IF_ERROR(index_schema.add_column(column));
    }
    scan.set_type(log_op_def::LOG_TABLE_SCAN); scan.set_ref_table_id(DATA_ID); scan.set_table_id(DATA_ID);
    sort.set_type(log_op_def::LOG_SORT); subplan.set_type(log_op_def::LOG_SUBPLAN_SCAN);
    word_column.set_ref_id(DATA_ID, WORD_ID); word_column.set_expr_type(T_REF_COLUMN);
    doc_column.set_ref_id(DATA_ID, DOC_ID); doc_column.set_expr_type(T_REF_COLUMN);
    word_expression.set_expr_type(T_FUN_SYS_WORD_SEGMENT);
    RETURN_IF_ERROR(scan.get_output_exprs().push_back(&word_column));
    RETURN_IF_ERROR(scan.get_output_exprs().push_back(&doc_column));
    RETURN_IF_ERROR(scan.generate_ddl_output_column_ids());
    scan.get_output_exprs().at(0) = &word_expression;
    RETURN_IF_ERROR(sort.get_sort_keys().push_back(OrderItem(&word_expression)));
    RETURN_IF_ERROR(sort.get_sort_keys().push_back(OrderItem(&doc_column)));
    RETURN_IF_ERROR(sort.add_child(&scan));
    return OB_SUCCESS;
  }
  bool eligible() { return ObStaticEngineCG::is_fts_word_first_sort(sort, index_schema, data_schema); }
};
struct RowValue {
  std::string word, doc;
  bool word_null = false, doc_null = false;
  uint64_t id = 0;
};
static std::string doc_id(uint64_t n)
{
  std::string bytes(16, '\0');
  for (int i = 0; i < 8; ++i) bytes[15 - i] = static_cast<char>(n >> (8 * i));
  return bytes;
}
static void make_datums(const RowValue &row, ObDatum (&d)[3], uint64_t &payload)
{
  if (row.word_null) d[0].set_null(); else d[0].set_string(row.word.data(), row.word.size());
  if (row.doc_null) d[1].set_null(); else d[1].set_string(row.doc.data(), row.doc.size());
  payload = row.id; d[2].ptr_ = reinterpret_cast<const char *>(&payload); d[2].set_uint(payload);
}
static RowValue snapshot(const StoredRow *row)
{
  const ObDatum *d = row->cells();
  return {d[0].is_null() ? std::string() : std::string(d[0].ptr_, d[0].len_),
          d[1].is_null() ? std::string() : std::string(d[1].ptr_, d[1].len_),
          d[0].is_null(), d[1].is_null(), d[2].get_uint64()};
}
struct RuntimeKeys {
  ObArray<ObSortFieldCollation> collations;
  ObArray<ObSortCmpFunc> functions;
  ObExpr word, doc, payload;
  ExprFixedArray expressions;
  explicit RuntimeKeys(ObIAllocator &arena) : expressions(arena) {}
  int init(bool doc_first = false, bool word_asc = true, bool doc_asc = true,
           ObCmpNullPos null_pos = NULL_LAST, bool third_key = false)
  {
    word.type_ = T_FUN_SYS_WORD_SEGMENT; word.datum_meta_ = ObDatumMeta(ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI, SCALE_UNKNOWN_YET);
    word.obj_meta_.set_varchar(); word.obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    doc.datum_meta_ = ObDatumMeta(ObVarcharType, CS_TYPE_BINARY, SCALE_UNKNOWN_YET);
    doc.obj_meta_.set_varchar(); doc.obj_meta_.set_collation_type(CS_TYPE_BINARY);
    payload.datum_meta_ = ObDatumMeta(ObUInt64Type, CS_TYPE_BINARY, SCALE_UNKNOWN_YET); payload.obj_meta_.set_uint64();
    RETURN_IF_ERROR(expressions.init(3));
    RETURN_IF_ERROR(expressions.push_back(&word)); RETURN_IF_ERROR(expressions.push_back(&doc)); RETURN_IF_ERROR(expressions.push_back(&payload));
    for (int pos = 0; pos < (third_key ? 3 : 2); ++pos) {
      const int col = pos == 2 ? 2 : (doc_first ? 1 - pos : pos);
      ObSortCmpFunc fn;
      const ObObjType type = col == 2 ? ObUInt64Type : ObVarcharType;
      const ObCollationType cs = col == 0 ? CS_TYPE_UTF8MB4_GENERAL_CI : CS_TYPE_BINARY;
      fn.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(type, type, null_pos, cs, SCALE_UNKNOWN_YET, false);
      if (nullptr == fn.cmp_func_) return OB_ERR_UNEXPECTED;
      RETURN_IF_ERROR(functions.push_back(fn));
      RETURN_IF_ERROR(collations.push_back(ObSortFieldCollation(col, cs, col == 0 ? word_asc : doc_asc, null_pos, false)));
    }
    return OB_SUCCESS;
  }
  bool eligible() { return ObSortOpImpl::can_use_fts_word_equal(collations, functions, expressions); }
};
class NativeSort : public ObSortOpImpl {
public:
  int append(const RowValue &value)
  {
    ObDatum datums[3]; uint64_t payload = 0; StoredRow *row = nullptr;
    make_datums(value, datums, payload);
    RETURN_IF_ERROR(datum_store_.add_row(datums, 3, 0, &row));
    return after_add_row(row);
  }
  int append_run(const std::vector<const StoredRow *> &rows)
  {
    void *mem = mem_context_->get_malloc_allocator().alloc(sizeof(ObSortOpChunk));
    if (nullptr == mem) return OB_ALLOCATE_MEMORY_FAILED;
    auto *chunk = new (mem) ObSortOpChunk(0, false);
    chunk->datum_store_.set_allocator(mem_context_->get_malloc_allocator());
    int ret = chunk->datum_store_.init(INT64_MAX, ObCtxIds::WORK_AREA, "FtsSortRun", false);
    for (auto *row : rows) if (OB_SUCCESS == ret) ret = chunk->datum_store_.add_row(*row);
    if (OB_SUCCESS == ret) ret = chunk->datum_store_.finish_add_row(false);
    if (OB_SUCCESS == ret) sort_chunks_.add_last(chunk);
    else { chunk->~ObSortOpChunk(); mem_context_->get_malloc_allocator().free(chunk); }
    return ret;
  }
  int prepare_ems()
  {
    // This fixture exercises a final in-memory EMS round without a server temp-file service.
    // With no registered auto manager, get_mem_bound() reads the default usable bound.
    profile_.set_max_bound(8 * 1024 * 1024);
    sql_mem_processor_.set_default_usable_mem_size(8 * 1024 * 1024);
    return sort();
  }
  int64_t row_count() const { return datum_store_.get_row_cnt(); }
  int64_t chunk_count() const { return sort_chunks_.get_size(); }
  int64_t bytes_used() const { return datum_store_.get_mem_used(); }
  Compare &compare() { return comp_; }
};
class FtsSortByteEqual : public ::testing::Test {
protected:
  lib::Worker worker_;
  lib::Worker *previous_worker_ = nullptr;
  std::unique_ptr<TestEnvironment> env_;
  std::unique_ptr<GateFixture> gate_;
  void SetUp() override
  {
    previous_worker_ = lib::Worker::self_;
    lib::Worker::set_worker_to_thread_local(&worker_); worker_.set_timeout_ts(INT64_MAX);
    static const int charset_result = ObCharset::init_charset();
    ASSERT_EQ(OB_SUCCESS, charset_result);
    env_.reset(new TestEnvironment); ASSERT_EQ(OB_SUCCESS, env_->init());
    gate_.reset(new GateFixture(*env_)); ASSERT_EQ(OB_SUCCESS, gate_->init()); ASSERT_TRUE(gate_->eligible());
  }
  void TearDown() override
  {
    gate_.reset(); env_.reset(); lib::Worker::set_worker_to_thread_local(previous_worker_);
  }
  int configure(NativeSort &sorter, RuntimeKeys &keys, bool rewind = true, bool doc_first = false)
  {
    RETURN_IF_ERROR(sorter.init(&keys.collations, &keys.functions, &env_->eval, &env_->exec,
        false, false, rewind, 0, INT64_MAX, false, ObChunkDatumStore::BLOCK_SIZE,
        NONE_COMPRESSOR, &keys.expressions));
    if (doc_first) std::swap(gate_->sort.get_sort_keys().at(0), gate_->sort.get_sort_keys().at(1));
    const bool admitted = gate_->eligible();
    if (doc_first) std::swap(gate_->sort.get_sort_keys().at(0), gate_->sort.get_sort_keys().at(1));
    sorter.set_fts_word_equal(admitted);
    return OB_SUCCESS;
  }
  void verify(NativeSort &sorter, RuntimeKeys &keys, const std::vector<RowValue> &input,
              std::vector<uint64_t> *order = nullptr)
  {
    ASSERT_EQ(&keys.functions, sorter.compare().sort_cmp_funs_);
    std::map<uint64_t, RowValue> remaining; for (const auto &r : input) ASSERT_TRUE(remaining.emplace(r.id, r).second);
    const StoredRow *row = nullptr; int ret = OB_SUCCESS; RowValue previous; bool have_previous = false;
    while (OB_SUCCESS == (ret = sorter.get_next_row(row))) {
      ASSERT_NE(nullptr, row); ASSERT_EQ(3u, row->cnt_); const RowValue value = snapshot(row);
      auto at = remaining.find(value.id); ASSERT_NE(remaining.end(), at);
      EXPECT_EQ(at->second.word, value.word); EXPECT_EQ(at->second.doc, value.doc);
      EXPECT_EQ(at->second.word_null, value.word_null); EXPECT_EQ(at->second.doc_null, value.doc_null); remaining.erase(at);
      if (have_previous) {
        ObDatum a[3], b[3]; uint64_t pa = 0, pb = 0; make_datums(previous, a, pa); make_datums(value, b, pb);
        int cmp = 0;
        for (int64_t k = 0; k < keys.collations.count() && cmp == 0; ++k) {
          const auto &field = keys.collations.at(k);
          ASSERT_EQ(OB_SUCCESS, keys.functions.at(k).cmp_func_(a[field.field_idx_], b[field.field_idx_], cmp, nullptr));
          if (!field.is_ascending_) cmp = -cmp;
        }
        EXPECT_LE(cmp, 0);
      }
      previous = value; have_previous = true; if (order) order->push_back(value.id);
    }
    EXPECT_EQ(OB_ITER_END, ret); EXPECT_TRUE(remaining.empty()); EXPECT_EQ(OB_ITER_END, sorter.get_next_row(row));
    EXPECT_EQ(&keys.functions, sorter.compare().sort_cmp_funs_);
  }
  void check_words(const std::vector<std::string> &words, bool include_null = false)
  {
    std::vector<RowValue> input;
    for (const auto &word : words) for (uint64_t id : {0ULL, 1ULL, 255ULL, 256ULL}) input.push_back({word, doc_id(id), false, false, input.size()});
    if (include_null) { input.push_back({"", doc_id(1), true, false, input.size()}); input.push_back({"", "", true, true, input.size()}); input.push_back({"", "", false, true, input.size()}); }
    std::reverse(input.begin(), input.end());
    for (bool word_asc : {false, true}) for (bool doc_asc : {false, true}) for (ObCmpNullPos np : {NULL_FIRST, NULL_LAST}) {
      RuntimeKeys keys(env_->arena); ASSERT_EQ(OB_SUCCESS, keys.init(false, word_asc, doc_asc, np)); ASSERT_TRUE(keys.eligible());
      NativeSort sorter; ASSERT_EQ(OB_SUCCESS, configure(sorter, keys));
      for (const auto &value : input) ASSERT_EQ(OB_SUCCESS, sorter.append(value));
      ASSERT_EQ(OB_SUCCESS, sorter.sort());
      std::vector<uint64_t> enabled_order, original_order;
      ASSERT_NO_FATAL_FAILURE(verify(sorter, keys, input, &enabled_order));
      NativeSort original; ASSERT_EQ(OB_SUCCESS, configure(original, keys));
      original.set_fts_word_equal(false);
      for (const auto &value : input) ASSERT_EQ(OB_SUCCESS, original.append(value));
      ASSERT_EQ(OB_SUCCESS, original.sort());
      ASSERT_NO_FATAL_FAILURE(verify(original, keys, input, &original_order));
      EXPECT_EQ(original_order, enabled_order);
    }
  }
};
TEST_F(FtsSortByteEqual, identical_strings) { check_words({"identical", "identical"}); }
TEST_F(FtsSortByteEqual, different_lengths) { check_words({"a", "aa", "aaa", "ab", "b"}); }
TEST_F(FtsSortByteEqual, case_equivalence) { check_words({"a", "A", "abc", "ABC"}); }
TEST_F(FtsSortByteEqual, accent_equivalence) { check_words({"e", "\xc3\xa9", "\xc3\x89", "f"}); }
TEST_F(FtsSortByteEqual, trailing_spaces) { check_words({"a", "a ", "a  ", " a", "a b"}); }
TEST_F(FtsSortByteEqual, empty_and_null) { check_words({"", " ", "a"}, true); }
TEST_F(FtsSortByteEqual, invalid_utf8) { check_words({std::string("\xff", 1), std::string("\xfe", 1), std::string("\xe4\xb8", 2), std::string("\xc0\xaf", 2), std::string("a\xff", 2)}); }
TEST_F(FtsSortByteEqual, embedded_nul) { check_words({std::string("\0", 1), std::string("a\0", 2), std::string("a\0b", 3), std::string("a\0c", 3), "a"}); }
TEST_F(FtsSortByteEqual, binary_docid) { check_words({"same", "same"}); }
TEST_F(FtsSortByteEqual, asc_desc_and_multiple_keys) { check_words({"e", "E", "\xc3\xa9", "f"}); }
TEST_F(FtsSortByteEqual, schema_and_logical_scope_rejections)
{
  auto &g = *gate_; ASSERT_TRUE(g.eligible());
  g.context.set_is_online_ddl(false); EXPECT_FALSE(g.eligible()); g.context.set_is_online_ddl(true);
  g.table.ddl_table_id_++; EXPECT_FALSE(g.eligible()); g.table.ddl_table_id_--;
  g.index_schema.set_index_type(INDEX_TYPE_FTS_DOC_WORD_LOCAL); EXPECT_FALSE(g.eligible()); g.index_schema.set_index_type(INDEX_TYPE_FTS_INDEX_LOCAL);
  g.index_schema.set_data_table_id(GateFixture::DATA_ID + 1); EXPECT_FALSE(g.eligible()); g.index_schema.set_data_table_id(GateFixture::DATA_ID);
  auto *column = g.data_schema.get_column_schema(GateFixture::WORD_ID); ASSERT_NE(nullptr, column);
  column->set_column_flags(VIRTUAL_GENERATED_COLUMN_FLAG); EXPECT_FALSE(g.eligible()); column->add_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG);
  column->set_data_type(ObLongTextType); EXPECT_FALSE(g.eligible()); column->set_data_type(ObVarcharType);
  column->set_collation_type(CS_TYPE_UTF8MB4_BIN); EXPECT_FALSE(g.eligible()); column->set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  g.scan.set_ref_table_id(GateFixture::DATA_ID + 1); EXPECT_FALSE(g.eligible()); g.scan.set_ref_table_id(GateFixture::DATA_ID);
  g.word_expression.set_expr_type(T_FUN_SYS_LOWER); EXPECT_FALSE(g.eligible()); g.word_expression.set_expr_type(T_FUN_SYS_WORD_SEGMENT);
  g.scan.get_output_exprs().at(0) = &g.doc_column; EXPECT_FALSE(g.eligible()); g.scan.get_output_exprs().at(0) = &g.word_expression;
  std::swap(g.scan.get_output_exprs().at(0), g.scan.get_output_exprs().at(1)); EXPECT_FALSE(g.eligible()); std::swap(g.scan.get_output_exprs().at(0), g.scan.get_output_exprs().at(1));
  g.sort.set_prefix_pos(1); EXPECT_FALSE(g.eligible()); g.sort.set_prefix_pos(0);
  g.sort.set_part_cnt(1); EXPECT_FALSE(g.eligible()); g.sort.set_part_cnt(0);
  g.sort.set_local_merge_sort(true); EXPECT_FALSE(g.eligible()); g.sort.set_local_merge_sort(false);
  g.sort.set_topn_expr(&g.doc_column); EXPECT_FALSE(g.eligible()); g.sort.set_topn_expr(nullptr);
  g.sort.set_topk_limit_expr(&g.doc_column); EXPECT_FALSE(g.eligible()); g.sort.set_topk_limit_expr(nullptr);
  ASSERT_TRUE(g.eligible());
  ASSERT_EQ(OB_SUCCESS, g.scan.get_output_exprs().push_back(&g.doc_column)); EXPECT_FALSE(g.eligible());
}
TEST_F(FtsSortByteEqual, subplan_origin_proof)
{
  auto &g = *gate_; g.subplan.set_subquery_id(20001); g.outer_column.set_ref_id(20001, OB_APP_MIN_COLUMN_ID); g.outer_column.set_expr_type(T_REF_COLUMN);
  ASSERT_EQ(OB_SUCCESS, g.subplan.get_access_exprs().push_back(&g.outer_column)); ASSERT_EQ(OB_SUCCESS, g.subplan.add_child(&g.scan));
  g.sort.set_child(0, &g.subplan); g.sort.get_sort_keys().at(0).expr_ = &g.outer_column; ASSERT_TRUE(g.eligible());
  g.outer_column.set_ref_id(20002, OB_APP_MIN_COLUMN_ID); EXPECT_FALSE(g.eligible()); g.outer_column.set_ref_id(20001, OB_APP_MIN_COLUMN_ID + 99); EXPECT_FALSE(g.eligible());
  g.outer_column.set_ref_id(20001, OB_APP_MIN_COLUMN_ID); g.subplan.get_access_exprs().at(0) = &g.doc_column; EXPECT_FALSE(g.eligible());
}
static int failing_compare_calls = 0, second_error_calls = 0;
struct CompareObservation {
  ObSortOpImpl::Compare *compare;
  const ObIArray<ObSortCmpFunc> *original;
  bool saw_production_table = false;
};
static thread_local CompareObservation *active_observation = nullptr;
struct ObserveCompareGuard {
  explicit ObserveCompareGuard(CompareObservation &observation) { active_observation = &observation; }
  ~ObserveCompareGuard() { active_observation = nullptr; }
};
static int fail_first(const ObDatum &, const ObDatum &, int &, const ObDatumAccessContext *) { ++failing_compare_calls; return OB_ERR_UNEXPECTED; }
static int fail_second(const ObDatum &, const ObDatum &, int &, const ObDatumAccessContext *)
{
  ++second_error_calls;
  if (active_observation != nullptr) {
    auto &o = *active_observation;
    o.saw_production_table |= o.compare->sort_cmp_funs_ != o.original
        && o.compare->sort_cmp_funs_->at(0).cmp_func_ != o.original->at(0).cmp_func_;
  }
  return OB_INVALID_ARGUMENT;
}
TEST_F(FtsSortByteEqual, runtime_scope_rejections)
{
  RuntimeKeys k(env_->arena); ASSERT_EQ(OB_SUCCESS, k.init()); ASSERT_TRUE(k.eligible());
  auto first = k.collations.at(0); const auto fn = k.functions.at(0).cmp_func_;
  k.collations.at(0).field_idx_ = UINT32_MAX; EXPECT_FALSE(k.eligible()); k.collations.at(0) = first;
  k.collations.at(0).cs_type_ = CS_TYPE_BINARY; EXPECT_FALSE(k.eligible()); k.collations.at(0) = first;
  k.word.datum_meta_.type_ = ObLongTextType; EXPECT_FALSE(k.eligible()); k.word.datum_meta_.type_ = ObVarcharType;
  k.word.datum_meta_.cs_type_ = CS_TYPE_UTF8MB4_BIN; EXPECT_FALSE(k.eligible()); k.word.datum_meta_.cs_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
  k.word.obj_meta_.set_type(ObLongTextType); k.word.obj_meta_.set_has_lob_header(); EXPECT_FALSE(k.eligible()); k.word.obj_meta_.set_varchar(); k.word.obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  k.functions.at(0).cmp_func_ = fail_first; EXPECT_FALSE(k.eligible()); k.functions.at(0).cmp_func_ = fn; ASSERT_TRUE(k.eligible());
  RuntimeKeys three(env_->arena); ASSERT_EQ(OB_SUCCESS, three.init(false, true, true, NULL_LAST, true)); EXPECT_FALSE(three.eligible());
  NativeSort sorter; ASSERT_EQ(OB_SUCCESS, configure(sorter, three));
  std::vector<RowValue> input = {{"same", doc_id(1), false, false, 2}, {"same", doc_id(1), false, false, 1}};
  for (const auto &r : input) ASSERT_EQ(OB_SUCCESS, sorter.append(r)); ASSERT_EQ(OB_SUCCESS, sorter.sort()); verify(sorter, three, input);
}
TEST_F(FtsSortByteEqual, sticky_error_and_guard_restoration)
{
  RuntimeKeys keys(env_->arena); ASSERT_EQ(OB_SUCCESS, keys.init());
  // The second-key failure is reached after the production equal-word shortcut.
  keys.functions.at(1).cmp_func_ = fail_second; ASSERT_TRUE(keys.eligible());
  NativeSort sorter; ASSERT_EQ(OB_SUCCESS, configure(sorter, keys));
  ASSERT_EQ(OB_SUCCESS, sorter.append({"same", doc_id(2), false, false, 1})); ASSERT_EQ(OB_SUCCESS, sorter.append({"same", doc_id(1), false, false, 2}));
  CompareObservation observation{&sorter.compare(), &keys.functions};
  ObserveCompareGuard observation_guard(observation);
  second_error_calls = 0; EXPECT_EQ(OB_INVALID_ARGUMENT, sorter.sort()); EXPECT_GT(second_error_calls, 0);
  EXPECT_TRUE(observation.saw_production_table);
  EXPECT_EQ(&keys.functions, sorter.compare().sort_cmp_funs_); const int calls = second_error_calls;
  EXPECT_EQ(OB_INVALID_ARGUMENT, sorter.sort()); EXPECT_EQ(calls, second_error_calls);
  sorter.reset(); keys.functions.at(0).cmp_func_ = fail_first; EXPECT_FALSE(keys.eligible());
  ASSERT_EQ(OB_SUCCESS, configure(sorter, keys)); ASSERT_EQ(OB_SUCCESS, sorter.append({"a", doc_id(1), false, false, 1})); ASSERT_EQ(OB_SUCCESS, sorter.append({"b", doc_id(2), false, false, 2}));
  failing_compare_calls = second_error_calls = 0; EXPECT_EQ(OB_ERR_UNEXPECTED, sorter.sort()); EXPECT_GT(failing_compare_calls, 0); EXPECT_EQ(0, second_error_calls);
  EXPECT_EQ(&keys.functions, sorter.compare().sort_cmp_funs_);
}
TEST_F(FtsSortByteEqual, status_error_precedes_equal_word_and_second_key)
{
  RuntimeKeys keys(env_->arena); ASSERT_EQ(OB_SUCCESS, keys.init());
  keys.functions.at(1).cmp_func_ = fail_second; ASSERT_TRUE(keys.eligible());
  NativeSort sorter; ASSERT_EQ(OB_SUCCESS, configure(sorter, keys));
  ASSERT_EQ(OB_SUCCESS, sorter.append({"same", doc_id(2), false, false, 1}));
  ASSERT_EQ(OB_SUCCESS, sorter.append({"same", doc_id(1), false, false, 2}));
  sorter.compare().cmp_count_ = 8191;
  env_->exec.get_physical_plan_ctx()->set_timeout_timestamp(1);
  second_error_calls = 0;
  EXPECT_EQ(OB_TIMEOUT, sorter.sort()); EXPECT_EQ(0, second_error_calls);
  EXPECT_EQ(&keys.functions, sorter.compare().sort_cmp_funs_);
  env_->exec.get_physical_plan_ctx()->set_timeout_timestamp(0);
}

TEST_F(FtsSortByteEqual, doc_first_remains_original)
{
  std::swap(gate_->sort.get_sort_keys().at(0), gate_->sort.get_sort_keys().at(1)); EXPECT_FALSE(gate_->eligible());
  std::swap(gate_->sort.get_sort_keys().at(0), gate_->sort.get_sort_keys().at(1));
  RuntimeKeys keys(env_->arena); ASSERT_EQ(OB_SUCCESS, keys.init(true)); EXPECT_FALSE(keys.eligible());
  NativeSort sorter; ASSERT_EQ(OB_SUCCESS, configure(sorter, keys, true, true));
  std::vector<RowValue> input = {{"z", doc_id(1), false, false, 1}, {"a", doc_id(2), false, false, 2}, {"A", doc_id(1), false, false, 3}};
  for (const auto &r : input) ASSERT_EQ(OB_SUCCESS, sorter.append(r)); ASSERT_EQ(OB_SUCCESS, sorter.sort()); verify(sorter, keys, input);
}
TEST_F(FtsSortByteEqual, in_memory_rewind_reuse_reset_and_eof_release)
{
  RuntimeKeys keys(env_->arena); ASSERT_EQ(OB_SUCCESS, keys.init()); NativeSort sorter; ASSERT_EQ(OB_SUCCESS, configure(sorter, keys));
  std::vector<RowValue> input = {{"a", doc_id(2), false, false, 0}, {"A", doc_id(1), false, false, 1}, {"b", doc_id(3), false, false, 2}};
  for (const auto &r : input) ASSERT_EQ(OB_SUCCESS, sorter.append(r)); ASSERT_EQ(OB_SUCCESS, sorter.sort()); std::vector<uint64_t> first, second;
  ASSERT_NO_FATAL_FAILURE(verify(sorter, keys, input, &first));
  ASSERT_EQ(OB_SUCCESS, sorter.rewind());
  ASSERT_NO_FATAL_FAILURE(verify(sorter, keys, input, &second));
  EXPECT_EQ(first, second);
  sorter.reuse(); EXPECT_EQ(0, sorter.row_count()); EXPECT_EQ(0, sorter.chunk_count()); EXPECT_EQ(0, sorter.bytes_used());
  for (auto it = input.rbegin(); it != input.rend(); ++it) ASSERT_EQ(OB_SUCCESS, sorter.append(*it)); ASSERT_EQ(OB_SUCCESS, sorter.sort()); ASSERT_NO_FATAL_FAILURE(verify(sorter, keys, input));
  sorter.reset(); ASSERT_EQ(OB_SUCCESS, configure(sorter, keys, false));
  for (const auto &r : input) ASSERT_EQ(OB_SUCCESS, sorter.append(r)); ASSERT_EQ(OB_SUCCESS, sorter.sort()); ASSERT_NO_FATAL_FAILURE(verify(sorter, keys, input));
  EXPECT_EQ(0, sorter.row_count()); EXPECT_EQ(0, sorter.chunk_count()); EXPECT_EQ(0, sorter.bytes_used()); sorter.reset(); EXPECT_FALSE(sorter.is_inited());
}
TEST_F(FtsSortByteEqual, populated_operator_rescan_reinitializes_same_spec)
{
  RuntimeKeys keys(env_->arena); ASSERT_EQ(OB_SUCCESS, keys.init());
  ObSortSpec spec(env_->arena, PHY_SORT);
  ASSERT_EQ(OB_SUCCESS, spec.all_exprs_.assign(keys.expressions));
  ASSERT_EQ(OB_SUCCESS, spec.sort_collations_.assign(keys.collations));
  ASSERT_EQ(OB_SUCCESS, spec.sort_cmp_funs_.assign(keys.functions));
  spec.fts_word_equal_ = gate_->eligible(); ASSERT_TRUE(spec.fts_word_equal_);
  spec.rows_ = 3; spec.width_ = 64; spec.max_batch_size_ = 16;
  ObSortOp op(env_->exec, spec, nullptr);
  const std::vector<RowValue> input = {{"same", doc_id(3), false, false, 0},
      {"same", doc_id(1), false, false, 1}, {"same", doc_id(2), false, false, 2}};
  for (int pass = 0; pass < 2; ++pass) {
    ASSERT_EQ(OB_SUCCESS, op.init_sort(input.size(), false));
    auto &sorter = op.sort_impl_;
    ASSERT_TRUE(sorter.is_inited()); ASSERT_TRUE(sorter.fts_word_equal_);
    for (const auto &value : input) {
      ObDatum datums[3]; uint64_t payload = 0; StoredRow *stored = nullptr;
      make_datums(value, datums, payload);
      ASSERT_EQ(OB_SUCCESS, sorter.datum_store_.add_row(datums, 3, 0, &stored));
      ASSERT_EQ(OB_SUCCESS, sorter.after_add_row(stored));
    }
    ASSERT_EQ(OB_SUCCESS, sorter.sort());
    ASSERT_EQ(&spec.sort_cmp_funs_, sorter.comp_.sort_cmp_funs_);
    ASSERT_EQ(3, sorter.datum_store_.get_row_cnt());
    const StoredRow *first = nullptr;
    ASSERT_EQ(OB_SUCCESS, sorter.get_next_row(first)); ASSERT_EQ(1u, snapshot(first).id);
    // Rescan a live, partially consumed production sort; no EOF cleanup has run.
    ASSERT_GT(sorter.datum_store_.get_mem_used(), 0);
    ASSERT_EQ(OB_SUCCESS, op.inner_rescan());
    EXPECT_FALSE(sorter.is_inited()); EXPECT_FALSE(sorter.fts_word_equal_);
    EXPECT_EQ(nullptr, sorter.comp_.sort_cmp_funs_);
    EXPECT_EQ(0, sorter.datum_store_.get_row_cnt()); EXPECT_EQ(0, sorter.datum_store_.get_mem_used());
    EXPECT_EQ(0, sorter.sort_chunks_.get_size()); EXPECT_TRUE(spec.fts_word_equal_);
  }
}
TEST_F(FtsSortByteEqual, multiple_real_in_memory_runs_ems)
{
  RuntimeKeys keys(env_->arena); ASSERT_EQ(OB_SUCCESS, keys.init()); NativeSort merge; ASSERT_EQ(OB_SUCCESS, configure(merge, keys));
  std::vector<RowValue> input = {{"a", doc_id(3), false, false, 0}, {"A", doc_id(1), false, false, 1}, {"\xc3\xa9", doc_id(8), false, false, 2}, {"e", doc_id(7), false, false, 3}, {"", "", true, true, 4}, {std::string("a\0b", 3), doc_id(255), false, false, 5}};
  for (size_t run = 0; run < 3; ++run) {
    NativeSort local; ASSERT_EQ(OB_SUCCESS, configure(local, keys)); for (size_t i = run; i < input.size(); i += 3) ASSERT_EQ(OB_SUCCESS, local.append(input[i])); ASSERT_EQ(OB_SUCCESS, local.sort());
    EXPECT_EQ(&keys.functions, local.compare().sort_cmp_funs_); std::vector<const StoredRow *> rows; const StoredRow *row = nullptr; int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = local.get_next_row(row))) rows.push_back(row); ASSERT_EQ(OB_ITER_END, ret); ASSERT_EQ(OB_SUCCESS, merge.append_run(rows));
  }
  ASSERT_FALSE(merge.use_compact_store()); ASSERT_EQ(3, merge.chunk_count()); ASSERT_EQ(OB_SUCCESS, merge.prepare_ems()); ASSERT_NO_FATAL_FAILURE(verify(merge, keys, input));
  ASSERT_EQ(OB_SUCCESS, merge.rewind()); ASSERT_NO_FATAL_FAILURE(verify(merge, keys, input)); merge.reuse(); EXPECT_EQ(0, merge.chunk_count()); EXPECT_EQ(0, merge.row_count());
}
#undef RETURN_IF_ERROR
} // namespace
} // namespace sql
} // namespace oceanbase
