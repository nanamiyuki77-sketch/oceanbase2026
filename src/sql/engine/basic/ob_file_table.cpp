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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_file_table.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "common/timezone/ob_time_convert.h"
#include "lib/allocator/page_arena.h"
#include "lib/charset/ob_charset.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"
#include <rapidjson/document.h>
#include <cerrno>
#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <glob.h>
#include <limits>
#include <string>
#include <sys/stat.h>
#include <vector>

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{

constexpr uint64_t MAX_PARQUET_FOOTER_SIZE = 64ULL * 1024 * 1024;
// shortcut: projected Parquet chunks are buffered per row group, add page streaming for larger row groups.
constexpr uint64_t MAX_PARQUET_BUFFER_SIZE = 512ULL * 1024 * 1024;

int64_t stat_mtime_ns(const struct stat &st)
{
#ifdef _WIN32
  return static_cast<int64_t>(st.st_mtime) * 1000000000L;
#else
  return static_cast<int64_t>(st.st_mtim.tv_sec) * 1000000000L + st.st_mtim.tv_nsec;
#endif
}

int get_file_stat(const ObString &path, int64_t &size, int64_t &mtime_ns)
{
  int ret = OB_SUCCESS;
  ObCStringHelper helper;
  struct stat st;
  if (0 != ::stat(helper.convert(path), &st)) {
    ret = OB_FILE_NOT_EXIST;
  } else if (!S_ISREG(st.st_mode)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    size = st.st_size;
    mtime_ns = stat_mtime_ns(st);
  }
  return ret;
}

bool equals_ci(const std::string &value, const char *literal)
{
  const size_t length = STRLEN(literal);
  bool equal = value.size() == length;
  for (size_t i = 0; equal && i < length; ++i) {
    equal = static_cast<unsigned char>(std::tolower(static_cast<unsigned char>(value[i]))) ==
            static_cast<unsigned char>(std::tolower(static_cast<unsigned char>(literal[i])));
  }
  return equal;
}

bool parse_int64(const std::string &value, int64_t &result)
{
  if (value.empty()) { return false; }
  errno = 0;
  char *end = nullptr;
  const long long parsed = std::strtoll(value.c_str(), &end, 10);
  const bool valid = errno != ERANGE && end == value.c_str() + value.size();
  if (valid) { result = static_cast<int64_t>(parsed); }
  return valid;
}

bool parse_double(const std::string &value, double &result)
{
  if (value.empty()) { return false; }
  errno = 0;
  char *end = nullptr;
  const double parsed = std::strtod(value.c_str(), &end);
  const bool valid = errno != ERANGE && end == value.c_str() + value.size() && std::isfinite(parsed);
  if (valid) { result = parsed; }
  return valid;
}

bool parse_date(const std::string &value, int32_t &result)
{
  return value.size() == 10 && value[4] == '-' && value[7] == '-' &&
         OB_SUCCESS == ObTimeConverter::str_to_date(ObString(value.size(), value.data()), result, 0);
}

bool parse_datetime(const std::string &value, int64_t &result)
{
  if (value.size() < 19 || (value[10] != ' ' && value[10] != 'T')) { return false; }
  std::string normalized(value);
  normalized[10] = ' ';
  ObTimeConvertCtx context(nullptr, false);
  return OB_SUCCESS == ObTimeConverter::str_to_datetime(
      ObString(normalized.size(), normalized.data()), context, result, nullptr, 0);
}

ObObjType infer_scalar_type(const std::string &value)
{
  int64_t integer = 0;
  double floating = 0;
  int32_t date = 0;
  int64_t datetime = 0;
  if (value.empty()) { return ObNullType; }
  if (equals_ci(value, "true") || equals_ci(value, "false")) { return ObTinyIntType; }
  if (parse_int64(value, integer)) { return ObIntType; }
  if (parse_double(value, floating)) { return ObDoubleType; }
  if (parse_datetime(value, datetime)) { return ObDateTimeType; }
  if (parse_date(value, date)) { return ObDateType; }
  return ObVarcharType;
}

ObObjType widen_type(ObObjType current, ObObjType incoming)
{
  if (incoming == ObNullType) { return current; }
  if (current == ObNullType || current == incoming) { return incoming; }
  if ((current == ObIntType && incoming == ObDoubleType) ||
      (current == ObDoubleType && incoming == ObIntType)) {
    return ObDoubleType;
  }
  if ((current == ObDateType && incoming == ObDateTimeType) ||
      (current == ObDateTimeType && incoming == ObDateType)) {
    return ObDateTimeType;
  }
  return ObVarcharType;
}

int copy_string(ObIAllocator &allocator, const std::string &source, ObString &target)
{
  return ob_write_string(allocator, ObString(source.size(), source.data()), target);
}

class CsvReader
{
public:
  CsvReader(const ObString &path, const FileTableOptions &options)
    : path_(path.ptr(), path.length()), options_(options), input_(), row_number_(0)
  {}

  int open()
  {
    input_.close();
    input_.clear();
    input_.open(path_, std::ios::in | std::ios::binary);
    row_number_ = 0;
    return input_.is_open() ? OB_SUCCESS : OB_FILE_NOT_EXIST;
  }

  int next(std::vector<std::string> &fields)
  {
    int ret = OB_SUCCESS;
    fields.clear();
    std::string field;
    bool quoted = false;
    bool at_field_start = true;
    bool saw_any = false;
    while (OB_SUCC(ret)) {
      const int raw = input_.get();
      if (raw == EOF) {
        if (quoted) {
          ret = OB_ERR_PARSE_SQL;
        } else if (!saw_any && field.empty() && fields.empty()) {
          ret = OB_ITER_END;
        } else {
          fields.push_back(field);
          ++row_number_;
        }
        break;
      }
      saw_any = true;
      const char ch = static_cast<char>(raw);
      if (quoted) {
        if (ch == options_.escape_ && options_.escape_ != options_.quote_) {
          const int escaped = input_.get();
          if (escaped == EOF) { ret = OB_ERR_PARSE_SQL; }
          else { field.push_back(static_cast<char>(escaped)); }
        } else if (ch == options_.quote_) {
          if (input_.peek() == options_.quote_) {
            input_.get();
            field.push_back(options_.quote_);
          } else {
            quoted = false;
          }
        } else {
          field.push_back(ch);
        }
      } else if (at_field_start && ch == options_.quote_) {
        quoted = true;
        at_field_start = false;
      } else if (ch == options_.delimiter_) {
        fields.push_back(field);
        field.clear();
        at_field_start = true;
      } else if (ch == '\n' || ch == '\r') {
        if (ch == '\r' && input_.peek() == '\n') { input_.get(); }
        fields.push_back(field);
        ++row_number_;
        break;
      } else {
        if (ch == options_.quote_) {
          ret = OB_ERR_PARSE_SQL;
        } else {
          field.push_back(ch);
          at_field_start = false;
        }
      }
    }
    return ret;
  }

private:
  std::string path_;
  FileTableOptions options_;
  std::ifstream input_;
  int64_t row_number_;
};

int normalize_headers(const std::vector<std::string> &source,
                      ObIAllocator &allocator,
                      ObIArray<FileTableColumn> &columns)
{
  int ret = OB_SUCCESS;
  std::vector<std::string> names;
  for (size_t i = 0; OB_SUCC(ret) && i < source.size(); ++i) {
    std::string base = source[i].empty() ? "column" + std::to_string(i + 1) : source[i];
    std::string candidate = base;
    int suffix = 2;
    bool duplicate = true;
    while (duplicate) {
      duplicate = false;
      for (const std::string &existing : names) {
        if (ObCharset::case_insensitive_equal(
            ObString(existing.size(), existing.data()), ObString(candidate.size(), candidate.data()))) {
          duplicate = true;
          candidate = base + "__" + std::to_string(suffix++);
          break;
        }
      }
    }
    names.push_back(candidate);
    FileTableColumn column;
    if (OB_FAIL(copy_string(allocator, candidate, column.name_))) {
    } else if (OB_FAIL(columns.push_back(column))) {
    }
  }
  return ret;
}

int infer_csv(const ObString &path, const FileTableOptions &options,
              ObIAllocator &allocator, ObIArray<FileTableColumn> &columns)
{
  int ret = OB_SUCCESS;
  CsvReader reader(path, options);
  std::vector<std::string> row;
  if (OB_FAIL(reader.open())) {
  } else if (OB_FAIL(reader.next(row))) {
  } else if (options.header_) {
    ret = normalize_headers(row, allocator, columns);
  } else {
    std::vector<std::string> generated;
    for (size_t i = 0; i < row.size(); ++i) { generated.push_back("column" + std::to_string(i + 1)); }
    ret = normalize_headers(generated, allocator, columns);
  }
  bool first_data = !options.header_;
  while (OB_SUCC(ret)) {
    if (!first_data) {
      ret = reader.next(row);
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; break; }
      if (OB_FAIL(ret)) { break; }
    }
    first_data = false;
    if (row.size() != static_cast<size_t>(columns.count())) {
      ret = OB_ERR_PARSE_SQL;
      break;
    }
    for (int64_t i = 0; i < columns.count(); ++i) {
      const bool is_null = options.null_string_.empty()
          ? row[i].empty()
          : options.null_string_ == ObString(row[i].size(), row[i].data());
      columns.at(i).nullable_ = columns.at(i).nullable_ || is_null;
      columns.at(i).type_ = widen_type(columns.at(i).type_, is_null ? ObNullType : infer_scalar_type(row[i]));
    }
  }
  return ret;
}

int json_value_type(const rapidjson::Value &value, ObObjType &type)
{
  int ret = OB_SUCCESS;
  if (value.IsNull()) { type = ObNullType; }
  else if (value.IsBool()) { type = ObTinyIntType; }
  else if (value.IsInt64()) { type = ObIntType; }
  else if (value.IsUint64()) {
    type = value.GetUint64() <= static_cast<uint64_t>(INT64_MAX) ? ObIntType : ObDoubleType;
  }
  else if (value.IsNumber()) { type = ObDoubleType; }
  else if (value.IsString()) { type = infer_scalar_type(std::string(value.GetString(), value.GetStringLength())); }
  else { ret = OB_NOT_SUPPORTED; }
  return ret;
}

int infer_jsonl(const ObString &path, ObIAllocator &allocator, ObIArray<FileTableColumn> &columns)
{
  int ret = OB_SUCCESS;
  std::ifstream input(std::string(path.ptr(), path.length()), std::ios::in | std::ios::binary);
  std::string line;
  int64_t rows = 0;
  if (!input.is_open()) { ret = OB_FILE_NOT_EXIST; }
  while (OB_SUCC(ret) && std::getline(input, line)) {
    if (!line.empty() && line.back() == '\r') { line.pop_back(); }
    if (line.empty()) { continue; }
    rapidjson::Document document;
    document.Parse(line.data(), line.size());
    if (document.HasParseError() || !document.IsObject()) {
      ret = OB_ERR_PARSE_SQL;
      break;
    }
    for (auto member = document.MemberBegin(); OB_SUCC(ret) && member != document.MemberEnd(); ++member) {
      const ObString name(member->name.GetStringLength(), member->name.GetString());
      int64_t index = -1;
      for (int64_t i = 0; i < columns.count(); ++i) {
        if (ObCharset::case_insensitive_equal(columns.at(i).name_, name)) { index = i; break; }
      }
      if (index < 0) {
        FileTableColumn column;
        column.nullable_ = rows > 0;
        if (OB_FAIL(ob_write_string(allocator, name, column.name_))) {
        } else if (OB_FAIL(columns.push_back(column))) {
        } else { index = columns.count() - 1; }
      }
      ObObjType incoming = ObNullType;
      if (OB_SUCC(ret) && OB_FAIL(json_value_type(member->value, incoming))) {
      } else if (OB_SUCC(ret)) {
        columns.at(index).nullable_ = columns.at(index).nullable_ || incoming == ObNullType;
        columns.at(index).type_ = widen_type(columns.at(index).type_, incoming);
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      bool found = false;
      for (auto member = document.MemberBegin(); !found && member != document.MemberEnd(); ++member) {
        found = ObCharset::case_insensitive_equal(columns.at(i).name_,
            ObString(member->name.GetStringLength(), member->name.GetString()));
      }
      if (!found) { columns.at(i).nullable_ = true; }
    }
    ++rows;
  }
  if (OB_SUCC(ret) && rows == 0) { ret = OB_ERR_PARSE_SQL; }
  return ret;
}

enum CompactType : uint8_t
{
  CT_STOP = 0, CT_TRUE = 1, CT_FALSE = 2, CT_BYTE = 3, CT_I16 = 4, CT_I32 = 5,
  CT_I64 = 6, CT_DOUBLE = 7, CT_BINARY = 8, CT_LIST = 9, CT_SET = 10,
  CT_MAP = 11, CT_STRUCT = 12
};

class CompactReader
{
public:
  CompactReader(const uint8_t *data, size_t size) : begin_(data), position_(data), end_(data + size) {}
  size_t consumed() const { return static_cast<size_t>(position_ - begin_); }
  size_t remaining() const { return static_cast<size_t>(end_ - position_); }
  bool byte(uint8_t &value) { return position_ < end_ ? (value = *position_++, true) : false; }
  bool varint(uint64_t &value)
  {
    value = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      uint8_t next = 0;
      if (!byte(next)) { return false; }
      value |= static_cast<uint64_t>(next & 0x7f) << shift;
      if ((next & 0x80) == 0) { return true; }
    }
    return false;
  }
  bool sint(int64_t &value)
  {
    uint64_t encoded = 0;
    if (!varint(encoded)) { return false; }
    value = static_cast<int64_t>((encoded >> 1) ^ (~(encoded & 1) + 1));
    return true;
  }
  bool binary(std::string &value)
  {
    uint64_t length = 0;
    if (!varint(length) || length > remaining()) { return false; }
    value.assign(reinterpret_cast<const char *>(position_), static_cast<size_t>(length));
    position_ += length;
    return true;
  }
  bool field(int16_t &last_id, int16_t &id, uint8_t &type)
  {
    uint8_t header = 0;
    if (!byte(header)) { return false; }
    type = header & 0x0f;
    if (type == CT_STOP) { id = 0; return true; }
    const int16_t delta = header >> 4;
    if (delta != 0) { id = last_id + delta; }
    else {
      int64_t decoded = 0;
      if (!sint(decoded) || decoded < INT16_MIN || decoded > INT16_MAX) { return false; }
      id = static_cast<int16_t>(decoded);
    }
    last_id = id;
    return true;
  }
  bool list(uint8_t &type, uint64_t &size)
  {
    uint8_t header = 0;
    if (!byte(header)) { return false; }
    type = header & 0x0f;
    size = header >> 4;
    return size != 15 || varint(size);
  }
  bool skip(uint8_t type)
  {
    bool ok = true;
    if (type == CT_TRUE || type == CT_FALSE) {
    } else if (type == CT_BYTE) {
      uint8_t ignored = 0; ok = byte(ignored);
    } else if (type == CT_I16 || type == CT_I32 || type == CT_I64) {
      uint64_t ignored = 0; ok = varint(ignored);
    } else if (type == CT_DOUBLE) {
      if (remaining() < sizeof(double)) { ok = false; } else { position_ += sizeof(double); }
    } else if (type == CT_BINARY) {
      std::string ignored; ok = binary(ignored);
    } else if (type == CT_LIST || type == CT_SET) {
      uint8_t element_type = 0; uint64_t count = 0;
      ok = list(element_type, count);
      for (uint64_t i = 0; ok && i < count; ++i) { ok = skip(element_type); }
    } else if (type == CT_MAP) {
      uint64_t count = 0; ok = varint(count);
      uint8_t types = 0;
      if (ok && count > 0) { ok = byte(types); }
      for (uint64_t i = 0; ok && i < count; ++i) {
        ok = skip(types >> 4) && skip(types & 0x0f);
      }
    } else if (type == CT_STRUCT) {
      int16_t last = 0;
      while (ok) {
        int16_t id = 0; uint8_t field_type = 0;
        ok = field(last, id, field_type);
        if (!ok || field_type == CT_STOP) { break; }
        ok = skip(field_type);
      }
    } else { ok = false; }
    return ok;
  }
private:
  const uint8_t *begin_;
  const uint8_t *position_;
  const uint8_t *end_;
};

struct ParquetSchemaElement
{
  int type_ = -1;
  int type_length_ = 0;
  int repetition_ = 0;
  int converted_type_ = -1;
  int num_children_ = 0;
  std::string name_;
};

struct ParquetColumnMeta
{
  int type_ = -1;
  int codec_ = 0;
  int64_t num_values_ = 0;
  int64_t total_compressed_size_ = 0;
  int64_t data_page_offset_ = -1;
  int64_t dictionary_page_offset_ = -1;
  std::vector<std::string> path_;
};

struct ParquetRowGroupMeta
{
  int64_t num_rows_ = 0;
  std::vector<ParquetColumnMeta> columns_;
};

struct ParquetMeta
{
  int64_t num_rows_ = 0;
  std::vector<ParquetSchemaElement> schema_;
  std::vector<ParquetRowGroupMeta> row_groups_;
};

bool read_compact_int(CompactReader &reader, int &value)
{
  int64_t decoded = 0;
  if (!reader.sint(decoded) || decoded < INT_MIN || decoded > INT_MAX) { return false; }
  value = static_cast<int>(decoded);
  return true;
}

bool read_compact_i64(CompactReader &reader, int64_t &value) { return reader.sint(value); }

bool parse_schema_element(CompactReader &reader, ParquetSchemaElement &schema)
{
  bool ok = true;
  int16_t last = 0;
  while (ok) {
    int16_t id = 0; uint8_t type = 0;
    ok = reader.field(last, id, type);
    if (!ok || type == CT_STOP) { break; }
    if (id == 1 && type == CT_I32) { ok = read_compact_int(reader, schema.type_); }
    else if (id == 2 && type == CT_I32) { ok = read_compact_int(reader, schema.type_length_); }
    else if (id == 3 && type == CT_I32) { ok = read_compact_int(reader, schema.repetition_); }
    else if (id == 4 && type == CT_BINARY) { ok = reader.binary(schema.name_); }
    else if (id == 5 && type == CT_I32) { ok = read_compact_int(reader, schema.num_children_); }
    else if (id == 6 && type == CT_I32) { ok = read_compact_int(reader, schema.converted_type_); }
    else { ok = reader.skip(type); }
  }
  return ok;
}

bool parse_column_metadata(CompactReader &reader, ParquetColumnMeta &column)
{
  bool ok = true;
  int16_t last = 0;
  while (ok) {
    int16_t id = 0; uint8_t type = 0;
    ok = reader.field(last, id, type);
    if (!ok || type == CT_STOP) { break; }
    if (id == 1 && type == CT_I32) { ok = read_compact_int(reader, column.type_); }
    else if (id == 3 && type == CT_LIST) {
      uint8_t item_type = 0; uint64_t count = 0;
      ok = reader.list(item_type, count) && item_type == CT_BINARY;
      for (uint64_t i = 0; ok && i < count; ++i) {
        std::string path; ok = reader.binary(path); if (ok) { column.path_.push_back(path); }
      }
    } else if (id == 4 && type == CT_I32) { ok = read_compact_int(reader, column.codec_); }
    else if (id == 5 && type == CT_I64) { ok = read_compact_i64(reader, column.num_values_); }
    else if (id == 7 && type == CT_I64) { ok = read_compact_i64(reader, column.total_compressed_size_); }
    else if (id == 9 && type == CT_I64) { ok = read_compact_i64(reader, column.data_page_offset_); }
    else if (id == 11 && type == CT_I64) { ok = read_compact_i64(reader, column.dictionary_page_offset_); }
    else { ok = reader.skip(type); }
  }
  return ok;
}

bool parse_column_chunk(CompactReader &reader, ParquetColumnMeta &column)
{
  bool ok = true;
  int16_t last = 0;
  while (ok) {
    int16_t id = 0; uint8_t type = 0;
    ok = reader.field(last, id, type);
    if (!ok || type == CT_STOP) { break; }
    if (id == 3 && type == CT_STRUCT) { ok = parse_column_metadata(reader, column); }
    else { ok = reader.skip(type); }
  }
  return ok;
}

bool parse_row_group(CompactReader &reader, ParquetRowGroupMeta &row_group)
{
  bool ok = true;
  int16_t last = 0;
  while (ok) {
    int16_t id = 0; uint8_t type = 0;
    ok = reader.field(last, id, type);
    if (!ok || type == CT_STOP) { break; }
    if (id == 1 && type == CT_LIST) {
      uint8_t item_type = 0; uint64_t count = 0;
      ok = reader.list(item_type, count) && item_type == CT_STRUCT;
      for (uint64_t i = 0; ok && i < count; ++i) {
        ParquetColumnMeta column; ok = parse_column_chunk(reader, column);
        if (ok) { row_group.columns_.push_back(column); }
      }
    } else if (id == 3 && type == CT_I64) { ok = read_compact_i64(reader, row_group.num_rows_); }
    else { ok = reader.skip(type); }
  }
  return ok;
}

bool parse_parquet_footer(const uint8_t *footer, size_t footer_size, ParquetMeta &metadata)
{
  if (footer == nullptr || footer_size == 0) { return false; }
  CompactReader reader(footer, footer_size);
  bool ok = true;
  int16_t last = 0;
  while (ok) {
    int16_t id = 0; uint8_t type = 0;
    ok = reader.field(last, id, type);
    if (!ok || type == CT_STOP) { break; }
    if (id == 2 && type == CT_LIST) {
      uint8_t item_type = 0; uint64_t count = 0;
      ok = reader.list(item_type, count) && item_type == CT_STRUCT;
      for (uint64_t i = 0; ok && i < count; ++i) {
        ParquetSchemaElement schema; ok = parse_schema_element(reader, schema);
        if (ok) { metadata.schema_.push_back(schema); }
      }
    } else if (id == 3 && type == CT_I64) { ok = read_compact_i64(reader, metadata.num_rows_); }
    else if (id == 4 && type == CT_LIST) {
      uint8_t item_type = 0; uint64_t count = 0;
      ok = reader.list(item_type, count) && item_type == CT_STRUCT;
      for (uint64_t i = 0; ok && i < count; ++i) {
        ParquetRowGroupMeta group; ok = parse_row_group(reader, group);
        if (ok) { metadata.row_groups_.push_back(group); }
      }
    } else { ok = reader.skip(type); }
  }
  return ok && !metadata.schema_.empty() && metadata.schema_[0].num_children_ >= 0;
}

int read_parquet_metadata(const ObString &path, ParquetMeta &metadata)
{
  int ret = OB_SUCCESS;
  std::ifstream input(std::string(path.ptr(), path.length()), std::ios::in | std::ios::binary);
  if (!input.is_open()) { ret = OB_FILE_NOT_EXIST; }
  else {
    input.seekg(0, std::ios::end);
    const std::streamoff size = input.tellg();
    if (size < 12) {
      ret = OB_ERR_PARSE_SQL;
    } else {
      uint8_t header[4] = {};
      uint8_t trailer[8] = {};
      input.seekg(0, std::ios::beg);
      input.read(reinterpret_cast<char *>(header), sizeof(header));
      input.seekg(size - static_cast<std::streamoff>(sizeof(trailer)), std::ios::beg);
      input.read(reinterpret_cast<char *>(trailer), sizeof(trailer));
      const uint32_t footer_size = static_cast<uint32_t>(trailer[0])
          | (static_cast<uint32_t>(trailer[1]) << 8)
          | (static_cast<uint32_t>(trailer[2]) << 16)
          | (static_cast<uint32_t>(trailer[3]) << 24);
      if (!input || 0 != MEMCMP(header, "PAR1", 4) || 0 != MEMCMP(trailer + 4, "PAR1", 4)
          || footer_size == 0 || footer_size > MAX_PARQUET_FOOTER_SIZE
          || footer_size > static_cast<uint64_t>(size - 12)) {
        ret = OB_ERR_PARSE_SQL;
      } else {
        std::vector<uint8_t> footer(footer_size);
        input.seekg(size - 8 - static_cast<std::streamoff>(footer_size), std::ios::beg);
        input.read(reinterpret_cast<char *>(footer.data()), footer.size());
        if (!input || !parse_parquet_footer(footer.data(), footer.size(), metadata)) {
          ret = OB_ERR_PARSE_SQL;
        }
      }
    }
  }
  return ret;
}

ObObjType parquet_obj_type(const ParquetSchemaElement &schema)
{
  ObObjType type = ObVarcharType;
  if (schema.converted_type_ == 6) { type = ObDateType; }
  else if (schema.converted_type_ == 9 || schema.converted_type_ == 10) { type = ObDateTimeType; }
  else if (schema.type_ == 0) { type = ObTinyIntType; }
  else if (schema.type_ == 1 || schema.type_ == 2) { type = ObIntType; }
  else if (schema.type_ == 4 || schema.type_ == 5) { type = ObDoubleType; }
  return type;
}

int infer_parquet(const ObString &path, ObIAllocator &allocator, ObIArray<FileTableColumn> &columns)
{
  int ret = OB_SUCCESS;
  ParquetMeta metadata;
  if (OB_FAIL(read_parquet_metadata(path, metadata))) {
  } else if (metadata.schema_[0].num_children_ + 1 != static_cast<int>(metadata.schema_.size())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "nested Parquet schemas");
  }
  for (size_t i = 1; OB_SUCC(ret) && i < metadata.schema_.size(); ++i) {
    const ParquetSchemaElement &schema = metadata.schema_[i];
    if (schema.num_children_ != 0 || schema.repetition_ == 2 || schema.type_ < 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "nested or repeated Parquet columns");
    } else {
      FileTableColumn column;
      column.type_ = parquet_obj_type(schema);
      column.nullable_ = schema.repetition_ != 0;
      if (OB_FAIL(copy_string(allocator, schema.name_, column.name_))) {
      } else if (OB_FAIL(columns.push_back(column))) {
      }
    }
  }
  return ret;
}

int set_string_obj(const std::string &value, ObIAllocator &allocator, ObObj &object)
{
  int ret = OB_SUCCESS;
  ObString copied;
  if (OB_FAIL(copy_string(allocator, value, copied))) {
  } else {
    object.set_varchar(copied);
    object.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    object.set_collation_level(CS_LEVEL_IMPLICIT);
  }
  return ret;
}

int convert_string(const std::string &value, ObObjType type, ObIAllocator &allocator, ObObj &object)
{
  int ret = OB_SUCCESS;
  if (type == ObIntType) {
    int64_t result = 0;
    if (!parse_int64(value, result)) { ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD; }
    else { object.set_int(result); }
  } else if (type == ObDoubleType) {
    double result = 0;
    if (!parse_double(value, result)) { ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD; }
    else { object.set_double(result); }
  } else if (type == ObTinyIntType) {
    if (equals_ci(value, "true")) { object.set_tinyint(1); }
    else if (equals_ci(value, "false")) { object.set_tinyint(0); }
    else { ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD; }
  } else if (type == ObDateType) {
    int32_t result = 0;
    if (!parse_date(value, result)) { ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD; }
    else { object.set_date(result); }
  } else if (type == ObDateTimeType) {
    int64_t result = 0;
    if (!parse_datetime(value, result)) { ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD; }
    else { object.set_datetime(result); }
  } else {
    ret = set_string_obj(value, allocator, object);
  }
  return ret;
}

class BaseFileScanner : public IFileTableScanner
{
public:
  BaseFileScanner(const ObString &path, const FileTableOptions &options,
                  const ObIArray<ObString> &column_names, const ObIArray<int64_t> &column_types,
                  const ObIArray<uint64_t> &projection, int64_t size, int64_t mtime)
    : path_(path.ptr(), path.length()), options_(options), column_names_(column_names),
      column_types_(column_types), projection_(projection), expected_size_(size),
      expected_mtime_ns_(mtime), rows_read_(0), row_allocator_("FileTableRow"), values_(), cells_(), row_()
  {}

protected:
  int check_unchanged()
  {
    int64_t size = 0;
    int64_t mtime = 0;
    const ObString path(path_.size(), path_.data());
    char canonical[PATH_MAX] = {};
    int ret = nullptr == ::realpath(path_.c_str(), canonical) ? OB_FILE_NOT_EXIST : OB_SUCCESS;
    if (OB_SUCC(ret) && path_ != canonical) {
      ret = OB_ERR_NO_PRIVILEGE;
    } else if (OB_SUCC(ret) && OB_FAIL(ObResolverUtils::check_secure_path(
               options_.secure_root_, ObString(canonical)))) {
    } else if (OB_SUCC(ret) && OB_FAIL(get_file_stat(path, size, mtime))) {
    } else if (OB_SUCC(ret) && (size != expected_size_ || mtime != expected_mtime_ns_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "file changed during file table scan");
    }
    return ret;
  }

  int project_row(const std::vector<std::string> &fields, const std::vector<bool> *present,
                  ObNewRow *&row)
  {
    int ret = OB_SUCCESS;
    row_allocator_.reuse();
    values_.reuse();
    cells_.reuse();
    if (fields.size() != static_cast<size_t>(column_types_.count())) { ret = OB_ERR_PARSE_SQL; }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_types_.count(); ++i) {
      ObObj value;
      const bool exists = present == nullptr || present->at(i);
      const bool is_null = !exists || (options_.null_string_.empty()
          ? fields[i].empty()
          : options_.null_string_ == ObString(fields[i].size(), fields[i].data()));
      if (is_null) { value.set_null(); }
      else if (OB_FAIL(convert_string(fields[i], static_cast<ObObjType>(column_types_.at(i)), row_allocator_, value))) {
      }
      if (OB_SUCC(ret) && OB_FAIL(values_.push_back(value))) {}
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < projection_.count(); ++i) {
      const int64_t index = projection_.at(i) - OB_APP_MIN_COLUMN_ID;
      if (index < 0 || index >= values_.count()) { ret = OB_ERR_UNEXPECTED; }
      else if (OB_FAIL(cells_.push_back(values_.at(index)))) {}
    }
    if (OB_SUCC(ret)) {
      row_.cells_ = cells_.get_data();
      row_.count_ = cells_.count();
      row = &row_;
      if ((++rows_read_ & 1023) == 0) { ret = check_unchanged(); }
    }
    return ret;
  }

  std::string path_;
  FileTableOptions options_;
  const ObIArray<ObString> &column_names_;
  const ObIArray<int64_t> &column_types_;
  const ObIArray<uint64_t> &projection_;
  int64_t expected_size_;
  int64_t expected_mtime_ns_;
  int64_t rows_read_;
  ObArenaAllocator row_allocator_;
  ObSEArray<ObObj, 16> values_;
  ObSEArray<ObObj, 16> cells_;
  ObNewRow row_;
};

struct ParquetPageHeader
{
  int type_ = -1;
  int uncompressed_size_ = 0;
  int compressed_size_ = 0;
  int num_values_ = 0;
  int encoding_ = 0;
  int definition_encoding_ = 3;
  int dictionary_values_ = 0;
  int num_nulls_ = 0;
  int num_rows_ = 0;
  int definition_bytes_ = 0;
  int repetition_bytes_ = 0;
  bool data_v2_compressed_ = true;
};

bool parse_data_page_header(CompactReader &reader, ParquetPageHeader &page)
{
  bool ok = true;
  int16_t last = 0;
  while (ok) {
    int16_t id = 0; uint8_t type = 0;
    ok = reader.field(last, id, type);
    if (!ok || type == CT_STOP) { break; }
    if (id == 1 && type == CT_I32) { ok = read_compact_int(reader, page.num_values_); }
    else if (id == 2 && type == CT_I32) { ok = read_compact_int(reader, page.encoding_); }
    else if (id == 3 && type == CT_I32) { ok = read_compact_int(reader, page.definition_encoding_); }
    else { ok = reader.skip(type); }
  }
  return ok;
}

bool parse_dictionary_page_header(CompactReader &reader, ParquetPageHeader &page)
{
  bool ok = true;
  int16_t last = 0;
  while (ok) {
    int16_t id = 0; uint8_t type = 0;
    ok = reader.field(last, id, type);
    if (!ok || type == CT_STOP) { break; }
    if (id == 1 && type == CT_I32) { ok = read_compact_int(reader, page.dictionary_values_); }
    else if (id == 2 && type == CT_I32) { ok = read_compact_int(reader, page.encoding_); }
    else { ok = reader.skip(type); }
  }
  return ok;
}

bool parse_data_page_header_v2(CompactReader &reader, ParquetPageHeader &page)
{
  bool ok = true;
  int16_t last = 0;
  while (ok) {
    int16_t id = 0; uint8_t type = 0;
    ok = reader.field(last, id, type);
    if (!ok || type == CT_STOP) { break; }
    if (id == 1 && type == CT_I32) { ok = read_compact_int(reader, page.num_values_); }
    else if (id == 2 && type == CT_I32) { ok = read_compact_int(reader, page.num_nulls_); }
    else if (id == 3 && type == CT_I32) { ok = read_compact_int(reader, page.num_rows_); }
    else if (id == 4 && type == CT_I32) { ok = read_compact_int(reader, page.encoding_); }
    else if (id == 5 && type == CT_I32) { ok = read_compact_int(reader, page.definition_bytes_); }
    else if (id == 6 && type == CT_I32) { ok = read_compact_int(reader, page.repetition_bytes_); }
    else if (id == 7 && (type == CT_TRUE || type == CT_FALSE)) { page.data_v2_compressed_ = type == CT_TRUE; }
    else { ok = reader.skip(type); }
  }
  return ok;
}

bool parse_page_header(const uint8_t *data, size_t size, ParquetPageHeader &page, size_t &header_size)
{
  CompactReader reader(data, size);
  bool ok = true;
  int16_t last = 0;
  while (ok) {
    int16_t id = 0; uint8_t type = 0;
    ok = reader.field(last, id, type);
    if (!ok || type == CT_STOP) { break; }
    if (id == 1 && type == CT_I32) { ok = read_compact_int(reader, page.type_); }
    else if (id == 2 && type == CT_I32) { ok = read_compact_int(reader, page.uncompressed_size_); }
    else if (id == 3 && type == CT_I32) { ok = read_compact_int(reader, page.compressed_size_); }
    else if (id == 5 && type == CT_STRUCT) { ok = parse_data_page_header(reader, page); }
    else if (id == 7 && type == CT_STRUCT) { ok = parse_dictionary_page_header(reader, page); }
    else if (id == 8 && type == CT_STRUCT) { ok = parse_data_page_header_v2(reader, page); }
    else { ok = reader.skip(type); }
  }
  header_size = reader.consumed();
  return ok && page.type_ >= 0 && page.compressed_size_ >= 0 && page.uncompressed_size_ >= 0;
}

bool decode_snappy(const uint8_t *data, size_t size, std::vector<uint8_t> &output)
{
  size_t position = 0;
  uint64_t expected = 0;
  int shift = 0;
  while (position < size && shift < 64) {
    const uint8_t next = data[position++];
    expected |= static_cast<uint64_t>(next & 0x7f) << shift;
    if ((next & 0x80) == 0) { break; }
    shift += 7;
  }
  if (expected > SIZE_MAX || expected > MAX_PARQUET_BUFFER_SIZE) { return false; }
  output.clear();
  output.reserve(static_cast<size_t>(expected));
  while (position < size && output.size() < expected) {
    const uint8_t tag = data[position++];
    const uint8_t kind = tag & 3;
    if (kind == 0) {
      uint64_t length = tag >> 2;
      if (length < 60) { ++length; }
      else {
        const int bytes = static_cast<int>(length - 59);
        length = 0;
        if (bytes < 1 || bytes > 4 || position + bytes > size) { return false; }
        for (int i = 0; i < bytes; ++i) { length |= static_cast<uint64_t>(data[position++]) << (8 * i); }
        ++length;
      }
      if (length > size - position || length > expected - output.size()) { return false; }
      output.insert(output.end(), data + position, data + position + length);
      position += length;
    } else {
      uint64_t length = 0; uint64_t offset = 0;
      if (kind == 1) {
        if (position >= size) { return false; }
        length = 4 + ((tag >> 2) & 7);
        offset = ((tag & 0xe0) << 3) | data[position++];
      } else if (kind == 2) {
        if (position + 2 > size) { return false; }
        length = 1 + (tag >> 2);
        offset = data[position] | (static_cast<uint64_t>(data[position + 1]) << 8);
        position += 2;
      } else {
        if (position + 4 > size) { return false; }
        length = 1 + (tag >> 2);
        offset = static_cast<uint64_t>(data[position])
            | (static_cast<uint64_t>(data[position + 1]) << 8)
            | (static_cast<uint64_t>(data[position + 2]) << 16)
            | (static_cast<uint64_t>(data[position + 3]) << 24);
        position += 4;
      }
      if (offset == 0 || offset > output.size() || length > expected - output.size()) { return false; }
      for (uint64_t i = 0; i < length; ++i) { output.push_back(output[output.size() - offset]); }
    }
  }
  return output.size() == expected;
}

bool decode_page_payload(int codec, const uint8_t *data, size_t size, size_t expected,
                         std::vector<uint8_t> &storage, const uint8_t *&decoded, size_t &decoded_size)
{
  if (size > MAX_PARQUET_BUFFER_SIZE || expected > MAX_PARQUET_BUFFER_SIZE) { return false; }
  bool ok = true;
  if (codec == 0) { decoded = data; decoded_size = size; }
  else if (codec == 1) {
    ok = decode_snappy(data, size, storage) && storage.size() == expected;
    decoded = storage.data(); decoded_size = storage.size();
  } else { ok = false; }
  return ok;
}

bool decode_hybrid(const uint8_t *data, size_t size, int bit_width, size_t expected,
                   std::vector<uint32_t> &values)
{
  size_t position = 0;
  values.clear();
  while (position < size && values.size() < expected) {
    uint64_t header = 0; int shift = 0;
    do {
      if (position >= size || shift >= 64) { return false; }
      const uint8_t next = data[position++];
      header |= static_cast<uint64_t>(next & 0x7f) << shift;
      shift += 7;
      if ((next & 0x80) == 0) { break; }
    } while (true);
    if ((header & 1) == 0) {
      const size_t count = static_cast<size_t>(header >> 1);
      const size_t bytes = static_cast<size_t>((bit_width + 7) / 8);
      if (position + bytes > size) { return false; }
      uint32_t value = 0;
      for (size_t i = 0; i < bytes; ++i) { value |= static_cast<uint32_t>(data[position++]) << (8 * i); }
      for (size_t i = 0; i < count && values.size() < expected; ++i) { values.push_back(value); }
    } else {
      const size_t count = static_cast<size_t>(header >> 1) * 8;
      const size_t bits = count * static_cast<size_t>(bit_width);
      const size_t bytes = (bits + 7) / 8;
      if (position + bytes > size) { return false; }
      const uint32_t mask = bit_width == 32 ? UINT32_MAX : ((1U << bit_width) - 1);
      for (size_t i = 0; i < count && values.size() < expected; ++i) {
        const size_t bit = i * static_cast<size_t>(bit_width);
        uint64_t word = 0;
        for (size_t j = 0; j < 5 && position + bit / 8 + j < position + bytes; ++j) {
          word |= static_cast<uint64_t>(data[position + bit / 8 + j]) << (8 * j);
        }
        values.push_back(static_cast<uint32_t>((word >> (bit & 7)) & mask));
      }
      position += bytes;
    }
  }
  return values.size() == expected;
}

bool decode_definition_levels(const uint8_t *data, size_t size, size_t count, bool length_prefixed,
                              std::vector<uint32_t> &levels, size_t &consumed)
{
  consumed = 0;
  if (length_prefixed) {
    if (size < 4) { return false; }
    const uint32_t length = static_cast<uint32_t>(data[0]) | (static_cast<uint32_t>(data[1]) << 8)
        | (static_cast<uint32_t>(data[2]) << 16) | (static_cast<uint32_t>(data[3]) << 24);
    if (length > size - 4) { return false; }
    consumed = 4 + length;
    return decode_hybrid(data + 4, length, 1, count, levels);
  }
  consumed = size;
  return decode_hybrid(data, size, 1, count, levels);
}

bool parquet_value_to_string(const uint8_t *data, size_t size, size_t &position,
                             const ParquetSchemaElement &schema, size_t ordinal, std::string &value)
{
  bool ok = true;
  char buffer[128] = {};
  if (schema.type_ == 0) {
    const size_t byte = ordinal / 8;
    if (byte >= size) { ok = false; }
    else { value = ((data[byte] >> (ordinal & 7)) & 1) ? "true" : "false"; }
  } else if (schema.type_ == 1) {
    if (position + 4 > size) { ok = false; }
    else {
      int32_t number = 0; MEMCPY(&number, data + position, 4); position += 4;
      if (schema.converted_type_ == 6) {
        int64_t length = 0;
        ok = OB_SUCCESS == ObTimeConverter::date_to_str(number, buffer, sizeof(buffer), length);
        if (ok) { value.assign(buffer, length); }
      } else { value = std::to_string(number); }
    }
  } else if (schema.type_ == 2) {
    if (position + 8 > size) { ok = false; }
    else {
      int64_t number = 0; MEMCPY(&number, data + position, 8); position += 8;
      if (schema.converted_type_ == 9 || schema.converted_type_ == 10) {
        const int64_t micros = schema.converted_type_ == 9 ? number * 1000 : number;
        int64_t length = 0;
        ok = OB_SUCCESS == ObTimeConverter::datetime_to_str(micros, nullptr, 6, buffer, sizeof(buffer), length);
        if (ok) { value.assign(buffer, length); }
      } else { value = std::to_string(number); }
    }
  } else if (schema.type_ == 4) {
    if (position + 4 > size) { ok = false; }
    else { float number = 0; MEMCPY(&number, data + position, 4); position += 4;
      const int length = snprintf(buffer, sizeof(buffer), "%.9g", number); value.assign(buffer, length); }
  } else if (schema.type_ == 5) {
    if (position + 8 > size) { ok = false; }
    else { double number = 0; MEMCPY(&number, data + position, 8); position += 8;
      const int length = snprintf(buffer, sizeof(buffer), "%.17g", number); value.assign(buffer, length); }
  } else if (schema.type_ == 6) {
    if (position + 4 > size) { ok = false; }
    else {
      uint32_t length = 0; MEMCPY(&length, data + position, 4); position += 4;
      if (length > size - position) { ok = false; }
      else { value.assign(reinterpret_cast<const char *>(data + position), length); position += length; }
    }
  } else if (schema.type_ == 7) {
    const size_t length = static_cast<size_t>(schema.type_length_);
    if (length > size - position) { ok = false; }
    else { value.assign(reinterpret_cast<const char *>(data + position), length); position += length; }
  } else { ok = false; }
  return ok;
}

bool decode_plain_values(const uint8_t *data, size_t size, size_t count,
                         const ParquetSchemaElement &schema, std::vector<std::string> &values)
{
  size_t position = 0;
  values.clear();
  for (size_t i = 0; i < count; ++i) {
    std::string value;
    if (!parquet_value_to_string(data, size, position, schema, i, value)) { return false; }
    values.push_back(value);
  }
  return true;
}

bool decode_dictionary_ids(const uint8_t *data, size_t size, size_t count,
                           const std::vector<std::string> &dictionary, std::vector<std::string> &values)
{
  if (size == 0) { return false; }
  const int bit_width = data[0];
  std::vector<uint32_t> ids;
  if (bit_width < 0 || bit_width > 32 || !decode_hybrid(data + 1, size - 1, bit_width, count, ids)) { return false; }
  values.clear();
  for (uint32_t id : ids) {
    if (id >= dictionary.size()) { return false; }
    values.push_back(dictionary[id]);
  }
  return true;
}

struct ParquetCell
{
  bool is_null_ = true;
  std::string value_;
};

bool append_data_page(const uint8_t *data, size_t size, const ParquetPageHeader &page,
                      const ParquetSchemaElement &schema, const std::vector<std::string> &dictionary,
                      bool v2, std::vector<ParquetCell> &cells)
{
  const bool nullable = schema.repetition_ != 0;
  std::vector<uint32_t> levels;
  size_t level_bytes = 0;
  if (nullable) {
    const size_t definition_size = v2 ? static_cast<size_t>(page.definition_bytes_) : size;
    if (definition_size > size || !decode_definition_levels(data, definition_size,
        static_cast<size_t>(page.num_values_), !v2, levels, level_bytes)) { return false; }
  } else {
    levels.assign(static_cast<size_t>(page.num_values_), 1);
  }
  size_t non_null = 0;
  for (uint32_t level : levels) { if (level == 1) { ++non_null; } }
  const uint8_t *encoded = data + level_bytes;
  const size_t encoded_size = size - level_bytes;
  std::vector<std::string> values;
  if (page.encoding_ == 0) {
    if (!decode_plain_values(encoded, encoded_size, non_null, schema, values)) { return false; }
  } else if (page.encoding_ == 2 || page.encoding_ == 8) {
    if (!decode_dictionary_ids(encoded, encoded_size, non_null, dictionary, values)) { return false; }
  } else { return false; }
  size_t value_index = 0;
  for (uint32_t level : levels) {
    ParquetCell cell;
    if (level == 1) { cell.is_null_ = false; cell.value_ = values[value_index++]; }
    cells.push_back(cell);
  }
  return true;
}

bool decode_parquet_column(const std::vector<uint8_t> &chunk, const ParquetColumnMeta &column,
                           const ParquetSchemaElement &schema, std::vector<ParquetCell> &cells)
{
  if (column.total_compressed_size_ < 0
      || chunk.size() != static_cast<size_t>(column.total_compressed_size_)) { return false; }
  size_t position = 0;
  const size_t end = chunk.size();
  std::vector<std::string> dictionary;
  cells.clear();
  while (position < end && cells.size() < static_cast<size_t>(column.num_values_)) {
    ParquetPageHeader page;
    size_t header_size = 0;
    if (!parse_page_header(chunk.data() + position, end - position, page, header_size)) { return false; }
    position += header_size;
    if (static_cast<size_t>(page.compressed_size_) > end - position) { return false; }
    const uint8_t *payload = chunk.data() + position;
    const size_t payload_size = static_cast<size_t>(page.compressed_size_);
    std::vector<uint8_t> decompressed;
    if (page.type_ == 2) {
      const uint8_t *decoded = nullptr; size_t decoded_size = 0;
      if (!decode_page_payload(column.codec_, payload, payload_size,
          static_cast<size_t>(page.uncompressed_size_), decompressed, decoded, decoded_size)
          || !decode_plain_values(decoded, decoded_size, static_cast<size_t>(page.dictionary_values_),
                                  schema, dictionary)) { return false; }
    } else if (page.type_ == 0) {
      const uint8_t *decoded = nullptr; size_t decoded_size = 0;
      if (!decode_page_payload(column.codec_, payload, payload_size,
          static_cast<size_t>(page.uncompressed_size_), decompressed, decoded, decoded_size)
          || !append_data_page(decoded, decoded_size, page, schema, dictionary, false, cells)) { return false; }
    } else if (page.type_ == 3) {
      const size_t levels_size = static_cast<size_t>(page.definition_bytes_ + page.repetition_bytes_);
      if (levels_size > payload_size) { return false; }
      const uint8_t *data_values = payload + levels_size;
      const size_t data_size = payload_size - levels_size;
      const uint8_t *decoded_values = data_values; size_t decoded_values_size = data_size;
      if (page.data_v2_compressed_) {
        if (!decode_page_payload(column.codec_, data_values, data_size,
            static_cast<size_t>(page.uncompressed_size_) - levels_size,
            decompressed, decoded_values, decoded_values_size)) { return false; }
      }
      std::vector<uint8_t> joined(payload, payload + levels_size);
      joined.insert(joined.end(), decoded_values, decoded_values + decoded_values_size);
      if (!append_data_page(joined.data() + page.repetition_bytes_,
          joined.size() - page.repetition_bytes_, page, schema, dictionary, true, cells)) { return false; }
    }
    position += payload_size;
  }
  return cells.size() == static_cast<size_t>(column.num_values_);
}

class ParquetFileScanner final : public BaseFileScanner
{
public:
  ParquetFileScanner(const ObString &path, const FileTableOptions &options,
                     const ObIArray<ObString> &names, const ObIArray<int64_t> &types,
                     const ObIArray<uint64_t> &projection, int64_t size, int64_t mtime)
    : BaseFileScanner(path, options, names, types, projection, size, mtime), input_(), metadata_(),
      projected_(), group_columns_(), group_index_(0), row_index_(0)
  {}
  int open() override
  {
    int ret = check_unchanged();
    input_.close();
    input_.clear();
    metadata_ = ParquetMeta(); projected_.assign(column_names_.count(), false);
    for (int64_t i = 0; i < projection_.count(); ++i) {
      const int64_t index = projection_.at(i) - OB_APP_MIN_COLUMN_ID;
      if (index < 0 || index >= column_names_.count()) { return OB_ERR_UNEXPECTED; }
      projected_[index] = true;
    }
    if (OB_SUCC(ret)) { ret = read_parquet_metadata(ObString(path_.size(), path_.data()), metadata_); }
    if (OB_SUCC(ret) && (metadata_.schema_.size() != static_cast<size_t>(column_names_.count() + 1))) {
      ret = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCC(ret)) {
      input_.open(path_, std::ios::in | std::ios::binary);
      if (!input_.is_open()) { ret = OB_FILE_NOT_EXIST; }
    }
    group_index_ = 0; row_index_ = 0; rows_read_ = 0; group_columns_.clear();
    return ret;
  }
  int get_next_row(ObNewRow *&row) override
  {
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret) && (group_index_ == 0
           || row_index_ >= static_cast<size_t>(metadata_.row_groups_[group_index_ - 1].num_rows_))) {
      if (group_index_ >= metadata_.row_groups_.size()) {
        if (OB_FAIL(check_unchanged())) {
          return ret;
        }
        return OB_ITER_END;
      }
      ret = load_group(group_index_++);
      row_index_ = 0;
    }
    if (OB_SUCC(ret)) {
      std::vector<std::string> fields(column_names_.count());
      std::vector<bool> present(column_names_.count(), false);
      for (size_t i = 0; i < group_columns_.size(); ++i) {
        if (projected_[i] && row_index_ < group_columns_[i].size() && !group_columns_[i][row_index_].is_null_) {
          fields[i] = group_columns_[i][row_index_].value_;
          present[i] = true;
        }
      }
      ++row_index_;
      ret = project_row(fields, &present, row);
    }
    return ret;
  }
  int rescan() override { return open(); }
  int close() override { input_.close(); group_columns_.clear(); return OB_SUCCESS; }
private:
  int read_column_chunk(const ParquetColumnMeta &column, std::vector<uint8_t> &chunk)
  {
    int ret = OB_SUCCESS;
    const int64_t start = column.dictionary_page_offset_ >= 0
        ? std::min(column.dictionary_page_offset_, column.data_page_offset_) : column.data_page_offset_;
    if (start < 0 || column.total_compressed_size_ <= 0
        || static_cast<uint64_t>(column.total_compressed_size_) > MAX_PARQUET_BUFFER_SIZE
        || column.total_compressed_size_ > expected_size_
        || start > expected_size_ - column.total_compressed_size_) {
      ret = OB_ERR_PARSE_SQL;
    } else {
      chunk.resize(static_cast<size_t>(column.total_compressed_size_));
      input_.clear();
      input_.seekg(start, std::ios::beg);
      input_.read(reinterpret_cast<char *>(chunk.data()), chunk.size());
      if (!input_) { ret = OB_IO_ERROR; }
    }
    return ret;
  }

  int load_group(size_t index)
  {
    int ret = OB_SUCCESS;
    const ParquetRowGroupMeta &group = metadata_.row_groups_[index];
    if (group.columns_.size() != static_cast<size_t>(column_names_.count())) { return OB_ERR_PARSE_SQL; }
    group_columns_.clear(); group_columns_.resize(group.columns_.size());
    for (size_t i = 0; OB_SUCC(ret) && i < group.columns_.size(); ++i) {
      if (projected_[i]) {
        std::vector<uint8_t> chunk;
        if (OB_FAIL(read_column_chunk(group.columns_[i], chunk))) {
        } else if (!decode_parquet_column(chunk, group.columns_[i], metadata_.schema_[i + 1],
                                          group_columns_[i])) {
          ret = OB_ERR_PARSE_SQL;
        }
      }
    }
    return ret;
  }
  std::ifstream input_;
  ParquetMeta metadata_;
  std::vector<bool> projected_;
  std::vector<std::vector<ParquetCell>> group_columns_;
  size_t group_index_;
  size_t row_index_;
};

class CsvFileScanner final : public BaseFileScanner
{
public:
  CsvFileScanner(const ObString &path, const FileTableOptions &options,
                 const ObIArray<ObString> &names, const ObIArray<int64_t> &types,
                 const ObIArray<uint64_t> &projection, int64_t size, int64_t mtime)
    : BaseFileScanner(path, options, names, types, projection, size, mtime),
      reader_(path, options), header_read_(false)
  {}
  int open() override
  {
    int ret = check_unchanged();
    if (OB_SUCC(ret) && OB_FAIL(reader_.open())) {}
    header_read_ = false;
    rows_read_ = 0;
    return ret;
  }
  int get_next_row(ObNewRow *&row) override
  {
    int ret = OB_SUCCESS;
    std::vector<std::string> fields;
    if (options_.header_ && !header_read_) {
      if (OB_FAIL(reader_.next(fields))) { return ret; }
      header_read_ = true;
    }
    if (OB_FAIL(reader_.next(fields))) {
      if (ret == OB_ITER_END) {
        const int check_ret = check_unchanged();
        if (check_ret != OB_SUCCESS) { ret = check_ret; }
      }
    } else {
      ret = project_row(fields, nullptr, row);
    }
    return ret;
  }
  int rescan() override { return open(); }
  int close() override { return OB_SUCCESS; }
private:
  CsvReader reader_;
  bool header_read_;
};

int json_to_string(const rapidjson::Value &value, std::string &text)
{
  int ret = OB_SUCCESS;
  if (value.IsNull()) { text.clear(); }
  else if (value.IsBool()) { text = value.GetBool() ? "true" : "false"; }
  else if (value.IsInt64()) { text = std::to_string(value.GetInt64()); }
  else if (value.IsUint64()) { text = std::to_string(value.GetUint64()); }
  else if (value.IsDouble()) { text = std::to_string(value.GetDouble()); }
  else if (value.IsString()) { text.assign(value.GetString(), value.GetStringLength()); }
  else { ret = OB_NOT_SUPPORTED; }
  return ret;
}

class JsonlFileScanner final : public BaseFileScanner
{
public:
  JsonlFileScanner(const ObString &path, const FileTableOptions &options,
                   const ObIArray<ObString> &names, const ObIArray<int64_t> &types,
                   const ObIArray<uint64_t> &projection, int64_t size, int64_t mtime)
    : BaseFileScanner(path, options, names, types, projection, size, mtime), input_()
  {}
  int open() override
  {
    int ret = check_unchanged();
    input_.close();
    input_.clear();
    if (OB_SUCC(ret)) {
      input_.open(path_, std::ios::in | std::ios::binary);
      if (!input_.is_open()) { ret = OB_FILE_NOT_EXIST; }
    }
    rows_read_ = 0;
    return ret;
  }
  int get_next_row(ObNewRow *&row) override
  {
    int ret = OB_SUCCESS;
    std::string line;
    while (std::getline(input_, line) && line.empty()) {}
    if (!input_ && line.empty()) {
      if (OB_FAIL(check_unchanged())) {
        return ret;
      }
      return OB_ITER_END;
    }
    if (!line.empty() && line.back() == '\r') { line.pop_back(); }
    rapidjson::Document document;
    document.Parse(line.data(), line.size());
    if (document.HasParseError() || !document.IsObject()) { ret = OB_ERR_PARSE_SQL; }
    std::vector<std::string> fields(column_names_.count());
    std::vector<bool> present(column_names_.count(), false);
    for (int64_t i = 0; OB_SUCC(ret) && i < column_names_.count(); ++i) {
      for (auto member = document.MemberBegin(); member != document.MemberEnd(); ++member) {
        if (ObCharset::case_insensitive_equal(column_names_.at(i),
            ObString(member->name.GetStringLength(), member->name.GetString()))) {
          present[i] = true;
          ret = json_to_string(member->value, fields[i]);
          break;
        }
      }
    }
    if (OB_SUCC(ret)) { ret = project_row(fields, &present, row); }
    return ret;
  }
  int rescan() override { return open(); }
  int close() override { input_.close(); return OB_SUCCESS; }
private:
  std::ifstream input_;
};

class GlobFileScanner final : public IFileTableScanner
{
public:
  GlobFileScanner(const ObString &pattern, const FileTableOptions &options,
                  const ObIArray<uint64_t> &projection)
    : pattern_(pattern.ptr(), pattern.length()), options_(options), projection_(projection),
      matches_(), position_(0), row_allocator_("FileGlobRow"), cell_(), row_()
  {}
  int open() override
  {
    int ret = OB_SUCCESS;
    matches_.clear();
    position_ = 0;
    glob_t result = {};
    const int code = ::glob(pattern_.c_str(), 0, nullptr, &result);
    if (code == GLOB_NOMATCH) { ret = OB_FILE_NOT_EXIST; }
    else if (code != 0) { ret = OB_IO_ERROR; }
    for (size_t i = 0; OB_SUCC(ret) && i < result.gl_pathc; ++i) {
      char canonical[PATH_MAX] = {};
      if (::realpath(result.gl_pathv[i], canonical) == nullptr) { ret = OB_FILE_NOT_EXIST; }
      else if (OB_FAIL(ObResolverUtils::check_secure_path(options_.secure_root_, ObString(canonical)))) {}
      else { matches_.push_back(canonical); }
    }
    globfree(&result);
    return ret;
  }
  int get_next_row(ObNewRow *&row) override
  {
    int ret = OB_SUCCESS;
    if (position_ >= matches_.size()) { ret = OB_ITER_END; }
    else if (projection_.empty()) {
      row_.cells_ = nullptr;
      row_.count_ = 0;
      row = &row_;
      ++position_;
    } else {
      row_allocator_.reuse();
      ObString path;
      if (OB_FAIL(copy_string(row_allocator_, matches_[position_++], path))) {
      } else {
        cell_.set_varchar(path);
        cell_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        row_.cells_ = &cell_;
        row_.count_ = 1;
        row = &row_;
      }
    }
    return ret;
  }
  int rescan() override { return open(); }
  int close() override { matches_.clear(); return OB_SUCCESS; }
private:
  std::string pattern_;
  FileTableOptions options_;
  const ObIArray<uint64_t> &projection_;
  std::vector<std::string> matches_;
  size_t position_;
  ObArenaAllocator row_allocator_;
  ObObj cell_;
  ObNewRow row_;
};

} // namespace

int resolve_file_table_path(const ObString &path, const ObString &secure_file_priv,
                            bool allow_glob, ObIAllocator &allocator, ObString &resolved_path)
{
  int ret = OB_SUCCESS;
  if (path.empty() || path.length() >= PATH_MAX) { return OB_INVALID_ARGUMENT; }
  std::string source(path.ptr(), path.length());
  const size_t wildcard = source.find_first_of("*?[");
  char canonical[PATH_MAX] = {};
  if (wildcard == std::string::npos) {
    if (::realpath(source.c_str(), canonical) == nullptr) { ret = OB_FILE_NOT_EXIST; }
    else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_file_priv, ObString(canonical)))) {}
    else { ret = ob_write_string(allocator, ObString(canonical), resolved_path); }
  } else if (!allow_glob) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const size_t slash = source.rfind('/', wildcard);
    const std::string directory = slash == std::string::npos ? "." : source.substr(0, slash);
    if (::realpath(directory.c_str(), canonical) == nullptr) { ret = OB_FILE_NOT_EXIST; }
    else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_file_priv, ObString(canonical)))) {}
    else {
      std::string result(canonical);
      result.append(source.substr(slash));
      ret = copy_string(allocator, result, resolved_path);
    }
  }
  return ret;
}

int infer_file_table_schema(FileTableType type, const ObString &path,
                            const FileTableOptions &options, ObIAllocator &allocator,
                            ObIArray<FileTableColumn> &columns, int64_t &file_size,
                            int64_t &file_mtime_ns)
{
  int ret = OB_SUCCESS;
  columns.reset();
  file_size = 0;
  file_mtime_ns = 0;
  if (type == FILE_TABLE_GLOB) {
    FileTableColumn column;
    column.type_ = ObVarcharType;
    column.nullable_ = false;
    if (OB_FAIL(ob_write_string(allocator, ObString("file"), column.name_))) {}
    else { ret = columns.push_back(column); }
  } else if (OB_FAIL(get_file_stat(path, file_size, file_mtime_ns))) {
  } else if (type == FILE_TABLE_CSV) {
    ret = infer_csv(path, options, allocator, columns);
  } else if (type == FILE_TABLE_JSONL) {
    ret = infer_jsonl(path, allocator, columns);
  } else if (type == FILE_TABLE_PARQUET) {
    ret = infer_parquet(path, allocator, columns);
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int create_file_table_scanner(FileTableType type, const ObString &path,
                              const FileTableOptions &options,
                              const ObIArray<ObString> &column_names,
                              const ObIArray<int64_t> &column_types,
                              const ObIArray<uint64_t> &projected_column_ids,
                              int64_t expected_file_size, int64_t expected_file_mtime_ns,
                              ObIAllocator &allocator, IFileTableScanner *&scanner)
{
  int ret = OB_SUCCESS;
  scanner = nullptr;
  void *memory = nullptr;
  if (type == FILE_TABLE_CSV) {
    if (OB_ISNULL(memory = allocator.alloc(sizeof(CsvFileScanner)))) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    else { scanner = new (memory) CsvFileScanner(path, options, column_names, column_types,
        projected_column_ids, expected_file_size, expected_file_mtime_ns); }
  } else if (type == FILE_TABLE_JSONL) {
    if (OB_ISNULL(memory = allocator.alloc(sizeof(JsonlFileScanner)))) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    else { scanner = new (memory) JsonlFileScanner(path, options, column_names, column_types,
        projected_column_ids, expected_file_size, expected_file_mtime_ns); }
  } else if (type == FILE_TABLE_PARQUET) {
    if (OB_ISNULL(memory = allocator.alloc(sizeof(ParquetFileScanner)))) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    else { scanner = new (memory) ParquetFileScanner(path, options, column_names, column_types,
        projected_column_ids, expected_file_size, expected_file_mtime_ns); }
  } else if (type == FILE_TABLE_GLOB) {
    if (OB_ISNULL(memory = allocator.alloc(sizeof(GlobFileScanner)))) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    else { scanner = new (memory) GlobFileScanner(path, options, projected_column_ids); }
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
