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

#pragma once

#include "common/object/ob_object.h"
#include "common/row/ob_row.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{

enum FileTableType : int64_t
{
  FILE_TABLE_NONE = 0,
  FILE_TABLE_CSV,
  FILE_TABLE_JSONL,
  FILE_TABLE_PARQUET,
  FILE_TABLE_GLOB
};

struct FileTableOptions
{
  FileTableOptions()
    : header_(true), delimiter_(','), quote_('"'), escape_('"'), null_string_(),
      secure_root_(), sample_size_(-1)
  {}
  bool header_;
  char delimiter_;
  char quote_;
  char escape_;
  common::ObString null_string_;
  common::ObString secure_root_;
  int64_t sample_size_;
};

struct FileTableColumn
{
  FileTableColumn() : name_(), type_(common::ObNullType), nullable_(false) {}
  common::ObString name_;
  common::ObObjType type_;
  bool nullable_;
};

int resolve_file_table_path(const common::ObString &path,
                            const common::ObString &secure_file_priv,
                            bool allow_glob,
                            common::ObIAllocator &allocator,
                            common::ObString &resolved_path);

int infer_file_table_schema(FileTableType type,
                            const common::ObString &path,
                            const FileTableOptions &options,
                            common::ObIAllocator &allocator,
                            common::ObIArray<FileTableColumn> &columns,
                            int64_t &file_size,
                            int64_t &file_mtime_ns);

class IFileTableScanner
{
public:
  virtual ~IFileTableScanner() = default;
  virtual int open() = 0;
  virtual int get_next_row(common::ObNewRow *&row) = 0;
  virtual int rescan() = 0;
  virtual int close() = 0;
};

int create_file_table_scanner(FileTableType type,
                              const common::ObString &path,
                              const FileTableOptions &options,
                              const common::ObIArray<common::ObString> &column_names,
                              const common::ObIArray<int64_t> &column_types,
                              const common::ObIArray<uint64_t> &projected_column_ids,
                              int64_t expected_file_size,
                              int64_t expected_file_mtime_ns,
                              common::ObIAllocator &allocator,
                              IFileTableScanner *&scanner);

} // namespace sql
} // namespace oceanbase
