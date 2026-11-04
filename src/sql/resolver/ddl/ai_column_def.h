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

#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace sql
{

struct AIColumnDef
{
  AIColumnDef()
      : max_retries_(0), column_id_(common::OB_INVALID_ID),
        state_column_id_(common::OB_INVALID_ID), request_column_id_(common::OB_INVALID_ID),
        result_spec_column_id_(common::OB_INVALID_ID), result_type_(common::ObNullType),
        vector_dimension_(0), is_embedding_(false)
  {}

  common::ObString column_name_;
  common::ObString state_column_name_;
  common::ObString request_column_name_;
  common::ObString result_spec_column_name_;
  common::ObString expression_;
  common::ObString normalized_expression_;
  common::ObString execution_expression_;
  common::ObSEArray<common::ObString, 4> dependency_names_;
  int64_t max_retries_;
  uint64_t column_id_;
  uint64_t state_column_id_;
  uint64_t request_column_id_;
  uint64_t result_spec_column_id_;
  common::ObObjType result_type_;
  int64_t vector_dimension_;
  bool is_embedding_;
};

} // namespace sql
} // namespace oceanbase
