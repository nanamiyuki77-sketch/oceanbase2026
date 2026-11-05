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

// Query-facing serialization policy for in-process Root Service commands
// converted from the former priority-10 single-threaded RPC queue. The
// interface lives below Rootserver so query execution does not depend on
// Rootserver headers.
#ifndef OCEANBASE_QUERY_COMMAND_OB_ROOT_SERVICE_SERIALIZATION_H_
#define OCEANBASE_QUERY_COMMAND_OB_ROOT_SERVICE_SERIALIZATION_H_

#include "lib/lock/ob_mutex.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/worker.h"

namespace oceanbase
{
namespace query
{

inline lib::ObMutex &root_service_serial_mutex()
{
  static lib::ObMutex mutex;
  return mutex;
}

// Lock acquisition obeys the caller's worker deadline. The callable executes
// under the supplied serialization lock and returns an OceanBase error code.
template <typename F>
inline int serialize_service_call(lib::ObMutex &mutex, F &&fn)
{
  int ret = mutex.lock(THIS_WORKER.get_timeout_ts());
  if (OB_SUCCESS != ret) {
  } else {
    struct UnlockGuard
    {
      explicit UnlockGuard(lib::ObMutex &mutex) : mutex_(mutex) {}
      ~UnlockGuard() { mutex_.unlock(); }
      lib::ObMutex &mutex_;
    } guard(mutex);
    ret = fn();
  }
  return ret;
}

// General Root Service commands retain the former process-wide serialization
// policy used by the single-threaded RPC queue.
template <typename F>
inline int serialize_root_service_call(F &&fn)
{
  return serialize_service_call(root_service_serial_mutex(), fn);
}

} // namespace query
} // namespace oceanbase

#endif // OCEANBASE_QUERY_COMMAND_OB_ROOT_SERVICE_SERIALIZATION_H_
