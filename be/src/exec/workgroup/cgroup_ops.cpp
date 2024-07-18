// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/workgroup/cgroup_ops.h"

#include <fcntl.h>
#include <fmt/compile.h>
#include <sys/stat.h>

#include <filesystem>
#include <memory>

#include "common/logging.h"
#include "common/status.h"
#include "common/statusor.h"
#include "fs/scoped_fd_closer.h"

namespace starrocks::workgroup {

// ------------------------------------------------------------------------------------
// utils
// ------------------------------------------------------------------------------------

static const std::string LOG_PREFIX = "[CGroup] ";

namespace {

Status write_cgroup_file(std::string_view path, int64_t value, bool is_append) {
    const int fd = open(path.data(), is_append ? O_RDWR | O_APPEND : O_RDWR);
    if (fd == -1) {
        LOG(WARNING) << LOG_PREFIX << "Failed to open cgroup file "
                     << "[path=" << path << "] [error=" << strerror(errno) << "]";
        return Status::InternalError(
                fmt::format("failed to open cgroup file [path={}] [error={}]", path, strerror(errno)));
    }
    ScopedFdCloser fd_closer(fd);

    const auto value_str = fmt::format("{}\n", value);
    if (const int ret = write(fd, value_str.c_str(), value_str.size()); ret == -1) {
        LOG(WARNING) << LOG_PREFIX << "Failed to write cgroup file "
                     << "[path=" << path << "] [value=" << value << "] [error=" << strerror(errno) << "]";
        return Status::InternalError(fmt::format("failed to write cgroup file [path={}] [value={}] [error={}]", path,
                                                 value, strerror(errno)));
    }
    LOG(INFO) << LOG_PREFIX << "Write successfully cgroup file [path=" << path << "] [value=" << value << "]";

    return Status::OK();
}

StatusOr<int64_t> read_cgroup_int64(std::string_view path) {
    const int fd = open(path.data(), O_RDONLY);
    if (fd == -1) {
        LOG(WARNING) << LOG_PREFIX << "Failed to open cgroup file "
                     << "[path=" << path << "] [error=" << strerror(errno) << "]";
        return Status::InternalError(
                fmt::format("failed to open cgroup file [path={}] [error={}]", path, strerror(errno)));
    }
    ScopedFdCloser fd_closer(fd);

    char buf[20];
    if (read(fd, buf, sizeof(buf)) < 0) {
        LOG(WARNING) << LOG_PREFIX << "Failed to read from cgroup file "
                     << "[path=" << path << "] [error=" << strerror(errno) << "]";
        return Status::InternalError(
                fmt::format("failed to read from cgroup file [path={}] [error={}]", path, strerror(errno)));
    }

    int64_t value;
    if (sscanf(buf, "%ld", &value) != 1) {
        LOG(WARNING) << LOG_PREFIX << "Cannot convert the content to int64_t "
                     << "[path=" << path << "] [content=" << buf << "] [error=" << strerror(errno) << "]";
        return Status::InternalError(fmt::format(
                "cannot convert the content to int64_t [path={}] [content={}] [error={}]", path, buf, strerror(errno)));
    }

    return value;
}

Status validate_cgroup_dir(std::string_view path) {
    if (access(path.data(), F_OK) != 0) {
        LOG(WARNING) << LOG_PREFIX << "CGroup directory does not exist "
                     << "[path=" << path << "] [error=" << strerror(errno) << "]";
        return Status::InternalError(
                fmt::format("cgroup directory does not exist [path={}] [error={}]", path, strerror(errno)));
    }
    if (access(path.data(), W_OK | R_OK) != 0) {
        LOG(WARNING) << LOG_PREFIX << "CGroup directory is not writable or readable "
                     << "[path=" << path << "] [error=" << strerror(errno) << "]";
        return Status::InternalError(fmt::format("cgroup directory is not writable or readable [path={}] [error={}]",
                                                 path, strerror(errno)));
    }
    return Status::OK();
}

Status create_cgroup_dir(std::string_view path) {
    if (mkdir(path.data(), S_IRWXU) != 0 && errno != EEXIST) {
        LOG(WARNING) << LOG_PREFIX << "Failed to create cgroup directory [path=" << path
                     << "] [error=" << strerror(errno) << "]";
        return Status::InternalError(
                fmt::format("failed to create cgroup directory [path={}] [error={}]", path, strerror(errno)));
    }
    return Status::OK();
}

Status delete_cgroup_dir(std::string_view path) {
    if (rmdir(path.data()) != 0 && errno != ENOENT) {
        LOG(WARNING) << LOG_PREFIX << "Failed to delete cgroup file "
                     << "[path=" << path << "] [error=" << strerror(errno) << "]";
        return Status::InternalError(
                fmt::format("failed to delete cgroup file [path={}] [error={}]", path, strerror(errno)));
    }
    return Status::OK();
}

} // namespace

// ------------------------------------------------------------------------------------
// CGroupOpsDummy
// ------------------------------------------------------------------------------------

class CGroupOpsDummy final : public CGroupOps {
public:
    Status init() override { return Status::OK(); }
    Status create_group(WorkGroupId) override { return Status::OK(); }
    Status set_cpu_weight(WorkGroupId, int64_t) override { return Status::OK(); }
    Status set_max_cpu_cores(WorkGroupId, int64_t) override { return Status::OK(); }
    Status attach(WorkGroupId, int64_t) override { return Status::OK(); }
};

// ------------------------------------------------------------------------------------
// CGroupOpsV1
// ------------------------------------------------------------------------------------

class CGroupOpsV1 final : public CGroupOps {
public:
    explicit CGroupOpsV1(int num_cpu_cores) : _num_cpu_cores(num_cpu_cores) {}
    ~CGroupOpsV1() override = default;

    Status init() override;
    Status create_group(WorkGroupId wgid) override;
    Status set_cpu_weight(WorkGroupId wgid, int64_t cpu_weight) override;
    Status set_max_cpu_cores(WorkGroupId wgid, int64_t max_cpu_cores) override;
    Status attach(WorkGroupId wgid, int64_t tid) override;

private:
    enum class BaseDir : uint8_t { ROOT, GROUP_ROOT };
    enum class CGroupFile : uint8_t { TASKS, CPU_WEIGHT, MAX_CPU_CORES, CFS_PERIOD };
    static std::string _to_string(BaseDir base_dir);
    static std::string _to_string(CGroupFile cgroup_file);
    static std::string _build_path(BaseDir base_dir, CGroupFile cgroup_file, WorkGroupId wgid = ABSENT_WORKGROUP_ID);
    static std::string _build_path(BaseDir base_dir, WorkGroupId wgid);
    static StatusOr<int64_t> _read_cfs_period_us();

    static inline const std::string ROOT_PATH = "/sys/fs/cgroup/cpu/starrocks";
    static inline const std::string GROUP_ROOT_PATH = ROOT_PATH + "/root";
    static constexpr int64_t DEFAULT_CPU_PERIOD_US = 100'000;

    const int _num_cpu_cores;
    int64_t _cpu_period_us = DEFAULT_CPU_PERIOD_US;
};

Status CGroupOpsV1::init() {
    RETURN_IF_ERROR(validate_cgroup_dir(ROOT_PATH));
    RETURN_IF_ERROR(validate_cgroup_dir(_build_path(BaseDir::ROOT, CGroupFile::CPU_WEIGHT)));
    RETURN_IF_ERROR(validate_cgroup_dir(_build_path(BaseDir::ROOT, CGroupFile::TASKS)));
    RETURN_IF_ERROR(validate_cgroup_dir(_build_path(BaseDir::ROOT, CGroupFile::MAX_CPU_CORES)));
    RETURN_IF_ERROR(validate_cgroup_dir(_build_path(BaseDir::ROOT, CGroupFile::CFS_PERIOD)));

    RETURN_IF_ERROR(delete_cgroup_dir(GROUP_ROOT_PATH));
    RETURN_IF_ERROR(create_cgroup_dir(GROUP_ROOT_PATH));

    ASSIGN_OR_RETURN(_cpu_period_us, _read_cfs_period_us());

    return Status::OK();
}

Status CGroupOpsV1::create_group(WorkGroupId wgid) {
    return create_cgroup_dir(_build_path(BaseDir::GROUP_ROOT, wgid));
}

Status CGroupOpsV1::set_cpu_weight(WorkGroupId wgid, int64_t cpu_weight) {
    const auto path = _build_path(BaseDir::GROUP_ROOT, CGroupFile::CPU_WEIGHT, wgid);
    const int normalized_cpu_weight = 1024 * cpu_weight / _num_cpu_cores;
    return write_cgroup_file(path, normalized_cpu_weight, false);
}

Status CGroupOpsV1::set_max_cpu_cores(WorkGroupId wgid, int64_t max_cpu_cores) {
    const auto path = _build_path(BaseDir::GROUP_ROOT, CGroupFile::MAX_CPU_CORES, wgid);
    const int normalized_cpu_period_us = _cpu_period_us * max_cpu_cores;
    return write_cgroup_file(path, normalized_cpu_period_us, false);
}

Status CGroupOpsV1::attach(WorkGroupId wgid, int64_t tid) {
    const auto path = _build_path(BaseDir::GROUP_ROOT, CGroupFile::TASKS, wgid);
    return write_cgroup_file(path, tid, true);
}

std::string CGroupOpsV1::_to_string(BaseDir base_dir) {
    switch (base_dir) {
    case BaseDir::ROOT:
        return ROOT_PATH;
    case BaseDir::GROUP_ROOT:
    default:
        return GROUP_ROOT_PATH;
    }
}

std::string CGroupOpsV1::_to_string(CGroupFile cgroup_file) {
    switch (cgroup_file) {
    case CGroupFile::TASKS:
        return "tasks";
    case CGroupFile::CPU_WEIGHT:
        return "cpu.shares";
    case CGroupFile::MAX_CPU_CORES:
        return "cpu.cfs_quota_us";
    case CGroupFile::CFS_PERIOD:
    default:
        return "cpu.cfs_period_us";
    }
}

std::string CGroupOpsV1::_build_path(BaseDir base_dir, CGroupFile cgroup_file, WorkGroupId wgid) {
    if (wgid == ABSENT_WORKGROUP_ID) {
        return fmt::format("{}/{}", _to_string(base_dir), _to_string(cgroup_file));
    }
    return fmt::format("{}/{}/{}", _to_string(base_dir), wgid, _to_string(cgroup_file));
}

std::string CGroupOpsV1::_build_path(BaseDir base_dir, WorkGroupId wgid) {
    return fmt::format("{}/{}", _to_string(base_dir), wgid);
}

StatusOr<int64_t> CGroupOpsV1::_read_cfs_period_us() {
    const auto path = _build_path(BaseDir::GROUP_ROOT, CGroupFile::CFS_PERIOD);
    if (const auto status_or_val = read_cgroup_int64(path); status_or_val.ok() && status_or_val.value() > 0) {
        return status_or_val.value();
    }

    RETURN_IF_ERROR(write_cgroup_file(path, DEFAULT_CPU_PERIOD_US, false));
    ASSIGN_OR_RETURN(const auto val, read_cgroup_int64(path));
    if (val <= 0) {
        LOG(WARNING) << LOG_PREFIX << "Value of cfs_period_us is non-positive after write " << DEFAULT_CPU_PERIOD_US
                     << "to cgroup file [path=" << path << "]";
        return Status::InternalError(
                fmt::format("value of cfs_period_us is non-positive after write {} to cgourp file [path={}]",
                            DEFAULT_CPU_PERIOD_US, path));
    }
    return val;
}

// ------------------------------------------------------------------------------------
// CGroupOps
// ------------------------------------------------------------------------------------

CGroupOpsPtr CGroupOps::create(int num_cpu_cores) {
    auto cgroup_ops_v1 = std::make_unique<CGroupOpsV1>(num_cpu_cores);
    if (const auto status = cgroup_ops_v1->init(); !status.ok()) {
        LOG(WARNING) << LOG_PREFIX << "Failed to initialize CGroupOpsV1 [error=" << status.to_string() << "]";
        return std::make_unique<CGroupOpsDummy>();
    }
    return cgroup_ops_v1;
}

} // namespace starrocks::workgroup
