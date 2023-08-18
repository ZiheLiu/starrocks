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

package com.starrocks.qe.scheduler;

import com.starrocks.common.Status;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.qe.RowBatch;
import com.starrocks.thrift.TReportExecStatusParams;

public interface Scheduler {
    default void exec() throws Exception {
        startScheduling();
    }

    /**
     * Start scheduling fragments of this job, mainly containing the following work:
     * <ul>
     *     <li> Instantiates multiple parallel instances of each fragment.
     *     <li> Assigns these fragment instances to appropriate workers (including backends and compute nodes).
     *     <li> Deploys them to the related workers, if the parameter {@code needDeploy} is true.
     * </ul>
     * <p>
     */
    void startScheduling() throws Exception;

    void updateFragmentExecStatus(TReportExecStatusParams params);

    default void cancel() {
        cancel(PPlanFragmentCancelReason.USER_CANCEL, "");
    }

    void cancel(PPlanFragmentCancelReason reason, String message);

    void onFinished();

    RowBatch getNext() throws Exception;

    boolean join(int timeoutSecond);

    boolean isDone();

    Status getExecStatus();

    CoordinatorState getState();

    RuntimeProfile getQueryProfile();

    void endProfile();

    String getSchedulerExplain() throws Exception;

}
