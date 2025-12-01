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

package com.starrocks.service.arrow.flight.sql;

import com.google.common.base.Preconditions;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.ProxyContextManager;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.sql.ast.KillStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.LargeInPredicateException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TMasterOpRequest;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.thrift.TUniqueId;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.starrocks.service.arrow.flight.sql.ArrowFlightSqlServiceImpl.buildSchema;

// inherit ConnectProcessor to record the audit log and Query Detail
public class ArrowFlightSqlConnectProcessor extends ConnectProcessor {
    private static final Logger LOG = LogManager.getLogger(ArrowFlightSqlConnectProcessor.class);

    private final CompletableFuture<Coordinator> deploymentFinished;
    private final String originStmt;

    public ArrowFlightSqlConnectProcessor(ConnectContext context, String originStmt) {
        this(context, new CompletableFuture<>(), originStmt);
    }

    public ArrowFlightSqlConnectProcessor(ConnectContext context, CompletableFuture<Coordinator> deploymentFinished,
                                          String originStmt) {
        super(context);
        this.deploymentFinished = deploymentFinished;
        this.originStmt = originStmt;
    }

    @Override
    public void processOnce() {
        // Set status of query to OK.
        ctx.getState().reset();
        executor = null;

        // Only handle query，so no need to dispatch
        ctx.setCommand(MysqlCommand.COM_QUERY);
        ctx.setStartTime();
        ctx.setResourceGroup(null);
        ctx.resetErrorCode();

        this.handleQuery();

        ctx.setLastQueryId(ctx.getQueryId());
        ctx.setQueryId(null);

        // Set command as sleep, so timeCheck will close the connection.
        // When client's last query is long long ago (controlled by waitTimeout session variable).
        ctx.setStartTime();
        ctx.setCommand(MysqlCommand.COM_SLEEP);
    }

    @Override
    protected void handleQuery() {
        MetricRepo.COUNTER_REQUEST_ALL.increase(1L);
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(ctx.getRemoteIP())
                .setUser(ctx.getQualifiedUser())
                .setAuthorizedUser(
                        ctx.getCurrentUserIdentity() == null ? "null" : ctx.getCurrentUserIdentity().toString())
                .setDb(ctx.getDatabase())
                .setCatalog(ctx.getCurrentCatalog());
        Tracers.register(ctx);

        StatementBase parsedStmt = null;
        try {
            parsedStmt = runWithParserStageRetry();
        } catch (IOException e) {
            // Client failed.
            LOG.warn("Process one query failed because IOException: ", e);
            ctx.getState().setError("StarRocks process failed");
            ctx.getState().setErrType(QueryState.ErrType.IO_ERR);
        } catch (StarRocksException e) {
            LOG.warn("Process one query failed. SQL: " + originStmt + ", because.", e);
            ctx.getState().setError(e.getMessage());
            // set is as ANALYSIS_ERR so that it won't be treated as a query failure.
            ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe StarRocks bug.
            LOG.warn("Process one query failed. SQL: " + originStmt + ", because unknown reason: ", e);
            ctx.getState().setError("Unexpected exception: " + e.getMessage());
            if (parsedStmt instanceof KillStmt) {
                // ignore kill stmt execute err(not monitor it)
                ctx.getState().setErrType(QueryState.ErrType.IGNORE_ERR);
            } else {
                ctx.getState().setErrType(QueryState.ErrType.INTERNAL_ERR);
            }
        } finally {
            Tracers.close();
        }

        setResultFromLeaderIfForwarded();

        if (executor != null) {
            auditAfterExec(originStmt, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
            executor.addFinishedQueryDetail();
        } else {
            auditAfterExec(originStmt, parsedStmt, null);
        }
    }

    @Override
    protected StmtExecutor doProxyExecute(TMasterOpResult result, TMasterOpRequest request, StatementBase statement,
                                          ProxyContextManager.ScopeGuard scopeGuard)
            throws Exception {
        if (request.isIsInternalStmt()) {
            throw new IllegalArgumentException("Arrow Flight SQL does not support internal statement");
        }

        ArrowFlightSqlConnectContext arrowCtx = (ArrowFlightSqlConnectContext) ctx;
        arrowCtx.resetForStatement();

        CompletableFuture<Void> processorFinished = new CompletableFuture<>();
        StmtExecutor executor = new StmtExecutor(ctx, statement, deploymentFinished);
        ctx.setExecutor(executor);
        executor.setProxy();

        scopeGuard.deactive();

        ArrowFlightSqlServiceImpl.submitTask(() -> {
            try {
                executor.execute();
                deploymentFinished.complete(null);
                processorFinished.complete(null);
            } catch (Exception e) {
                deploymentFinished.completeExceptionally(e);
                processorFinished.completeExceptionally(e);
            } finally {
                scopeGuard.activate();
                scopeGuard.close();
            }
        });

        Coordinator coordinator = deploymentFinished.get();
        if (arrowCtx.returnFromFE()) {
            processorFinished.get();
            return executor;
        }

        if (coordinator == null || ctx.getState().isError()) {
            return executor;
        }

        Preconditions.checkState(coordinator instanceof DefaultCoordinator,
                "Coordinator is not DefaultCoordinator, cannot proceed with BE execution.");
        DefaultCoordinator defaultCoordinator = (DefaultCoordinator) coordinator;

        ExecutionFragment rootFragment = defaultCoordinator.getExecutionDAG().getRootFragment();
        FragmentInstance rootFragmentInstance = rootFragment.getInstances().get(0);

        ComputeNode worker = rootFragmentInstance.getWorker();
        result.setArrow_flight_sql_result_backend_id(worker.getId());

        TUniqueId rootFragmentInstanceId = rootFragmentInstance.getInstanceId();
        result.setArrow_flight_sql_result_fragment_id(rootFragmentInstanceId);

        ExecPlan execPlan = defaultCoordinator.getJobSpec().getExecPlan();
        Preconditions.checkNotNull(execPlan, "execPlan is null");
        Schema schema = buildSchema(execPlan, ctx.getSessionVariable());
        result.setArrow_flight_sql_result_schema(schema.serializeAsMessage());

        return executor;
    }

    public void handleResultFromFE() {
        if (ctx.getState().isError()) {
            reportError(ctx);
        }

        ArrowFlightSqlConnectContext arrowCtx = (ArrowFlightSqlConnectContext) ctx;

        if (executor != null && executor.isForwardToLeader()) {
            ShowResultSet resultSet = executor.getShowResultSet();
            if (resultSet != null) {
                arrowCtx.addShowResult(DebugUtil.printId(arrowCtx.getQueryId()), resultSet);
                return;
            }
        }

        String queryId = DebugUtil.printId(arrowCtx.getExecutionId());
        if (arrowCtx.getResult(queryId) == null) {
            arrowCtx.setEmptyResultIfNotExist(queryId);
        }
    }

    public Triple<Long, TUniqueId, Schema> handleResultFromBE(Coordinator coordinator) {
        if (ctx.getState().isError()) {
            reportError(ctx);
        }

        if (executor != null && executor.isForwardToLeader()) {
            Triple<Long, TUniqueId, Schema> info = executor.getProxyArrowFlightSQLResultInfo();
            if (info != null) {
                return info;
            }
        }

        if (coordinator == null) {
            reportError(ctx);
        }

        Preconditions.checkState(coordinator instanceof DefaultCoordinator,
                "Coordinator is not DefaultCoordinator, cannot proceed with BE execution.");
        DefaultCoordinator defaultCoordinator = (DefaultCoordinator) coordinator;

        ExecutionFragment rootFragment = defaultCoordinator.getExecutionDAG().getRootFragment();
        FragmentInstance rootFragmentInstance = rootFragment.getInstances().get(0);
        ComputeNode worker = rootFragmentInstance.getWorker();
        TUniqueId rootFragmentInstanceId = rootFragmentInstance.getInstanceId();

        ExecPlan execPlan = defaultCoordinator.getJobSpec().getExecPlan();
        Preconditions.checkNotNull(execPlan, "execPlan is null");
        Schema schema = buildSchema(execPlan, ctx.getSessionVariable());

        return Triple.of(worker.getId(), rootFragmentInstanceId, schema);
    }

    private void reportError(ConnectContext ctx) throws RuntimeException {
        String errMsg = ctx.getState().getErrorMessage();
        if (StringUtils.isEmpty(errMsg)) {
            errMsg = "Unknown error";
        }
        throw new RuntimeException(String.format("failed to process query [queryID=%s] [error=%s]",
                DebugUtil.printId(ctx.getExecutionId()), errMsg));
    }

    private StatementBase runWithParserStageRetry() throws Exception {
        try {
            return executeQueryAttempt();
        } catch (LargeInPredicateException e) {
            final boolean originalEnableLargeInPredicate = ctx.getSessionVariable().enableLargeInPredicate();
            try {
                ctx.getSessionVariable().setEnableLargeInPredicate(false);
                LOG.warn("Retrying query with enable_large_in_predicate=false");
                Tracers.record(Tracers.Module.BASE, "retry_with_large_in_predicate_exception", "true");
                ((ArrowFlightSqlConnectContext) ctx).resetForStatement();
                return executeQueryAttempt();
            } finally {
                ctx.getSessionVariable().setEnableLargeInPredicate(originalEnableLargeInPredicate);
            }
        }
    }

    private StatementBase executeQueryAttempt() throws Exception {
        StatementBase parsedStmt = parse(originStmt, ctx.getSessionVariable());
        Tracers.init(ctx, parsedStmt.getTraceMode(), parsedStmt.getTraceModule());

        executor = new StmtExecutor(ctx, parsedStmt, deploymentFinished);
        ctx.setIsLastStmt(true);
        ctx.setExecutor(executor);

        executor.addRunningQueryDetail(parsedStmt);
        executor.execute();

        return parsedStmt;
    }

    private StatementBase parse(String sql, SessionVariable sessionVariables) {
        List<StatementBase> stmts;
        try (Timer ignored = Tracers.watchScope(Tracers.Module.PARSER, "Parser")) {
            stmts = com.starrocks.sql.parser.SqlParser.parse(sql, sessionVariables);
        }

        if (stmts.size() > 1) {
            throw new RuntimeException("arrow flight sql query does not support execute multiple query");
        }

        StatementBase parsedStmt = stmts.get(0);
        parsedStmt.setOrigStmt(new OriginStatement(sql));
        return parsedStmt;
    }

    private void setResultFromLeaderIfForwarded() {
        if (executor == null || !executor.isForwardToLeader()) {
            return;
        }

        if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            return;
        }

        ArrowFlightSqlConnectContext arrowCtx = (ArrowFlightSqlConnectContext) ctx;
        ShowResultSet resultSet = executor.getShowResultSet();
        if (resultSet != null) {
            arrowCtx.addShowResult(DebugUtil.printId(arrowCtx.getQueryId()), resultSet);
        }
    }
}
