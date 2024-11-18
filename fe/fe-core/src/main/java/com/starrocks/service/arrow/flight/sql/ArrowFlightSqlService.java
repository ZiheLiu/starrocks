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

import com.starrocks.service.FrontendOptions;
import com.starrocks.service.arrow.flight.sql.auth2.ArrowFlightSqlAuthenticator;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class ArrowFlightSqlService {

    private static final Logger LOG = LogManager.getLogger(ArrowFlightSqlService.class);

    protected volatile boolean running;

    private final Location location;
    private final Location feEndpoint;
    private final FlightServer flightServer;

    public ArrowFlightSqlService(int port) {
        BufferAllocator allocator = new RootAllocator();
        this.location = Location.forGrpcInsecure("0.0.0.0", port);
        this.feEndpoint = Location.forGrpcInsecure(FrontendOptions.getLocalHostAddress(), port);

        ArrowFlightSqlSessionManager sessionManager = new ArrowFlightSqlSessionManager();

        ArrowFlightSqlServiceImpl producer = new ArrowFlightSqlServiceImpl(sessionManager, feEndpoint);
        ArrowFlightSqlAuthenticator authenticator = new ArrowFlightSqlAuthenticator(sessionManager);

        this.flightServer = FlightServer.builder(allocator, location, producer)
                .headerAuthenticator(authenticator)
                .build();
    }

    public void start() {
        try {
            flightServer.start();
            running = true;
            LOG.info("[ARROW] Arrow Flight SQL server start [location={}] [feEndpoint={}].", location, feEndpoint);
            flightServer.awaitTermination();
        } catch (InterruptedException e) {
            LOG.error("[ARROW] Arrow Flight SQL server was interrupted", e);
            Thread.currentThread().interrupt();
            System.exit(-1);
        } catch (Exception e) {
            LOG.error("[ARROW] Arrow Flight SQL server start failed");
            System.exit(-1);
        }
    }

    public void stop() {
        if (running) {
            running = false;
            try {
                LOG.info("[ARROW] Stopping Arrow Flight SQL server .");
                flightServer.shutdown();
                flightServer.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.warn("[ARROW] Interrupted while stopping Arrow Flight SQL server", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOG.warn("[ARROW] Error while stopping Arrow Flight SQL server", e);
            }
        }
    }

}