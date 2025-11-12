#!/usr/bin/env python
# -- coding: utf-8 --
###########################################################################
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
###########################################################################

import mysql.connector

from lib import close_conn


class MysqlPreparedStmtLib(object):
    def __init__(self):
        self.connector = None
        self.prepared_stmt_cursor = None

    def connect(self, query_dict):
        if "database" not in query_dict:
            self.connector = mysql.connector.connect(
                host=query_dict["host"],
                user=query_dict["user"],
                port=int(query_dict["port"]),
                passwd=query_dict["password"],
                allow_multi_statements = True,
            )
        else:
            self.connector = mysql.connector.connect(
                host=query_dict["host"],
                user=query_dict["user"],
                port=int(query_dict["port"]),
                passwd=query_dict["password"],
                db=query_dict["database"],
                allow_multi_statements = True,
            )
        self.prepared_stmt_cursor = self.connector.cursor(prepared=True)

    def close(self):
        if self.prepared_stmt_cursor:
            self.prepared_stmt_cursor.close()
            self.prepared_stmt_cursor = None
        if self.connector:
            close_conn(self.connector, "MySQL for Prepared Statement")
            self.connector = None

    def execute_prepared(self, sql, params):
        self.prepared_stmt_cursor.execute(sql, params)
        return self.prepared_stmt_cursor.fetchall()

    def execute(self, sql):
        cursor = self.connector.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()
