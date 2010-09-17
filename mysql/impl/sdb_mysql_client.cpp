/* Copyright (C) 2009 - ScaleDB Inc.

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; version 2 of the License.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

#include "../incl/sdb_mysql_client.h"
#include "../../scaledb/incl/SdbStorageAPI.h"
#ifdef __DEBUG_CLASS_CALLS
#include "../../../cengine/engine_util/incl/debug_class.h"
#endif

SdbMysqlClient::SdbMysqlClient(char* host, char* user, char* password, char* dbName, char* socket,
        unsigned int port, unsigned char debugLevel) :
	mysql_(0), host_(host), user_(user), password_(password), dbName_(dbName), socket_(socket),
	        port_(port), connected_(false), debugLevel_(debugLevel) {

#ifdef __DEBUG_CLASS_CALLS
	DebugClass::countClassConstructor("SdbMysqlClient");
#endif
	mysql_ = mysql_init(NULL);
	mysql_->reconnect = 1;
}

SdbMysqlClient::~SdbMysqlClient() {
#ifdef __DEBUG_CLASS_CALLS
	DebugClass::countClassDestructor("SdbMysqlClient");
#endif

	if (connected_ && mysql_) {
		mysql_close(mysql_);
	}
}

//////////////////////////////////////////////////////////////////////
//
// public function to connect and execute a query on a generic RDBMS system
// 0 is success, other numbers are failure codes
//////////////////////////////////////////////////////////////////////
int SdbMysqlClient::executeQuery(char* query, unsigned long length, bool bEngineOption) {

#ifdef SDB_DEBUG
	if (debugLevel_) {
		SDBDebugStart();
		SDBDebugPrintString("\nSdbMysqlClient::executeQuery called with:\n");
		SDBDebugPrintString(query);
		SDBDebugPrintNewLine(1);
		SDBDebugEnd();
	}
#endif

	if (!connected_) {
		if (!this->connect()) {
			return 1;
		}
	}

	int retCode = 0;

	if (bEngineOption == true) {
		retCode = sendQuery(SET_STORAGE_ENGINE_SCALEDB, (unsigned long) strlen(
		        SET_STORAGE_ENGINE_SCALEDB));
		if (retCode)
			return retCode;
	}

	retCode = sendQuery(query, length);
	return retCode;
}

//////////////////////////////////////////////////////////////////////
//
// private function to connect to a generic RDBMS system
// assumes any required initialization step is successfully done
//////////////////////////////////////////////////////////////////////
bool SdbMysqlClient::connect() {

	bool result = false;

#ifdef SDB_DEBUG
	if (debugLevel_) {
		SDBDebugStart();
		SDBDebugPrintString("\nSdbMysqlClient connecting to MYSQL with following params:\n");
		SDBDebugPrintString("\thost = ");
		SDBDebugPrintString(host_ ? host_ : (char*)"NULL");
		SDBDebugPrintString("\tuser = ");
		SDBDebugPrintString(user_ ? user_ : (char*)"NULL");
		SDBDebugPrintString("\tpassword = ");
		SDBDebugPrintString(password_ ? password_ : (char*)"NULL");
		SDBDebugPrintString("\tdatabaseName = ");
		SDBDebugPrintString(dbName_ ? dbName_ : (char*)"NULL");
		SDBDebugPrintString("\tsocket = ");
		SDBDebugPrintString(socket_ ? socket_ : (char*)"NULL");
		SDBDebugPrintString("\tport = ");
		SDBDebugPrintInt(port_);
		SDBDebugPrintNewLine(1);
		SDBDebugEnd();
	}
#endif

	if (!mysql_) {

#ifdef SDB_DEBUG
		if (debugLevel_) {
			SDBDebugStart();
			SDBDebugPrintString("\nmysql_init failed in SdbMysqlClient::connect\n");
			SDBDebugEnd();
		}
#endif

		return result;
	}

	// If other nodes take a long time to return the query result, we may get error 1159
	// (Got timeout reading communication packets ) issued by the method cli_read_query_result in client.c file.  
	// To play safe, we set the connect timeout to an high limit in case the server is very busy.  
#ifdef SDB_DEBUG
	unsigned int timeoutInSeconds = 3600; // to use with a debugger breakpoint
#else
	unsigned int timeoutInSeconds = 3600;
#endif

	int retCode = 0;
	retCode = mysql_options(mysql_, MYSQL_OPT_CONNECT_TIMEOUT, &timeoutInSeconds);
	if (retCode)
		SDBTerminate(0, "Error in setting mysql_options MYSQL_OPT_CONNECT_TIMEOUT");
	retCode = mysql_options(mysql_, MYSQL_OPT_READ_TIMEOUT, &timeoutInSeconds);
	if (retCode)
		SDBTerminate(0, "Error in setting mysql_options MYSQL_OPT_READ_TIMEOUT");
	retCode = mysql_options(mysql_, MYSQL_OPT_WRITE_TIMEOUT, &timeoutInSeconds);
	if (retCode)
		SDBTerminate(0, "Error in setting mysql_options MYSQL_OPT_WRITE_TIMEOUT");

	// mysql_real_connect(MYSQL *mysql, const char *host, const char *user, const char *passwd, const char *db, 
	//	unsigned int port, const char *unix_socket, unsigned long client_flag)
	// Note that do NOT use client flag CLIENT_MULTI_STATEMENTS because we need to know exactly
	// which statement fails.  The multi-statement executions cause confusion.
	if (mysql_real_connect(mysql_, host_, user_, password_, dbName_, port_, socket_, 0)) {
		result = true;
	}

#ifdef SDB_DEBUG
	if (debugLevel_) {
		SDBDebugStart();
		SDBDebugPrintString("\nmysql_real_connect returned: ");
		if (result) {
			SDBDebugPrintString("true");
		}
		else {
			SDBDebugPrintString("false");
		}
		SDBDebugPrintNewLine(1);
		SDBDebugEnd();
	}
#endif

	connected_ = result;

	return result;
}

int SdbMysqlClient::sendQuery(char* query, unsigned long length) {

	int rc = 0;
	rc = mysql_real_query(mysql_, query, length);

	unsigned int mysqlErrorNum = 0;
	if (rc) { // if there is an error, we fetch MySQL error number.
		mysqlErrorNum = mysql_errno(mysql_);
#ifdef SDB_DEBUG_LIGHT
		if (debugLevel_) {
			SDBDebugStart();
			SDBDebugPrintString("\nmysql_real_query (");
			SDBDebugPrintString(query);
			SDBDebugPrintString(") fails with MySQL error number  ");
			SDBDebugPrintInt(mysqlErrorNum);
			SDBDebugPrintString("; MySQL error message: ");
			const char* msg = mysql_error(mysql_);
			if (msg) {
				SDBDebugPrintString((char*) msg);
			}
			SDBDebugPrintNewLine(1);
			SDBDebugEnd();
		}
#endif

		SDBTerminate(0, "Replicate DDL to other node failed");
	}

	// clear the state after the query
	MYSQL_RES* resultSet = mysql_store_result(mysql_);
	if (resultSet) {
		mysql_free_result(resultSet);
	}

	// If it is error 1007: Can't create database '%s'; database exists,
	// then we ignore this error because a user database test is often created during installation.
	// Further a user may create database and a user table using another engine before he uses ScaleDB engine.
	// No need to check this because we change to use "CREATE DATABASE IF NOT EXISTS dbName"
	//if ( (rc) && (mysqlErrorNum == 1007) )
	//	rc = 0;

	return rc;
}
