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

SdbMysqlClient::SdbMysqlClient(char* host, char* user, char* password, char* socket, unsigned int port, unsigned char debugLevel) : 

	mysql_(0), host_(host), user_(user), password_(password), socket_(socket), port_(port), connected_(false), debugLevel_(debugLevel) {

	mysql_ = mysql_init(NULL);

	mysql_->reconnect= 1;
}

SdbMysqlClient::~SdbMysqlClient(){

	if (connected_ && mysql_) {
		mysql_close(mysql_);
	}
}

//////////////////////////////////////////////////////////////////////
//
// public function to connect and execute a query on a generic RDBMS system
// 0 is success, other numbers are failure codes
//////////////////////////////////////////////////////////////////////
int SdbMysqlClient::executeQuery(char* query, unsigned long length){

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

	int result = sendQuery(query, length);

	return result;
}

//////////////////////////////////////////////////////////////////////
//
// private function to connect to a generic RDBMS system
// assumes any required initialization step is successfully done
//////////////////////////////////////////////////////////////////////
bool SdbMysqlClient::connect(){

	bool result = false;

#ifdef SDB_DEBUG
	if (debugLevel_) {
		SDBDebugStart();
		SDBDebugPrintString("\nSdbMysqlClient connecting to MYSQL with following params:\n");
		SDBDebugPrintString("\n\thost = ");
		SDBDebugPrintString(host_ ? host_ : (char*)"NULL");
		SDBDebugPrintString("\n\tuser = ");
		SDBDebugPrintString(user_ ? user_ : (char*)"NULL");
		SDBDebugPrintString("\n\tpassword = ");
		SDBDebugPrintString(password_ ? password_ : (char*)"NULL");
		SDBDebugPrintString("\n\tsocket = ");
		SDBDebugPrintString(socket_ ? socket_ : (char*)"NULL");
		SDBDebugPrintString("\n\tport = ");
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

	// set the connect timeout to an upper limit;  TODO: does not help.
	//unsigned int timeoutInSeconds = 600;
	//int retCode = mysql_options(mysql_, MYSQL_OPT_CONNECT_TIMEOUT, &timeoutInSeconds);
	//if (retCode)
	//	DataUtil::terminateEngine(__FILE__, __LINE__, "Error in setting mysql_options");

	// (MYSQL *mysql, const char *host, const char *user, const char *passwd, const char *db, 
	//	unsigned int port, const char *unix_socket, unsigned long client_flag)
	// Note that do NOT use client flag CLIENT_MULTI_STATEMENTS because we need to know exactly
	// which statement fails.  The multi-statement executions cause confusion.
	if (mysql_real_connect(mysql_, host_, user_, password_, NULL, port_, socket_, 0)) {
		result = true;
	}

#ifdef SDB_DEBUG
	if (debugLevel_) {
		SDBDebugStart();
		SDBDebugPrintString("\nmysql_real_connect returned: ");
		if (result){
			SDBDebugPrintString("true");
		}
		else{
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

	int rc = 1;

	rc = mysql_real_query(mysql_, query, length);



#ifdef SDB_DEBUG
	if (debugLevel_) {
		SDBDebugStart();
		SDBDebugPrintString("\nmysql_real_query (");
		SDBDebugPrintString(query);
		SDBDebugPrintString(") returned: ");
		SDBDebugPrintInt(rc);
		if (!rc) {
			SDBDebugPrintString(" success");
		}
		else {
			// TODO: If other nodes take a long time (more than 20 seconds) to return the query result,  
			// we may get packet_error (error number 1159) issued by the method cli_read_query_result in client.c file.  
			SDBDebugPrintString(" failure: MySQL error number ");
			unsigned int mysqlErrorNum = mysql_errno(mysql_);
			SDBDebugPrintInt(mysqlErrorNum);
			SDBDebugPrintString("; MySQL error message: ");
			const char* msg = mysql_error(mysql_);
			if (msg) {
				SDBDebugPrintString((char*)msg);
			}
		}
		SDBDebugPrintNewLine(1);
		SDBDebugEnd();
	}
#endif


	// clear the state after the query
	MYSQL_RES* resultSet = mysql_store_result(mysql_);
	if (resultSet) {
		mysql_free_result(resultSet);
	}

	return rc;
}
