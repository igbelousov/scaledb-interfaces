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

//
//  File Name: mysql_txn.h
//
//  Description: This class contains MySQL thread_id, ScaleDB user id, and lock count information.
//    A MysqlTxn object is instantiated when a user first executes query.
//    A MysqlTxn object is freed when a user logs off (or close connection).
//
//  Version: 1.0
//
//  Copyright: ScaleDB, Inc. 2007
//
//  History: 10/18/2007  RCH   converted from Java.
//

#ifdef SDB_MYSQL

#ifndef _MYSQL_TXN_H
#define _MYSQL_TXN_H

/*
Note that ScaleDB's header files must come before MySQL header file.
This is because we have STL header files which must be declared before C header files.
 */
#include "../../scaledb/incl/SdbStorageAPI.h"

#define ERRORNUM_INTERFACE_MYSQL_TXN 20000
#define INITIAL_LOCK_TABLES_IN_VECTOR  10
#define MAX_BLOB_COLUMNS 64

struct QueryManagerInfo {  	
	unsigned short queryMgrId_;
	unsigned short designatorId_;
	char* designatorName_;  
    char* tableAliasName_; // table alias name, used for multiple designators on same real tables
	char* pKey_;   // points to the key value used in index_next_same
	unsigned int keyLength_;
};

// Compiler usually takes 16 bytes for this structure
struct ScaledbSavepointInfo {
	char* savepointName_;
	uint64 logId_;
};

typedef struct ScaledbSavepointInfo ScaledbSavepointInfo_struct;

class MysqlTxn {

public:

    // constructor and destructor
    MysqlTxn();
    ~MysqlTxn(); 

	unsigned long getMysqlThreadId() { return mysqlThreadId_; }
	void setMysqlThreadId(unsigned long aThreadId) { mysqlThreadId_ = aThreadId; }
	unsigned int getScaleDbUserId() { return scaleDbUserId_; }
	void setScaleDbUserId(unsigned int aUserId) { scaleDbUserId_ = aUserId; }
	unsigned int getScaledbDbId() { return scaledbDbId_; }
	void setScaledbDbId(unsigned int aScaledbDbId) { scaledbDbId_ = aScaledbDbId; }
	unsigned int getScaleDbTxnId() { return scaleDbTxnId_; }
	void setScaleDbTxnId(unsigned int aTxnId) { scaleDbTxnId_ = aTxnId; }
	bool getActiveTxn() { return (activeTxn_>0 ? true : false); }	// go around compiler bug
	void setActiveTrn(bool aBool)  { activeTxn_ = (aBool) ? 1 : 0; }

	void addQueryManagerId(bool isRealIndex, char* pDesignatorName, char* pTabAlias, char* pKey, 
			unsigned int aKenLength, unsigned short aQueryMgrId);
	unsigned short findQueryManagerId(char* aDesignatorName, char *aTabAlias, char* aKey, unsigned int aKenLength,
										bool virtualTableFlag = false);
	void freeAllQueryManagerIds();
	char* getDesignatorNameByQueryMrgId(unsigned short aQueryMgrId);
	unsigned short getDesignatorIdByQueryMrgId(unsigned short aQueryMgrId);

    void setLastStmtSavePointId(uint64 id) {
        lastStmtSavePointId_ = id;
    }
    uint64 getLastStmtSavePointId() {
        return lastStmtSavePointId_;
    }

	// add a table name specified in LOCK TABLES statement
	void addLockTableName(char* pLockTableName);
	// unlock a table which was specified in an earlier LOCK TABLES statement.
	// Return non-zero if the table name is added by an earlier LOCK TABLES statement.
	int removeLockTableName(char* pLockTableName);
	// unlock all tables which were specified in earlier LOCK TABLES statements
	void releaseAllLockTables();
	// get the net number of lock tables
	unsigned int getNumberOfLockTables() { return SDBArrayGetNumberOfNetElements(pLockTablesArray_); }

	int lockCount_;   // number of table locks used in a statement
	int numberOfLockTables_;		// number of tables specified in LOCK TABLES statement
	unsigned long  txnIsolationLevel_;	// transaciton isolation level
	int QueryManagerIdCount_;

private:
	unsigned long mysqlThreadId_;
	unsigned int scaleDbUserId_;	// The UserId is actually a session id.
    unsigned int scaleDbTxnId_;
	unsigned short scaledbDbId_;	// current DbId used by ScaleDB
//	bool  activeTxn_;				// whether or not it is within a transaction at the moment
	unsigned int  activeTxn_;			// need to use integer rather than bool due to a compiler bug??
	SdbDynamicArray* pLockTablesArray_;	// pointer to vector holding all lock table names
    uint64  lastStmtSavePointId_;   // last stmt save point id

	QueryManagerInfo queryMgrArray_[METAINFO_MAX_QUERY_MANAGER_ID];
};

#endif   // _MYSQL_TXN_H

#endif	// SDB_MYSQL

