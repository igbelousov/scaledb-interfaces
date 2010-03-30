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


//////////////////////////////////////////////////////////////////////
//
//  File Name: mysql_txn.cpp
//
//  Description: This file contains the relevant information for a user thread.
//    We save both MySQL thread_id, ScaleDB user id, transaction id and lock count information here.
//
//  Version: 1.0
//
//  Copyright: ScaleDB, Inc. 2007
//
//  History: 10/18/2007  RCH   converted from Java.
//
//
//////////////////////////////////////////////////////////////////////
#ifdef SDB_MYSQL

#include "../incl/mysql_txn.h"
#include <string.h>
#ifdef __DEBUG_CLASS_CALLS
#include "../../../cengine/engine_util/incl/debug_class.h"
#endif

MysqlTxn::MysqlTxn() {  //constructor
#ifdef __DEBUG_CLASS_CALLS
	DebugClass::countClassConstructor("MysqlTxn");
#endif
    mysqlThreadId_ = 0;
	scaleDbUserId_ = 0;
    scaleDbTxnId_ = 0;
	scaledbDbId_ = 0;
	activeTxn_ = 0;
    lockCount_ = 0;
	numberOfLockTables_ = 0;
    lastStmtSavePointId_ = 0;
	ddlFlag_ = 0;
	pAlterTableName_ = NULL;

	QueryManagerIdCount_ = 0;
	for (int i=0; i < METAINFO_MAX_QUERY_MANAGER_ID; ++i) {
		queryMgrArray_[i].pDesignatorName_ = NULL;
        queryMgrArray_[i].pHandler_ = NULL;
		queryMgrArray_[i].pKey_ = NULL;
		queryMgrArray_[i].keyLength_ = 0;
		queryMgrArray_[i].queryMgrId_ = 0;
        queryMgrArray_[i].scanSequential_ = false;
	}

    // use a vector to save all lock table names
	pLockTablesArray_ = SDBArrayInit( 10, 10, sizeof (void *));
}


MysqlTxn::~MysqlTxn() {   // destructor
#ifdef __DEBUG_CLASS_CALLS
	DebugClass::countClassDestructor("MysqlTxn");
#endif
	// memory released in method freeAllQueryManagerIds()
	SDBArrayFreeWithMembers(pLockTablesArray_);
	if (pAlterTableName_)
		RELEASE_MEMORY(pAlterTableName_);
}


// save the query manager id for a given designator in a given table handler.
// Note that the table handler can determine the table name, table alias name.
// Using table handler can uniquely define the right table object currently being used by MySQL query processor.
void MysqlTxn::addQueryManagerId(bool isRealIndex, char* pDesignatorName, void* pHandler, char* pKey, 
				unsigned int aKenLength, unsigned short aQueryMgrId, unsigned char mysqlInterfaceDebugLevel) {
	//queryMgrArray_[QueryManagerIdCount_].pMetaInfo_ = pMetaInfo;
	queryMgrArray_[QueryManagerIdCount_].queryMgrId_ = aQueryMgrId;
	queryMgrArray_[QueryManagerIdCount_].pDesignatorName_ = SDBUtilDuplicateString(pDesignatorName);
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel > 1) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MysqlTxn::addQueryManagerId, add new designator ");
		SDBDebugPrintString( queryMgrArray_[QueryManagerIdCount_].pDesignatorName_ );
		SDBDebugPrintString(" pHandler= ");
		SDBDebugPrintPointer( pHandler );
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	if ( isRealIndex )
		queryMgrArray_[QueryManagerIdCount_].designatorId_ = SDBGetIndexNumberByName(scaledbDbId_, pDesignatorName);
	else
		queryMgrArray_[QueryManagerIdCount_].designatorId_ = 0;

    queryMgrArray_[QueryManagerIdCount_].pHandler_ = pHandler;
	queryMgrArray_[QueryManagerIdCount_].pKey_ = pKey;
	queryMgrArray_[QueryManagerIdCount_].keyLength_ = aKenLength;
    queryMgrArray_[QueryManagerIdCount_].scanSequential_ = false;

	++QueryManagerIdCount_ ;
	
}


// Find the query manager id based on the designator name for a given table handler
unsigned short MysqlTxn::findQueryManagerId(char* aDesignatorName, void* pHandler, char* aKey, 
											unsigned int aKenLength, bool virtualTableFlag) {
	for (int i=0; i < QueryManagerIdCount_; ++i) {

		// We found the query manger id if both the table handler object and designator match. 
		if ( (queryMgrArray_[i].pHandler_ == pHandler) &&
				SDBUtilCompareStrings(aDesignatorName, queryMgrArray_[i].pDesignatorName_, true) ) {

			queryMgrArray_[i].pKey_ = (char*) aKey;    // update its key value
			queryMgrArray_[i].keyLength_ = aKenLength;
			return (queryMgrArray_[i].queryMgrId_);
		}
	}
	
	return 0;
}


void MysqlTxn::freeAllQueryManagerIds(unsigned char mysqlInterfaceDebugLevel) {
	for (int i=0; i < QueryManagerIdCount_; ++i) {
		SDBCloseQueryManager(queryMgrArray_[i].queryMgrId_  );

#ifdef SDB_DEBUG_LIGHT
		if (mysqlInterfaceDebugLevel > 1) {
			SDBDebugStart();			// synchronize threads printout	
			SDBDebugPrintHeader("MysqlTxn::freeAllQueryManagerIds, release designator ");
			SDBDebugPrintString( queryMgrArray_[i].pDesignatorName_ );
			SDBDebugPrintString(" pHandler= ");
			SDBDebugPrintPointer( queryMgrArray_[i].pHandler_ );
			SDBDebugEnd();			// synchronize threads printout	
		}
#endif
		RELEASE_MEMORY( queryMgrArray_[i].pDesignatorName_ );
        queryMgrArray_[i].pHandler_ = NULL;
		queryMgrArray_[i].pKey_ = NULL;
		queryMgrArray_[i].keyLength_ = 0;
		queryMgrArray_[i].queryMgrId_ = 0;
        queryMgrArray_[i].scanSequential_ = false;
	}
	QueryManagerIdCount_ = 0;
}

char* MysqlTxn::getDesignatorNameByQueryMrgId(unsigned short aQueryMgrId) {
	for (int i=0; i < QueryManagerIdCount_; ++i) {
		if ( queryMgrArray_[i].queryMgrId_ == aQueryMgrId ) {
			return (queryMgrArray_[i].pDesignatorName_);
		}
	}

	return NULL;	// not found
}

void MysqlTxn::setScanType(unsigned short queryMgrId, bool sequentialScan/*=false*/) {
	for (int i=0; i < QueryManagerIdCount_; ++i) {
		if ( queryMgrArray_[i].queryMgrId_ == queryMgrId ) {
	        queryMgrArray_[i].scanSequential_ = sequentialScan;
		}
	}
}

bool MysqlTxn::isSequentialScan(unsigned short queryMgrId) {
	for (int i=0; i < QueryManagerIdCount_; ++i) {
		if ( queryMgrArray_[i].queryMgrId_ == queryMgrId ) {
	        return queryMgrArray_[i].scanSequential_;
		}
	}
    return false;
}


unsigned short MysqlTxn::getDesignatorIdByQueryMrgId(unsigned short aQueryMgrId) {
	for (int i=0; i < QueryManagerIdCount_; ++i) {
		if ( queryMgrArray_[i].queryMgrId_ == aQueryMgrId ) {
			return (queryMgrArray_[i].designatorId_);
		}
	}

	return 0;	// not found
}


// add a table name specified in LOCK TABLES statement
void MysqlTxn::addLockTableName(char* pLockTableName) {
	
	unsigned int maxPosition = SDBArrayGetNumberOfElements(pLockTablesArray_);
	unsigned int i = 1;
	for ( i=1; i <= maxPosition; ++i) {	// element starts at position 1
		if ( SDBArrayIsElementDefined(pLockTablesArray_, i) == false)
			continue;	// skip the not-defined element which has been removed earlier

		char* pCurrTableName = (char*) SDBArrayGetPtr(pLockTablesArray_, i);
		if ( SDBUtilCompareStrings(pCurrTableName, pLockTableName, true, (unsigned short)strlen(pLockTableName)) )
			break;
	}

	if ( i > maxPosition ) {	// add lock table name if not found.
		unsigned short position = SDBArrayGetUnUsedPosition(pLockTablesArray_);	// get unused location in the LockTables array
		char* pLockTableNameLC = SDBUtilGetStrInLower( pLockTableName );
		// put lock table name (in lower case) in array
		SDBArrayPutPtr(pLockTablesArray_, position, pLockTableNameLC );
		numberOfLockTables_ += 1;
	}
}

// unlock a table which was specified in an earlier LOCK TABLES statement
// For DROP TABLE statement, it is fine if we do not find the given table name.
// Return non-zero if the table name is added by an earlier LOCK TABLES statement.
int MysqlTxn::removeLockTableName(char* pLockTableName) {
	int retValue = 0;
	unsigned short tableNum;

	unsigned int maxPosition = SDBArrayGetNumberOfElements(pLockTablesArray_);
	for (unsigned int i=1; i <= maxPosition; ++i) {	// element starts at position 1
		if ( SDBArrayIsElementDefined(pLockTablesArray_, i) == false)
			continue;	// skip the not-defined element which has been removed earlier

		char* pCurrTableName = (char*) SDBArrayGetPtr(pLockTablesArray_, i);
		if ( SDBUtilCompareStrings(pCurrTableName, pLockTableName, true, (unsigned short)strlen(pLockTableName)) ) {
			tableNum = SDBGetTableNumberByName(scaleDbUserId_, scaledbDbId_, pLockTableName );
			if ( tableNum == 0 ) 
				SDBTerminate(IDENTIFIER_INTERFACE + ERRORNUM_INTERFACE_MYSQL_TXN + 2,  // ERROR - 16020002
					"Table definition is out of sync between MySQL and ScaleDB engine.\0" );

			// instruct the engine to release the table level lock
			SDBReleaseLockTable(scaleDbUserId_, scaledbDbId_, tableNum);

			// remove this table from the LockTables array
			RELEASE_MEMORY( pCurrTableName );
			SDBArrayPutNull(pLockTablesArray_, i);  	// need to set pointer to NULL as we already deleted the object
			SDBArrayRemove(pLockTablesArray_, i);

			if (numberOfLockTables_ > 0) {
				numberOfLockTables_ -= 1;
				retValue = 1;
			}

			break;
		}
	}

	return retValue;
}


// unlock all tables which were specified in earlier LOCK TABLES statements
void MysqlTxn::releaseAllLockTables() {
	unsigned short tableNum;

	unsigned int maxPosition = SDBArrayGetNumberOfElements(pLockTablesArray_);
	for (unsigned int i=1; i <= maxPosition; ++i) {	// element starts at position 1
		if ( SDBArrayIsElementDefined(pLockTablesArray_, i) == false)
			continue;	// skip the not-defined element which has been removed earlier

		char* pCurrTableName = (char*) SDBArrayGetPtr(pLockTablesArray_, i);
		tableNum = SDBGetTableNumberByName(scaleDbUserId_, scaledbDbId_, pCurrTableName );
		if ( tableNum > 0 ) {
			// We can ignore the case when tableNum is 0 as a table may be dropped by another user.
			// Instruct the engine to release the table level lock
			SDBReleaseLockTable(scaleDbUserId_, scaledbDbId_, tableNum);
		}

		// remove this table from the LockTables array
		RELEASE_MEMORY( pCurrTableName );
		SDBArrayPutNull(pLockTablesArray_, i);	// need to set pointer to NULL as we already deleted the object
		SDBArrayRemove(pLockTablesArray_, i);

		numberOfLockTables_ -= 1;
	}
}


// save the table name used in ALTER TABLE statement (used in cluster environment) 
void MysqlTxn::addAlterTableName(char* pAlterTableName) { 
	if (pAlterTableName_)
		RELEASE_MEMORY( pAlterTableName_ );

	pAlterTableName_= SDBUtilDuplicateString(pAlterTableName); 
};


// release the memory allocated for pAlterTableName_
void MysqlTxn::removeAlterTableName() {
	if (pAlterTableName_)
		RELEASE_MEMORY( pAlterTableName_ );

	pAlterTableName_ = NULL;
}

#endif	// SDB_MYSQL
