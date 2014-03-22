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

#define SOURCE_MYSQL_TXN	64

MysqlTxn::MysqlTxn() { //constructor
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
}

MysqlTxn::~MysqlTxn() { // destructor
#ifdef __DEBUG_CLASS_CALLS
	DebugClass::countClassDestructor("MysqlTxn");
#endif
	// memory released in method freeAllQueryManagerIds()
	if (pAlterTableName_)
		FREE_MEMORY(pAlterTableName_);
}

void MysqlTxn::addAlterTableName(char* pAlterTableName) {
	if (pAlterTableName_)
		FREE_MEMORY(pAlterTableName_);

	pAlterTableName_ = SDBUtilDuplicateString(pAlterTableName);
}
;

// release the memory allocated for pAlterTableName_
void MysqlTxn::removeAlterTableName() {
	if (pAlterTableName_)
		FREE_MEMORY(pAlterTableName_);

	pAlterTableName_ = NULL;
}

#endif	// SDB_MYSQL
