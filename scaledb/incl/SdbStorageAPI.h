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

/*
  File Name: SdbStorageAPI.h

  Description: ScaleDB Storage API

  Version: 1.0

  Copyright: ScaleDB, Inc. 2007

  History: 09/23/2009  Venu

*/

#ifndef _SDB_STORAGE_API_H
#define _SDB_STORAGE_API_H

#ifdef __cplusplus
extern "C"
{
#endif

#ifdef RC_INVOKED
#define stdin
#endif

#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#ifdef __cplusplus
}
#endif

#if defined(WIN32) || defined(WINDOWS) || defined(_WIN_) || defined(_WIN32)

#ifndef SDB_WINDOWS
#define SDB_WINDOWS
#endif
#include <windows.h>

#if defined(_WIN32_WINNT) && (_WIN32_WINNT >= 0x6000)
#define SDB_WIN6_OR_GREATER
#endif //_WIN32_WINNT

#endif //WIN32

#if defined(linux) || defined(__linux) || defined(__linux__) || defined(__gnu_linux__)
#ifndef SDB_LINUX
#define SDB_LINUX
#endif
#endif //linux

#ifndef __MYSQL_INTERFACE__
#define __MYSQL_INTERFACE__
#endif

#ifndef SDB_STANDALONE
    #ifndef SDB_MYSQL
        #define SDB_MYSQL
    #endif //SDB_MYSQL
#endif

#include <stdio.h>
#include <stdlib.h>

//TODO: fix this
typedef int int32;
typedef unsigned int uint32;
typedef short int16;
typedef unsigned short uint16;
typedef long long int64;
typedef unsigned long long uint64;

// Header which has common defines between ScaleDB and Interface
#include "SdbCommon.h"

#define SDBTerminate(err, msg) SDBTerminateEngine(err, msg, __FILE__, __LINE__)
#define SdbDynamicArray void

#ifdef GET_MEMORY
#undef GET_MEMORY
#undef RELEASE_MEMORY
#undef RESIZE_MEMORY
#endif 

#ifdef SDB_DEBUG
#define GET_MEMORY SDBTestedMalloc
#define RELEASE_MEMORY SDBTestedFree	
#define RESIZE_MEMORY SDBTestedRealloc
#else
#define GET_MEMORY malloc
#define RELEASE_MEMORY free	
#define RESIZE_MEMORY realloc
#endif

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 INITIALIZATION FUNCTIONS
//
//////////////////////////////////////////////////////////////////////////////
*/
unsigned short SDBGlobalInit(char* engineConfigFileName);
unsigned short SDBGlobalEnd();
unsigned short SDBOpenMasterDbAndRecoverLog(unsigned int userId);
bool SDBStorageEngineIsInited();
void SDBTerminateEngine(int errCode, const char *msg, char *file, int line);

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 DATABASE MANAGEMENT
//
//////////////////////////////////////////////////////////////////////////////
*/
unsigned short SDBOpenDatabaseByName(unsigned int userId, char *databaseName, char *databaseFsName=0, char *databaseCsName=0, unsigned short ddlFlag=0);
void SDBOpenDatabaseById(unsigned int userId, unsigned short databaseId);

unsigned short SDBGetDatabaseNumberByName(unsigned short userId, char *databaseName, bool openTables=false);
char* SDBGetDatabaseNameByNumber(unsigned short databaseId);
bool SDBGetDatabaseStatusByNumber(unsigned short databaseId);
unsigned short SDBOpenAllDBFiles(unsigned short userId, unsigned short dbId);
// SDBLockMetaInfo is cluster-wide lock per database.
unsigned short SDBLockMetaInfo(unsigned short userId, unsigned short dbId=SDB_MASTER_DBID);
unsigned short SDBLockAndOpenDatabaseMetaInfo(unsigned short userId, char *databaseName);
unsigned short SDBLockAndRemoveDatabaseMetaInfo(unsigned short userId, unsigned short dbId, unsigned short ddlFlag, char *databaseName);
char * SDBGetClusterPassword();
char * SDBGetClusterUser();
int SDBGetSystemParamInt(const char *param, int defValue);
char* SDBGetSystemParamString(const char *param);

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 USER MANAGEMENT
//
//////////////////////////////////////////////////////////////////////////////
*/
unsigned int SDBGetNewUserId(unsigned short userType = SDB_USER_TYPE_DEFAULT);
void SDBRemoveUserById(unsigned int userId);
void SDBShowUserActivity(unsigned int userId);
void SDBShowUserLockStatus(unsigned int userId);
void SDBShowAllUsersLockStatus();

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 TABLE MANAGEMENT
//
//////////////////////////////////////////////////////////////////////////////
*/

void SDBOpenFile(unsigned short userId, unsigned short dbId, unsigned short tableId);
// When we close a table, we also remove table information from metadata cache
void SDBCloseFile(unsigned short userId, unsigned short dbId, unsigned short tableId);

// Initialize Database Table
unsigned short SDBValidateInitDatabaseTable(const char *dbName, const char *tableName);

// Create new user table. Return newly created table id
unsigned short SDBCreateTable(unsigned int userId, unsigned short dbId, char* tableName, unsigned long long autoIncrBaseValue, 
							  char *tableFsName=0, char *tableCsName=0, bool virtualTable = false, bool hasOverflow = false, unsigned short ddlFlag=0);

// Drop a table
unsigned short SDBDeleteTable(unsigned int userId, unsigned short dbId, 
							  char* tableName, unsigned short ddlFlag=0); 

unsigned short SDBDeleteTableById(unsigned int userId, unsigned short dbId, unsigned short tableId); 


// Rename a table
unsigned short SDBRenameTable(unsigned int userId, unsigned short dbId, 
							  char* fromTableName, char *fromTableFsName, 
							  char* toTableName, char *toTableFsName, char *toTableCsName,
							  bool useTableNameInDesignator = false, unsigned short ddlFlag=0);

// Truncate a table
unsigned short SDBTruncateTable(unsigned int userId, unsigned short dbId, char* tableName, unsigned short stmtFlag); 

// Can table be dropped?
unsigned short SDBCanTableBeDropped(unsigned int userId, unsigned short dbId, char* tableName);


unsigned short SDBGetNumberOfFieldsInTableByTableNumber(unsigned short dbId, unsigned short tableNumber);

// open the table with specified table file name.  This method returns the table id.
unsigned short SDBOpenTable(unsigned short userId, unsigned short dbId, char *pTableFsName);
// Use this method to open files related to a table if tableId is known.
void SDBOpenTableFiles(unsigned short userId, unsigned short dbId, unsigned short tableId);
// close an open table based on table name (not its file-system-safe name)
// the calling method needs to decide if we need to lock table before closing it.
unsigned short SDBCloseTable(unsigned short userId, unsigned short dbId, char *pTableName, bool bLockTable, bool bNeedToCommit);

unsigned short SDBGetTableNumberByName(unsigned short userId, unsigned short dbId, const char *tableName);
unsigned short SDBGetTableNumberByFileSystemName(unsigned short userId, unsigned short dbId, const char *tableName);
char* SDBGetTableNameByNumber(unsigned short userId, unsigned short dbId, unsigned short tableNumber);
char* SDBGetTableCaseSensitiveNameByNumber(unsigned short userId, unsigned short dbId, unsigned short tableNumber);

bool SDBTableIsVirtual(const char *tableName);

bool SDBLockTable(unsigned short userId, unsigned short dbId, unsigned short tableId, unsigned char lockLevel);
void SDBReleaseLockTable(unsigned short userId, unsigned short dbId, unsigned short tableId);
void SDBLockMetaInfoForTable(unsigned short userId, unsigned short dbId, unsigned short tableId, unsigned char lockLevel);
bool SDBIsTableWithIndexes(unsigned short dbId, unsigned short tableId);

char *SDBGetTableFileSystemNameByTableNumber(unsigned short dbId, unsigned short tableId);
unsigned char SDBGetTableLockLevel(unsigned short userId, unsigned short dbId, unsigned short tableId);

// the fast version to avoid resolving table name again
unsigned long long SDBGetTableStats(unsigned short userId, unsigned short dbId, unsigned short tableId, SDB_TABLE_STAT_INFO stat); 
unsigned long long SDBGetTableStats(unsigned short userId, unsigned short dbId, char* tableName, SDB_TABLE_STAT_INFO stat); 

unsigned short SDBGetNumberOfFrontFixedColumns(unsigned short dbId, unsigned short tableId);
unsigned short SDBGetFrontFixedColumnsLength(unsigned short dbId, unsigned short tableId);

char* SDBGetForeignKeyClause(unsigned short userId, unsigned short dbId, unsigned short tableId);

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 KEY MANAGEMENT
//
//////////////////////////////////////////////////////////////////////////////
*/

// Create index on specified table Return index Id
unsigned short SDBCreateIndex(unsigned int userId, unsigned short dbId, unsigned short tableId, 
							  char *indexName, char **keyFields, unsigned short *keySizes, 
							  bool isPrimaryKey, bool isNonUniqueIndex, char * parentIndexName, 
							  unsigned short ddlFlag, unsigned short externalId, bool isHashIndex);

unsigned short SDBDropIndex(unsigned int userId, unsigned short dbId, unsigned short tableId, char *indexName);

unsigned short SDBDefineForeignKey(unsigned int userId, unsigned short dbId, 
								   char *tableName, char *parentTableName, char* pForeignKeyName,
								   char **fieldsInTable, char **fieldsInParentTable);

char *SDBGetParentIndexByForeignFields(unsigned short dbmsId, char *tableName, char **fields, char **foreignFields);
// copy the foreign key constraints of an existing table (identified by existingTableName) into a new table with tableId
unsigned short SDBCopyForeignKey(unsigned int userId, unsigned short dbId, unsigned short tableId, char* existingTableName);
// delete a single foreign key constraint of an existing table 
unsigned short SDBDeleteForeignKeyConstraint(unsigned int userId, unsigned short dbId, unsigned short tableId, char* pConstraintName);

// return true if a foreign key constraint exists
bool SDBCheckConstraintIfExits(unsigned int userId, unsigned short dbId, char* pTableName, char* pConstraintName);

unsigned short SDBRebuildIndexes(unsigned int userId, unsigned short dbId, char *tableName);
unsigned short SDBRebuildIndexesByTableId(unsigned int userId, unsigned short dbId, unsigned short tableNumber);

unsigned short SDBDisableTableIndexes(unsigned userId, unsigned short dbId, const char *tableName);
unsigned short SDBEnableTableIndexes(unsigned short userId, unsigned short dbId, const char *tableName);

char* SDBGetIndexNameByNumber(unsigned short dbId, unsigned short indexId);
unsigned short SDBGetIndexNumberByName(unsigned short dbId, char *name);
unsigned short SDBGetIndexExternalId(unsigned short dbId, unsigned short indexId);
unsigned short SDBGetIndexLevel(unsigned short dbId, unsigned short indexId);
unsigned short SDBGetParentIndex(unsigned short dbId, unsigned short indexId, unsigned short curLevel=0);
unsigned short SDBGetNumberOfFieldsInTableByIndex(unsigned short dbId, unsigned short indexId);
unsigned short SDBGetNumberOfKeyFields(unsigned short dbId, unsigned short indexId);
unsigned short SDBGetNumberOfFieldsInParentTableByIndex(unsigned short dbId, unsigned short indexId);
char* SDBGetKeyFieldNameInIndex(unsigned short dbId, unsigned short indexId, unsigned short keyNumber);
char *SDBGetTableNameByIndex(unsigned short dbId, unsigned short indexId);
unsigned short SDBGetTableColumnPositionByIndex(unsigned short dbId, unsigned short indexId, unsigned short fieldId);
unsigned short SDBGetTableRowLengthByIndex(unsigned short dbId, unsigned short indexId);

unsigned short SDBGetLastIndexError(unsigned short userId);
unsigned short SDBGetLastIndexPositionInTable(unsigned short dbId, unsigned short indexId);

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 COLUMN MANAGEMENT
//
//////////////////////////////////////////////////////////////////////////////
*/

// Create new Field in given table
unsigned short SDBCreateField(unsigned int userId, unsigned short dbId, unsigned short tableId, 
							  char *fieldName, unsigned char fieldType, unsigned short fieldSize, 
							  unsigned int maxDataLength, char *defaultValue, bool autoIncr, 
							  unsigned short ddlFlag);

bool SDBIsFieldAutoIncrement(unsigned short dbId, unsigned short tableId, unsigned short fieldId);
char* SDBGetFileDataField(unsigned short userId, unsigned short tableId, unsigned short fieldId);
void SDBSetAutoIncrBaseValue(unsigned short dbId, unsigned short tableId, unsigned long long value);
void SDBSetOverflowFlag(unsigned short dbId, unsigned short tableId, bool ovfFlag);

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 TRANSACTIONAL MANAGEMENT
//
//////////////////////////////////////////////////////////////////////////////
*/

unsigned short SDBStartTransaction(unsigned int userId);
unsigned short SDBCommit(unsigned int userId);
unsigned short SDBSyncToDisk(unsigned int userId);

long SDBGetTransactionIdForUser(unsigned userId);
bool SDBIsUserInTransaction(unsigned int userId);
unsigned short SDBRollBack(unsigned int userId, char *savePointName = NULL);

unsigned short SDBSetSavePoint(unsigned short userId, char *savePointName);
bool SDBRemoveSavePoint(unsigned int userId, char *savePoinName);
unsigned short SDBRollBackToSavePointId(unsigned int userId, unsigned long long savePointId);
unsigned long long SDBGetSavePointId(unsigned int userId);

void SDBSetIsolationLevel(unsigned int userId, unsigned short isolationLevel);

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 ROW MANAGEMENT
//
//////////////////////////////////////////////////////////////////////////////
*/
unsigned int SDBInsertRow(unsigned short userId, unsigned short dbId, unsigned short tableId, 
						  unsigned char* rowData, unsigned short partitionId, unsigned short source, 
						  unsigned long long queryId=0);

unsigned int SDBDeleteRow(unsigned short userId, unsigned short dbId, unsigned short tableId, 
						  unsigned char* rowData, unsigned short partitionId , unsigned short source, 
						  unsigned long long queryId=0);

unsigned int SDBUpdateRow(unsigned short userId, unsigned short dbId, unsigned short tableId, 
						  unsigned char* oldRowData, unsigned char* newRowData, unsigned short partitionId, 
						  unsigned long long queryId=0);

unsigned int SDBInsertRowAPI(unsigned short userId, unsigned short dbmsId, unsigned short tableId, unsigned int rowId , 
							 unsigned short source, unsigned long long queryId=0, unsigned short sdbCommandType=0);

unsigned int SDBUpdateRowAPI(unsigned short userId, unsigned short dbmsId, unsigned short tableId, unsigned int rowId , 
							 unsigned long long queryId=0);

unsigned int SDBDeleteRowAPI(unsigned short userId, unsigned short dbmsId, unsigned short tableId, unsigned int rowId , 
							 unsigned short source, unsigned long long queryId=0);


unsigned short SDBPrepareStrField(unsigned short userId, unsigned short dbmsId, unsigned short tableId, 
								  unsigned short fieldId, char *ptrToData, short dataSize, unsigned short groupType=0);

unsigned short SDBPrepareNumberField(unsigned short userId, unsigned short dbmsId, unsigned short tableId, 
									 unsigned short fieldId, void *ptrToInt, unsigned short groupType=0);

unsigned short SDBPrepareVarField(unsigned short userId, unsigned short dbmsId, unsigned short tableId, 
								  unsigned short fieldId, char *ptrToData, unsigned int dataSize, 
								  unsigned short groupType, bool isNull);

unsigned short SDBResetRow(unsigned short userId, unsigned short dbId, unsigned short tableId, unsigned short groupType);

unsigned short SDBGetUserRowTableId(unsigned short userId);
char* SDBGetUserRowColumn(unsigned short userId, unsigned short fieldId);
unsigned short SDBGetUserRowColumnLength(unsigned short userId, unsigned short fieldId);

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 QUERY MANAGEMENT
//
//////////////////////////////////////////////////////////////////////////////
*/

unsigned short SDBGetQueryManagerId(unsigned int userId);

void SDBCloseQueryManager(unsigned int queryManagerId);

void SDBFreeQueryManagerBuffers(unsigned int queryManagerId);

unsigned short SDBPrepareSequentialScan(unsigned int userId, unsigned short queryMgrId, unsigned short dbId, char *tableName, 
										unsigned long long queryId, bool releaseLocksAfterRead);

//unsigned short SDBNextSequential(unsigned short queryMgrId);

unsigned short SDBGetSeqRowByPosition(unsigned int userId, unsigned short queryMgrId, unsigned int rowId);

unsigned short SDBPrepareQuery(unsigned int userId, unsigned short queryMgrId, unsigned short partitionId, 
							   unsigned long long queryId, bool releaseLocksAfterRead) ;

unsigned short SDBNext(unsigned int userId, unsigned short queryMgrId);

void SDBResetQuery(unsigned short queryMgrId);

unsigned int SDBGetRowsCount(unsigned short queryMgrId);

void SDBCloseAllQueryManagerIds(unsigned int userId);

char* SDBGetFieldValueByTable(unsigned short queryMgrId, unsigned short dbId, unsigned short tableId, 
							  unsigned short fieldId, char* ptrToColumnValue, unsigned int* fieldSize);

long long SDBCountRef(unsigned int userId, unsigned short queryMgrId, unsigned short dbId, unsigned short tableId, unsigned long long queryId);

// Query Cursor

void SDBSetActiveQueryManager(unsigned short queryMgrId);
unsigned short SDBDefineQuery(unsigned short queryMgrId, unsigned short dbId, unsigned short indexId, char *fieldName, 
							  char* key);
unsigned short SDBDefineQueryPrefix(unsigned short queryMgrId, unsigned short dbId, unsigned short indexId, char *fieldName, 
									char* key, bool useStarForPrefixEnd, int keyPrefixSize, bool usePoundSign);
unsigned short SDBDefineRangeQuery(unsigned short queryMgrId, unsigned short dbId, unsigned short indexId, char *fieldName, 
								bool includeStartValue, char* startKey, bool includeEndValue, char* endKey);
unsigned short SDBQueryCursorNextSequential(unsigned int userId, unsigned short queryMgrId);
unsigned short SDBQueryCursorNext(unsigned int userId, unsigned short queryMgrId);
bool SDBQueryCursorFieldIsNull(unsigned short queryMgrId, unsigned short fieldId);
bool SDBQueryCursorFieldIsNullByIndex(unsigned short queryMgrId, unsigned short indexId, unsigned short fieldId);
void SDBQueryCursorFreeBuffers(unsigned short queryMgrId);
void SDBQueryCursorReset(unsigned short queryMgrId);
unsigned short SDBQueryCursorCopySeqVarColumn(unsigned short queryMgrId, unsigned short columnId, char *dest, unsigned int *dataSize);
char *SDBQueryCursorGetFieldByTableId(unsigned short queryMgrId, unsigned short tableId, unsigned short fieldId);
char *SDBQueryCursorGetFieldByTableDataSize(unsigned short queryMgrId, unsigned short tableNumber, unsigned short fieldNumber, unsigned int *dataSize);
unsigned short SDBQueryCursorGetFieldByTable(unsigned short queryMgrId, unsigned short tableNumber, unsigned short fieldNumber, char *dest, unsigned int *dataSize=0); 
char *SDBQueryCursorGetSeqColumn(unsigned short queryMgrId, unsigned short columnId, unsigned int *dataSize = 0);
unsigned int SDBQueryCursorGetSeqRowPosition(unsigned short queryMgrId);
unsigned int SDBQueryCursorGetIndexCursorRowPosition(unsigned short queryMgrId);
char *SDBQueryCursorGetDataByIndex(unsigned short queryMgrId, unsigned short indexId);
char *SDBQueryCursorGetDataByIndexName(unsigned short queryMgrId, char* indexName);
void SDBQueryCursorPrintStats(unsigned short queryMgrId );
void SDBQueryCursorGetFirst(unsigned short queryMgrId, unsigned short dbId, unsigned short indexId);
void SDBQueryCursorGetLast(unsigned short queryMgrId, unsigned short dbId, unsigned short indexId);
unsigned short SDBQueryCursorDefineQueryAllValues(unsigned short queryMgrId, unsigned short dbId, unsigned short indexId, bool retrieveSubordinates);
void SDBQueryCursorSetFlags(unsigned short queryMgrId, unsigned short indexId, bool distinct, SDB_KEY_SEARCH_DIRECTION keyDirection, bool matchPrefix, bool getData);

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 CLUSTER MANAGEMENT
//
//////////////////////////////////////////////////////////////////////////////
*/
bool SDBNodeIsCluster();
void SDBGetConnectedNodesList(unsigned short userId, unsigned short bufferLength, char *buffer);
/*
//////////////////////////////////////////////////////////////////////////////
//
//                 DEBUG FUNCTIONS
//
//////////////////////////////////////////////////////////////////////////////
*/

unsigned char SDBGetDebugLevel(void);
void SDBDebugStart();
void SDBDebugEnd();
void SDBDebugPrintHeader(const char *str);
void SDBDebugPrintString(const char *str);
void SDBDebugPrintInt(int val);
void SDBDebugPrint8ByteUnsignedLong(unsigned long long l);
void SDBDebugPrintNewLine(int lines=1);
void SDBDebugPrintHexByteArray(char data[], int position, int size);
void SDBDebugPrintPointer(void *ptr);
void SDBDebugFlush();
void SDBLogSqlStmt(unsigned int userId, char* pStatement, unsigned long long stmtId);

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 UTILITY FUNCTIONS
//
//////////////////////////////////////////////////////////////////////////////
*/

unsigned short SDBMapToMySQLError(unsigned short error);
void SDBPrintMemoryInfo();
void SDBPrintStructure(unsigned short dbId);
void SDBPrintFilesInfo();
char* SDBUtilPtr2String(char *dstPtr, const void* srcPtr);
// this method allocates memory space for result without freeing first param
char *SDBUtilAppendString(char *ptr1, char *ptr2);
// this method allocates memory space for result and free's first param
char *SDBUtilAppendStringFreeFirstParam(char *ptr1, char *ptr2);  

unsigned short SDBUtilGetStrLength(char *ptr);
char *SDBUtilDuplicateString(char *ptr);
char *SDBUtilDoubleSpecialCharInString(char* pName, char specialChar);
char *SDBUtilGetStrInLower(char *ptr);
bool SDBUtilCompareStrings(const char *str1, const char *str2, bool inLower, unsigned short length = 0);
char* SDBUtilStrstrCaseInsensitive(char* s1, char* s2);
char *SDBUtilDecodeCharsInStrings(char *before);
char* SDBUtilFindDesignatorName(char* pTblFsName, char* pKeyName, int externalKeyNum, bool useExternalKeyNum=true);
void SDBUtilIntToMemoryLocation(unsigned long long number, unsigned char* destination, unsigned short bytesToCopy);
unsigned long long SDBGetNumberFromField(char *fieldValue, unsigned short offset, unsigned short bytes);
bool SDBUtilAreAllBytesZero(unsigned char*ptr, unsigned int length);
void SDBUtilGetIpFromInet(unsigned int *inet, char *dest, unsigned int sizeOfBuffer);

//TODO: FIX THIS..
void* SDBGetSdbMemoryPtr();
void* SDBGetTableDesignators(unsigned short dbId, unsigned short tableId);
void SDBSetThreadName(char *pThreadName);


/*
//////////////////////////////////////////////////////////////////////////////
//
//                 THREAD SAFE DYNAMIC ARRAY
//
//////////////////////////////////////////////////////////////////////////////
*/

SdbDynamicArray *SDBArrayInit(unsigned int initialElements, unsigned int addedElements, unsigned char elementSize);
void SDBArrayFree(SdbDynamicArray *ptr);
void SDBArrayFreeWithMembers(SdbDynamicArray *ptr);
void SDBArrayPutPtr(SdbDynamicArray *array, unsigned int pos, void *ptr);
void SDBArrayPutNull(SdbDynamicArray *array, unsigned int pos);
void* SDBArrayGetPtr(SdbDynamicArray *array, unsigned int pos);
void SDBArrayRemove(SdbDynamicArray *array, unsigned int pos);
unsigned int SDBArrayGetNumberOfNetElements(SdbDynamicArray *array);
unsigned int SDBArrayGetNumberOfElements(SdbDynamicArray *array);
bool SDBArrayIsElementDefined(SdbDynamicArray *array, unsigned int pos);
unsigned int SDBArrayGetNextElementPosition(SdbDynamicArray *array, unsigned int pos);
void SDBArrayGet(SdbDynamicArray *array, unsigned int pos, unsigned char *dest);
unsigned short SDBArrayGetUnUsedPosition(SdbDynamicArray *array);

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 MEMORY MANAGEMENT
//
//////////////////////////////////////////////////////////////////////////////
*/
char *SDBTestedMalloc( unsigned int size , unsigned long id = 0);
void SDBTestedFree( void *ptr );
char *SDBTestedRealloc( void *ptr, unsigned int size );

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 ScaleDB Configuration Parameters
//
//////////////////////////////////////////////////////////////////////////////
*/

char* SDBGetDataDirectory();
char* SDBGetLogDirectory();
unsigned int SDBGetBufferSizeIndex();
unsigned int SDBGetBufferSizeData();
unsigned int SDBGetMaxFileHandles();
char SDBGetAioFlag();
unsigned int SDBGetMaxColumnLengthInBaseFile();
unsigned int SDBGetDeadlockMilliseconds();
unsigned int SDBGetClusterPort();
char* SDBGetDebugString();


#endif //_SDB_STORAGE_API_H

