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
#define SDB_API	800

// Memory Management calls for the interface have different names from those defined in environment.h to avoid redefinitions and confusion
//	These #defines are necessary because some of the interface functions call our own memory management functions in the DataUtil class.
#if defined(SDB_DEBUG) || defined(SDB_DEBUG_MALLOC)
#define ALLOCATE_MEMORY(a,b,c) SDBTestedMalloc(a,b,c)
#define FREE_MEMORY SDBTestedFree	
#define REALLOCATE_MEMORY SDBTestedRealloc
#else
#define ALLOCATE_MEMORY(a,b,c) malloc(a)
#define FREE_MEMORY( pMem ) \
	if ( pMem ) \
	{ \
		free( pMem ); \
	}
#define REALLOCATE_MEMORY realloc
#endif

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 INITIALIZATION FUNCTIONS
//
//////////////////////////////////////////////////////////////////////////////
*/
unsigned short SDBGlobalInit(char* engineConfigFileName, unsigned int clusterPort);
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
unsigned short SDBOpenDatabaseByName(unsigned int userId, char *databaseName, char *databaseFsName=0, char *databaseCsName=0);
unsigned short SDBGetDatabaseNumberByName(unsigned short userId, char *databaseName, bool openTables=false);
char* SDBGetDatabaseNameByNumber(unsigned short databaseId);
bool SDBGetDatabaseStatusByNumber(unsigned short databaseId);
unsigned short SDBDropDbms(unsigned short userId, unsigned short dbId, unsigned short ddlFlag, char *databaseName);
unsigned short SDBDCloseDbms(unsigned short userId, unsigned short dbId, char *databaseName) ;
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
unsigned int SDBUSerSQLStatmentEnd(unsigned short userId) ;

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 TABLE MANAGEMENT
//
//////////////////////////////////////////////////////////////////////////////
*/

void SDBOpenFile(unsigned short userId, unsigned short dbId, unsigned short tableId);
// When we close a table, we also remove table information from metadata cache
void SDBCloseTable(unsigned short userId, unsigned short dbId, unsigned short tableId, unsigned short partitionId);

// Initialize Database Table
unsigned short SDBValidateInitDatabaseTable(const char *dbName, const char *tableName);

// Create new user table. Return newly created table id
unsigned short SDBCreateTable(unsigned int userId, unsigned short dbId, char* tableName, unsigned long long autoIncrBaseValue,
							  char *tableFsName=0, char *tableCsName=0, bool virtualTable = false, bool hasOverflow = false, bool streamTable =false, bool dimensionTable =false,unsigned long long dimensionSize=0,
							  unsigned short tableCharSet=0, unsigned short ddlFlag=0);

//Add partition info into meta tables
unsigned short SDBAddPartitions(unsigned short userId, unsigned short dbId, unsigned short tableId, char* name, unsigned short partitionId,  bool parIdAvailable);

//Get PartitionId
unsigned short SDBGetPartitionId(unsigned short userId, unsigned short dbId, char* pTblFsName, char* pTableName);

// Drop a table
unsigned short SDBDeleteTable(unsigned int userId, unsigned short dbId, 
							  char* tableName, char* partitionName, unsigned short partitionId, unsigned short ddlFlag=0);

unsigned short SDBRemoveLocalTableInfo(unsigned int userId, unsigned short dbId, unsigned short tableId, bool removeFromMemory); 

//Rename a partition
unsigned short SDBRenamePartition(unsigned int userId, unsigned short sdbDbId_, char* pTableName, char* fromPartitionName, char* toPartitionName,
											unsigned short fromPartitionId);

// Rename a table
unsigned short SDBRenameTable(unsigned int userId, unsigned short dbId, unsigned short partitionId,
							  char* fromTableName, char *fromTableFsName, 
							  char* toTableName, char *toTableFsName, char *toTableCsName,
							  bool useTableNameInDesignator, unsigned short ddlFlag, bool isALterStmt);

// Truncate a table
unsigned short SDBTruncateTable(unsigned int userId, unsigned short dbId, char* tableName, unsigned short partitionId, unsigned short stmtFlag);

// Can table be dropped?
unsigned short SDBCanTableBeDropped(unsigned int userId, unsigned short dbId, char* tableName);

unsigned short SDBCanTableBeTruncated(unsigned int userId, unsigned short dbId, char* tableName, unsigned short partitionId);

unsigned short SDBGetNumberOfFieldsInTableByTableNumber(unsigned short dbId, unsigned short tableNumber);

// open the table with specified table file name.  This method returns the table id.
unsigned short SDBOpenTable(unsigned short userId, unsigned short dbId, char *pTableFsName, unsigned short partitionId, bool openTableMetaDataOnly);

// close an open table based on table name (not its file-system-safe name)
// the calling method needs to decide if we need to lock table before closing it.
unsigned short SDBCloseTable(unsigned short userId, unsigned short dbId, char *pTableName, unsigned short partitionId, bool bLockTable, bool bNeedToCommits, bool isTableFlush,bool removeLocal);
unsigned short SDBCloseParentTables(unsigned short userId, unsigned short dbId, char* pTableFsName);

int SDBNumberShards(unsigned short dbId, unsigned short tableNumber);
unsigned short SDBGetTableNumberByName(unsigned short userId, unsigned short dbId, const char *tableName);
unsigned char SDBGetColumnTypeByNumber(unsigned short dbId, unsigned short tableNumber, unsigned short columnNumber);
unsigned short SDBGetTableNumberByFileSystemName(unsigned short userId, unsigned short dbId, const char *tableName);
char* SDBGetTableNameByNumber(unsigned short userId, unsigned short dbId, unsigned short tableNumber);
char* SDBGetTableCaseSensitiveNameByNumber(unsigned short userId, unsigned short dbId, unsigned short tableNumber);
unsigned short SDBGetColumnNumberByName(unsigned short dbId, unsigned short tableNumber, const char *columnName);
unsigned short SDBGetColumnOffsetByNumber(unsigned short dbId, unsigned short tableNumber, unsigned short columnNumber);
unsigned short SDBGetColumnSizeByNumber(unsigned short dbId, unsigned short tableNumber, unsigned short columnNumber);
unsigned short SDBGetMaxColumnLength(unsigned short dbId, unsigned short tableNumber, unsigned short columnNumber);
unsigned short SDBGetOffsetAuxiliary(unsigned short dbId, unsigned short tableNumber);

bool SDBTableIsVirtual(const char *tableName);

bool SDBLockTable(unsigned short userId, unsigned short dbId, unsigned short tableId, unsigned short partitionId, unsigned char lockLevel);
bool SDBSessionLock(unsigned short userId, unsigned short dbId, unsigned short tableId, unsigned short partitionId, unsigned long long resourceId, unsigned char lockLevel);

char* SDBstat_getMonitorStatistics();
long long SDBstat_getMemoryUsage();
int SDBstat_getBlockName(char *buf, int len);
long long SDBstat_connectedVolumnsCount();
long long SDBstat_IOThreadsCount();
long long SDBstat_SLMThreadsCount();
long long SDBstat_IndexThreadsCount();
unsigned long long SDBGetStatistics( int type );




long long SDBstat_IndexCacheSize();
long long SDBstat_IndexCacheUsed();
long long SDBstat_DataCacheSize();
long long SDBstat_DataCacheUsed();
long long SDBstat_BlobCacheSize();
long long SDBstat_BlobCacheUsed();

long long SDBstat_DBIndexCacheHits();
long long SDBstat_DBIndexBlockReads();
long long SDBstat_IndexBlockSyncWrites();
long long SDBstat_IndexBlockASyncWrites();
long long SDBstat_IndexCacheHitsStorageNode();
long long SDBstat_IndexBlockReadsStorageNode();
long long SDBstat_IndexBlockSyncWritesStorageNode();
long long SDBstat_IndexBlockASyncWritesStorageNode();

long long SDBstat_DBDataCacheHits();
long long SDBstat_DBDataBlockReads();
long long SDBstat_DataBlockSyncWrites();
long long SDBstat_DataBlockASyncWrites();
long long SDBstat_DataCacheHitsStorageNode();
long long SDBstat_DataBlockReadsStorageNode();
long long SDBstat_DataBlockSyncWritesStorageNode();
long long SDBstat_DataBlockASyncWritesStorageNode();

long long SDBstat_DBBlobCacheHits();
long long SDBstat_DBBlobBlockReads();
long long SDBstat_BlobBlockSyncWrites();
long long SDBstat_BlobBlockASyncWrites();
long long SDBstat_BlobCacheHitsStorageNode();
long long SDBstat_BlobBlockReadsStorageNode();
long long SDBstat_BlobBlockSyncWritesStorageNode();
long long SDBstat_BlobBlockASyncWritesStorageNode();





long long SDBstat_BlockSendsSequentialScan();
long long SDBstat_RowsReturnedSequentialScan();
long long SDBstat_QueriesByIndex();
long long SDBstat_QueryUniqueKeyLookup();
long long SDBstat_QueryNonUniqueKeyLookup();
long long SDBstat_RangeLookup();
long long SDBstat_QuerIndexRowsReturned();
long long SDBstat_RowsReturnedDynamicHash();
long long SDBstat_NumberInserts();
long long SDBstat_NumberUpdates();
long long SDBstat_NumberDeletes();
long long SDBstat_NumberRollbacks();
long long SDBstat_NumberCommits();






void SDBReleaseLockTable(unsigned short userId, unsigned short dbId, unsigned short tableId, unsigned short partitionId);
bool SDBIsTableWithIndexes(unsigned short dbId, unsigned short tableId);
unsigned short SDBGetIndexTableNumberForTable( unsigned short dbId, unsigned short tableId );
bool SDBGetTableStatus(unsigned short userId, unsigned short dbId, unsigned short tableId);

char *SDBGetTableFileSystemNameByTableNumber(unsigned short dbId, unsigned short tableId);
unsigned char SDBGetTableLockLevel(unsigned short userId, unsigned short dbId, unsigned short tableId, unsigned short partitionId);

// the fast version to avoid resolving table name again
unsigned long long SDBGetTableStats( unsigned short userId, unsigned short dbId, unsigned short tableId, unsigned short partitionId, SDB_TABLE_STAT_INFO stat, unsigned short usKeyPart = 0 );
unsigned long long SDBGetTableStats( unsigned short userId, unsigned short dbId, char* tableName,        unsigned short partitionId, SDB_TABLE_STAT_INFO stat, unsigned short usKeyPart = 0 );
char* SDBGetForeignKeyClause(unsigned short userId, unsigned short dbId, unsigned short tableId);

unsigned short SDBGetNumberOfPartitionsByTableId(unsigned short userId, unsigned short dbId, unsigned short tableId);
//unsigned short SDBGetLastPartitionId(unsigned short userId, unsigned short dbId, unsigned short tableId);
void SDBSetPartitionInfo(unsigned short userId, unsigned short dbId, unsigned short tableId, unsigned short lastPartitionId, unsigned short numberOfPartitions, char* pTableFsName);

unsigned short SDBInsertFrmData(unsigned short userId, unsigned short dbId, unsigned short tableId, char* frmData, unsigned short frmLength, char* parData, unsigned short parLength);
unsigned short SDBUpdateFrmData(unsigned short userId, unsigned short dbId, unsigned short tableId, char* frmData, unsigned short frmLength);
unsigned short SDBDeleteFrmData(unsigned short userId, unsigned short dbId, unsigned short tableId );
char *SDBGetFrmData(unsigned short userId, unsigned short dbId, unsigned short tableId, unsigned int *frmLength, char** parData, unsigned int *parLength);

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
							  unsigned short ddlFlag, unsigned short externalId, unsigned char indexType);

unsigned short SDBDropIndex(unsigned int userId, unsigned short dbId, unsigned short tableId, char *indexName);

unsigned short SDBDefineForeignKey(unsigned int userId, unsigned short dbId, 
								   char *tableName, char *parentTableName, char* pForeignKeyName,
								   char **fieldsInTable, char **fieldsInParentTable);

unsigned short SDBGetNumberOfForeignTables(unsigned int userId, unsigned short dbId, char *tableName);

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

unsigned short *SDBGetFieldsMap(unsigned short dbId, unsigned short tableNumber);
char* SDBGetIndexNameByNumber(unsigned short dbId, unsigned int indexId);
unsigned int SDBGetIndexNumberByName(unsigned short dbId, char *name);
unsigned short SDBGetIndexTableNumberByName( unsigned short dbId, char *name );
unsigned short SDBGetIndexExternalId(unsigned short dbId, unsigned int indexId);
unsigned short SDBGetIndexLevel(unsigned int indexId);
unsigned short SDBGetIndexLevel(unsigned short dbId, unsigned short designatorId);
unsigned short SDBGetParentIndex(unsigned short dbId, unsigned int indexId, unsigned short curLevel=0);
unsigned short SDBGetNumberOfFieldsInTableByIndex(unsigned short dbId, unsigned int indexId);
unsigned short SDBGetNumberOfKeyFields(unsigned short dbId, unsigned int indexId);
unsigned short SDBGetNumberOfFieldsInParentTableByIndex(unsigned short dbId, unsigned int indexId);
char* SDBGetKeyFieldNameInIndex(unsigned short dbId, unsigned int indexId, unsigned short keyNumber);
char *SDBGetTableNameByIndex(unsigned short dbId, unsigned int indexId);
unsigned short SDBGetTableColumnPositionByIndex(unsigned short dbId, unsigned int indexId, unsigned short fieldId);
unsigned short SDBGetTableRowLengthByIndex(unsigned short dbId, unsigned int indexId);

unsigned short SDBGetLastUserErrorNumber(unsigned short userId);
unsigned short SDBGetLastIndexError(unsigned short userId);
char *SDBGetLastUserErrorMessageGeneric(unsigned short userId);
char *SDBGetLastUserErrorMessageDetail(unsigned short userId);
unsigned short SDBGetErrorMessage(unsigned short userId, char *buff, unsigned short buffLength) ;
unsigned short SDBGetLastIndexError(unsigned short userId);
unsigned short SDBGetLastIndexPositionInTable(unsigned short dbId, unsigned int indexId);
unsigned short SDBIsStreamingTable(unsigned short dbId, unsigned short tableNumber);
unsigned short SDBIsDimensionTable(unsigned short dbId, unsigned short tableNumber);
unsigned short SDBIsNonUniqueIndex(unsigned short dbId, unsigned int indexId);
unsigned short SDBStreamingDelete(unsigned short userId, unsigned short dbId, unsigned short tableNumber, unsigned short partitionId,unsigned long long key, unsigned short column, bool delete_all, unsigned long long queryId);
unsigned short SDBSetErrorMessage(unsigned short userId,  unsigned short sdb_error_code, char *buff);
unsigned short SDBGetSystemLevelIndexType();
unsigned long  SDBGetRangeKeyFieldID(unsigned short dbId, unsigned short tableId);
unsigned long  SDBGetRangeKey(unsigned short dbId, unsigned short tableId);

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
							  unsigned short ddlFlag, unsigned short fieldFlag, bool isRangeKey, 
							  bool isMapped, bool isInsertCascade, bool isStreamingKey, bool isAttributeValue);

bool SDBIsFieldAutoIncrement(unsigned short dbId, unsigned short tableId, unsigned short fieldId);
char* SDBGetFileDataField(unsigned short userId, unsigned short tableId, unsigned short fieldId);
unsigned long long  SDBGetAutoIncrValue(unsigned short dbId, unsigned short tableId);
unsigned long long  SDBGetActualAutoIncrValue(unsigned short userId,unsigned short dbId, unsigned short tableId);
void SDBSetOverflowFlag(unsigned short dbId, unsigned short tableId, bool ovfFlag);

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 TRANSACTIONAL MANAGEMENT
//
//////////////////////////////////////////////////////////////////////////////
*/
void SDBKillQueryByUserId(unsigned int userId);
unsigned short SDBStartTransaction( unsigned int userId, bool bAutocommit = false );
unsigned short SDBCommit(unsigned int userId, bool releaseSessionLocks);
unsigned short SDBSyncToDisk(unsigned int userId);

long SDBGetTransactionIdForUser(unsigned userId);
bool SDBIsUserInTransaction(unsigned int userId);
unsigned short SDBRollBack(unsigned int userId, char *savePointName , unsigned short savePointNameLength, bool releaseSessionLocks);

unsigned short SDBSetSavePoint(unsigned short userId, char *savePointName, unsigned short savePointNameLength);
bool SDBRemoveSavePoint(unsigned int userId, char *savePoinName, unsigned short savePointNameLength);
unsigned long long SDBRollBackToSavePointId(unsigned int userId, unsigned long long savePointId, bool releaseSessionLocks);
unsigned long long SDBGetSavePointId(unsigned int userId);
void SDBSetStmtId(unsigned int userId);
unsigned long long  SDBGetStmtId(unsigned int userId);

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

unsigned int SDBInsertRowAPI(unsigned short userId, unsigned short dbmsId, unsigned short tableId, unsigned short partitionId , unsigned long long numOfBulkInsertRows, unsigned long long numOfInsertedRows,
							 unsigned long long queryId=0, unsigned short sdbCommandType=0);

unsigned int SDBUpdateRowAPI(unsigned short userId, unsigned short dbmsId, unsigned short tableId, unsigned short partitionId, unsigned long long rowId ,
							 unsigned long long queryId=0);

unsigned long long SDBGetUserRowId(unsigned short userId);

unsigned int SDBDeleteRowAPI(unsigned short userId, unsigned short dbmsId, unsigned short tableId, unsigned short partitionId, 
							unsigned long long rowId, unsigned long long queryId=0);


unsigned short SDBPrepareStrField(unsigned short userId, unsigned short dbmsId, unsigned short tableId, 
								  unsigned short fieldId, char *ptrToData, short dataSize, unsigned short groupType=0);

unsigned short SDBPrepareNumberField(unsigned short userId, unsigned short dbmsId, unsigned short tableId, 
									 unsigned short fieldId, void *ptrToInt, unsigned short groupType=0);

unsigned short SDBPrepareVarField(unsigned short userId, unsigned short dbmsId, unsigned short tableId, 
								  unsigned short fieldId, char *ptrToData, unsigned int dataSize, 
								  unsigned short groupType, bool isNull);


unsigned short SDBResetRow(unsigned short userId, unsigned short dbId,  unsigned short partitionId, unsigned short tableId, unsigned short groupType);
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

typedef enum SDBFieldType {SDB_CHAR,SDB_NUM,SDB_VAR_CHAR,SDB_BLOB,SDB_GEOMETRY, NO_TYPE};
typedef enum SDBCopyDataType {SDB_CPY_DATA, SDB_CPY_PTR};


typedef struct SDBFieldTemplate{
	unsigned short  fieldNum_;					// 1.  field number in SDB row 
	unsigned char * nullByte_;					// 2a. mark as null in bitmap   
	unsigned char   nullBit_;					// 2b. offset to mark in bitmap 
	unsigned short  offset_;					// 3.  offset in output row 
	unsigned char * varLength_;					// 4a. where to copy real SDB  data size - NULL if no copy is needed 
	unsigned short  varLengthBytes_;			// 4b. how many bytes the SDB data size is composed of  
	SDBCopyDataType  copyDataMethod_;			// 5. weather to copy ptr only or whole data 
	unsigned int  length_; // for debug only 
	SDBFieldType  type_;  // for debug only 
} SDBFieldTemplate;

#define SdbDynamicArray void
#define SdbConditionStack  void

typedef struct SimpleCondItem {
	void  * field_;
	char  * value_;
	double  value_decimal_; 
	int  size_;
	char * value() { return value_ ? value_ : (char *)&value_decimal_;}
}SimpleCondItem;


typedef struct SimpleCondition {
	SimpleCondItem item_[128];
	int  numOfItems_;
} SimpleCondition;

typedef struct SDBRowTemplate {
	SdbDynamicArray *  fieldArray_;     // array of field templates 
	unsigned short	autoIncfieldNum_;   // the index of an nulled value  auto increment field 
	short numOfblobs_;					// number of  blobs in the row to read/write 
	short numOfOvf_;					// number of varchar fields + blob fields in the row to read/write 
	unsigned int length_;               // number of fields 
} SDBRowTemplate;


typedef struct SDBKeyPartTemplate {
	unsigned char * ptr_;       // 1. ptr to key part - NULL if key = NULL 
    bool     allValues_;        // 2. flag that the field is "all values" 
	unsigned int  length_;      // 3. length of key - debug only 
	SDBFieldType  type_;        // 4. type for debug only 
} SDBKeyPartTemplate;

typedef struct SDBKeyTemplate {
	SdbDynamicArray *  keyArray_;
	unsigned int arrayLength_;
	unsigned int keyLength_;
} SDBKeyTemplate;

#define DESIGNATOR_NUMBER_MASK 0XFFFF
#define DESIGNATOR_LEVEL_OFFSET 16
#define GET_DESIGNATOR_NUMBER(index_id) ((unsigned short)(index_id & DESIGNATOR_NUMBER_MASK))
#define GET_DESIGNATOR_LEVEL(index_id) ((unsigned short)(index_id >> DESIGNATOR_LEVEL_OFFSET))


bool SDBEqualDataTypes(unsigned char internalType, SDBFieldType externalType) ;

unsigned short SDBGetQueryManagerId(unsigned int userId);

bool SDBIsValidQueryManagerId(unsigned int queryManagerId, unsigned int userId);

void SDBFreeQueryManager(unsigned int userId,unsigned int queryManagerId);

void SDBCheckAllQueryManagersAreFree(unsigned int userId);

void SDBFreeQueryManagerBuffers(unsigned int queryManagerId);

unsigned short SDBPrepareRowByTemplate(unsigned short userId, unsigned char * rowBuf, SDBRowTemplate &rowTemplate,unsigned short dbmsId,unsigned short groupType);
unsigned short SDBPrepareRowByTemplateEnterExit(unsigned short userId, unsigned char * rowBuf, SDBRowTemplate &rowTemplate,unsigned short dbmsId,unsigned short groupType);

unsigned short SDBPrepareSequentialScan( unsigned int userId, unsigned short queryMgrId, unsigned short dbId, unsigned short partitionId, char *tableName, unsigned long long queryId,
										 bool releaseLocksAfterRead, SDBRowTemplate& rowTemplate,
										 const unsigned char* pConditionString = NULL, unsigned int conditionStringLength = 0,
										 const unsigned char* pAnalyticsString = NULL, unsigned int analyticsStringLength = 0 );

unsigned short SDBEndSequentialScan( unsigned int userId, unsigned short queryMgrId, unsigned short dbId, unsigned short partitionId, char *tableName, unsigned long long queryId );

unsigned short SDBEndIndexedQuery( unsigned int userId, unsigned short queryMgrId, unsigned short dbId, unsigned short partitionId, char *tableName, unsigned long long queryId );

unsigned short SDBGetSeqRowByPosition(unsigned int userId, unsigned short queryMgrId, unsigned long long rowId);

unsigned short SDBPrepareQuery( unsigned int userId, unsigned short queryMgrId, unsigned short partitionId, unsigned long long queryId,
								bool releaseLocksAfterRead, unsigned short sdbCommandType, bool isPointQuery, SDBRowTemplate& rowTemplate, bool prefetch,
								const unsigned char* pConditionString, unsigned int lengthConditionString,
								const unsigned char* pAnalyticsString, unsigned int lengthAnalyticsString );

long long SDBGetRowsCount(unsigned short queryMgrId);

void SDBCloseAllQueryManagerIds(unsigned int userId);

char* SDBGetFieldValueByTable(unsigned short queryMgrId, unsigned short dbId, unsigned short tableId, 
							  unsigned short fieldId, char* ptrToColumnValue, unsigned int* fieldSize);

long long SDBCountRef(unsigned int userId, unsigned short queryMgrId, unsigned short dbId, unsigned short tableId, 
					unsigned short partitionId, unsigned long long queryId);

long long SDBGetTableRowCount( unsigned int userId, unsigned short dbId, unsigned short tableId, unsigned short partitionId );
long long SDBRowsInRange(unsigned int userId, unsigned short queryMgrId ,unsigned short dbId, unsigned short tableId, unsigned short partitionId, unsigned int indexId, unsigned char *minKey, SDBKeyTemplate & minKeyTemplate, SdbKeySearchDirection minkeyDirection,unsigned char *maxKey, SDBKeyTemplate & maxKeyTemplate,SdbKeySearchDirection maxkeyDirection);

// Query Cursor

void SDBSetActiveQueryManager(unsigned short queryMgrId);

unsigned short SDBDefineQueryPrefix(unsigned short queryMgrId, unsigned short dbId, unsigned int indexId, char *fieldName, 
									char* key, bool useStarForPrefixEnd, int keyPrefixSize, bool usePoundSign);
unsigned short SDBQueryCursorNextSequential(unsigned int userId, unsigned short queryMgrId, unsigned char *outBuff, SDBRowTemplate *outTemplate, unsigned short sdbCommandType);
unsigned short SDBQueryCursorNext(unsigned int userId, unsigned short queryMgrId, unsigned char *outBuff, SDBRowTemplate *outTemplate, unsigned short sdbCommandType);
bool SDBQueryCursorFieldIsNull(unsigned short queryMgrId, unsigned short fieldId);
bool SDBQueryCursorFieldIsNullByIndex(unsigned short queryMgrId, unsigned int indexId, unsigned short fieldId);
void SDBQueryCursorFreeBuffers(unsigned short queryMgrId);
void SDBQueryCursorReset(unsigned short queryMgrId);
unsigned short SDBQueryCursorCopySeqVarColumn(unsigned int userId, unsigned short queryMgrId, unsigned short columnId, char *dest, unsigned int *dataSize);
char *SDBQueryCursorGetFieldByTableId(unsigned short queryMgrId, unsigned short tableId, unsigned short fieldId);
char *SDBQueryCursorGetFieldByTableVarData(unsigned int userId, unsigned short queryMgrId, unsigned short tableNumber, unsigned short fieldNumber, unsigned int *dataSize);
unsigned short SDBQueryCursorGetFieldByTable(unsigned int userId, unsigned short queryMgrId, unsigned short tableNumber, unsigned short fieldNumber, char *dest, unsigned int *dataSize=0); 
char *SDBQueryCursorGetSeqColumn(unsigned int userId, unsigned short queryMgrId, unsigned short columnId, unsigned int *dataSize = 0);
unsigned long long SDBQueryCursorGetSeqRowPosition(unsigned short queryMgrId);
unsigned long long SDBQueryCursorGetIndexCursorRowPosition(unsigned short queryMgrId);
char *SDBQueryCursorGetDataByIndex(unsigned short queryMgrId, unsigned int indexId);
char *SDBQueryCursorGetDataByIndexName(unsigned short queryMgrId, char* indexName);
void SDBQueryCursorPrintStats(unsigned short queryMgrId );
unsigned short SDBDefineQueryByTemplate(unsigned short queryMgrId, unsigned short dbId, unsigned int indexId, char *keyBuf,SDBKeyTemplate & keyTemplate);
void SDBDefineQueryRangeKey( unsigned short queryMgrId, unsigned short dbId, unsigned int indexId,  SDBKeyTemplate& keyTemplate, unsigned char keyDirection );


unsigned short SDBQueryCursorDefineQueryAllValues(unsigned short queryMgrId, unsigned short dbId, unsigned int indexId, bool retrieveSubordinates);
void SDBQueryCursorSetFlags(unsigned short queryMgrId, unsigned int indexId, bool distinct, SDB_KEY_SEARCH_DIRECTION keyDirection, bool matchPrefix, bool getData,bool readJustKey);

unsigned short SDBQueryCursorCopyToRowBufferWithRowTemplate(unsigned int userId, unsigned short queryMgrId,unsigned char * dest,SDBRowTemplate & rowTemplate, unsigned long long & rowAddress);
unsigned short SDBQueryCursorCopyToRowBufferWithRowTemplate(unsigned int userId, unsigned short queryMgrId, unsigned int indexId,unsigned char * dest,SDBRowTemplate & rowTemplate, unsigned long long & rowAddress);
unsigned short SDBQueryCursorCopyToRowBufferWithRowTemplateWithEngine(unsigned int userId, unsigned short queryMgrId,unsigned char * dest,SDBRowTemplate & rowTemplate, unsigned long long & rowAddress);
unsigned short SDBQueryCursorCopyToRowBufferWithRowTemplateWithEngine(unsigned int userId, unsigned short queryMgrId, unsigned int indexId,unsigned char * dest,SDBRowTemplate & rowTemplate, unsigned long long & rowAddress);


//////////////////////////////////////////////////////////////////////////////
//
//                 CLUSTER MANAGEMENT
//
//////////////////////////////////////////////////////////////////////////////

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
void SDBSetSqlCommand(unsigned int userId,unsigned short sdbCommandType) ;

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
unsigned short SDBUtilGetNumber(char *ptr, unsigned short length);
char *SDBUtilDuplicateString(char *ptr);
char *SDBUtilDoubleSpecialCharInString(char* pName, char specialChar);
char *SDBUtilGetStrInLower(char *ptr);
bool SDBUtilCompareStrings(const char *str1, const char *str2, bool inLower, unsigned short length = 0);
char* SDBUtilStrstrCaseInsensitive(char* s1, char* s2);
char* SDBUtilStrstrCaseInsensitive(char* s1, char* s2);
char* SDBUtilFindComment(char* s1, char* s2);
int SDBUtilFindCommentIntValue(char* s1, char* s2); 
// Search a substring (str) in SQL stmt - comments are ignored
char* SDBUtilSqlSubStrIgnoreComments(char* sqlStmt, char* subStr, bool ignoreComments);
int SDBUtilStringToInt(char *str);
int SDBUtilStringToInt(char *str, int len);
char *SDBUtilDecodeCharsInStrings(char *before);
char* SDBUtilFindDesignatorName(char* pTblFsName, char* pKeyName, int externalKeyNum, bool useExternalKeyNum, char *nameBuff, int nameBuffLength);
void SDBUtilIntToMemoryLocation(unsigned long long number, unsigned char* destination, unsigned short bytesToCopy);
unsigned long long SDBGetNumberFromField(char *fieldValue, unsigned short offset, unsigned short bytes);
bool SDBUtilAreAllBytesZero(unsigned char*ptr, unsigned int length);
void SDBUtilGetIpFromInet(unsigned int *inet, char *dest, unsigned int sizeOfBuffer);
void SDBUtilReverseString(char *str);

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

SdbDynamicArray* SDBArrayInit( unsigned int initialElements, unsigned int addedElements, unsigned char elementSize, bool useZerothElement );
void SDBArrayFree(SdbDynamicArray *ptr);
void SDBArrayFreeWithMembers(SdbDynamicArray *ptr);
void SDBArrayPutPtr(SdbDynamicArray *array, unsigned int pos, void *ptr);
void SDBArrayPutFieldTemplate(SdbDynamicArray *array, unsigned int pos,SDBFieldTemplate & f);
void SDBArrayPutKeyPartTemplate(SdbDynamicArray *array, unsigned int pos,SDBKeyPartTemplate & f);
void SDBArrayPutNull(SdbDynamicArray *array, unsigned int pos);
void* SDBArrayGetPtr(SdbDynamicArray *array, unsigned int pos);
void SDBArrayRemove(SdbDynamicArray *array, unsigned int pos);
unsigned int SDBArrayGetNumberOfNetElements(SdbDynamicArray *array);
unsigned int SDBArrayGetNumberOfElements(SdbDynamicArray *array);
bool SDBArrayIsElementDefined(SdbDynamicArray *array, unsigned int pos);
unsigned int SDBArrayGetNextElementPosition(SdbDynamicArray *array, unsigned int pos);
void SDBArrayGet(SdbDynamicArray *array, unsigned int pos, unsigned char *dest);
unsigned int SDBArrayGetUnusedPosition( SdbDynamicArray* array );

void SDBArrayPut(SdbDynamicArray *array,unsigned long long &f);
unsigned long long SDBArrayGet(SdbDynamicArray *array, unsigned int pos);
void SDBArraySort(SdbDynamicArray *array);
void SDBArrayReset(SdbDynamicArray *array);

SdbConditionStack *SDBConditionStackInit(unsigned int initialElements);
void SDBConditionStackFree(SdbConditionStack * ptr);
SimpleCondition  SDBConditionStackPop(SdbConditionStack * stack);
SimpleCondition  SDBConditionStackTop(SdbConditionStack * stack);
bool SDBConditionStackPush(SdbConditionStack * stack,SimpleCondition  & item);
void SDBConditionStackClearAll(SdbConditionStack * stack);
bool SDBConditionStackIsEmpty(SdbConditionStack * stack);
bool SDBConditionStackIsFull(SdbConditionStack * stack);
void printMYSQLConditionBuffer( unsigned char * sqlBuffer, unsigned int sqlBufferLength );
/*
//////////////////////////////////////////////////////////////////////////////
//
//                 MEMORY MANAGEMENT
//
//////////////////////////////////////////////////////////////////////////////
*/
char *SDBTestedMalloc( unsigned int size , unsigned short file, unsigned short line);
void SDBTestedFree( void *ptr );
char *SDBTestedRealloc( void *ptr, unsigned int size );
void *SdbGetMemory( unsigned int size , unsigned short file, unsigned short line);
void SdbFreeMemory( void *ptr );
void *SdbReallocMemory( void *ptr, unsigned int newSize );

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
unsigned int SDBGetMaxColumnLengthInBaseFile();
unsigned int SDBGetMaxKeyLength();
unsigned int SDBGetDeadlockMilliseconds();
bool SDBForceAnalyticsEngine();
char* SDBGetDebugString();

/*
//////////////////////////////////////////////////////////////////////////////
//
//                 ScaleDB Debug Methods
//
//////////////////////////////////////////////////////////////////////////////
*/
#ifdef SDB_DEBUG
void dummyMethod(unsigned short uId,unsigned short retValue, unsigned short methodId);
void dummyEnterMethod(unsigned short uId);
#endif

#define API_OPEN_MASTER_DB_AND_LOG			1
#define API_OPEN_DBMS_BY_NAME				2
#define API_GET_DBMS_NUMBER_BY_NAME			3
#define API_OPEN_ALL_DB_FILES				4
#define API_LOCK_META_INFO					5
#define API_LOCK_AND_OPEN_DBMS_META_INFO	6
#define API_DROP_DBMS						7
#define API_CLOSE_TABLE_A					8
#define API_CREATE_TABLE					9
#define API_GET_PARTITION_ID				10
#define API_DELETE_TABLE					11
#define API_DELETE_TABLE_BY_ID				12	
#define API_RENAME_PARTITION				13
#define API_TRUNCATE_TABLE					14
#define API_CAN_TABLE_BE_DROPPED			15
#define API_OPEN_TABLE						16
#define API_CLOSE_TABLE_B					17
#define API_SET_PARTITION_INFO				18
#define API_LOCK_TABLE						19
#define API_RELEASE_TABLE_LOCK				20
#define API_LOCK_TABLE_META_INFO			21
#define API_GET_TABLE_STATUS				22
#define API_GET_TABLE_STATS_1				23
#define API_GET_FOREIGN_KEY					24
#define API_INSERT_FRM						25
#define API_GET_FRM							26
#define API_DELETE_FRM						27
#define API_CREATE_FIELD					28
#define API_GET_FIELD_DATA					29
#define API_START_TRAN						30
#define API_COMMIT							31
#define API_SYNC_DISK						32
#define API_ROLLBACK						33
#define API_SET_SAVEPOINT					34
#define API_REMOVE_SAVEPOINT				35
#define API_ROLLBACK_SAVEPOINT				36
#define API_CREATE_INDEX					37
#define API_DEFINE_FOREIGN_KEY				38
#define API_CHECK_CONSTRAINS				39
#define API_COPY_FOREIGN_KEY				40
#define API_DELETE_FOREIGN_KEY_CONSTRAINS	41
#define API_REBUILD_INDEXES					42
#define API_REBUILD_TABLE_INDEXES			43
#define API_DISABLE_TABLE_INDEXES			44
#define API_ENABLE_TABLE_INDEXES			45
#define API_INSERT_ROW						46
#define API_DELETE_ROW						47
#define API_UPDATE_ROW						48
#define API_PREPARE_VAR_FIELD				49
#define API_INSERT_ROW_API					50
#define API_UPDATE_ROW_API					51
#define API_DELETE_ROW_API					52
#define API_PREPARE_SEQUENTIAL_SCAN			53
#define API_GET_SEQUENTIAL_ROW_BY_POS		54
#define API_PREPARE_QUERY					55
#define API_COUNT_REF						56
#define API_CURSOR_NEXT_SEQUENTIAL			57
#define API_CURSOR_NEXT						58
#define API_COPY_SEQ_VAR_COLUMN				59
#define API_GET_TABLE_FIELD_VAR_DATA		60
#define API_GET_FIELD_BY_TABLE				61
#define API_GET_SEQUENTIAL_COLUMN			62
#define API_GET_NODES_LIST					63
#define API_OPEN_DBMS_BY_ID					64
#define API_RENAME_TABLE					65
#define API_ADD_PARTITION					66
#define API_TABLE_ROW_COUNT					67
#define API_DESIGNAOR_BY_NAME				68
#define API_COPY_ROW_A						69
#define API_COPY_ROW_B						70
#define API_GET_TABLE_LOCK_LEVEL			71
#define API_GET_TABLE_STATS_2				72
#define API_END_SEQUENTIAL_SCAN				73
#define API_END_INDEXED_QUERY				74


#endif //_SDB_STORAGE_API_H

