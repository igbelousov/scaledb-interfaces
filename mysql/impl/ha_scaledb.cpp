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
//  File Name: ha_scaledb.cpp
//
//  Description: ScaleDB table handler.  ha_scaledb is a subclass of MySQL's handler class. 
//
//  Version: 1.0
//
//  Copyright: ScaleDB, Inc. 2007
//
//  History: 08/21/2007  RCH   coded.
//
*/
   
#ifdef SDB_MYSQL

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "../incl/sdb_mysql_client.h" // this should be included before ha_scaledb.h
#include "../incl/ha_scaledb.h"
#ifdef  __DEBUG_CLASS_CALLS
#include "../../../cengine/engine_util/incl/debug_class.h"
#endif

#include <sys/stat.h>

unsigned char ha_scaledb::mysqlInterfaceDebugLevel_ = 0;		// defines the debug level for Interface component

const char* mysql_key_flag_strings[] = {
	"HA_READ_KEY_EXACT",              
	"HA_READ_KEY_OR_NEXT",            
	"HA_READ_KEY_OR_PREV",            
	"HA_READ_AFTER_KEY",              
	"HA_READ_BEFORE_KEY",             
	"HA_READ_PREFIX",                 
	"HA_READ_PREFIX_LAST",            
	"HA_READ_PREFIX_LAST_OR_PREV"
};

static char* scaledb_config_file = NULL;

static int scaledb_init_func();
static handler *scaledb_create_handler(handlerton *hton,
									   TABLE_SHARE *table, 
									   MEM_ROOT *mem_root);

static int scaledb_close_connection(handlerton *hton, THD* thd);
static int scaledb_commit(handlerton *hton, THD* thd, bool all);  // inside handlerton 
static int scaledb_rollback(handlerton *hton, THD* thd, bool all);  // inside handlerton 
static void scaledb_drop_database(handlerton* hton, char* path);

static int scaledb_savepoint_set(handlerton *hton, THD *thd, void *sv);
static int scaledb_savepoint_rollback(handlerton *hton, THD* thd, void *sv);
static int scaledb_savepoint_release(handlerton *hton, THD* thd, void *sv);

static void start_new_stmt_trx(THD *thd, handlerton *hton);
static void rollback_last_stmt_trx(THD *thd, handlerton *hton);

/* Variables for scaledb share methods */
static HASH scaledb_open_tables; ///< Hash used to track the number of open tables; variable for scaledb share methods
pthread_mutex_t scaledb_mutex;   ///< This is the mutex used to init the hash; variable for scaledb share methods


// convert ScaleDB error code to MySQL error code
static int convertToMysqlErrorCode(int scaledbErrorCode) {
	int mysqlErrorCode = 0;

	switch ( scaledbErrorCode ) {
		case SUCCESS: 
			mysqlErrorCode = 0;
			break;
		case DATA_EXISTS: 
			mysqlErrorCode = HA_ERR_FOUND_DUPP_KEY;
			break;
		case KEY_DOES_NOT_EXIST: 
			mysqlErrorCode = HA_ERR_KEY_NOT_FOUND;
			break;
		case WRONG_PARENT_KEY: 
			//			mysqlErrorCode = HA_ERR_CANNOT_ADD_FOREIGN;
			mysqlErrorCode = HA_ERR_NO_REFERENCED_ROW;
			break;
			//	case QUERY_KEY_DOES_NOT_EXIST: 
			//		mysqlErrorCode = HA_ERR_KEY_NOT_FOUND;
			//		break;
		case DEAD_LOCK: 
		case LOCK_TABLE_FAILED:
			mysqlErrorCode = HA_ERR_LOCK_WAIT_TIMEOUT;
			break;
		case METAINFO_ATTEMPT_DROP_REFERENCED_TABLE: 
		case ATTEMPT_TO_DELETE_KEY_WITH_SUBORDINATES:
			mysqlErrorCode = HA_ERR_ROW_IS_REFERENCED;
			break;
		case QUERY_END:
		case STOP_TRAVERSAL:
		case QUERY_KEY_DOES_NOT_EXIST: 
			mysqlErrorCode = HA_ERR_END_OF_FILE;
			break;
		default:
			mysqlErrorCode = HA_ERR_GENERIC;
			break;
	}

	return mysqlErrorCode;
}


// find the DB name, table name, and path name from a fully qualified character array.
// An example of name is "./test/TableName".
// dbName is 'test', tableName is 'TableName', pathName is './'
// Note that Windows system uses backslash to separate DB name from table name.

void fetchIdentifierName(const char* name, char* dbName, char* tblName, char* pathName) {
	int i, j;
	for (i=strlen(name); i>=0; --i) {
		if ((name[i] == '/') || (name[i] == '\\'))
			break;
	}
	int tblStartPos = i + 1;
	for (j=0; tblStartPos + j <= (int) strlen(name); ++j)
		tblName[j]=name[tblStartPos + j];
	tblName[j]='\0';

	int dbStartPos = 0;
	if (tblStartPos > 1) {  // DB name is also specified
		for (i=tblStartPos-2; i>=0; --i) {
			if ((name[i] == '/') || (name[i] == '\\'))
				break;
		}
		dbStartPos = i + 1;
		for (j=0; dbStartPos + j < tblStartPos - 1; ++j)
			dbName[j]=name[dbStartPos + j];
		dbName[j]='\0';
	}

	for (j=0; j < dbStartPos; ++j) {
		if ( name[j] >= 'A' && name[j] <= 'Z' )
			pathName[j] = name[j] - 'A' + 'a';  // use lower case characters only
		else
			pathName[j] = name[j];
	}
	pathName[j] = '\0';
}

/* convert LF to space, remove extra separators, convert to lower case letters 
*/

void convertSeparatorLowerCase( char* toQuery, char* fromQuery) {
	int i = 0;
	int j = 0;

	while ( fromQuery[i] != '\0' ) {
		if ( fromQuery[i] == '`' ) {	// need to handle the quote character of an identifier
			do {	// need to copy the entire identifier name without skipping blanks until next backtick
				if ( (fromQuery[i] >= 'A') && (fromQuery[i] <= 'Z') )
					toQuery[j++] = fromQuery[i++] - 'A' + 'a';
				else
					toQuery[j++] = fromQuery[i++];
			} while (fromQuery[i] != '`');

			toQuery[j++] = fromQuery[i++];
		} else {
			if ( (fromQuery[i] >= 'A') && (fromQuery[i] <= 'Z') )
				toQuery[j] = fromQuery[i] - 'A' + 'a';
			else if ( (fromQuery[i]==' ') || (fromQuery[i] == '\t') || (fromQuery[i]=='\f')
				|| (fromQuery[i]=='\n') || (fromQuery[i]=='\r') ) {
					while ( (fromQuery[i+1]==' ') || (fromQuery[i+1] == '\t') || (fromQuery[i+1]=='\f')
						|| (fromQuery[i+1]=='\n') || (fromQuery[i+1]=='\r') )
						i = i+1;    // remove the extra separator
					toQuery[j] = ' ';
			} else
				toQuery[j] = fromQuery[i];

			++i;
			++j;
		}
	}

	if ( i == 0 )
		toQuery[0] = '\0';
	else
		toQuery[j] = '\0';	// set NULL after last byte
}


// A utility function to send SQL statement to execute on other nodes of a cluster machine
bool sendStmtToOtherNodes(unsigned short dbId, char* pStatement, bool bIgnoreDB, bool bEngineOption) {
	char* pStmtWithHint = SDBUtilAppendString(pStatement, SCALEDB_HINT_PASS_DDL);
	bool bRetValue = ha_scaledb::sqlStmt(dbId, pStmtWithHint, bIgnoreDB, bEngineOption);
	RELEASE_MEMORY(pStmtWithHint);
	return bRetValue;
}


static uchar* scaledb_get_key(SCALEDB_SHARE *share, size_t* length,
							  my_bool not_used __attribute__((unused)))
{
#ifdef SDB_DEBUG_LIGHT

	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing scaledb_get_key(...) ");
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	*length=share->table_name_length;
	return (uchar*) share->table_name;
}

/* This function is executed only once when MySQL server process comes up.
*/

static int scaledb_init_func(void *p)
{
	DBUG_ENTER("scaledb_init_func");

	handlerton* scaledb_hton;

	scaledb_hton = (handlerton *)p;
	VOID(pthread_mutex_init(&scaledb_mutex,MY_MUTEX_INIT_FAST));
	(void) hash_init(&scaledb_open_tables,system_charset_info,32,0,0,
		(hash_get_key) scaledb_get_key,0,0);

	scaledb_hton->state = SHOW_OPTION_YES;
	scaledb_hton->db_type= (enum legacy_db_type)((int)(DB_TYPE_FIRST_DYNAMIC - 1));

	// The following are pointers to functions.
	// Once we uncommet a pointer, then we need to implement the corresponding function.
	scaledb_hton->close_connection = scaledb_close_connection;

	scaledb_hton->savepoint_offset = 0;
	scaledb_hton->savepoint_set = scaledb_savepoint_set;
	scaledb_hton->savepoint_rollback = scaledb_savepoint_rollback;
	scaledb_hton->savepoint_release = scaledb_savepoint_release;

	scaledb_hton->commit = scaledb_commit;
	scaledb_hton->rollback = scaledb_rollback;
	/*
	scaledb_hton->prepare=scaledb_xa_prepare;
	scaledb_hton->recover=scaledb_xa_recover;
	scaledb_hton->commit_by_xid=scaledb_commit_by_xid;
	scaledb_hton->rollback_by_xid=scaledb_rollback_by_xid;
	scaledb_hton->create_cursor_read_view=scaledb_create_cursor_view;
	scaledb_hton->set_cursor_read_view=scaledb_set_cursor_view;
	scaledb_hton->close_cursor_read_view=scaledb_close_cursor_view;
	*/

	scaledb_hton->create = scaledb_create_handler;
	scaledb_hton->drop_database = scaledb_drop_database;
	/*
	scaledb_hton->panic=scaledb_end;
	scaledb_hton->start_consistent_snapshot=scaledb_start_trx_and_assign_read_view;
	scaledb_hton->flush_logs=scaledb_flush_logs;
	scaledb_hton->show_status=scaledb_show_status;
	scaledb_hton->release_temporary_latches=scaledb_release_temporary_latches;
	*/

	// bug #88, truncate should call delete_all_rows
	//scaledb_hton->flags = HTON_CAN_RECREATE;
	scaledb_hton->flags = HTON_TEMPORARY_NOT_SUPPORTED | HTON_NO_PARTITION ;

	if (SDBGlobalInit(scaledb_config_file)) {
        sql_print_error("________________________________________________");
        sql_print_error("ScaleDB: Failed to initialize the storage engine");
        sql_print_error("Check the configuration file and its location");
        sql_print_error("________________________________________________");
        DBUG_RETURN(1);
	}

	ha_scaledb::mysqlInterfaceDebugLevel_ = SDBGetDebugLevel();
#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintString("\nMySQL Interface: executing scaledb_init_func()");
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	DBUG_RETURN(0);
}


static int scaledb_done_func(void *p)
{
	DBUG_ENTER("scaledb_done_func");
#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintString("\nMySQL Interface: executing scaledb_done_func()");
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	int error= 0;
	if ( !SDBStorageEngineIsInited() )
		DBUG_RETURN(0);

	if (scaledb_open_tables.records)
		error= 1;

	hash_free(&scaledb_open_tables);
	SDBGlobalEnd();

	pthread_mutex_destroy(&scaledb_mutex);
	DBUG_RETURN(0);
}

/** @brief
Skeleton of simple lock controls. The "share" it creates is a structure we will
pass to each scaledb handler. Do you have to have one of these? Well, you have
pieces that are used for locking, and they are needed to function.
*/
static SCALEDB_SHARE *get_share(const char *table_name, TABLE *table)
{

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing get_share(table name: ");
		SDBDebugPrintString((char*) table_name);
		SDBDebugPrintString(" )");
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	SCALEDB_SHARE *share;
	uint length;
	char *tmp_name;

	pthread_mutex_lock(&scaledb_mutex);
	length=(uint) strlen(table_name);

	if (!(share=(SCALEDB_SHARE*) hash_search(&scaledb_open_tables,
		(uchar*) table_name,
		length)))
	{
		if (!(share=(SCALEDB_SHARE *)
			my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
			&share, sizeof(*share),
			&tmp_name, length+1,
			NullS)))
		{
			pthread_mutex_unlock(&scaledb_mutex);
			return NULL;
		}

		share->use_count=0;
		share->table_name_length=length;
		share->table_name=tmp_name;
		strmov(share->table_name,table_name);
		if (my_hash_insert(&scaledb_open_tables, (uchar*) share))
			goto error;
		thr_lock_init(&share->lock);
		pthread_mutex_init(&share->mutex,MY_MUTEX_INIT_FAST);
	}
	share->use_count++;
	pthread_mutex_unlock(&scaledb_mutex);

	return share;

error:
	pthread_mutex_destroy(&share->mutex);
	my_free((uchar *) share, MYF(0));

	return NULL;
}

/** @brief
Free lock controls. We call this whenever we close a table. If the table had
the last reference to the share, then we free memory associated with it.
*/
static int free_share(SCALEDB_SHARE *share)
{
#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing free_share(...) ");
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	pthread_mutex_lock(&scaledb_mutex);
	if (!--share->use_count)
	{
		hash_delete(&scaledb_open_tables, (uchar*) share);
		thr_lock_delete(&share->lock);
		pthread_mutex_destroy(&share->mutex);
		my_free((uchar *) share, MYF(0));
	}
	pthread_mutex_unlock(&scaledb_mutex);

	return 0;
}

/* We close user connection when he logs off.
We need to free MysqlTxn object for this user session.
*/
static int scaledb_close_connection(handlerton *hton, THD* thd)
{
	DBUG_ENTER("scaledb_close_connection");
	MysqlTxn* pMysqlTxn = (MysqlTxn *) *thd_ha_data(thd, hton);

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();	
		SDBDebugPrintHeader("MySQL Interface: executing scaledb_close_connection( ");
		SDBDebugPrintString("hton=");
		SDBDebugPrint8ByteUnsignedLong((unsigned long long )hton);
		SDBDebugPrintString(") pMysqlTxn=");
		SDBDebugPrint8ByteUnsignedLong((unsigned long long)pMysqlTxn);
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	// we assume that we get the user ID that is related to the lost connection
	unsigned short userId = pMysqlTxn->getScaleDbUserId();

	if ( pMysqlTxn != NULL ) {

		SDBRollBack( userId );

		if ( pMysqlTxn->getNumberOfLockTables() > 0 ){	// need to release outstanding locked tables before logoff
			pMysqlTxn->releaseAllLockTables();
		}
		
		SDBRemoveUserById(userId);

		delete pMysqlTxn;
		*thd_ha_data(thd, hton) = NULL;
	}

	DBUG_RETURN(0);
}


/*
Set a savepoint.
sv points to an uninitialized storage area of requested size.
The actual user's savepoint name is saved at sv + savepoint_offset + 32.
*/

static int scaledb_savepoint_set(handlerton *hton, THD *thd, void *sv) {

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing scaledb_savepoint_set(...) ");
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	DBUG_ENTER("scaledb_savepoint_set");
	bool isActiveTran = false;
	unsigned int userId;

	MysqlTxn* userTxn = (MysqlTxn *) *thd_ha_data(thd, hton);
	if ( userTxn != NULL ) {
		userId = userTxn->getScaleDbUserId();
		isActiveTran = userTxn->getActiveTxn();
	}

	// make sure it is not in automcommit mode.
	DBUG_ASSERT(thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN) || thd->in_sub_stmt);

	/* must happen inside of a transaction */
	DBUG_ASSERT(isActiveTran);

	// We use the memory address of sv where the argument sv is not the user-defined savepoint name.
	// According to Sergei Golubchik of MySQL, savepoint name is not exposed to storage engine.
	char pSavepointName[17];
	SDBSetSavePoint(userId, SDBUtilPtr2String(pSavepointName, sv));

	DBUG_RETURN(0);
}

/*
rollback to a specified savepoint.
The user's savepoint name is saved at sv + savepoint_offset
*/

static int scaledb_savepoint_rollback(handlerton *hton, THD *thd, void *sv) {

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing scaledb_savepoint_rollback(...) ");
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	DBUG_ENTER("scaledb_savepoint_rollback");
	int errorNum = 0;
	bool isActiveTran = false;
	unsigned int userId;

	MysqlTxn* userTxn = (MysqlTxn *) *thd_ha_data(thd, hton);
	if ( userTxn != NULL ) {
		userId = userTxn->getScaleDbUserId();
		isActiveTran = userTxn->getActiveTxn();
	}

	// make sure it is not in automcommit mode.
	DBUG_ASSERT(thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN) || thd->in_sub_stmt);

	/* must happen inside of a transaction */
	DBUG_ASSERT(isActiveTran);

	char pSavepointName[17];
	SDBUtilPtr2String(pSavepointName, sv);
	SDBRollBack(userId, SDBUtilPtr2String(pSavepointName, sv));

	DBUG_RETURN(errorNum);
}


/*
releases a specified savepoint.
The user's savepoint name is saved at sv + savepoint_offset
*/

static int scaledb_savepoint_release(handlerton *hton, THD *thd, void *sv) {

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing scaledb_savepoint_release(...) ");
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	DBUG_ENTER("scaledb_savepoint_release");
	int errorNum = 0;
	bool isActiveTran = false;
	unsigned int userId;

	MysqlTxn* userTxn = (MysqlTxn *) *thd_ha_data(thd, hton);
	if ( userTxn != NULL ) {
		userId = userTxn->getScaleDbUserId();
		isActiveTran = userTxn->getActiveTxn();
	}

	// make sure it is not in automcommit mode.
	DBUG_ASSERT(thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN) || thd->in_sub_stmt);

	/* must happen inside of a transaction */
	DBUG_ASSERT(isActiveTran);

	char pSavepointName[17];
	SDBUtilPtr2String(pSavepointName, sv);

	if ( SDBRemoveSavePoint(userId, pSavepointName) == false )
		errorNum = HA_ERR_NO_SAVEPOINT;

	DBUG_RETURN(errorNum);
}


/*
Commits a transaction or ends an SQL statement

MySQL query processor calls this method directly if there is an implied commit.
For example, START TRANSACTION, BEGIN WORK
*/
static int scaledb_commit(
						  handlerton *hton, 
						  THD* 	thd,	/* user thread */
						  bool	all)	/* true if it is a real commit, false if it is an end of statement */
{
	DBUG_ENTER("scaledb_commit");
	MysqlTxn* userTxn = (MysqlTxn *) *thd_ha_data(thd, hton);
	unsigned int userId = userTxn->getScaleDbUserId();

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing scaledb_commit(hton=");
		SDBDebugPrint8ByteUnsignedLong((uint64)hton);
		SDBDebugPrintString(", thd=");
		SDBDebugPrint8ByteUnsignedLong((uint64)thd);
		SDBDebugPrintString(", all=");
		SDBDebugPrintString(all ? "true" : "false");
		SDBDebugPrintString(")");
		SDBDebugPrintString("; ScaleDbUserId:");
		SDBDebugPrintInt( userId );
		SDBDebugPrintString("; Query:");
		SDBDebugPrintString(thd->query());
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	unsigned int sqlCommand = thd_sql_command(thd);
	if (sqlCommand == SQLCOM_LOCK_TABLES)
		DBUG_RETURN(0);		// do nothing if it is a LOCK TABLES statement. 

	if (all || (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN ))) {
		// should see how innobase_commit sets the condition for reference.
		// we need to commit txn under either of these two conditions:
		// (a) A user issues commit statement
		// (b) a user issues a single SQL statement with autocommit=1
		// In a single update statement after LOCK TABLES, MySQL may skip external_lock() 
		// and call this method directly.

		// if ( userTxn != NULL ) {
		//if ( (SDBNodeIsCluster() == true) && (!(userTxn->getDdlFlag() & SDBFLAG_ALTER_TABLE_KEYS)) &&
		//	(sqlCommand==SQLCOM_ALTER_TABLE || sqlCommand==SQLCOM_CREATE_INDEX || sqlCommand==SQLCOM_DROP_INDEX) ) {

		if ( (SDBNodeIsCluster() == true) && 
			 (!(userTxn->getDdlFlag() & SDBFLAG_DDL_SECOND_NODE)) &&
			 ((sqlCommand==SQLCOM_ALTER_TABLE || sqlCommand==SQLCOM_CREATE_INDEX || sqlCommand==SQLCOM_DROP_INDEX) 
				&& (userTxn->getDdlFlag() & SDBFLAG_ALTER_TABLE_CREATE) ) // ensure that commit is called inside processing ALTER TABLE
			) {
			// For regular ALTER TABLE, primary node will commit in delete_table method which is at very end of processing.
			// At this point, we only need to sync the changed data pages to disk.
			SDBSyncToDisk( userId );
		}
		else {
			// For all other cases, we should perform the normal commit.
			// This includes UNLOCK TABLE statement to release locks.
			// This also includes the case that, when auto_commit==0, an additional commit method is called before ALTER TABLE is executed.
			SDBCommit( userId );
			// After calling commit(), lockCount_ should be 0 except ALTER TABLE, CREATE/DROP INDEX.
			userTxn->lockCount_ = 0;
			if (userTxn->getDdlFlag() & SDBFLAG_ALTER_TABLE_KEYS)
				userTxn->setDdlFlag(0);	// reset the flag as we may check this flag in next SQL statement
		}

		userTxn->setActiveTrn( false );
	}  // else   TBD: mark it as the end of a statement.  Do we need logic here?

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugFlush();
	}
#endif

	DBUG_RETURN(0);
}


/*
Rollbacks a transaction or the latest SQL statement
*/
static int scaledb_rollback(
							handlerton *hton, 
							THD* 	thd,	/* user thread */
							bool	all )
{
	DBUG_ENTER("scaledb_rollback");

		//SDBDebugStart();
		//SDBDebugPrintHeader("MySQL called rollback for user: ");
		//SDBDebugPrintInt( userTxn->getScaleDbUserId() );
		//SDBDebugEnd();


#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing scaledb_rollback(...) ");
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	// all = 1; rollback complete transaction
	// all = 0; rollback last statement 

	if (!all){
		rollback_last_stmt_trx(thd, hton);
		DBUG_RETURN(0);
	}

	MysqlTxn* userTxn = (MysqlTxn *) *thd_ha_data(thd, hton);
	if ( userTxn != NULL ) {
		unsigned int userId = userTxn->getScaleDbUserId();
		SDBRollBack( userId );
		userTxn->setActiveTrn( false );
		unsigned int sqlCommand = thd_sql_command(thd);

		// After calling commit(), lockCount_ should be 0 except ALTER TABLE, CREATE/DROP INDEX.
		// In a single update statement after LOCK TABLES, MySQL may skip external_lock() 
		// and call this method directly.
		if ( (sqlCommand != SQLCOM_ALTER_TABLE) && (sqlCommand != SQLCOM_CREATE_INDEX) &&
			(sqlCommand != SQLCOM_DROP_INDEX) ) 
			userTxn->lockCount_ = 0;
	}  // else   TBD: issue an internal error message

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugFlush();
	}
#endif

	DBUG_RETURN(0);
}


static handler* scaledb_create_handler(handlerton *hton,
									   TABLE_SHARE *table, 
									   MEM_ROOT *mem_root)
{
#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing scaledb_create_handler(...) ");
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	return new (mem_root) ha_scaledb(hton, table);
}

static const char *ha_scaledb_exts[] = {
	NullS
};


// find the DB name from the specified path name where the database name is the last directory of the path.  
// For example, dbName is 'test' in the pathName "....\test\".
// Note that Windows system uses backslash to separate the directory name.
// This method allocated memory for char* dbName.  The calling method needs to release memory.
char* fetchDatabaseName(char* pathName) {
	int i;
	for (i=strlen(pathName)-2; i>=0; --i) {
		if ((pathName[i] == '/') || (pathName[i] == '\\'))
			break;
	}
	int dbStartPos = i + 1;
	int dbNameLen = strlen(pathName) - 1 - dbStartPos;
	char* dbName = (char*) GET_MEMORY( dbNameLen + 1 );
	memcpy(dbName, pathName+dbStartPos, dbNameLen);
	dbName[dbNameLen] = '\0';

	return dbName;
}


// drop a user database.
// parameter hton is the ScaleDB handlerton. 
// parameter char* path has value such as ".\sdbtest01\".  The database name is the last directory of the path. 
// MySQL will first call delete_table() to drop each user table in the given database to be dropped.
// In this method, we need to drop all the meta tables in the given database.
static void scaledb_drop_database(handlerton* hton, char* path) {

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing scaledb_drop_database(...) ");
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	unsigned short retValue = 0;
	int errorNum = 0;
	THD* thd = current_thd;
	unsigned int userId = 1;	// the system user
	MysqlTxn* userTxn = (MysqlTxn *) *thd_ha_data(thd, hton);
	if (userTxn == NULL) {
		// MySQL calls all storage engine to drop a user database even the storage engine has no user tables in it.
		// When userTxn has NULL, it means that other storage engine has tables, but not in ScaleDB storage engine.
		// need to check if the the very first meta table file exists.  If so, we need to continue
		// to clean up the meta files.
		// if the first meta table file no longer exists, then we exit.
#ifdef SDB_DEBUG_LIGHT
		if (ha_scaledb::mysqlInterfaceDebugLevel_) {
			SDBDebugStart();
			SDBDebugPrintHeader("nothing to delete in scaledb_drop_database\n");
			SDBDebugFlush();
			SDBDebugEnd();
		}
#endif
		return;
	} else
		userId = userTxn->getScaleDbUserId();

	// If the ddl statement has the key word SCALEDB_HINT_PASS_DDL,
	// then we need to update memory metadata only.  (NOT the metadata on disk).
	unsigned short ddlFlag = SDBFLAG_DDL_META_TABLES;
	if (SDBNodeIsCluster() == true)
		if ( strstr(thd->query(), SCALEDB_HINT_PASS_DDL) != NULL )
			ddlFlag |= SDBFLAG_DDL_SECOND_NODE;

	// First we fetch database name from the path
	char* pDbName = fetchDatabaseName( path );

	unsigned short dbId = SDBLockAndOpenDatabaseMetaInfo(userId, pDbName);

	// The primary node need to pass the DDL statement to engine so that it can be propagated to other nodes
	// We also need to make sure that the scaledb hint is not found in the user query.
	bool dropDbReady = true;
	unsigned int sqlCommand = thd_sql_command(thd);
	if ( (SDBNodeIsCluster() == true) && ( !(ddlFlag & SDBFLAG_DDL_SECOND_NODE)) )
		if (sqlCommand == SQLCOM_DROP_DB) {
			dropDbReady = sendStmtToOtherNodes(dbId, thd->query(), true, false);
		}

	if (dropDbReady)
		retValue = SDBLockAndRemoveDatabaseMetaInfo(userId, dbId, ddlFlag, pDbName);

	RELEASE_MEMORY( pDbName );
}


void ha_scaledb::print_header_thread_info(const char *msg) {

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintString("\n");
		SDBDebugPrintString(msg);
		if (ha_scaledb::mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif
}


// constructor
ha_scaledb::ha_scaledb(handlerton *hton, TABLE_SHARE *table_arg)
:handler(hton, table_arg) {

#ifdef __DEBUG_CLASS_CALLS
	DebugClass::countClassConstructor("ha_scaledb");
#endif

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		print_header_thread_info("MySQL Interface: executing ha_scaledb::ha_scaledb(...) ");
	}
#endif

	debugCounter_ = 0;
	deleteRowCount_ = 0;

	sdbDbId_ = 0;	
	sdbTableNumber_ = 0;
	//pMetaInfo_ = NULL;
	sdbQueryMgrId_ = 0;
	//pQm_ = NULL;
	pSdbMysqlTxn_ = NULL;
	beginningOfScan_ = false;
	deleteAllRows_ = false;
	sdbRowIdInScan_ = 0;
	extraChecks_ = 0;
	sdbCommandType_ = 0;
	numberOfFrontFixedColumns_ = 0;
	frontFixedColumnsLength_ = 0;
	releaseLocksAfterRead_ = false;
	virtualTableFlag_ = false;
	bRangeQuery_ = false;
}

ha_scaledb::~ha_scaledb() {
#ifdef __DEBUG_CLASS_CALLS
	DebugClass::countClassDestructor("ha_scaledb");
#endif
}

// output handle and MySQL user thread id
void ha_scaledb::outputHandleAndThd() {
	SDBDebugPrintString(", handler=");
	SDBDebugPrint8ByteUnsignedLong((unsigned long long)this);
	SDBDebugPrintString(", thd=");
	THD* thd = ha_thd();
	SDBDebugPrint8ByteUnsignedLong((unsigned long long)thd);
	if (thd) {
		SDBDebugPrintString(", Query:");
		SDBDebugPrintString(thd->query());
	}
}


// initialize DB id and Table id.  Returns 0 if there is an error
unsigned short ha_scaledb::initializeDbTableId(char* pDbName, char* pTblName, bool isFileName, bool allowTableClosed) {
	unsigned short retValue = SUCCESS;
	if ( !pDbName )
		pDbName = table->s->db.str;
	if ( !pTblName )
		pTblName = table->s->table_name.str;

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::initializeDbTableId(pDbName=");
		SDBDebugPrintString(pDbName);
		SDBDebugPrintString(", pTblName=");
		SDBDebugPrintString(pTblName);
		SDBDebugPrintString(")");
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	this->sdbDbId_ = SDBGetDatabaseNumberByName( sdbUserId_, pDbName );
	if ( this->sdbDbId_ == 0 ) {
		retValue = DATABASE_NAME_UNDEFINED;
		SDBTerminate(IDENTIFIER_INTERFACE + ERRORNUM_INTERFACE_MYSQL + 5, "Database definition is out of sync between MySQL and ScaleDB engine.\0" );
	}

	virtualTableFlag_ = SDBTableIsVirtual(pTblName);
	if ( !virtualTableFlag_ ) {
		this->sdbTableNumber_ = SDBGetTableNumberByName(sdbUserId_, sdbDbId_, pTblName );
		if ( this->sdbTableNumber_ == 0 ) {

			if (isFileName) {
				// We always try TableFsName again.  For some rare cases, we do not have tableName, but we have TableFsName. 
				sdbTableNumber_ = SDBGetTableNumberByFileSystemName( sdbUserId_, sdbDbId_, pTblName );
				if ( sdbTableNumber_ == 0 ) {
					if (allowTableClosed)
						return TABLE_NAME_UNDEFINED;
					else	// Try one more time by opening the table
						sdbTableNumber_ = SDBOpenTable(sdbUserId_, sdbDbId_, pTblName);  // bug137
				}
			}

			if ( this->sdbTableNumber_ == 0 ) {
				retValue = TABLE_NAME_UNDEFINED;
				SDBTerminate(IDENTIFIER_INTERFACE + ERRORNUM_INTERFACE_MYSQL + 6,  // ERROR - 16010006
					"Database definition is out of sync between MySQL and ScaleDB engine.\0" );
			}
		}
	}

	return retValue;
}


// pass the value in configuration parameter max_column_length_in_base_file
uint ha_scaledb::max_supported_key_part_length() const {

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
	SDBDebugStart();
	SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::max_supported_key_part_length() ");
	SDBDebugFlush();
	SDBDebugEnd();
	}
#endif

	return SDBGetMaxColumnLengthInBaseFile();
}

void start_new_stmt_trx(THD *thd, handlerton *hton)
{
	MysqlTxn* userTxn = (MysqlTxn *) *thd_ha_data(thd, hton);

	if (userTxn) {
		userTxn->setLastStmtSavePointId(SDBGetSavePointId(userTxn->getScaleDbUserId()));
	}
}


void rollback_last_stmt_trx(THD *thd, handlerton *hton)
{
	MysqlTxn* userTxn = (MysqlTxn *) *thd_ha_data(thd, hton);

	if (userTxn) {
		SDBRollBackToSavePointId(userTxn->getScaleDbUserId(), userTxn->getLastStmtSavePointId());
	}
}


// MySQL calls this method for every table it is going to use at the beginning of every statement.
// Thus, if a table is touched for the first time, it implicitly starts a transaction.
// Note that a table handler is used exclusively by a single user thread after the first external_lock call
// of a SQL statement.  The user thread will release the table handler in the second external_lock call.
// That is there are always a pair of external_lock calls for each table accessed in a SQL statement.
// We can use this function to store the pointer pSdbMysqlTxn_ for a given user thread in the handler.  
// We will also use this function to communicate to ScaleDB that a new SQL statement has started 
// and that we must store a savepoint to our transaction handler, so that we are able to roll back
// the SQL statement in case of an error. */
int ha_scaledb::external_lock(
							  THD*	thd,	/* handle to the user thread */
							  int	lock_type)	/* lock type */
{
	DBUG_ENTER("ha_scaledb::external_lock");
	DBUG_PRINT("enter",("lock_type: %d", lock_type));

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::external_lock(lock_type=");
		switch (lock_type) {
			case F_UNLCK:
				SDBDebugPrintString("F_UNLCK");
				break;
			case F_RDLCK:
				SDBDebugPrintString("F_RDLCK");
				break;
			case F_WRLCK:
				SDBDebugPrintString("F_WRLCK");
				break;
				// 			NOT DEFINED ON LINUX
				//			case _LK_LOCK:
				//				SDBDebugPrintString("_LK_LOCK");
				//				break;
			default:
				SDBDebugPrintString("Lock Type ");
				SDBDebugPrintInt(lock_type);
				break;
		}

		SDBDebugPrintString(")");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	int retValue = 0;
	bRangeQuery_ = false;	// this flag is set to true between the pair of external_lock calls.
	placeSdbMysqlTxnInfo( thd );

	this->sqlCommand_ = thd_sql_command(thd);
	switch (sqlCommand_) {
		case SQLCOM_INSERT:
			sdbCommandType_ = SDB_COMMAND_INSERT;
			break;
		case SQLCOM_LOAD:
			sdbCommandType_ = SDB_COMMAND_LOAD;
			break;
		case SQLCOM_ALTER_TABLE: 
		case SQLCOM_CREATE_INDEX: 
		case SQLCOM_DROP_INDEX: 
			sdbCommandType_ = SDB_COMMAND_ALTER_TABLE;
			break;
		default:
			sdbCommandType_ = 0;
			break;
	}

	// On a non-primary node, we do nothing for DDL statements.
	// CREATE TABLE ... SELECT statement calls this method as well.
	if (sqlCommand_==SQLCOM_CREATE_TABLE || sdbCommandType_==SDB_COMMAND_ALTER_TABLE) { 
		if ( thd->query() && strstr(thd->query(), SCALEDB_HINT_PASS_DDL) != NULL ) { // must be last comparison as it is expensive
			// this is secondary node
			if (lock_type != F_UNLCK)
				// set up ddlFlag_ for subsequent use between the pair of external_lock method calls.
				pSdbMysqlTxn_->setOrOpDdlFlag( (unsigned short)SDBFLAG_DDL_SECOND_NODE );
			else
				pSdbMysqlTxn_->setDdlFlag(0);	// reset the flag as MySQL may reuse this handler object
			DBUG_RETURN(0);
		} else {	
			// this is primary node
			if ( (sdbCommandType_==SDB_COMMAND_ALTER_TABLE) && (lock_type != F_UNLCK) 
				&& (strstr(table->s->table_name.str, MYSQL_TEMP_TABLE_PREFIX)==NULL) ) {
				// We can find the alter-table name in the first external_lock on the regular table name
					pSdbMysqlTxn_->addAlterTableName(table->s->table_name.str);
			}
		}
	}

	// fetch sdbTableNumber_ if it is 0 (which means a new table handler).
	if ( (sdbTableNumber_ == 0) && (!virtualTableFlag_) ) 
		retValue = initializeDbTableId();

	// need to check scaledbDbId_ in MysqlTxn object because the same table handler may be used by a new user session.
	if ( pSdbMysqlTxn_->getScaledbDbId() == 0 ) 
		pSdbMysqlTxn_->setScaledbDbId(sdbDbId_);

	// determine if reads will be referenced again in the same SQL statement
	if (lock_type == F_WRLCK) {
		releaseLocksAfterRead_ = false;
	}
	else {
		releaseLocksAfterRead_ = true;
	}


	// lock the table if the user has an explicit lock tables statement
	if ( sqlCommand_ == SQLCOM_UNLOCK_TABLES ) {	// now the user has an explicit UNLOCK TABLES statement

		retValue = pSdbMysqlTxn_->removeLockTableName(table->s->table_name.str );

		// UNLOCK TABLES implicitly commits any active transaction, but only if LOCK TABLES has been used to acquire table locks.
		// Hence we need to call commit() to release table locks if this is the last unlock table and it is not
		// in a transaction.
		if ( retValue && (pSdbMysqlTxn_->numberOfLockTables_ == 0) && (pSdbMysqlTxn_->getActiveTxn() == false) ) {
			pSdbMysqlTxn_->freeAllQueryManagerIds(mysqlInterfaceDebugLevel_);
			SDBCommit( sdbUserId_ );	// call engine to commit directly
			//scaledb_commit(ht, thd, true);
		}

		DBUG_RETURN(0);
	}

	if (lock_type != F_UNLCK) {

#ifdef SDB_DEBUG
		SDBLogSqlStmt(sdbUserId_, thd->query(), thd->query_id);	// inform engine to log user query for DML
#endif
		bool all_tx = false;

		// lock the table if the user has an explicit lock tables statement
		if ( (sqlCommand_ == SQLCOM_LOCK_TABLES) ) {	// now the user has an explicit LOCK TABLES statement
			if ( thd_in_lock_tables(thd) ) {

				unsigned char lockLevel = REFERENCE_READ_ONLY;	// table level read lock
				if ( lock_type > F_RDLCK )
					lockLevel = REFERENCE_LOCK_EXCLUSIVE;	// table level write lock

				bool bGetLock = SDBLockTable(sdbUserId_, sdbDbId_, sdbTableNumber_, lockLevel);
				if (bGetLock == false)	// failed to lock the table
					DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));

				pSdbMysqlTxn_->addLockTableName( table->s->table_name.str );

				// Need to register with MySQL for the beginning of a transaction so that we will be called
				// to perform commit/rollback statements issued by end users.  LOCK TABLES implies a transaction.
				trans_register_ha(thd, true, ht);
			} 
		}
		else {	// the normal case
			if ( pSdbMysqlTxn_->lockCount_ == 0 ) {
				pSdbMysqlTxn_->txnIsolationLevel_ = thd->variables.tx_isolation;
				all_tx= test(thd->options & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN | OPTION_TABLE_LOCK));  //TBD ??

				// issue startTransaction() only for the very first statement
				if ( pSdbMysqlTxn_->getActiveTxn() == false ) {
					sdbUserId_ = pSdbMysqlTxn_->getScaleDbUserId();

#ifdef SDB_DEBUG_LIGHT
#ifdef SDB_WINDOWS
					char buf[128];
					sprintf(buf, "Scaledb User %d Thread", sdbUserId_); 
					SDBSetThreadName(buf); // for convenience in Visual Studio debugger
#endif
#endif

					SDBStartTransaction( sdbUserId_ );
					long txnId = SDBGetTransactionIdForUser( sdbUserId_ );
					pSdbMysqlTxn_->setScaleDbTxnId( txnId );
					pSdbMysqlTxn_->setActiveTrn( true );
				}
			}

			// Because the close method first imposes exclusive table level lock and then removes all metadata information from memory,
			// we need to impose a table level shared lock in order to protect the memory metadata to be used in DML. 
			// For TRUNCATE TABLE, secondary node should not impose lock as primary node already imposes an exclusive table level lock.
			// For all other cases, we should impose a table level shared lock.
			if ( ((sqlCommand_ != SQLCOM_TRUNCATE) || (strstr(thd->query(), SCALEDB_HINT_PREFIX) == NULL)) && (!virtualTableFlag_) ) {
				unsigned char lockLevel = DEFAULT_REFERENCE_LOCK_LEVEL;
				if ( (sqlCommand_ == SQLCOM_LOAD) && (lock_type > F_RDLCK) )
					lockLevel = REFERENCE_LOCK_EXCLUSIVE;	// impose table level write lock for LOAD DATA statement
				bool bGetLock = SDBLockTable(sdbUserId_, sdbDbId_, sdbTableNumber_, lockLevel);
				if (bGetLock == false)	// failed to lock the table
					DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
			}

			// increment the count of table locks in a statement.
			// For statements in LOCK TABLES scope, its count is incremented in start_stmt().
			pSdbMysqlTxn_->lockCount_ += 1;
		}

		// Need to register with MySQL for the beginning of a transaction so that we will be called
		// to perform commit/rollback statements issued by end users

		trans_register_ha(thd, false, ht);

		if (thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
			trans_register_ha(thd, true, ht);
		}

		// set some useful constants in case a table has all fixed length columns
		numberOfFrontFixedColumns_ = SDBGetNumberOfFrontFixedColumns(sdbDbId_, sdbTableNumber_);
		frontFixedColumnsLength_ = SDBGetFrontFixedColumnsLength(sdbDbId_, sdbTableNumber_);

		/*
		else if (txn->stmt == 0) {  //TBD: if there is no savepoint defined
		txn->stmt= txn->new_savepoint();	// to add savepoint
		trans_register_ha(thd, false, ht);
		}
		*/
	} else {	// need to release locks
		beginningOfScan_ = false;
		if ( pSdbMysqlTxn_->lockCount_ > 0 ) {

			if ( pSdbMysqlTxn_->getActiveTxn() )
				start_new_stmt_trx(thd, ht);

			pSdbMysqlTxn_->lockCount_ -= 1;
		}

		if ( pSdbMysqlTxn_->lockCount_ == 0 ) {  // TBD: If the lockCount_ is not correct, then we may have QueryManagerId not released
			pSdbMysqlTxn_->freeAllQueryManagerIds(mysqlInterfaceDebugLevel_);
			if ( !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN) ) {	// in auto_commit mode

				// MySQL query processor does NOT execute autocommit for pure SELECT transactions.
				// We should commit for SELECT statement in order for ScaleDB engine to release the right locks.
				// if ( pSdbMysqlTxn_->getActiveTxn() )	// 2008-10-18: this condition is not needed
				if ( sqlCommand_ == SQLCOM_SELECT )
					SDBCommit( sdbUserId_ );	// call engine to commit directly
					//scaledb_commit(ht, thd, false);

			}

			//if (pSdbMysqlTxn_->stmt != NULL) {
			//	/* Commit the transaction if we're in auto-commit mode */
			//	scaledb_commit(thd, false);
			//	delete txn->stmt; // delete savepoint
			//	txn->stmt= NULL;
		}
	}

	DBUG_RETURN(0);
}


// If a user issues LOCK TABLES statement, MySQL will call ::external_lock only once to release lock.
// In this case, MySQL will call this method at the beginning of each SQL statement after LOCK TABLES statement.
int ha_scaledb::start_stmt(THD* thd, thr_lock_type lock_type) {
	DBUG_ENTER("ha_scaledb::start_stmt");
	DBUG_PRINT("enter",("lock_type: %d", lock_type));

	print_header_thread_info("MySQL Interface: executing ha_scaledb::start_stmt()");

	int retValue = 0;
	placeSdbMysqlTxnInfo( thd );	

	this->sqlCommand_ = thd_sql_command(thd);
	switch (sqlCommand_) {
		case SQLCOM_INSERT:
			sdbCommandType_ = SDB_COMMAND_INSERT;
			break;
		case SQLCOM_LOAD:
			sdbCommandType_ = SDB_COMMAND_LOAD;
			break;
		case SQLCOM_ALTER_TABLE: 
		case SQLCOM_CREATE_INDEX: 
		case SQLCOM_DROP_INDEX: 
			sdbCommandType_ = SDB_COMMAND_ALTER_TABLE;
			break;
		default:
			sdbCommandType_ = 0;
			break;
	}

	if (sdbCommandType_==SDB_COMMAND_ALTER_TABLE) {
		if ( (thd->query()) && (strstr(thd->query(), SCALEDB_HINT_PASS_DDL) == NULL) )
		{	// This is the primary node in a cluster system
			if (strstr(table->s->table_name.str, MYSQL_TEMP_TABLE_PREFIX)==NULL)  
				// We can find the alter-table name (same logic used in the first external_lock when there is no LOCK TABLES)
				pSdbMysqlTxn_->addAlterTableName(table->s->table_name.str);	// must be the regular table name, not temporary table
		}
	}

	bool all_tx = false;
	if (pSdbMysqlTxn_->lockCount_ == 0) {
		pSdbMysqlTxn_->txnIsolationLevel_ = thd->variables.tx_isolation;

		if ( (sdbTableNumber_ == 0) && (!virtualTableFlag_) ) 
			retValue = initializeDbTableId();

		sdbUserId_ = pSdbMysqlTxn_->getScaleDbUserId();
		pSdbMysqlTxn_->setScaledbDbId(sdbDbId_);
		long txnId = SDBGetTransactionIdForUser( sdbUserId_ );
		pSdbMysqlTxn_->setScaleDbTxnId( txnId );
		pSdbMysqlTxn_->setActiveTrn( true );
	}

	pSdbMysqlTxn_->lockCount_ += 1;

	// Need to register with MySQL for the beginning of a transaction so that we will be called
	// to perform commit/rollback statements issued by end users
	trans_register_ha(thd, false, ht);

	all_tx= ( thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN) > 0 ) ? true : false; 
	if ( all_tx )
		trans_register_ha(thd, true, ht);	
	else
		start_new_stmt_trx(thd, ht);

	DBUG_RETURN(0);
}


const char **ha_scaledb::bas_ext() const
{
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
	SDBDebugStart();
	SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::bas_ext() ");
	SDBDebugFlush();
	SDBDebugEnd();
	}
#endif

	return ha_scaledb_exts;
}


// This method is outside of a user transaction.  It opens a user database and saves value in sdbDbId_.
// It does not open individual table files.
unsigned short ha_scaledb::openUserDatabase(char* pDbName, char *pDbFsName, bool bIsPrimaryNode, bool bIsAlterTableStmt, unsigned short ddlFlag/*=0*/) {

	unsigned short retCode = 0;

	// Because this method is called outside of a normal user transaction,
	// hence we use a different user id to open database and table in order to avoid commit issue.
	unsigned int userIdforOpen = SDBGetNewUserId();

	// Note that SDBLockMetaInfo is expensive, we should avoid it as much as possible.
	// SDBLockMetaInfo should be called by the first user who opens the database and/or table.

	bool needToOpenDbFiles = false;
	sdbDbId_ = SDBGetDatabaseNumberByName( userIdforOpen, pDbFsName );
	if ( sdbDbId_ == 0 )
		needToOpenDbFiles = true;
	else {
		// If we use configuration parameter 'db_directory' to define a database location,
		// then we may set a Dbid without opening the database.
		// In this case, we need to open the user database here.
		if ( SDBGetDatabaseStatusByNumber(sdbDbId_) == false )
			needToOpenDbFiles = true;
	}

	// First we open the user database

	if ( needToOpenDbFiles ) {
		unsigned short dbIdToLock = sdbDbId_;
		if (dbIdToLock == 0)
			dbIdToLock = SDB_MASTER_DBID;

		// Primary node locks the metadata of the user db (or master DB if user db is not defined) before opening a user db.
		// For ALTER TABLE, primary node performs lockMetaInfo in ::create, not before that point.
		// Secondary node should NOT impose lockMetaInfo because primary node already did.
		if ( bIsPrimaryNode ) {
			if ( bIsAlterTableStmt == false )
				retCode = SDBLockMetaInfo(userIdforOpen, dbIdToLock);
			if (retCode) {
				SDBRemoveUserById(userIdforOpen);
				return retCode;
			}
		}

		if (sdbDbId_ == 0) {
			sdbDbId_ = SDBOpenDatabase(userIdforOpen, pDbName, pDbFsName, pDbFsName, ddlFlag);
		} else
			SDBOpenDatabaseById(userIdforOpen, sdbDbId_);

		// We can commit without problem for this new user
		SDBCommit(userIdforOpen);
	}

	pSdbMysqlTxn_->setScaledbDbId(sdbDbId_);
	SDBRemoveUserById(userIdforOpen);
	return retCode;
}


// open a table's data files and index files.  Parameter name is file-system safe.
// MySQL calls this method when a given table is first accessed by any user.
// Hence this method is outside of a user transaction, i.e. outside of the pairing external_lock calls.
int ha_scaledb::open(const char *name, int mode, uint test_if_locked) {
	DBUG_ENTER("ha_scaledb::open");

#ifdef SDB_DEBUG_LIGHT

	if (mysqlInterfaceDebugLevel_) {
		print_header_thread_info("MySQL Interface: executing ha_scaledb::open");
		SDBDebugStart();
		SDBDebugPrintHeader("ha_scaledb::open name = ");
		SDBDebugPrintString((char *)name);
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	if (!(share = get_share(name, table)))
		DBUG_RETURN(1);

	stats.block_size =  METAINFO_BLOCK_SIZE;   // index block size
	thr_lock_data_init(&share->lock,&lock,NULL);

	int errorNum = 0;
	unsigned short retCode = 0;
	char dbFsName[METAINFO_MAX_IDENTIFIER_SIZE] = {0};	// database name that is compliant with file system
	char tblFsName[METAINFO_MAX_IDENTIFIER_SIZE] = {0}; // table name that is compliant with file system
	char pathName[METAINFO_MAX_IDENTIFIER_SIZE] = {0};
	fetchIdentifierName(name, dbFsName, tblFsName, pathName); 

	THD* thd = ha_thd();
	placeSdbMysqlTxnInfo( thd );	

	unsigned int sqlCommand = thd_sql_command(thd);
	bool bIsAlterTableStmt = false;
	if (sqlCommand==SQLCOM_ALTER_TABLE || sqlCommand==SQLCOM_CREATE_INDEX || sqlCommand==SQLCOM_DROP_INDEX)
		bIsAlterTableStmt = true;
	bool openTempFile = false;
	if (strstr(tblFsName, MYSQL_TEMP_TABLE_PREFIX))
		openTempFile = true;

	bool bIsPrimaryNode = true;
	if (SDBNodeIsCluster() == true)	{	// if this is a cluster machine
		if ( strstr(thd->query(), SCALEDB_HINT_PASS_DDL) != NULL ) {		// if this is a non-primary node
			bIsPrimaryNode = false;

			// For ALTER TABLE, secondary node should NOT open table file.
			// For CREATE TABLE t2 SELECT 1, 'aa'; secondary node should exit early as well.
			// For CREATE TABLE t2 SELECT * FROM t1, secondary node should continue because we need to open the select-from table.
			//if ( (bIsAlterTableStmt && openTempFile) ||
			if ( (bIsAlterTableStmt) ||
				( (sqlCommand==SQLCOM_CREATE_TABLE) && (pSdbMysqlTxn_->getDdlFlag() & SDBFLAG_DDL_SECOND_NODE) ) )
				DBUG_RETURN(errorNum);
		}
	}

#ifdef SDB_DEBUG
	if (mysqlInterfaceDebugLevel_ > 4) 
		SDBShowAllUsersLockStatus();
#endif

	// First, we open the user database by retrieving the metadata information.
	retCode = openUserDatabase(table->s->db.str, dbFsName, bIsPrimaryNode, bIsAlterTableStmt, pSdbMysqlTxn_->getDdlFlag());
	if (retCode) 
		DBUG_RETURN(convertToMysqlErrorCode(retCode));


	char* pTblFsName = &tblFsName[0];	// points to the beginning of tblFsName
	virtualTableFlag_ = SDBTableIsVirtual(pTblFsName);
	if (virtualTableFlag_) {
		pTblFsName = pTblFsName + SDB_VIRTUAL_VIEW_PREFIX_BYTES;
		char* pUnderscores = strstr(pTblFsName, "___");
		*pUnderscores = 0;	//  make pTblFsName stop at the beginning of "___"
	}

	// Because MySQL calls this method outside of a normal user transaction,
	// hence we use a different user id to open database and table in order to avoid commit issue.
	unsigned int userIdforOpen = SDBGetNewUserId();

	// Second, we open the specified table for either single node or primary node of a cluster.
	// Also need to open non-temp table files on secondary nodes for later processing.

	sdbTableNumber_ = SDBGetTableNumberByFileSystemName(sdbUserId_, sdbDbId_, pTblFsName);
	if (sdbTableNumber_ == 0) {

		if (SDBNodeIsCluster() == true)	{	// if this is a cluster machine
			// need to open the table because it may be closed by secondary node processing on DDL.
			// Secondary node should NOT impose lockMetaInfo because primary node already did.
			// If we open a temp file for ALTER TABLE, then do not lockMetaInfo as it is inside a user transaction.
			if ( bIsPrimaryNode ) {
				if (bIsAlterTableStmt && openTempFile)	// inside ALTER TABLE
					sdbTableNumber_ = SDBOpenTable(sdbUserId_, sdbDbId_, pTblFsName);
				else {	// outside of a user transaction
					retCode = SDBLockMetaInfo(userIdforOpen, sdbDbId_);
					if (retCode == SUCCESS)
						sdbTableNumber_ = SDBOpenTable(userIdforOpen, sdbDbId_, pTblFsName);	
				}
			}

			// For CREATE TABLE ... SELECT, Secondary node needs to open the select-from table without calling SDBLockMetaInfo. 
			// Secondary node should NOT open the new table which is inside of external_lock pair.
			if ( (sqlCommand==SQLCOM_CREATE_TABLE) && (bIsPrimaryNode==false)
				&& (pSdbMysqlTxn_->getDdlFlag() == 0) )		// this condition says the to-be-opened table is outside external_lock pair
				sdbTableNumber_ = SDBOpenTable(userIdforOpen, sdbDbId_, pTblFsName);
		}
		else {	// on a single node solution
			if (bIsAlterTableStmt && openTempFile)	// this is inside ALTER TABLE
				sdbTableNumber_ = SDBOpenTable(sdbUserId_, sdbDbId_, pTblFsName);
			else {	// outside of a user transaction
				retCode = SDBLockMetaInfo(userIdforOpen, sdbDbId_);
				if (retCode == SUCCESS)
					sdbTableNumber_ = SDBOpenTable(userIdforOpen, sdbDbId_, pTblFsName);	
			}
		}
	}

	// We can commit without problem for this new user
	SDBCommit(userIdforOpen);
	SDBRemoveUserById(userIdforOpen);
	errorNum = convertToMysqlErrorCode(retCode);
	if (errorNum == 0)
		errorNum = info(HA_STATUS_VARIABLE | HA_STATUS_CONST);

	DBUG_RETURN(errorNum);
}


// The close() method can be called when thd is either defined or undefined.
// In a high-concurrency environment, MySQL may create multiple table handlers for same table.
// Hence, this method may be called multiple times for a single table when MySQL decides to release the extra
// table handlers.  Today, we use sqlCommand to decide whether to not we need to remove table metadata from memory.
// For FLUSH statements, we need to flush user data to disk and remove memory metadata. How about DDL?
// TODO: If the current method does not work well in a high-concurrency environment, then we need to maintain
// a counter on open/close a table in TableInfo object.  We remove table metadata from memory when the counter is 0.
int ha_scaledb::close(void) {
	DBUG_ENTER("ha_scaledb::close");
	THD* thd = current_thd;	// need to use this function in order to get the correct user query in a multi-user environment
				// another possibility is to use ha_thd() to fetch current user thread.  may try this one later.

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::close(void) ");
		SDBDebugPrintString(", handler=");
		SDBDebugPrint8ByteUnsignedLong((unsigned long long)this);
		SDBDebugPrintString(", thd=");
		SDBDebugPrint8ByteUnsignedLong((unsigned long long)thd);
		if (thd) {
			SDBDebugPrintString(", Query:");
			SDBDebugPrintString(thd->query());
		}
		SDBDebugEnd();
	}
#endif

	int errorCode = 0;
	unsigned int userId = 0;
	bool needToRemoveFromScaledbCache = false;
	bool needToCommit = false;
	unsigned short ddlFlag = 0;
	if (virtualTableFlag_) {	// do nothing for virtual table
		sdbTableNumber_ = 0;
		DBUG_RETURN(free_share(share));
	}

	unsigned int sqlCommand;
	if (thd) {
		// thd is defined.  In this case, a user executes a DDL.
		placeSdbMysqlTxnInfo( thd );

		bool bIsAlterTableStmt = false;
		sqlCommand = thd_sql_command(thd);
		if (sqlCommand==SQLCOM_ALTER_TABLE || sqlCommand==SQLCOM_CREATE_INDEX || sqlCommand==SQLCOM_DROP_INDEX)
			bIsAlterTableStmt = true;
		if ( strstr(thd->query(), SCALEDB_HINT_PASS_DDL) )
			ddlFlag |= SDBFLAG_DDL_SECOND_NODE;		// this flag is used locally.

		// For ALTER TABLE, CREATE/DROP INDEX, secondary node should not close table because the temp table
		// was never created and the table-to-be-altered is already closed in FLUSH TABLE statement.
		// For CREATE TABLE ... SELECT statement, secondary node should exit early because the new table
		// has not been committed by primary node.
		if ( SDBNodeIsCluster() && (ddlFlag & SDBFLAG_DDL_SECOND_NODE) ) {
			if ( (bIsAlterTableStmt) || (sqlCommand==SQLCOM_CREATE_TABLE) )
				DBUG_RETURN(free_share(share));
		}

		if ( bIsAlterTableStmt || sqlCommand==SQLCOM_CREATE_TABLE || sqlCommand==SQLCOM_DROP_TABLE || sqlCommand==SQLCOM_DROP_DB 
			|| sqlCommand==SQLCOM_FLUSH || sqlCommand==SQLCOM_RENAME_TABLE || sqlCommand==SQLCOM_ALTER_TABLESPACE )
			needToRemoveFromScaledbCache = true;

		if (bIsAlterTableStmt) {
			if (pSdbMysqlTxn_->getDdlFlag() & SDBFLAG_ALTER_TABLE_CREATE)
				// For regular ALTER TABLE, primary node will commit in delete_table method which is at very end of processing.
				needToCommit = false;
			else	// statement such as  ALTER TABLE t1 DISABLE KEYS;
				needToCommit = true;
		} else if (sqlCommand==SQLCOM_FLUSH || sqlCommand==SQLCOM_CREATE_TABLE)	// test case 206.sql
			needToCommit = true;

		userId = sdbUserId_;
	}
	else {	// thd is NOT defined.  A system thread closes the opened tables.  Bug 969
		userId = 1;	// this is system user ID
		needToRemoveFromScaledbCache = true;
		needToCommit = true;
	}

	// MySQL may call this method multiple times on same table if a table is referenced more than
	// one time in a previous SQL statement.  Example: INSERT INTO tbug261 SELECT  a + 1 , b FROM tbug261;
	// Need to call SDBCloseTable instead of SDBCloseFile so that ScaleDB engine tolerates multiple calls of this method.
	// Also MySQL instantiates a table handler object if another user thread is holding a handler object for the same table.
	// MySQL remembers how many handler objects it creates for a given table.  It will call this method one time
	// for each instantiated table handler.  Hence we remove table information from metainfo for DDL/FLUSH statements only
	// or this method is called from a system thread (such as mysqladmin shutdown command).
	if (needToRemoveFromScaledbCache) {
		if (ddlFlag & SDBFLAG_DDL_SECOND_NODE)
			errorCode = SDBCloseTable(userId, sdbDbId_, table->s->table_name.str, false, false);
		else
			errorCode = SDBCloseTable(userId, sdbDbId_, table->s->table_name.str, true, needToCommit);

		if (errorCode)
			DBUG_RETURN(convertToMysqlErrorCode(errorCode));
	}

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 1) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("In ha_scaledb::close, needToRemoveFromScaledbCache=");
		if (needToRemoveFromScaledbCache)
			SDBDebugPrintString("true");
		else
			SDBDebugPrintString("false");

		SDBDebugPrintString(", needToCommit=");
		if (needToCommit)
			SDBDebugPrintString("true");
		else
			SDBDebugPrintString("false");

		if (thd) {
			SDBDebugPrintString("sqlCommand=");
			SDBDebugPrintInt(sqlCommand);
		}

		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

#ifdef SDB_DEBUG
	if (mysqlInterfaceDebugLevel_ > 4) {
		// print user lock status on the primary node
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("In ha_scaledb::close, print user locks after SDBCloseTable");
		SDBDebugEnd();			// synchronize threads printout	
		SDBShowUserLockStatus(sdbUserId_);
	}
#endif

	// set table number to 0 since this table handler is closed.
	sdbTableNumber_ = 0;
	DBUG_RETURN(free_share(share));
}


// This method saves a MySQL transaction information for a given user thread.
// When isTransient is true (outside the pair of external_locks calls), we do NOT save information into ha_scaledb.
// When it is false, we save the returned pointer (possible others) into ha_scaledb member variables.
MysqlTxn* ha_scaledb::placeSdbMysqlTxnInfo(THD* thd, bool isTransient /*=false*/) {

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::placeSdbMysqlTxnInfo(thd=");
		SDBDebugPrint8ByteUnsignedLong((uint64)thd);
		SDBDebugPrintString(", isTransient=");
		if ( isTransient ) 
			SDBDebugPrintString("true)");
		else
			SDBDebugPrintString("false)");

		if (mysqlInterfaceDebugLevel_>1) {
			SDBDebugPrintString(", handler=");
			SDBDebugPrint8ByteUnsignedLong((uint64)this);
		}
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	MysqlTxn* pMysqlTxn = (MysqlTxn *) *thd_ha_data(thd, ht);
	if (pMysqlTxn == NULL) {	// a new user session
		pMysqlTxn = new MysqlTxn();
#ifdef SDB_DEBUG_LIGHT
		if (ha_scaledb::mysqlInterfaceDebugLevel_) {
			SDBDebugStart();			// synchronize threads printout	
			SDBDebugPrintHeader("MySQL Interface: new MysqlTxn");
			SDBDebugPrintString(", thd=");
			SDBDebugPrint8ByteUnsignedLong((uint64)thd);
			SDBDebugPrintString(", pMysqlTxn=");
			SDBDebugPrint8ByteUnsignedLong((uint64)pMysqlTxn);
			SDBDebugEnd();			// synchronize threads printout	
		}
#endif
		pMysqlTxn->setMysqlThreadId( thd->thread_id );
		unsigned int scaleDbUserId = SDBGetNewUserId();
		pMysqlTxn->setScaleDbUserId( scaleDbUserId );

		// save the new pointer into the per-connection place

		*thd_ha_data(thd, ht) = pMysqlTxn ;
	}

	if (isTransient == false) {		// save into member variables if it is not transient
		this->pSdbMysqlTxn_ = pMysqlTxn;
		this->sdbUserId_ = pMysqlTxn->getScaleDbUserId();
	}

	return pMysqlTxn;
}


// This method packs a MySQL row into ScaleDB engine row buffer 
// When we prepare the old row value for update operation, 
// rowBuf1 points to old MySQL row buffer and rowBuf2 points to new MySQL row buffer.
// For all other cases, these 2 row buffers point to same row buffer
unsigned short ha_scaledb::placeMysqlRowInEngineBuffer(unsigned char* rowBuf1, unsigned char* rowBuf2, unsigned short groupType, 
													   bool checkAutoIncField, bool &needToUpdateAutoIncrement) {

	unsigned short retCode = 0;
	if ( (sdbTableNumber_ == 0) && (!virtualTableFlag_) ) 
		retCode = initializeDbTableId();

	if ( (sdbTableNumber_ == 0) && (!virtualTableFlag_) ) 
		SDBTerminate(IDENTIFIER_INTERFACE + ERRORNUM_INTERFACE_MYSQL + 4,  // ERROR - 16010004
		"Table definition is out of sync between MySQL and ScaleDB engine.\0" );

	needToUpdateAutoIncrement = false;
	enum_field_types fieldType;
	unsigned short fieldId;

	SDBResetRow(sdbUserId_, sdbDbId_, sdbTableNumber_, groupType);

	Field*	pField;
	unsigned char* pFieldValue;
	unsigned int realVarLength;
	unsigned char mysqlVarLengthBytes;
	unsigned char* pBlobFieldValue;	// point to BLOB or TEXT value
	Field* pAutoIncrField = table->found_next_number_field;	// points to auto_increment field

	for (unsigned short i=0; i < table->s->fields; ++i) {
		bool useNextAutoIncrValue = false;
		bool setNull = false;

		pField = table->field[i];

		fieldId = i + 1;  // field ID should start with 1 in ScaleDB engine; it starts with 0 in MySQL
		pFieldValue = rowBuf1 + (pField->ptr - rowBuf2);
		// note that table->field[i]->ptr points to the column in the rowBuf2 (or new_row) 

		if ( pField->null_bit ) {	// This field is nullable
			unsigned int nullByteOffset = (unsigned int)((char *)pField->null_ptr - (char *)table->record[0]);
			if (rowBuf1[nullByteOffset] & pField->null_bit) {
				pFieldValue = NULL;
				setNull = true;
			}
		}

		fieldType = pField->type();

		switch (fieldType) {
			// the case statement should be listed based on the decending use frequency
			case MYSQL_TYPE_LONG:
			case MYSQL_TYPE_SHORT:
			case MYSQL_TYPE_TINY:
			case MYSQL_TYPE_LONGLONG:
			case MYSQL_TYPE_INT24:
			case MYSQL_TYPE_FLOAT:
			case MYSQL_TYPE_DOUBLE:
				if ( checkAutoIncField && pAutoIncrField && !setNull) {	// check to see if this table has an auto_increment column
					if ( pAutoIncrField->field_index == i ) { 
						if ((pFieldValue == NULL) || SDBUtilAreAllBytesZero(pFieldValue, pField->pack_length()) )
							// When auto_incrment column has value NULL or 0, we let engine set the next sequence value.
							useNextAutoIncrValue = true;
						needToUpdateAutoIncrement = true;
					}
				}
				if (useNextAutoIncrValue == false)
					SDBPrepareNumberField(sdbUserId_, sdbDbId_, sdbTableNumber_, fieldId, pFieldValue, groupType);
				break;

			case MYSQL_TYPE_STRING:
				SDBPrepareStrField(sdbUserId_, sdbDbId_, sdbTableNumber_, fieldId, (char*) pFieldValue, pField->pack_length(), groupType);
				break;

			case MYSQL_TYPE_DATE:
			case MYSQL_TYPE_DATETIME:
			case MYSQL_TYPE_TIME:
			case MYSQL_TYPE_TIMESTAMP:
			case MYSQL_TYPE_YEAR:
			case MYSQL_TYPE_ENUM:
			case MYSQL_TYPE_BIT:
			case MYSQL_TYPE_SET:
				SDBPrepareNumberField(sdbUserId_, sdbDbId_, sdbTableNumber_, fieldId, pFieldValue, groupType);
				break;

			case MYSQL_TYPE_NEWDECIMAL:
				SDBPrepareStrField(sdbUserId_, sdbDbId_, sdbTableNumber_, fieldId, (char*) pFieldValue, ((Field_new_decimal*)pField)->bin_size, groupType);
				break;

			case MYSQL_TYPE_VARCHAR:
			case MYSQL_TYPE_VAR_STRING:
				if (setNull){
					retCode = SDBPrepareVarField(sdbUserId_, sdbDbId_, sdbTableNumber_, fieldId, (char*) NULL, 0, groupType, true);
				}
				else{
					if ( ((Field_varstring*) pField)->length_bytes == 1 ) {
						mysqlVarLengthBytes = 1;
						realVarLength = (unsigned int) *pFieldValue;
					}
					else {
						mysqlVarLengthBytes = 2;
						realVarLength = (unsigned int) *( (unsigned short*)pFieldValue );
					}
					retCode = SDBPrepareVarField(sdbUserId_, sdbDbId_, sdbTableNumber_, fieldId, 
						(char*) pFieldValue + mysqlVarLengthBytes, realVarLength, groupType, false);
				}
				break;

			case MYSQL_TYPE_BLOB:

				if (setNull){
					retCode = SDBPrepareVarField(sdbUserId_, sdbDbId_, sdbTableNumber_, fieldId, (char*) NULL, 0, groupType, true);
				}
				else {
					mysqlVarLengthBytes = ((Field_blob*)pField)->pack_length_no_ptr();  // number of bytes to save length
					realVarLength =  (unsigned int)SDBGetNumberFromField((char*)pFieldValue, 0, mysqlVarLengthBytes);
					memcpy( &pBlobFieldValue, pFieldValue + mysqlVarLengthBytes, sizeof(void *) );
					retCode = SDBPrepareVarField(sdbUserId_, sdbDbId_, sdbTableNumber_, fieldId, 
						(char*) pBlobFieldValue, realVarLength, groupType, false);
				}
				break;

			default:

#ifdef SDB_DEBUG_LIGHT
				SDBDebugPrintHeader("These data types are not supported yet.");
#endif
				retCode = METAINFO_WRONG_FIELD_TYPE;
				break;
		}	// switch (fieldType)

		if (retCode > 0)
			break; 
	}
	return retCode;
}


/* 
write_row() inserts a row.  
Parameter buf is a byte array of data in MySQL format with a size of table->s->reclength.  
You can use the field information to extract the data from the native byte array type.  
table->s->fields: gives the number of column in the current open table.
*/
int ha_scaledb::write_row(unsigned char* buf)
{
	DBUG_ENTER("ha_scaledb::write_row");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::write_row(...) on table ");
		SDBDebugPrintString(table->s->table_name.str );
		SDBDebugPrintString(" ");
		//SDBDebugPrintString("id= ");
		//int bug481Id = *( (int*)table->field[0]->ptr );  // get first column value which is an integer
		//SDBDebugPrintInt(bug481Id);
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	int errorNum = 0;
	ha_statistic_increment(&SSV::ha_write_count);

	// On a secondary node, we should not insert any record as it is done by the primary node.
	if (SDBNodeIsCluster() == true) {
		if ( pSdbMysqlTxn_->getDdlFlag() & SDBFLAG_DDL_SECOND_NODE )
				DBUG_RETURN(errorNum);
	}

	/* If we have a timestamp column, update it to the current time */
	if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_INSERT)
		table->timestamp_field->set_time();

	my_bitmap_map* org_bitmap= dbug_tmp_use_all_columns(table, table->read_set);

	// this variable is modified in the placeMysqlRowInEngineBuffer function
	bool needToUpdateAutoIncrement = false;

	unsigned short retValue = placeMysqlRowInEngineBuffer( (unsigned char *) buf, (unsigned char *) buf, 0, true, needToUpdateAutoIncrement);

	if (retValue == 0) {
		retValue = SDBInsertRowAPI(sdbUserId_, sdbDbId_, sdbTableNumber_, 0, 0, ((THD*)ha_thd())->query_id, sdbCommandType_);
	}

	if (retValue == 0 && needToUpdateAutoIncrement){
		((THD*)ha_thd())->first_successful_insert_id_in_cur_stmt = get_scaledb_autoincrement_value();
	}

#ifdef __DEBUG_CLASS_CALLS  // can enable it to check memory leak
	static int localCounter = 0;
	localCounter++;
	if ( localCounter % 10000 == 0 ) { // the number can be set to meet the need
		SDBDebugPrintHeader("After inserting ");
		SDBDebugPrintInt(localCounter );
		SDBDebugPrintString(" records: \n");
		DebugClass::printClassCalls();
	}
#endif

	errorNum = convertToMysqlErrorCode(retValue);

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: ha_scaledb::write_row(...) returning ");
		SDBDebugPrintInt(errorNum);
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	dbug_tmp_restore_column_map(table->read_set, org_bitmap);
	DBUG_RETURN( errorNum );
}


// this function should be called when the latest auto increment value has changed so that
// it can be returned in calls to last_insert_id() function
uint64 ha_scaledb::get_scaledb_autoincrement_value(){

	unsigned short tableId = SDBGetTableNumberByName(sdbUserId_, sdbDbId_, table->s->table_name.str);

	// find the auto increment field number
	unsigned short totalNumberOfFields = SDBGetNumberOfFieldsInTableByTableNumber(sdbDbId_, tableId);
	unsigned short fieldNumber;

	for ( fieldNumber=1; fieldNumber<=totalNumberOfFields; ++fieldNumber ) {
		if ( SDBIsFieldAutoIncrement(sdbDbId_, tableId, fieldNumber) )
			break;
	}

	if ( SDBGetUserRowTableId(sdbUserId_) != tableId ) {
		return 0;
	}

	char* buffer = SDBGetUserRowColumn(sdbUserId_, fieldNumber);
	unsigned short length = SDBGetUserRowColumnLength(sdbUserId_, fieldNumber);

	uint64 number = SDBGetNumberFromField(buffer, 0, length);

	return number;
}


// Update HA_CREATE_INFO object.  Used in SHOW CREATE TABLE and ALTER TABLE ... enable keys
void ha_scaledb::update_create_info(HA_CREATE_INFO* create_info) {

	print_header_thread_info("MySQL Interface: executing ha_scaledb::update_create_info(...)");
	// do nothing on a secondary node
	if (SDBNodeIsCluster() == true) {
		if ( pSdbMysqlTxn_->getDdlFlag() & SDBFLAG_DDL_SECOND_NODE )
			return;
	}

	THD* thd = ha_thd();
	placeSdbMysqlTxnInfo( thd );

	unsigned short retValue = 0;
	// need to set Dbid and TableId in case SHOW CREATE TABLE is the first statement in a user session
	if ( (sdbTableNumber_ == 0) && (!virtualTableFlag_) ) 
		retValue = initializeDbTableId();

	//if ( records() > 0 ) 	// Bug 1022
	// need to adjust auto_increment_value to the next integer
	if (!(create_info->used_fields & HA_CREATE_USED_AUTO)) {
		info(HA_STATUS_AUTO);
		create_info->auto_increment_value = stats.auto_increment_value + 1;
	}
}


// For SHOW CREATE TABLE statement, the method recreates foreign key constraint clause
// based on ScaleDB's metadata information, and then return it to MySQL 
char* ha_scaledb::get_foreign_key_create_info() {
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::get_foreign_key_create_info() on table ");
		SDBDebugPrintString( table->s->table_name.str );
		SDBDebugPrintString(" ");
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	THD* thd = ha_thd();
	placeSdbMysqlTxnInfo( thd );

	return SDBGetForeignKeyClause(sdbUserId_, sdbDbId_, sdbTableNumber_);
}


// This method frees the string containing foreign key clause
void ha_scaledb::free_foreign_key_create_info(char* str) {
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::free_foreign_key_create_info(...) on table ");
		SDBDebugPrintString( table->s->table_name.str );
		SDBDebugPrintString(" ");
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	if (str) {
		RELEASE_MEMORY(str);
	}
}


// This method updates a record with both the old row data and the new row data specified 
int ha_scaledb::update_row(const unsigned char* old_row, unsigned char* new_row) {
	DBUG_ENTER("ha_scaledb::update_row");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::update_row(...) on table ");
		SDBDebugPrintString( table->s->table_name.str );
		SDBDebugPrintString(" ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	int errorNum = 0;
	ha_statistic_increment(&SSV::ha_update_count);

	/* If we have a timestamp column, update it to the current time */
	if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_UPDATE)
		table->timestamp_field->set_time();

	my_bitmap_map* org_bitmap= dbug_tmp_use_all_columns(table, table->read_set);

	unsigned int retValue = 0;
	bool funcRetValue = false;
	retValue = placeMysqlRowInEngineBuffer( (unsigned char*) old_row, new_row, 2, false, funcRetValue );  // place old row into ScaleDB buffer 1 for comparison
	if (retValue == 0)
		retValue = placeMysqlRowInEngineBuffer( new_row, new_row, 1, false, funcRetValue );    // place new row into ScaleDB buffer 2 for comparison
	if (retValue > 0) {
		dbug_tmp_restore_column_map(table->read_set, org_bitmap);
		DBUG_RETURN( convertToMysqlErrorCode(retValue) );
	}

	uint64 queryId = ((THD*)ha_thd())->query_id;
	retValue = SDBUpdateRowAPI(sdbUserId_, sdbDbId_, sdbTableNumber_, sdbRowIdInScan_, queryId);
	errorNum = convertToMysqlErrorCode(retValue);

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		if (errorNum){
			SDBDebugStart();			// synchronize threads printout	
			SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::update_row mysqlErrorCode ");
			SDBDebugPrintInt(errorNum);
			SDBDebugPrintString(" and scaledbErrorCode ");
			SDBDebugPrintInt(retValue);
			SDBDebugEnd();			// synchronize threads printout	
		}
	}
#endif

	dbug_tmp_restore_column_map(table->read_set, org_bitmap);
	DBUG_RETURN( errorNum );
}


// -----------------------------------------------------------------
// the MySQL table has non-unique key. The SDB table is unoque. 
// the additional fields needs to be added for the update
//
// Method returns false if fields data that needs to be added are not found.
// -----------------------------------------------------------------
bool ha_scaledb::addSdbKeyFields(){

	bool retValue = true;
	//unsigned short fieldLength;
	//unsigned short sdbFieldsCount = pMetaInfo_->getNumberOfFieldsInTableByTableNumber(sdbTableNumber_);
	//char *ptrToOldSdb;		// pointer to the data of the query with the SDB additional fields

	//for (unsigned short i = 1; i <= sdbFieldsCount; i++){
	//	
	//	if (pMetaInfo_->isColumnWithNonUniqueKey(sdbTableNumber_, i)){
	//		// field #i was added to make the "MySQL non-unique key" unisye

	//		fieldLength = pMetaInfo_->getTableColumnPhysicalSize(sdbTableNumber_, i);

	//		
	//		ptrToOldSdb = pSdbEngine->query( sdbQueryMgrId_ )->getFieldByTable(sdbTableNumber_, i);

	//		if (!ptrToOldSdb){
	//			retValue = false;
	//			break;				
	//		}

	//		pSdbEngine->manager(sdbUserId_)->prepareStrField( sdbDbId_, sdbTableNumber_, i , ptrToOldSdb, fieldLength, 0);
	//	}
	//}

	return retValue;;
}


// This method deletes a record with row data pointed by buf 
int ha_scaledb::delete_row(const unsigned char* buf) {
	DBUG_ENTER("ha_scaledb::delete_row");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		print_header_thread_info("MySQL Interface: executing ha_scaledb::delete_row(...)");
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::delete_row(...)");
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	int errorNum = 0;
	ha_statistic_increment(&SSV::ha_delete_count);

	my_bitmap_map* org_bitmap= dbug_tmp_use_all_columns(table, table->read_set);

	unsigned int retValue = 0;
	bool funcRetValue = false;
	retValue = placeMysqlRowInEngineBuffer( (unsigned char*) buf, (unsigned char*) buf, 0, false, funcRetValue );

	if (retValue == 0 ) {
		retValue = SDBDeleteRowAPI(sdbUserId_, sdbDbId_, sdbTableNumber_, sdbRowIdInScan_, 0, ((THD*)ha_thd())->query_id);
	}

	if (retValue != SUCCESS && retValue != ATTEMPT_TO_DELETE_KEY_WITH_SUBORDINATES) {
		THD* thd = ha_thd();
		scaledb_rollback(ht, thd, true);
	}
	errorNum = convertToMysqlErrorCode(retValue);

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		if (errorNum){
			SDBDebugStart();			// synchronize threads printout	
			SDBDebugPrintHeader("MySQL Interface: ha_scaledb::delete_row failed");
			SDBDebugEnd();			// synchronize threads printout	
		}
		else{
			++deleteRowCount_;
			SDBDebugStart();			// synchronize threads printout	
			SDBDebugPrintHeader("MySQL Interface: ha_scaledb::delete_row succeeded. deleteRowCount_=");
			SDBDebugPrintInt(deleteRowCount_);
			SDBDebugEnd();			// synchronize threads printout	
		}
	}
#endif

	dbug_tmp_restore_column_map(table->read_set, org_bitmap);
	DBUG_RETURN( errorNum );
}


// This method deletes all records of a ScaleDB table.
// On a cluster system, it involves 4 steps:
// 1. the primary node needs to impose a table-level-write-lock.
// 2. the primary node sends the TRUNCATE TABLE statement with a hint so that all the non-primary nodes will close the table files.
// 3. the primary node deletes all the table files and then re-open the table files on its node.
// 4. the primary node sends the TRUNCATE TABLE statement with open file hint so that all the non-primary nodes will open the table files.
int ha_scaledb::delete_all_rows()
{
	DBUG_ENTER("ha_scaledb::delete_all_rows");
	print_header_thread_info("MySQL Interface: executing ha_scaledb::delete_all_rows() ");

	int errorNum = 0;
	//pMetaInfo_ = pSdbEngine->getMetaInfo( sdbDbId_ );
	char* tblName = SDBGetTableNameByNumber(sdbUserId_, sdbDbId_, sdbTableNumber_);
	int retValue = SDBCanTableBeDropped(sdbUserId_, sdbDbId_, tblName);
	if (retValue == SUCCESS) {
		deleteAllRows_ = true;

		THD* thd = ha_thd();
		sqlCommand_ = thd_sql_command(thd);

		if ( sqlCommand_ == SQLCOM_TRUNCATE ) {
			unsigned short stmtFlag = 0;
			if (SDBNodeIsCluster() == true) {
				if ( strstr(thd->query(), SCALEDB_HINT_PREFIX) != NULL )
					stmtFlag |= SDBFLAG_DDL_SECOND_NODE;
				if ( strstr(thd->query(), SCALEDB_HINT_CLOSEFILE) != NULL )
					stmtFlag |= SDBFLAG_CMD_CLOSE_FILE;
				if ( strstr(thd->query(), SCALEDB_HINT_OPENFILE) != NULL )
					stmtFlag |= SDBFLAG_CMD_OPEN_FILE;
			}


			// step 1: the primary node of a cluster needs to impose a table-level-write-lock.
			// For non-cluster solution, we also need to impose a table-level-write-lock.
			if ( !(stmtFlag & SDBFLAG_DDL_SECOND_NODE) ) {
				if (SDBLockTable(sdbUserId_, sdbDbId_, sdbTableNumber_, REFERENCE_LOCK_EXCLUSIVE) == false)
					DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
			}

			bool truncateTableReady = true;

			// step 2: the primary node sends the TRUNCATE TABLE statement with a hint so that 
			// all the non-primary nodes will close the table files
			if ((SDBNodeIsCluster() == true) && 
				!(stmtFlag & SDBFLAG_DDL_SECOND_NODE) ) {
					char* stmtWithHint = SDBUtilAppendString(thd->query(), SCALEDB_HINT_CLOSEFILE);
					truncateTableReady = sqlStmt(sdbDbId_, stmtWithHint);
					RELEASE_MEMORY(stmtWithHint);
			}

			// step 3 and main execution body for both primary node and secondary nodes
			if (truncateTableReady)
				retValue = SDBTruncateTable(sdbUserId_, sdbDbId_, tblName, stmtFlag);

			// step 4: the primary node sends the TRUNCATE TABLE statement with open file hint so that 
			// all the non-primary nodes will open the table files.
			if ((SDBNodeIsCluster() == true) && 
				!(stmtFlag & SDBFLAG_DDL_SECOND_NODE) ) {
					char* stmtWithHint = SDBUtilAppendString(thd->query(), SCALEDB_HINT_OPENFILE);
					truncateTableReady = sqlStmt(sdbDbId_, stmtWithHint);
					RELEASE_MEMORY(stmtWithHint);
			}
			errorNum = convertToMysqlErrorCode( retValue );
		} 
		else	// need to tell MySQL to issue delete_row calls 1 by 1
			errorNum = HA_ERR_WRONG_COMMAND;
	} else
		errorNum = convertToMysqlErrorCode( retValue );


	DBUG_RETURN(errorNum);
}


// This method fetches a single row using row position
int ha_scaledb::fetchRowByPosition(unsigned char* buf, unsigned int pos) {

	DBUG_ENTER("ha_scaledb::fetchRowByPosition");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::fetchRowByPosition from table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(", alias: ");
		SDBDebugPrintString((char *)table->alias);
		SDBDebugPrintString(", position: ");
		SDBDebugPrint8ByteUnsignedLong(pos);
		SDBDebugPrintString(", using Query Manager ID: ");
		SDBDebugPrintInt(sdbQueryMgrId_);
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	int errorNum = 0;

	int retValue = (int) SDBGetSeqRowByPosition(sdbUserId_, sdbQueryMgrId_, pos);

	if (retValue == SUCCESS)
		retValue = copyRowToMySQLBuffer(buf);

	if (retValue != SUCCESS)
		table->status = STATUS_NOT_FOUND;

	errorNum = convertToMysqlErrorCode( retValue );
	DBUG_RETURN( errorNum );
}


// This method fetches a single row using next() method
int ha_scaledb::fetchSingleRow(unsigned char* buf) {

	DBUG_ENTER("ha_scaledb::fetchSingleRow");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::fetchSingleRow from table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(", alias: ");
		SDBDebugPrintString((char *)table->alias);
		SDBDebugPrintString(", using Query Manager ID: ");
		SDBDebugPrintInt(sdbQueryMgrId_);
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}

	++debugCounter_;
#endif

retryFetch:
	int retValue = 0;

	if (active_index == MAX_KEY)
		retValue = SDBQueryCursorNextSequential(sdbUserId_, sdbQueryMgrId_);
	else
		retValue = SDBQueryCursorNext(sdbUserId_, sdbQueryMgrId_);

	if (retValue != SUCCESS) {
		// no row
		table->status = STATUS_NOT_FOUND;
		DBUG_RETURN( convertToMysqlErrorCode( retValue ) );
	}

	// has row, copy the row data
	retValue = copyRowToMySQLBuffer(buf);

	if (retValue == ROW_RETRY_FETCH)
		goto retryFetch;

	retValue = convertToMysqlErrorCode( retValue );

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ >= 5) {
		if (!retValue){
			SDBDebugStart();			// synchronize threads printout	
			SDBDebugPrintHeader("MySQL Interface: ha_scaledb::fetchSingleRow is returning with the following row fetched:\n");
			SDBDebugPrintHexByteArray((char*)buf, 0, table->s->reclength);
			SDBDebugPrintNewLine();
			SDBDebugEnd();			// synchronize threads printout	
		}
	}
#endif

	DBUG_RETURN( retValue );
}


// copy scaledb engine row to mysql row.. should be called after fetch
int ha_scaledb::copyRowToMySQLBuffer(unsigned char* buf) {

	bool hasNullValue;
	unsigned short fieldId;
	int errorNum = 0;
	char* pSdbField = NULL;
	Field*	pField;
	unsigned char* pFieldBuf;	// pointer to the buffer location holding the field value

	my_bitmap_map* org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);
	memset(buf, 0, table->s->null_bytes);  
	unsigned short retValue = 0;

	// before looping through the fields we should free any buffers that were allocated for 
	// retrieving blobs.  We do this once for each row retrieved.
	// This should also be done for the final time in the reset function of this class which is called
	// when the query is completed
	SDBQueryCursorFreeBuffers(sdbQueryMgrId_);

	// First we copy the fixed-length columns at the front of a table record in one batch.
	// This is feasible because data formats are the same for fixed-length columns

	for (int i=0; i < (int) numberOfFrontFixedColumns_; ++i) {
		fieldId = i + 1;  // field ID should start with 1 in ScaleDB engine; it starts with 0 in MySQL
		pField = table->field[i];

		if ( pField->null_ptr ) {	// This field is nullable
			if (active_index == MAX_KEY) 
				hasNullValue = SDBQueryCursorFieldIsNull(sdbQueryMgrId_, fieldId);
			else
				hasNullValue = SDBQueryCursorFieldIsNullByIndex(sdbQueryMgrId_, sdbDesignatorId_, fieldId);

			if ( hasNullValue ) {
				unsigned int nullByteOffset = (unsigned int)((char *)pField->null_ptr - (char *)table->record[0]);
				buf[nullByteOffset] |= pField->null_bit;
				continue;
			}
		}
	}

	// get the position of the first column in MySQL row buffer.
	pField = table->field[0];
	pFieldBuf = buf + ( pField->ptr - table->record[0] );

	// get the position of the first column in ScaleDB row buffer.
	if (active_index == MAX_KEY)
		pSdbField = SDBQueryCursorGetSeqColumn(sdbQueryMgrId_, 1 );
	else 
		pSdbField = SDBQueryCursorGetFieldByTableId(sdbQueryMgrId_, sdbTableNumber_, 1);

	// copy all the fixed-length columns in one batch.
	if (numberOfFrontFixedColumns_ > 0)
		memcpy(pFieldBuf, pSdbField, frontFixedColumnsLength_);


	// Second, we copy the subsequent columns one-by-one starting with the first variable-length column

	for (int i=numberOfFrontFixedColumns_; i < (int) table->s->fields; ++i) {

		fieldId = i + 1;  // field ID should start with 1 in ScaleDB engine; it starts with 0 in MySQL
		pField = table->field[i];

		if ( pField->null_ptr ) {	// This field is nullable
			if (active_index == MAX_KEY) 
				hasNullValue = SDBQueryCursorFieldIsNull(sdbQueryMgrId_, fieldId);
			else
				hasNullValue = SDBQueryCursorFieldIsNullByIndex(sdbQueryMgrId_, sdbDesignatorId_, fieldId);

			if ( hasNullValue ) {
				unsigned int nullByteOffset = (unsigned int)((char *)pField->null_ptr - (char *)table->record[0]);
				buf[nullByteOffset] |= pField->null_bit;
				continue;
			}
		}

		short mySqlFieldType = pField->type();
		bool varCharType = false;
		bool blobType = false;
		switch (mySqlFieldType){
				case MYSQL_TYPE_BLOB:
				case MYSQL_TYPE_TINY_BLOB:
				case MYSQL_TYPE_MEDIUM_BLOB:
				case MYSQL_TYPE_LONG_BLOB:
					blobType = true;	
					break;
				case MYSQL_TYPE_VARCHAR:
				case MYSQL_TYPE_VAR_STRING:
					varCharType = true;
					break;
				default:
					break;
		}	// switch

		// Use the field offset to find the right location containing the field value.  
		// pField->ptr points to the field data for the new row.
		// During update operation, we need to fetch field data into the old row.  Bug 101
		// This is the location where will write the records that was fetched
		pFieldBuf = buf + ( pField->ptr - table->record[0] );

		// special handling for VARCHARs
		if (varCharType) {

			unsigned short mysqlBytesForDataSize = ((Field_varstring*) pField)->length_bytes;
			if (mysqlBytesForDataSize < 1 || mysqlBytesForDataSize > 8) {
				SDBTerminate(0, "obtained invalid number of bytes for varstring size from length_bytes");
			}

			char* destination = (char*)(pFieldBuf+mysqlBytesForDataSize);

			unsigned int bytesForData = 0;
			if (active_index == MAX_KEY) {
				SDBQueryCursorCopySeqVarColumn(sdbQueryMgrId_, fieldId, destination, &bytesForData);
			}
			else {
				SDBQueryCursorGetFieldByTable(sdbQueryMgrId_, sdbTableNumber_, fieldId, destination, &bytesForData);
			}
			SDBUtilIntToMemoryLocation((uint64)bytesForData, pFieldBuf, mysqlBytesForDataSize);	// bug 340
		}
		else if (blobType) {

			// in mysql how many byte location do we write the data size to
			unsigned short mysqlBytesForDataSize = ((Field_blob*)pField)->pack_length_no_ptr();
			if (mysqlBytesForDataSize < 1 || mysqlBytesForDataSize > 8) {
				SDBTerminate(0, "obtained invalid number of bytes for blob size from pack_length_no_ptr()");
			}

			unsigned int bytesForData = 0;
			if (active_index == MAX_KEY) {

				pSdbField = SDBQueryCursorGetSeqColumn(sdbQueryMgrId_, fieldId, &bytesForData);
			}
			else {

				pSdbField = SDBQueryCursorGetFieldByTableDataSize(sdbQueryMgrId_, sdbTableNumber_, fieldId, &bytesForData);
			}

			// get field by table failed because there was not enough memory
			// available to allocate for holding the requested BLOB
			// you may recommend the user increase the value of bytes_large_memory_allocations
			if (!pSdbField){
				// table->status = STATUS_GARBAGE;
				dbug_tmp_restore_column_map(table->write_set, org_bitmap);
				return HA_ERR_OUT_OF_MEM;
			}

			// write the blob pointer to the mysql buffer
			//				*(char**)(pField->ptr+mysqlBytesForDataSize) = pSdbField;
			*(char**)(pFieldBuf+mysqlBytesForDataSize) = pSdbField;

			// write the size of blob to mysql buffer
			SDBUtilIntToMemoryLocation((uint64)bytesForData, pFieldBuf, mysqlBytesForDataSize);
		}
		// not a variable length column
		else {
			if (active_index == MAX_KEY) {
				pSdbField = SDBQueryCursorGetSeqColumn(sdbQueryMgrId_, fieldId );
			}
			else {
				pSdbField = SDBQueryCursorGetFieldByTableId(sdbQueryMgrId_, sdbTableNumber_, fieldId);
			}
		}

		// If ptr has NULL, then we do not have right table record.  go to next one.
		if ( !pSdbField && (!varCharType) ) {
			dbug_tmp_restore_column_map(table->write_set, org_bitmap);
			return ROW_RETRY_FETCH;
		}

		placeEngineFieldInMysqlBuffer(pFieldBuf, pSdbField, pField);

#ifdef SDB_BUG974
		if (fieldId < 5) {	// enable this code only when you debug Bug974
			SDBDebugStart();			// synchronize threads printout	
			int intValue = *((int *)pSdbField);
			if (fieldId == 1) {
				if (intValue == 5000) {
					SDBDebugPrintHeader("This is last record in the test case: ");
					SDBDebugPrintString("can set a breakpoint here."); // set a breakpoint here
				}
				SDBDebugPrintHeader("adm_note_id=");
			}

			SDBDebugPrintInt( intValue );
			SDBDebugPrintString(", ");
			SDBDebugEnd();			// synchronize threads printout	
		}
#endif
	}	//	for (int i=numberOfFrontFixedColumns_; i < (int) table->s->fields; ++i) {


	table->status = 0;
	dbug_tmp_restore_column_map(table->write_set, org_bitmap);
	if (errorNum == 0){ 
		if (active_index == MAX_KEY) {
			// update the last sequential row id
			sdbRowIdInScan_ = SDBQueryCursorGetSeqRowPosition(sdbQueryMgrId_);
		}
		else {
			sdbRowIdInScan_ = SDBQueryCursorGetIndexCursorRowPosition(sdbQueryMgrId_);
		}
	}
	else {
		sdbRowIdInScan_ = 0;
	}

	return errorNum;
}


// -------------------------------------------------------------------------------------------
//	Place a ScaleDB field in a MySQL buffer
// -------------------------------------------------------------------------------------------
void ha_scaledb::placeEngineFieldInMysqlBuffer(unsigned char* destBuff, char* ptrToField, Field* pField) {
	unsigned int variableLength = 0;
	short mySqlFieldType = pField->type();

	switch (mySqlFieldType) {
		// the case statement should be listed based on the decending use frequency
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_FLOAT:
			memcpy(destBuff, ptrToField, 4);
			break;

		case MYSQL_TYPE_SHORT:
			memcpy(destBuff, ptrToField, 2);
			break;

		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_INT24:	
			memcpy(destBuff, ptrToField, 3);
			break;

		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_LONGLONG:
		case MYSQL_TYPE_DOUBLE:
			memcpy(destBuff, ptrToField, 8);
			break;

		case MYSQL_TYPE_TINY:
			*destBuff = *ptrToField;		// Copy 1 byte only
			break;

		case MYSQL_TYPE_NEWDECIMAL:	// we treat decimal as a string
			memcpy(destBuff, ptrToField, ((Field_new_decimal*) pField)->bin_size);
			break;

		case MYSQL_TYPE_BIT:	// copy its length in memory
			memcpy(destBuff, ptrToField, ((Field_bit*) pField)->pack_length());
			break;

		case MYSQL_TYPE_SET:	// copy its length in memory
			memcpy(destBuff, ptrToField, ((Field_set*) pField)->pack_length());
			break;

		case MYSQL_TYPE_YEAR:
			*destBuff = *ptrToField;		// Copy 1 byte only
			break;

		case MYSQL_TYPE_ENUM:	// copy 1 or 2 bytes
			memcpy(destBuff, ptrToField, ((Field_enum*) pField)->pack_length());
			break;

		case MYSQL_TYPE_STRING:
			memcpy(destBuff, ptrToField, ((Field_bit*) pField)->pack_length());
			break;

		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
			//			variableLength = pSdbEngine->query( sdbQueryMgrId_ )->getFieldLengthByTable(table->s->table_name.str, 
			//											(char*) pField->field_name);
			//			fieldBuf.set( ptrToField, variableLength, fieldBuf.charset());	// let fieldBuf point to the source field string
			//			pField->store(fieldBuf.ptr(), fieldBuf.length(), fieldBuf.charset());	// store field value at the assigned position
			break;

		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
			//			variableLength = pSdbEngine->query( sdbQueryMgrId_ )->getFieldLengthByTable(table->s->table_name.str, 
			//											(char*) pField->field_name);
			//			fieldBuf.set( ptrToField, variableLength, fieldBuf.charset());	// let fieldBuf point to the source field string
			//			pField->store(fieldBuf.ptr(), fieldBuf.length(), fieldBuf.charset());	// store field value at the assigned position
			break;

		default:
#ifdef SDB_DEBUG_LIGHT
			SDBDebugStart();			// synchronize threads printout	
			SDBDebugPrintHeader("These data types are not supported yet.");
			SDBDebugEnd();			// synchronize threads printout	
#endif
			break;
	}	// switch

}


// This method fetches a single virtual row using nextByDesignator() method
int ha_scaledb::fetchVirtualRow(unsigned char* buf) {
	DBUG_ENTER("ha_scaledb::fetchVirtualRow");

	print_header_thread_info("MySQL Interface: executing ha_scaledb::fetchVirtualRow(...) ");

	int errorNum = 0;
	errorNum = SDBQueryCursorNext(sdbUserId_, sdbQueryMgrId_);	// always use the Multi-Table Index to fetch record

	if (errorNum != SUCCESS) {
		// no row
		table->status = STATUS_NOT_FOUND;
		errorNum = convertToMysqlErrorCode( errorNum );
		DBUG_RETURN( errorNum );
	}

	char* pCurrDesignator = table->s->table_name.str + SDB_VIRTUAL_VIEW_PREFIX_BYTES;
	unsigned short designatorLastLevel = SDBGetIndexNumberByName(sdbDbId_, pCurrDesignator);

	char *dataPtr;
	unsigned short numberOfFields;
	unsigned short numberOfFieldsInParents;
	unsigned short sdbRowOffset;
	unsigned short designatorFetched;
	unsigned short offset;
	Field* pField;
	unsigned char* pFieldBuf;	// pointer to the buffer location holding the field value

	// Now we start from level 1 and go down

	unsigned short totalLevel =  SDBGetIndexLevel(sdbDbId_, designatorLastLevel);
	for (int currLevel=0; currLevel<totalLevel; ++currLevel) {

		designatorFetched = SDBGetParentIndex(sdbDbId_, designatorLastLevel, currLevel+1);

		if (designatorFetched == 0){  // no more designator founds
			errorNum = HA_ERR_END_OF_FILE;
			table->status = STATUS_NOT_FOUND;
			break;
		}
		dataPtr = SDBQueryCursorGetDataByIndex(sdbQueryMgrId_, designatorFetched);

		offset = getOffsetByDesignator(designatorFetched); // position of designated data in buff

		numberOfFields = SDBGetNumberOfFieldsInTableByIndex(sdbDbId_, designatorFetched);
		numberOfFieldsInParents = SDBGetNumberOfFieldsInParentTableByIndex(sdbDbId_, designatorFetched);

		for (unsigned short i = 0; i < numberOfFields; ++i){
			sdbRowOffset =SDBGetTableColumnPositionByIndex(sdbDbId_, designatorFetched, i + 1);
			pField = table->field[numberOfFieldsInParents + i];

			pFieldBuf = buf + ( pField->ptr - table->record[0] );
			placeEngineFieldInMysqlBuffer(pFieldBuf, dataPtr + sdbRowOffset, pField);
		}

	}

	DBUG_RETURN( errorNum );
}


// -------------------------------------------------------------------------------------------
//	Maps designators to position in MySQL buffer.
//	For example: If ScaleDB structure is A->B->C (C subordinated to B, B subordinated to A)
//	and all rows are 100 bytes long, A would be maped to position 0 in the MySQL buffer, B to
//	position 100 and C to position 200.
// -------------------------------------------------------------------------------------------
unsigned short ha_scaledb::getOffsetByDesignator(unsigned short designator) {

	print_header_thread_info("MySQL Interface: executing ha_scaledb::getOffsetByDesignator(...) ");

	unsigned short level = SDBGetIndexLevel(sdbDbId_, designator);
	unsigned short offset = 0;
	for ( unsigned short i = 1; i < level; ++ i){
		offset+= SDBGetTableRowLengthByIndex(sdbDbId_, SDBGetParentIndex(sdbDbId_, designator, i));
	}
	return offset;
}


// prepare query manager
void ha_scaledb::prepareIndexOrSequentialQueryManager()
{
	unsigned short retValue = 0;

	if ( (sdbTableNumber_ == 0) && (!virtualTableFlag_) ) 
		retValue = initializeDbTableId();

	if (SDBIsTableWithIndexes(sdbDbId_, sdbTableNumber_)) {
		prepareFirstKeyQueryManager();
	}
	else {
		// prepare for sequential scan
		char* pTableFsName = SDBGetTableFileSystemNameByTableNumber(sdbDbId_, sdbTableNumber_);
		char* designatorName = SDBUtilFindDesignatorName(pTableFsName, "sequential", active_index);

		sdbQueryMgrId_ = pSdbMysqlTxn_->findQueryManagerId(designatorName, (void*) this, NULL, 0);
		if ( !sdbQueryMgrId_ ) {  // need to get a new one and save sdbQueryMgrId_ into MysqlTxn object
			sdbQueryMgrId_ = SDBGetQueryManagerId(sdbUserId_);
			 // save sdbQueryMgrId_
			pSdbMysqlTxn_->addQueryManagerId(false, designatorName, (void*) this, NULL, 0, sdbQueryMgrId_, mysqlInterfaceDebugLevel_); 
		}
		RELEASE_MEMORY( designatorName );
		sdbDesignatorName_ = pSdbMysqlTxn_->getDesignatorNameByQueryMrgId(sdbQueryMgrId_);	// point to name string on heap
		sdbDesignatorId_ = pSdbMysqlTxn_->getDesignatorIdByQueryMrgId(sdbQueryMgrId_);

		SDBResetQuery( sdbQueryMgrId_ );  // remove the previously defined query
		SDBSetActiveQueryManager(sdbQueryMgrId_); // cache the object for performance

		retValue = SDBPrepareSequentialScan(sdbUserId_, sdbQueryMgrId_, sdbDbId_, table->s->table_name.str, 
					((THD*)ha_thd())->query_id, releaseLocksAfterRead_);
	}
}


// prepare primary or first-key query manager
void ha_scaledb::prepareFirstKeyQueryManager()
{
	char* pDesignatorName = NULL; 

	if (virtualTableFlag_) {
		pDesignatorName = table->s->table_name.str + SDB_VIRTUAL_VIEW_PREFIX_BYTES;
		active_index = 0;	// has to be primary key for virtual view
	}
	else {

		SdbDynamicArray* pDesigArray = SDBGetTableDesignators(sdbDbId_, sdbTableNumber_);

		unsigned short designator; 
		unsigned short i = SDBArrayGetNextElementPosition(pDesigArray, 0);	// set to position of first element
		SDBArrayGet(pDesigArray, i, (unsigned char *)&designator);			// get designator number from pDesigArray
		pDesignatorName = SDBGetIndexNameByNumber(sdbDbId_, designator); 
		// Find MySQL index number and force full table scan to use the first available index 
		active_index = (unsigned int) SDBGetIndexExternalId(sdbDbId_, designator);   
	}

	sdbQueryMgrId_ = pSdbMysqlTxn_->findQueryManagerId(pDesignatorName, (void*) this, (char*)NULL, 0, virtualTableFlag_);
	if ( !sdbQueryMgrId_ ) {  // need to get a new one and save sdbQueryMgrId_ into MysqlTxn object
		sdbQueryMgrId_ = SDBGetQueryManagerId(sdbUserId_);
		pSdbMysqlTxn_->addQueryManagerId(true, pDesignatorName, (void*) this, (char*) NULL, 0, sdbQueryMgrId_, mysqlInterfaceDebugLevel_);
	}
	SDBSetActiveQueryManager(sdbQueryMgrId_); // cache the object for performance
	sdbDesignatorName_ = pSdbMysqlTxn_->getDesignatorNameByQueryMrgId(sdbQueryMgrId_);
	sdbDesignatorId_ = pSdbMysqlTxn_->getDesignatorIdByQueryMrgId(sdbQueryMgrId_);
}


// prepare query manager for index reads
void ha_scaledb::prepareIndexQueryManager(unsigned int indexNum, const uchar* key, uint key_len) {

	if (!sdbQueryMgrId_) {
		KEY* pKey = table->key_info + indexNum;

		char* designatorName = NULL;
		if (virtualTableFlag_)
			designatorName = SDBUtilGetStrInLower(table->s->table_name.str + SDB_VIRTUAL_VIEW_PREFIX_BYTES);
		else {
			char* pTableFsName = SDBGetTableFileSystemNameByTableNumber(sdbDbId_, sdbTableNumber_);
			designatorName = SDBUtilFindDesignatorName(pTableFsName, pKey->name, indexNum);
		}

		sdbQueryMgrId_ = pSdbMysqlTxn_->findQueryManagerId(designatorName, (void*) this, (char*) key, key_len, virtualTableFlag_);
		if ( !sdbQueryMgrId_ ) {  // need to get a new one and save sdbQueryMgrId_ into MysqlTxn object
			sdbQueryMgrId_ = SDBGetQueryManagerId(sdbUserId_);
			// save sdbQueryMgrId_
			pSdbMysqlTxn_->addQueryManagerId(true, designatorName, (void*) this, (char*) key, key_len, sdbQueryMgrId_, mysqlInterfaceDebugLevel_);
		}

#ifdef SDB_DEBUG_LIGHT
		if (mysqlInterfaceDebugLevel_) {
			SDBDebugStart();			// synchronize threads printout	
			SDBDebugPrintHeader("MySQL Interface: designator: ");
			if (designatorName)
				SDBDebugPrintString( designatorName );
			else
				SDBTerminate(IDENTIFIER_INTERFACE + ERRORNUM_INTERFACE_MYSQL + 11, 
					"The designator name should NOT be NULL!!");		// ERROR - 16010011
				
			SDBDebugPrintString(" table: ");
			SDBDebugPrintString( table->s->table_name.str );
			SDBDebugEnd();			// synchronize threads printout	
		}
		if (mysqlInterfaceDebugLevel_ > 5) {
			SDBDebugStart();			// synchronize threads printout	
			SDBDebugPrintHeader(" QueryManagerId= ");
			SDBDebugPrintInt( sdbQueryMgrId_ );
			SDBDebugPrintString(" \n");
			//pQm_->showQueryDef();
			//pQm_->showSearchKey(true);
			//pQm_->debugPath(true);
			//pQm_->showIndexTraversal(true);
			SDBDebugEnd();			// synchronize threads printout	
		}
#endif

		RELEASE_MEMORY( designatorName );
	}

	sdbDesignatorName_ = pSdbMysqlTxn_->getDesignatorNameByQueryMrgId(sdbQueryMgrId_);
	sdbDesignatorId_ = pSdbMysqlTxn_->getDesignatorIdByQueryMrgId(sdbQueryMgrId_);
	SDBResetQuery( sdbQueryMgrId_ );  // remove the previously defined query
	SDBSetActiveQueryManager(sdbQueryMgrId_); // cache the object for performance
}


// Prepare query by assinging key values to the corresponding key fields
int ha_scaledb::prepareIndexKeyQuery(const uchar* key, uint key_len, enum ha_rkey_function find_flag) {

	// now we prepare query by assigning key values to the corresponding key fields
	char* keyOffset = (char*) key;	// points to key value (may include additional bytes for NULL or variable string.
	char* pCurrentKeyValue;			// points to key value only (already skip the additional bytes for NULL or variale string.
	KEY_PART_INFO* pKeyPart;
	enum_field_types fieldType;
	Field* pField;
	char* pFieldName = NULL;
	int keyLengthLeft = (int) key_len;
	unsigned int realVarLength;
	unsigned int mysqlVarLengthBytes;
	int columnLen = 0;
	int offsetLength;
	bool keyValueIsNull;
	bool stringField;

	int retValue = 0; 

	KEY* pKey = table->key_info + active_index;  // find out which index to use
	int numOfKeyFields = (int) pKey->key_parts;

	prepareIndexQueryManager(active_index, key, key_len);

	if (!key || key_len == 0){
		if (find_flag == HA_READ_AFTER_KEY){
			SDBQueryCursorGetFirst(sdbQueryMgrId_, sdbDbId_, sdbDesignatorId_);
			SDBQueryCursorDefineQueryAllValues(sdbQueryMgrId_, sdbDbId_, sdbDesignatorId_, false);
			return SDBPrepareQuery(sdbUserId_, sdbQueryMgrId_, 0,((THD*)ha_thd())->query_id, releaseLocksAfterRead_);	
		}
		if (find_flag == HA_READ_BEFORE_KEY){
			SDBQueryCursorGetLast(sdbQueryMgrId_, sdbDbId_, sdbDesignatorId_);
			SDBQueryCursorDefineQueryAllValues(sdbQueryMgrId_, sdbDbId_, sdbDesignatorId_, false);
			return SDBPrepareQuery(sdbUserId_, sdbQueryMgrId_, 0,((THD*)ha_thd())->query_id, releaseLocksAfterRead_);	
		}
	}

	// keep track of the end key
	char* pEndKeyOffset = NULL;
	char* pCurrentEndKeyValue;
	bRangeQuery_ = false;	// TODO: remove this line after Moshe implements QueryManager::defineRangeQuery
	if (bRangeQuery_) {
		pEndKeyOffset = (char*) end_range->key;
	}

	for ( int i=0; i < numOfKeyFields; ++i ) {
		pKeyPart = pKey->key_part + i;
		pField = pKeyPart->field;
		fieldType = pField->type();
		realVarLength = 0;
		mysqlVarLengthBytes = 0;
		offsetLength = 0;
		keyValueIsNull = false;
		stringField = false;
		//pkeyValuePtr[i] = NULL;

		if ( (keyOffset != NULL) && (keyLengthLeft >= 0) ) {

			//check if the key is nullable, first byte tells if it is NULL
			if (!(pField->flags & NOT_NULL_FLAG)) {
				offsetLength = 1;

				if (*keyOffset != 0) {
					//toggle the NULL bit
					pField->set_null();
					keyValueIsNull = true;	// SQL stmt has a clause "column is NULL"
				}
			}

			switch ( fieldType ) {  // byte-flip integer field, 
			// case statements should be listed in the decending use frequency
			case MYSQL_TYPE_LONG:	
			case MYSQL_TYPE_TIMESTAMP:	// TIMESTAMP is treated as a 4-byte integer
			case MYSQL_TYPE_FLOAT:		// FLOAT is treated as a 4-byte number
				columnLen = 4;	// no need to flip byte here.  The engine should do it.
				break;

			case MYSQL_TYPE_SHORT:
				columnLen = 2;	// no need to flip byte here.  The engine should do it.
				break;

			case MYSQL_TYPE_DATE:	// DATE is treated as an 3-byte integer
			case MYSQL_TYPE_TIME:		// TIME is treated as a 3-byte integer
			case MYSQL_TYPE_INT24:
				columnLen = 3;
				break;

			case MYSQL_TYPE_LONGLONG:	
			case MYSQL_TYPE_DATETIME:	// DATETIME is treated as an 8-byte integer
			case MYSQL_TYPE_DOUBLE:		// DOUBLE is treated as a 8-byte number
				columnLen = 8;
				break;

			case MYSQL_TYPE_TINY:  case MYSQL_TYPE_YEAR:
				columnLen = 1;
				break;

			case MYSQL_TYPE_BIT:
				columnLen = ((Field_bit*) pField)->pack_length();
				break;

			case MYSQL_TYPE_SET:
				columnLen = ((Field_set*) pField)->pack_length();
				break;

			case MYSQL_TYPE_NEWDECIMAL:
				columnLen = ((Field_new_decimal*) pField)->bin_size;
				break;

			case MYSQL_TYPE_ENUM:
				columnLen = ((Field_enum*) pField)->pack_length();
				break;

			case MYSQL_TYPE_VARCHAR:	
			case MYSQL_TYPE_VAR_STRING:
			case MYSQL_TYPE_TINY_BLOB:
			case MYSQL_TYPE_BLOB:
			case MYSQL_TYPE_MEDIUM_BLOB:  
			case MYSQL_TYPE_LONG_BLOB: 
				realVarLength = (unsigned int) *( (unsigned short*)(keyOffset+offsetLength));
				mysqlVarLengthBytes = 2;
				columnLen = pField->field_length + mysqlVarLengthBytes ;
				stringField = true;
				break;

			case MYSQL_TYPE_STRING:
				columnLen = pField->field_length;
				break;

			default:
				SDBTerminate(IDENTIFIER_INTERFACE + ERRORNUM_INTERFACE_MYSQL + 1, 
					"This data type cannot be used as index.");		// ERROR - 16010001
				break;
			}	// switch

			keyOffset = keyOffset + offsetLength; 
			keyLengthLeft = keyLengthLeft - offsetLength;
		}	// if ( (keyOffset != NULL) && (keyLengthLeft >= 0) ) 

		//        if ( !virtualTableFlag_ ) {

		if ( keyValueIsNull )	// user specifies "column is NULL" in SQL statement
			pCurrentKeyValue = NULL;
		else {
			pCurrentKeyValue = keyOffset+mysqlVarLengthBytes;
			if (bRangeQuery_)
				pCurrentEndKeyValue = pEndKeyOffset+mysqlVarLengthBytes;
		}

		pFieldName = (char*) pField->field_name;
		if ( virtualTableFlag_ )
			pFieldName = pFieldName + 3;	// skip first 3 letters Lx_ in the generated column name

		char *designatorName = pSdbMysqlTxn_->getDesignatorNameByQueryMrgId(sdbQueryMgrId_);
		if ( stringField ) {
			retValue =SDBDefineQueryPrefix(sdbQueryMgrId_, sdbDbId_, sdbDesignatorId_, pFieldName, pCurrentKeyValue, false, realVarLength, false);
		}
		else{
			if (bRangeQuery_) {	// Example: SELECT * FROM t1 WHERE c1 BETWEEN 5 AND 8;
				bool bIncludeStartValue = false;
				if (find_flag == HA_READ_KEY_OR_NEXT)
					bIncludeStartValue = true;		// Example: SELECT * FROM t1 WHERE c1>=5 AND c1<8;
				bool bIncludeEndValue = true;
				if (end_range->flag == HA_READ_BEFORE_KEY)
					bIncludeEndValue = false;		// Example: SELECT * FROM t1 WHERE c1>=5 AND c1<8;

				retValue = SDBDefineRangeQuery(sdbQueryMgrId_, sdbDbId_, sdbDesignatorId_, pFieldName, 
							bIncludeStartValue, pCurrentKeyValue, bIncludeEndValue, pCurrentEndKeyValue);
			}
			else
				retValue = SDBDefineQuery(sdbQueryMgrId_, sdbDbId_, sdbDesignatorId_, pFieldName, pCurrentKeyValue);
		}

		//unsigned int endRangeLen = end_range->length;	// TBD: Bug 1103 enable for HA_READ_KEY_OR_NEXT only
		switch (find_flag) {
			// case statements are listed in decending use frequency
			case HA_READ_KEY_EXACT:
				SDBQueryCursorSetFlags(sdbQueryMgrId_, sdbDesignatorId_, false,SDB_KEY_SEARCH_DIRECTION_EQ, true, true);
				break;

			case HA_READ_AFTER_KEY:
				SDBQueryCursorSetFlags(sdbQueryMgrId_, sdbDesignatorId_, true,SDB_KEY_SEARCH_DIRECTION_GT, false, true);
				break;

			case HA_READ_KEY_OR_NEXT:
				SDBQueryCursorSetFlags(sdbQueryMgrId_, sdbDesignatorId_, false,SDB_KEY_SEARCH_DIRECTION_GE, false, true);
				break;

			case HA_READ_BEFORE_KEY:
				SDBQueryCursorSetFlags(sdbQueryMgrId_, sdbDesignatorId_, true,SDB_KEY_SEARCH_DIRECTION_LT, false, true);
				break;

			case HA_READ_PREFIX_LAST:
			case HA_READ_PREFIX_LAST_OR_PREV:
				SDBQueryCursorSetFlags(sdbQueryMgrId_, sdbDesignatorId_, false,SDB_KEY_SEARCH_DIRECTION_LE, false, true);
				break;

			default:
				SDBQueryCursorSetFlags(sdbQueryMgrId_, sdbDesignatorId_, false,SDB_KEY_SEARCH_DIRECTION_EQ, true, true);
				break;
		}

		if ( retValue != SUCCESS )
			SDBTerminate(0, "ScaleDB internal error: wrong designator name!");

		//        }  // if ( !virtualTableFlag_ )

		keyOffset = keyOffset + columnLen; 
		if (bRangeQuery_)
			pEndKeyOffset = pEndKeyOffset + columnLen; 

		keyLengthLeft = keyLengthLeft - columnLen;
		if ( keyLengthLeft == 0 ) 
			break;
	}	// for ( int i=0; i < numOfKeyFields; ++i )

	//if (virtualTableFlag_) 
	//	retValue = pSdbEngine->query( sdbQueryMgrId_ )->defineQuery(sdbDbId_, sdbDesignatorId_, pkeyValuePtr, false);

	// we execute the query
	retValue = SDBPrepareQuery(sdbUserId_, sdbQueryMgrId_, 0,((THD*)ha_thd())->query_id, releaseLocksAfterRead_);	

	return retValue;
}


// This method retrieves a record based on index/key 
int ha_scaledb::index_read(uchar* buf, const uchar* key, uint key_len, enum ha_rkey_function find_flag) {
	DBUG_ENTER("ha_scaledb::index_read");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_read(...) on table ");
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString( table->s->table_name.str );
		SDBDebugPrintString(", alias: ");
		SDBDebugPrintString( table->alias );
		outputHandleAndThd();
		SDBDebugPrintString(", Query Manager ID #");
		SDBDebugPrintInt(sdbQueryMgrId_);
		SDBDebugPrintString(", Index read flag ");
		SDBDebugPrintString(mysql_key_flag_strings[find_flag]);
		SDBDebugPrintString(", key_len ");
		SDBDebugPrintInt(key_len);
		SDBDebugPrintString(", key ");
		SDBDebugPrintHexByteArray((char*)key, 0, key_len);
		SDBDebugEnd();			// synchronize threads printout	

		// initialize the debugging counter
		readDebugCounter_ = 1;
	}
#endif

	ha_statistic_increment(&SSV::ha_read_key_count);

	int retValue = prepareIndexKeyQuery(key, key_len, find_flag);

	if ( retValue == 0 ) {
		// fetch the actual row
		if (virtualTableFlag_)
			retValue = fetchVirtualRow(buf);
		else
			retValue = fetchSingleRow(buf);
	} else {
		retValue = convertToMysqlErrorCode(retValue);
		table->status = STATUS_NOT_FOUND;
	}

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 1) {
		SDBDebugStart();			// synchronize threads printout	

		if (!retValue){
			SDBDebugPrintHeader("ha_scaledb::index_read returned a valid row");
		}
		else {
			SDBDebugPrintHeader("ha_scaledb::index_read returned code: ");
			SDBDebugPrintInt(retValue);
		}

		SDBDebugFlush();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	DBUG_RETURN(retValue);
}


// this function is called at the end of the query and can be used to clear state for that query
int ha_scaledb::reset() {
	print_header_thread_info("MySQL Interface: executing ha_scaledb::reset()");
	SDBFreeQueryManagerBuffers(sdbQueryMgrId_);
	return 0;
}


// This method retrieves a record based on index/key.  The index number is a parameter 
int ha_scaledb::index_read_idx(uchar* buf, uint keynr, const uchar* key,
							   uint key_len, enum ha_rkey_function find_flag) {
	DBUG_ENTER("ha_scaledb::index_read_idx");

	print_header_thread_info("MySQL Interface: executing ha_scaledb::index_read_idx(...) ");

	active_index = keynr;
	DBUG_RETURN( index_read(buf, key, key_len, find_flag) );
}


// returns the next value in ascending order from the index
// the method should not consider what key was used in the original index_read
// but just return the next item in the index
int ha_scaledb::index_next(unsigned char* buf) {
	DBUG_ENTER("ha_scaledb::index_next");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_next(...) ");
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString( table->s->table_name.str );
		SDBDebugPrintString(", alias: ");
		SDBDebugPrintString( table->alias );
		outputHandleAndThd();
		SDBDebugPrintString(", Query Manager ID #");
		SDBDebugPrintInt(sdbQueryMgrId_);
		SDBDebugPrintString(" counter = ");
		SDBDebugPrintInt(++readDebugCounter_);
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	ha_statistic_increment(&SSV::ha_read_next_count);
	int errorNum = 0;
	// Bug 1132: still need to call SDBQueryCursorSetFlags as distinct parameter may have diffrent value than the one set in index_read.
	SDBQueryCursorSetFlags(sdbQueryMgrId_, sdbDesignatorId_, false,SDB_KEY_SEARCH_DIRECTION_GE, false, true);

	if (virtualTableFlag_)
		errorNum = fetchVirtualRow(buf);
	else
		errorNum = fetchSingleRow(buf);

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_  && errorNum) {
		SDBDebugStart();			
		SDBDebugPrintHeader("MySQL Interface: ha_scaledb::index_next returned errorNum ");
		SDBDebugPrintInt(errorNum);
		SDBDebugEnd();	
	}
#endif

	DBUG_RETURN(errorNum);
}


// This method reads the next row matching the key value given as the parameter.
// For example, an index consists of two columns.  But a user query specifies value for one column only.
// SELECT * FROM t1 WHERE c1 = 5;  Note that an index consists of two columns c1 and c2.
// In index_read method, we specify SDB_KEY_SEARCH_DIRECTION_EQ.  Here we specify SDB_KEY_SEARCH_DIRECTION_GE. 
int ha_scaledb::index_next_same(uchar* buf, const uchar* key, uint keylen) {
	DBUG_ENTER("ha_scaledb::index_next_same");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_next_same(...) ");
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString( table->s->table_name.str );
		SDBDebugPrintString(", alias: ");
		SDBDebugPrintString( table->alias );
		outputHandleAndThd();
		SDBDebugPrintString(", Query Manager ID #");
		SDBDebugPrintInt(sdbQueryMgrId_);
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	int errorNum = 0;
	// Bug 1132: still need to call SDBQueryCursorSetFlags as distinct parameter may have diffrent value than the one set in index_read.
	SDBQueryCursorSetFlags(sdbQueryMgrId_, sdbDesignatorId_, false,SDB_KEY_SEARCH_DIRECTION_GE, true, true);

	if (virtualTableFlag_)
		errorNum = fetchVirtualRow(buf);
	else
		errorNum = fetchSingleRow(buf);

	DBUG_RETURN(errorNum);
}


//This method returns the first key value in index
int ha_scaledb::index_first(uchar* buf) {
	DBUG_ENTER("ha_scaledb::index_first");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_first(...) ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	ha_statistic_increment(&SSV::ha_read_first_count);

	THD* thd = ha_thd();
	sqlCommand_ = thd_sql_command(thd);
	// For CREATE TABLE ... SELECT, a secondary node should exit early.
	if ( (SDBNodeIsCluster() == true) && (sqlCommand_ == SQLCOM_CREATE_TABLE) ) {
		if ( pSdbMysqlTxn_->getDdlFlag() & SDBFLAG_DDL_SECOND_NODE )
				DBUG_RETURN(HA_ERR_END_OF_FILE);
	}
	
	int errorNum = index_read(buf, NULL, 0, HA_READ_AFTER_KEY);

	/* MySQL does not allow return value HA_ERR_KEY_NOT_FOUND */
	if (errorNum == HA_ERR_KEY_NOT_FOUND) {
		errorNum = HA_ERR_END_OF_FILE;
	}

	DBUG_RETURN(errorNum);
}


//This method returns the last key value in index
int ha_scaledb::index_last(uchar * buf) {
	DBUG_ENTER("ha_scaledb::index_last");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_last(...) ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	ha_statistic_increment(&SSV::ha_read_last_count);

	int errorNum = index_read(buf, NULL, 0, HA_READ_BEFORE_KEY);

	/* MySQL does not allow return value HA_ERR_KEY_NOT_FOUND */
	if (errorNum == HA_ERR_KEY_NOT_FOUND) {
		errorNum = HA_ERR_END_OF_FILE;
	}

	DBUG_RETURN(errorNum);
}


//This method returns the prev key value in index
int ha_scaledb::index_prev(uchar * buf) {
	DBUG_ENTER("ha_scaledb::index_prev");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_prev(...) ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	ha_statistic_increment(&SSV::ha_read_prev_count);
	int errorNum = 0;
	// Bug 1132: still need to call SDBQueryCursorSetFlags as distinct parameter may have diffrent value than the one set in index_read.
	SDBQueryCursorSetFlags(sdbQueryMgrId_, sdbDesignatorId_, false,SDB_KEY_SEARCH_DIRECTION_LE, false, true);

	if (virtualTableFlag_)
		errorNum = fetchVirtualRow(buf);
	else
		errorNum = fetchSingleRow(buf);

	DBUG_RETURN(errorNum);
}


// This method prepares for a statement that need to access all records of a table.
// The statement can be SELECT, DELETE, UPDATE, etc.
int ha_scaledb::rnd_init(bool scan) {
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::rnd_init(...) ");
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString( table->s->table_name.str );
		SDBDebugPrintString(", alias: ");
		SDBDebugPrintString( table->alias );
		outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout
	}
#endif

	DBUG_ENTER("ha_scaledb::rnd_init");
	int errorCode = SUCCESS;
	THD* thd = ha_thd();
	sqlCommand_ = thd_sql_command(thd);

	// on a non-primary node, we do nothing for DDL statements.
	// CREATE TABLE ... SELECT also calls this method.
	// This method utilizees a different handler object than external_lock.
	if (SDBNodeIsCluster() == true) {
		if ( pSdbMysqlTxn_->getDdlFlag() & SDBFLAG_DDL_SECOND_NODE )
			DBUG_RETURN( convertToMysqlErrorCode(errorCode) );
	}

	// For select statement, we use the sequential scan since full table scan is faster in this case.
	// For non-select statement, if the table has index, then we prefer to use index 
	// (most likely the primary key) to fetch each record of a table.

	active_index = MAX_KEY;   // we use sequential scan by default

	beginningOfScan_ = true;
	if ( virtualTableFlag_ ) {
		prepareFirstKeyQueryManager();	// have to use multi-table index for virtual view
		DBUG_RETURN(convertToMysqlErrorCode(errorCode));
	}

	if ( scan ) {  

		// For SELECT and INSERT ... SELECT statements, we always use full table sequential scan (even there exists an index).
		// For all other statements, we use the first available index if an index exists.
		if ( !(sqlCommand_ == SQLCOM_SELECT || sqlCommand_ == SQLCOM_INSERT_SELECT) && (SDBIsTableWithIndexes(sdbDbId_, sdbTableNumber_)) ) {
			prepareFirstKeyQueryManager();
		}

		// set up table level lock if it is a real scan operation
		unsigned char tableLockLevel = SDBGetTableLockLevel(sdbUserId_, sdbDbId_, sdbTableNumber_);
		if ( (tableLockLevel == 0) || (tableLockLevel == DEFAULT_REFERENCE_LOCK_LEVEL) ) { 
			// For alter table and its equivalent, need to set to table level write lock.
			// For an explicit delete-all statement, we also set it to table level write lock.
			// For all other cases, set to table level read lock.
			// It is difficult to tell if a user is doig update-all.  This is because MySQL query optimizer
			// will use full table scan if the update condition is on a non-index column.

			switch (sqlCommand_) {
			case SQLCOM_ALTER_TABLE:	
			case SQLCOM_CREATE_INDEX:
			case SQLCOM_DROP_INDEX:
				tableLockLevel = REFERENCE_LOCK_EXCLUSIVE;
				break;

			case SQLCOM_DELETE:
				if ( deleteAllRows_ )
					tableLockLevel = REFERENCE_LOCK_EXCLUSIVE;
				else	// delete records based on a condition defined on a non-index column
					tableLockLevel = DEFAULT_REFERENCE_LOCK_LEVEL;
				break;

			default:
				tableLockLevel = DEFAULT_REFERENCE_LOCK_LEVEL;
				break;
			}

			if ( sqlCommand_ != SQLCOM_SELECT ) {
				if (SDBLockTable(sdbUserId_, sdbDbId_, sdbTableNumber_, tableLockLevel) == false)
					errorCode = LOCK_TABLE_FAILED;
			}
		}

	}	// 	if ( scan )

	DBUG_RETURN(convertToMysqlErrorCode(errorCode));
}


// This method ends a full table scan
int ha_scaledb::rnd_end() {  
	DBUG_ENTER("ha_scaledb::rnd_end");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::rnd_end(...) ");
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString(table->s->table_name.str );
		SDBDebugPrintString(", alias: ");
		SDBDebugPrintString( table->alias );
		outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	int errorNum = 0;
	DBUG_RETURN(errorNum);
}


// This method returns the next record
int ha_scaledb::rnd_next(uchar* buf) {
	DBUG_ENTER("ha_scaledb::rnd_next");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::rnd_next(...), index: ");
		SDBDebugPrintInt(active_index);
		SDBDebugPrintString(", Query Manager ID #");
		SDBDebugPrintInt(sdbQueryMgrId_);
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString( table->s->table_name.str );
		SDBDebugPrintString(", alias: ");
		SDBDebugPrintString( table->alias );
		outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	int errorNum = 0;
	int retValue = 0;

	// Return an error code end-of-file so that MySQL, on a non-primary node, stops sending rnd_next calls
	// on a non-primary node, we do nothing.
	// The flag ddlFlag_ was set earlier in ::rnd_init() method.
	if (SDBNodeIsCluster() == true) {
		if ( pSdbMysqlTxn_->getDdlFlag() & SDBFLAG_DDL_SECOND_NODE )
			DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	ha_statistic_increment(&SSV::ha_read_rnd_next_count);

	if (beginningOfScan_) {
		if (active_index == MAX_KEY) {
			// prepare for sequential scan
			char* pTableFsName = SDBGetTableFileSystemNameByTableNumber(sdbDbId_, sdbTableNumber_);
			char* designatorName = SDBUtilFindDesignatorName(pTableFsName, "sequential", active_index);

			sdbQueryMgrId_ = pSdbMysqlTxn_->findQueryManagerId(designatorName, (void*) this, NULL, 0);
			if ( !sdbQueryMgrId_ ) {  // need to get a new one and save sdbQueryMgrId_ into MysqlTxn object
				sdbQueryMgrId_ = SDBGetQueryManagerId(sdbUserId_);
				// save sdbQueryMgrId_
				pSdbMysqlTxn_->addQueryManagerId(false, designatorName, (void*) this, NULL, 0, sdbQueryMgrId_, mysqlInterfaceDebugLevel_);
			}
			RELEASE_MEMORY( designatorName );
			SDBSetActiveQueryManager(sdbQueryMgrId_); // cache the object for performance
			sdbDesignatorName_ = pSdbMysqlTxn_->getDesignatorNameByQueryMrgId(sdbQueryMgrId_);
			sdbDesignatorId_ = 0;	// no index is used.

			SDBResetQuery( sdbQueryMgrId_ );  // remove the previously defined query
			retValue = (int) SDBPrepareSequentialScan(sdbUserId_, sdbQueryMgrId_, sdbDbId_, table->s->table_name.str, 
						((THD*)ha_thd())->query_id, releaseLocksAfterRead_);

			// We fetch the result record and save it into buf
			if ( retValue == 0 ) {
				errorNum = fetchSingleRow(buf);
			} else {
				errorNum = convertToMysqlErrorCode(retValue);
				table->status = STATUS_NOT_FOUND;
			}
            pSdbMysqlTxn_->setScanType(sdbQueryMgrId_, true);

		} else {	// use index
			errorNum = index_first(buf);
			if (errorNum == HA_ERR_KEY_NOT_FOUND) {
				errorNum = HA_ERR_END_OF_FILE;
			}
		}       

		beginningOfScan_ = false;
	} else {	// scaning the 2nd record and forwards

		if (virtualTableFlag_)
			errorNum = fetchVirtualRow(buf);
		else
			errorNum = fetchSingleRow(buf);
	}

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::rnd_next(...) returns : ");
		SDBDebugPrintInt(errorNum);
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	DBUG_RETURN(errorNum);
}


// This method is called after each call to rnd_next() if the data needs to be ordered.
void ha_scaledb::position(const uchar* record)
{
	DBUG_ENTER("ha_scaledb::position");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::position(...) on table ");
		SDBDebugPrintString( table->s->table_name.str );
		SDBDebugPrintString(" ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	unsigned long long rowPos;
    if (table->key_info && pSdbMysqlTxn_->isSequentialScan(sdbQueryMgrId_) == false) {
		// get the index cursor row position
		rowPos = (unsigned long long)SDBQueryCursorGetIndexCursorRowPosition(sdbQueryMgrId_);
	}
	else {
		//non index, return current row position
		rowPos = SDBQueryCursorGetSeqRowPosition(sdbQueryMgrId_);
	}

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("position row id: ");
		SDBDebugPrint8ByteUnsignedLong(rowPos);
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	my_store_ptr(ref, sizeof(rowPos), rowPos);
	ref_length = sizeof(rowPos);

	DBUG_VOID_RETURN;
}

// This method is used for finding previously marked with position().
int ha_scaledb::rnd_pos(uchar * buf, uchar *pos)
{
	DBUG_ENTER("ha_scaledb::rnd_pos");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::rnd_pos(...) ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	ha_statistic_increment(&SSV::ha_read_rnd_count);

	int retValue = 0;

	if (beginningOfScan_) {
		//prepare once
		if (!sdbQueryMgrId_) {
			unsigned int old_active_index = active_index;
			prepareFirstKeyQueryManager();
			active_index = old_active_index;
		}
		SDBResetQuery(sdbQueryMgrId_);  // remove the previously defined query
		retValue = (int) SDBPrepareSequentialScan(sdbUserId_, sdbQueryMgrId_, sdbDbId_, table->s->table_name.str, 
					((THD*)ha_thd())->query_id, releaseLocksAfterRead_);
		beginningOfScan_ = false;
        pSdbMysqlTxn_->setScanType(sdbQueryMgrId_, true);
	}

	if (retValue == 0) {
		int64 rowPos = my_get_ptr(pos,ref_length);

		if (rowPos > SDB_MAX_ROWID_VALUE){
			return HA_ERR_END_OF_FILE;
		}

		unsigned int rowid = (unsigned int)rowPos;

#ifdef SDB_DEBUG_LIGHT
		if (mysqlInterfaceDebugLevel_) {
			SDBDebugStart();			// synchronize threads printout	
			SDBDebugPrintHeader("rnd_pos row id to be fetched: ");
			SDBDebugPrint8ByteUnsignedLong(rowPos);
			SDBDebugEnd();			// synchronize threads printout	
		}
#endif
		retValue = fetchRowByPosition(buf, rowid);
	}
	retValue = convertToMysqlErrorCode(retValue);

	DBUG_RETURN(retValue);
}


// This method sets up public variables for query optimizer to use 
int ha_scaledb::info(uint flag)
{
	DBUG_ENTER("ha_scaledb::info");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::info(...) ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	if (SDBNodeIsCluster() == true) {
		if ( pSdbMysqlTxn_->getDdlFlag() & SDBFLAG_DDL_SECOND_NODE )
			DBUG_RETURN(0);
	}

	if ( sdbTableNumber_ == 0 ) {
		sdbTableNumber_ = SDBGetTableNumberByName(sdbUserId_, sdbDbId_, table->s->table_name.str);
		if (sdbTableNumber_ == 0)
			DBUG_RETURN(convertToMysqlErrorCode(TABLE_NAME_UNDEFINED));
	}

	if (flag & HA_STATUS_ERRKEY) {
		errkey = get_last_index_error_key();
		DBUG_RETURN(0);
	}

	//if (flag & HA_STATUS_AUTO) {
	//	stats.auto_increment_value = 69;
	//}

	if ( (flag & HA_STATUS_VARIABLE) || // update the 'variable' part of the info:
		(flag & HA_STATUS_NO_LOCK) )
	{  
		stats.records = (ulong) SDBGetTableStats(sdbDbId_, sdbTableNumber_, SDB_STATS_INFO_FILE_RECORDS);
		stats.deleted = (ulong) SDBGetTableStats(sdbDbId_, sdbTableNumber_, SDB_STATS_INFO_FILE_DELETED);
		stats.data_file_length = (ulong) SDBGetTableStats(sdbDbId_, sdbTableNumber_, SDB_STATS_INFO_FILE_LENGTH);
		stats.index_file_length = SDBGetTableStats(sdbDbId_, sdbTableNumber_, SDB_STATS_INDEX_FILE_LENGTH) / 2;

		//if (stats.index_file_length == 0)
		//    stats.index_file_length = 1;

		stats.mean_rec_length = (ulong)(stats.records == 0 ? 0 : SDBGetTableStats(sdbDbId_, sdbTableNumber_, SDB_STATS_INFO_REC_LENGTH));
	}

#if 0
	if (flag & HA_STATUS_CONST) {  // update the 'constant' part of the info:

		for (unsigned int i = 0; i < table->s->keys; i++) {

			if (!table->key_info[i].rec_per_key)
				continue;

			unsigned short indexKey;
			unsigned short storageId;

			for (unsigned int j = 0; j < table->key_info[i].key_parts; j++) {

				if (!table->key_info[j].key_part)
					break;

				char* pTableFsName = pMetaInfo_->getTableFileSystemNameByTableNumber(sdbTableNumber_);
				char* designatorName = SDBUtilFindDesignatorName(pTableFsName, table->key_info[j].name, i);

				indexKey = MAKE_TABLE_ID(pMetaInfo_->getParentDesignator(pMetaInfo_->getDesignatorNumberByName(designatorName), 1));
				storageId = pMetaInfo_->getStorageId(indexKey);
				int64 keySize = pSdbEngine->getAnchor()->getBufferManager()->getFileInfo(storageId, 0, SDB_INDEX_FILE_LENGTH); 
				int64 keyRowLength = pMetaInfo_->getTableRowLengthByDesignator(pMetaInfo_->getDesignatorNumberByName(designatorName));
				RELEASE_MEMORY( designatorName );

				int64 rowsPerKey = (keySize/keyRowLength) / 2;
				if (rowsPerKey == 0)
					rowsPerKey = 1;

				table->key_info[i].rec_per_key[j] = (ulong)rowsPerKey;
			}
		}
	}
#endif
	if (flag & HA_STATUS_TIME) {
		//TODO: store this to fileinfo; and update only when it is not set
		char path[FN_REFLEN];
		my_snprintf(path, sizeof(path), "%s%s", share->table_name, reg_ext);
		unpack_filename(path,path);
		struct stat	statinfo;
		if (!stat(path, &statinfo))
			stats.create_time = (ulong) statinfo.st_ctime;
	}

	if ( (flag & HA_STATUS_AUTO) && table->found_next_number_field && pSdbMysqlTxn_) {
		stats.auto_increment_value = get_scaledb_autoincrement_value();
	}

	DBUG_RETURN(0);
}


// store_lock method can modify the lock level.
// Before adding the lock into the table lock handler (or before external_lock call), mysqld calls 
// this method with the requested locks.  Hence we should not save any ha_scaledb member variables.
// When releasing locks, this method is not called.  MySQL calls external_lock to release locks.  
THR_LOCK_DATA **ha_scaledb::store_lock(THD *thd,
									   THR_LOCK_DATA **to,
									   enum thr_lock_type lock_type)
{
#ifdef SDB_DEBUG_LIGHT

	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::store_lock(thd=");
		SDBDebugPrint8ByteUnsignedLong((uint64)thd);
		SDBDebugPrintString(")");
		if (mysqlInterfaceDebugLevel_>1) {
			SDBDebugPrintString(", handler=");
			SDBDebugPrint8ByteUnsignedLong((uint64)this);
			SDBDebugPrintString(", Query:");
			SDBDebugPrintString(thd->query());
		}
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	// We save transaction isolation level here for a given user thread (save into User object).
	// Note that transaction isolation level is set for a user, not for a table handler.
	placeSdbMysqlTxnInfo( thd );
	if (lock_type != TL_IGNORE) {
		enum_tx_isolation level = (enum_tx_isolation) thd_tx_isolation(thd);
		switch(level) {
			// the following cases are listed in the descending order of frequency
			case ISO_READ_COMMITTED: 
				SDBSetIsolationLevel(sdbUserId_, SDB_ISOLATION_READ_COMMITTED);
				break;

			case ISO_REPEATABLE_READ:
				SDBSetIsolationLevel(sdbUserId_, SDB_ISOLATION_REPEATABLE_READ);
				break;

			case ISO_READ_UNCOMMITTED: 
				SDBSetIsolationLevel(sdbUserId_, SDB_ISOLATION_READ_UNCOMMITTED);
				break;

			case ISO_SERIALIZABLE: 
				SDBSetIsolationLevel(sdbUserId_, SDB_ISOLATION_SERIALIZABLE);
				break;

			default: 
				SDBSetIsolationLevel(sdbUserId_, SDB_ISOLATION_READ_COMMITTED);
				break;
		}

	}


	// MySQL's default lock level is at table level.  It is very restrictive and has low conncurrency.
	// We need to adjust its lock level in order to allow Row Level Locking

	int thdInLockTables = thd_in_lock_tables(thd);
	unsigned int sqlCommand = thd_sql_command(thd);

	if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK) {

		if (lock_type == TL_READ && sqlCommand == SQLCOM_LOCK_TABLES) {
			// In processing LOCK TABLES ... READ LOCAL, we treat READ LOCAL equivalent to READ. 
			// This shows same behavior as myisam and innodb

			lock_type = TL_READ_NO_INSERT;	// equivalent to table level read lock
		}

		// We allow concurrent writes if a thread is not running LOCK TABLE, DISCARD/IMPORT TABLESPACE or TRUNCATE TABLE.
		// Note that ALTER TABLE uses a TL_WRITE_ALLOW_READ	< TL_WRITE_CONCURRENT_INSERT.

		// We allow concurrent writes if MySQL is at the start of a stored procedure call (SQLCOM_CALL) or in a
		// stored function call (MySQL has set thd_sql_command(thd) to true).

		if ( (lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE)
			&& !(thdInLockTables && sqlCommand == SQLCOM_LOCK_TABLES)
			//&& !thd_tablespace_op(thd)	// We do not support tablespace yet
			&& sqlCommand != SQLCOM_TRUNCATE
			&& sqlCommand != SQLCOM_OPTIMIZE
			&& sqlCommand != SQLCOM_CREATE_TABLE) {

				lock_type = TL_WRITE_ALLOW_WRITE;
		}

		// In processing INSERT INTO t1 SELECT ... FROM t2 ..., MySQL would use the lock TL_READ_NO_INSERT on t2.
		// This imposes a table level read lock on t2.  This is consisten with our design.  No need to change lock type.

		// We allow concurrent writes if MySQL is at the start of a stored procedure call (SQLCOM_CALL).
		// Also MySQL sets thd_in_lock_tables() true. We need to change to normal read for this case.

		if ( (lock_type == TL_READ_NO_INSERT) && (sqlCommand != SQLCOM_INSERT_SELECT) 
			&& (thdInLockTables == false) ) {

				lock_type = TL_READ;
		}

		lock.type = lock_type;
	}

	*to++ = &lock;
	return to;
}


// This method returns true if the DDL statement has string "engine = scaledb"
bool hasEngineEqualScaledb(char* sqlStatement) {
	bool retValue = false;
	char* pEngine = strstr(sqlStatement, "engine");
	char* pScaledb = strstr(sqlStatement, "scaledb");
	if ( (pEngine) && (pScaledb) && (pEngine > sqlStatement) && (pScaledb > pEngine) )
		retValue = true;

	return retValue;
}


/* 
Create a table. You do not want to leave the table open after a call to
this (the database will call ::open if it needs to).
Parameter name contains database name and table name that are file system compliant names.
The user-defined table name is saved in table_arg->s->table_name.str
*/
int ha_scaledb::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
	DBUG_ENTER("ha_scaledb::create");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::create(name= ");
		SDBDebugPrintString( (char*) name );
		SDBDebugPrintString(" )");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugFlush();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	// we don't support temp tables -- REJECT
	if(create_info->options & HA_LEX_CREATE_TMP_TABLE){
		DBUG_RETURN(HA_ERR_UNSUPPORTED);
	}

	////////////////////////////////////////////////////////////////////
	// core dump prevention code
	if (!table_arg){
		SDBDebugPrintHeader("table_arg is NULL");
		SDBDebugFlush();
		DBUG_RETURN(HA_ERR_UNKNOWN_CHARSET);		
	}

	if (!table_arg->s){
		SDBDebugPrintString("table_arg->s is NULL");
		SDBDebugFlush();
		DBUG_RETURN(HA_ERR_UNKNOWN_CHARSET);		
	}

	if (!table_arg->s->db.str){
		SDBDebugPrintString("table_arg->s->db.str is NULL");
		SDBDebugFlush();
		DBUG_RETURN(HA_ERR_UNKNOWN_CHARSET);		
	}

	if (!table_arg->s->table_name.str){
		SDBDebugPrintString("table_arg->s->table_name.str is NULL");
		SDBDebugFlush();
		DBUG_RETURN(HA_ERR_UNKNOWN_CHARSET);		
	}

	// TODO: As of 4/20/2010, we support charset in either ascii or latin1 only.
	// After we support utf8, we need to revise this checking.
	if ( (strncmp(create_info->default_table_charset->csname, "ascii", 5) != 0) &&
		 (strncmp(create_info->default_table_charset->csname, "latin1", 6) != 0) )
		DBUG_RETURN(HA_ERR_UNKNOWN_CHARSET);		

	// Check if Database name and table name are US ASCII for now. 
	//if ( (DataUtil::isUsAscii(table_arg->s->db.str) == false) || 
	//	(DataUtil::isUsAscii(table_arg->s->table_name.str) == false) )
	//	DBUG_RETURN(HA_ERR_UNKNOWN_CHARSET);

	char dbFsName[METAINFO_MAX_IDENTIFIER_SIZE] = {0};	// database name that is compliant with file system
	char tblFsName[METAINFO_MAX_IDENTIFIER_SIZE] = {0}; // table name that is compliant with file system
	char pathName[METAINFO_MAX_IDENTIFIER_SIZE] = {0};
	// First we put db and table information into metadata memory
	fetchIdentifierName(name, dbFsName, tblFsName, pathName); 

	char* pDbName = table_arg->s->db.str;				// points to user-defined database name
	char* pTableName = table_arg->s->table_name.str;	// points to user-defined table name.  This name is case sensitive

	int retCode = SUCCESS;
	bool bIsAlterTableStmt = false;
	unsigned int errorNum = 0;
	THD* thd = ha_thd();
	placeSdbMysqlTxnInfo( thd );	
#ifdef SDB_DEBUG
	SDBLogSqlStmt(sdbUserId_, thd->query(), thd->query_id);	// inform engine to log user query for DDL
#endif

	unsigned short ddlFlag = 0;
	// use this flag to decide if we want to create entries on metatables.
	// When set to true, we need to read metadata information into memory from metatables.

	if (SDBNodeIsCluster() == true)
		if ( strstr(thd->query(), SCALEDB_HINT_PASS_DDL) != NULL ) {
		ddlFlag |= SDBFLAG_DDL_SECOND_NODE;		// this flag is used locally.

		// For CREATE TABLE t1 SELECT 1, 'hello'; there is no select-from table.
		// Secondary node needs to set up this flag here instead of inside external_lock pair.
		pSdbMysqlTxn_->setOrOpDdlFlag( (unsigned short)SDBFLAG_DDL_SECOND_NODE );
	}

	unsigned int sqlCommand = thd_sql_command(thd);

	// signal that we have reached ::create method in processing ALTER TABLE statement.
	// This flag is checked in scaledb_commit method if it is called before running an ALTER TABLE statement.
	if (sqlCommand==SQLCOM_ALTER_TABLE || sqlCommand==SQLCOM_CREATE_INDEX || sqlCommand==SQLCOM_DROP_INDEX) {
		pSdbMysqlTxn_->setOrOpDdlFlag( (unsigned short)SDBFLAG_ALTER_TABLE_CREATE );
		bIsAlterTableStmt = true;
	}

	if (ddlFlag & SDBFLAG_DDL_SECOND_NODE) {
		// For CREATE TABLE statement, secondary node does not need to open table.  MySQL will later issue open() call if needed.  
		// For ALTER TABLE statement, secondary node need to exit without creating the temporary table.
		DBUG_RETURN(errorNum);
	}

	// Only the single node solution or the primary node of a cluster will proceed after this point.
	retCode = openUserDatabase(pDbName, dbFsName, true, bIsAlterTableStmt, ddlFlag);
	if (retCode) 
		DBUG_RETURN( convertToMysqlErrorCode(retCode) );


	// make the entire CREATE TABLE statement a transaction.  The transaction finishes after it performs
	// createTable, createField, createIndex.

	bool bCreateTableSelect = false;
	char* pCreateTableStmt = (char*) GET_MEMORY( thd->query_length() + 1 );
	// convert LF to space, remove extra space, convert to lower case letters 
	convertSeparatorLowerCase( pCreateTableStmt, thd->query());
	char* pSelect = strstr(pCreateTableStmt, "select ");
	if ( (sqlCommand==SQLCOM_CREATE_TABLE) && (pSelect) )	// DDL contains key word 'select'
		bCreateTableSelect = true;

	switch (sqlCommand) {
		case SQLCOM_CREATE_TABLE:
			if (!bCreateTableSelect) {
				SDBStartTransaction( sdbUserId_ );
				pSdbMysqlTxn_->setScaleDbTxnId( SDBGetTransactionIdForUser( sdbUserId_ ) );
				pSdbMysqlTxn_->setActiveTrn( true );
			}
			// else txn starts in external_lock since it is a CREATE TABLE ... SELECT statement.
			break;

		case SQLCOM_ALTER_TABLE:
			if (pSdbMysqlTxn_->getAlterTableName()==NULL) {
				// If the to-be-altered table is NOT a ScaleDB table,
				// for cluster, we return an error (Bug 934); 
				// for single node solution, we start a user transaction				
				if (SDBNodeIsCluster() == true) {
					DBUG_RETURN( HA_ERR_UNSUPPORTED );	// disallow this
				} else {
					SDBStartTransaction( sdbUserId_ );
					pSdbMysqlTxn_->setScaleDbTxnId( SDBGetTransactionIdForUser( sdbUserId_ ) );
					pSdbMysqlTxn_->setActiveTrn( true );
				}
			}
			// else txn starts in external_lock since it is a ScaleDB table.
			break;

		default:	// for SQLCOM_CREATE_INDEX and SQLCOM_DROP_INDEX, txn starts in external_lock
			break;
	}


	// Primary node needs to impose lockMetaInfo before proceeding with DDL statement.
	// Secondary nodes do NOT need to lock the metadata any more.
	// SDBLockMetaInfo is cluster-wide lock per database.  
	retCode = SDBLockMetaInfo(sdbUserId_, sdbDbId_);
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("In ha_scaledb::create, SDBLockMetaInfo retCode=");
		SDBDebugPrintInt( retCode );
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif
	if (retCode) {
		RELEASE_MEMORY(pCreateTableStmt);
		pSdbMysqlTxn_->removeAlterTableName();
		DBUG_RETURN( convertToMysqlErrorCode(retCode) );
	}

	virtualTableFlag_ = SDBTableIsVirtual(tblFsName);
	if ( virtualTableFlag_ ) {
		// If we are creating a virtual view, then we can have an early exit.
		if ((SDBNodeIsCluster() == true) && (errorNum == 0))
			if ((sqlCommand == SQLCOM_CREATE_TABLE) && ( !(ddlFlag & SDBFLAG_DDL_SECOND_NODE) ) ) {
				sendStmtToOtherNodes(sdbDbId_, thd->query(), false, true);
			}

			RELEASE_MEMORY(pCreateTableStmt);
			DBUG_RETURN(errorNum);
	}

	// On a cluster system, primary node needs to send FLUSH TABLE statement to secondary nodes here
	// in order to close the table files of the table to be altered.  The DDL is sent after primary node finishes processing.

	if ( (SDBNodeIsCluster() == true) && ( !(ddlFlag & SDBFLAG_DDL_SECOND_NODE) ) ) {
		// For ALTER TABLE, CREATE/DROP INDEX statements, primary node issues FLUSH TABLE statement so that
		// all other nodes can close the table to be altered.
		if (sqlCommand==SQLCOM_ALTER_TABLE || sqlCommand==SQLCOM_CREATE_INDEX || sqlCommand==SQLCOM_DROP_INDEX) {
			char* pFlushTableStmt = SDBUtilAppendString("flush table ", pSdbMysqlTxn_->getAlterTableName());
			bool bFlushTableReady = sendStmtToOtherNodes(sdbDbId_, pFlushTableStmt, false, false);
			pSdbMysqlTxn_->removeAlterTableName();
			RELEASE_MEMORY(pFlushTableStmt);

			if (!bFlushTableReady) {
				SDBCommit(sdbUserId_);
				RELEASE_MEMORY( pCreateTableStmt );
				DBUG_RETURN( convertToMysqlErrorCode(retCode) );
			}
		}
	}

	// find out if we will have overflows
	bool hasOverflow = has_overflow_fields(thd, table_arg);

	sdbTableNumber_ = SDBCreateTable(sdbUserId_, sdbDbId_, pTableName, create_info->auto_increment_value, tblFsName, pTableName, virtualTableFlag_, hasOverflow, ddlFlag);
	if ( sdbTableNumber_ == 0 ) {	// createTable fails, need to rollback
		SDBRollBack(sdbUserId_);
		RELEASE_MEMORY( pCreateTableStmt );
		DBUG_RETURN(HA_ERR_GENERIC);
	}

	if (hasOverflow)
		SDBSetOverflowFlag(sdbDbId_, sdbTableNumber_, true); // this table will now have over flows.

	// Second we add column information to metadata memory
	errorNum = add_columns_to_table(thd, table_arg, ddlFlag);
	if (errorNum){
		// Fix for 882, No need to rollback, or deleteTableById here, as both of these were done by
		// the add_columns_to_table, as it returned a non zero errorNum.
		// SDBRollBack(sdbUserId_);	// rollback new table record in transaction
		// SDBDeleteTableById(sdbDbId_, sdbTableNumber_);

		RELEASE_MEMORY( pCreateTableStmt );
		DBUG_RETURN(errorNum);
	}

	int numOfKeys = (int) table_arg->s->keys ;

	// store info about foreign keys so the foreign key index can be created properly when designator is created
	SdbDynamicArray *fkInfoArray = SDBArrayInit(numOfKeys+1, numOfKeys+1, sizeof(void*)); //+1 because we use the locations 1,2,3 rather than 0,1,2

	if ( numOfKeys > 0 ) {  // index/designator exists
		// create the foreign key metadata
		errorNum = create_fks(thd, table_arg, pTableName, fkInfoArray, pCreateTableStmt);
		if (errorNum){
			SDBRollBack(sdbUserId_);	// rollback new table record in transaction
			SDBDeleteTableById(sdbUserId_, sdbDbId_, sdbTableNumber_);
			RELEASE_MEMORY( pCreateTableStmt );
			DBUG_RETURN(errorNum);
		}

		errorNum = add_indexes_to_table(thd, table_arg, pTableName, ddlFlag, fkInfoArray, pCreateTableStmt);
		if (errorNum){
			SDBRollBack(sdbUserId_);	// rollback new table record in transaction
			SDBDeleteTableById(sdbUserId_, sdbDbId_, sdbTableNumber_);
			RELEASE_MEMORY( pCreateTableStmt );
			DBUG_RETURN(errorNum);
		}
	}

	for (unsigned short i = SDBArrayGetNextElementPosition(fkInfoArray, 0); i; i = SDBArrayGetNextElementPosition(fkInfoArray, i)) {
        MysqlForeignKey *ptr = (MysqlForeignKey *)SDBArrayGetPtr(fkInfoArray, i);
	    delete ptr;
    }
    SDBArrayFree(fkInfoArray);

	// Bug 1008: A user may CREATE TABLE and then RENAME TABLE immediately. 
	// In order to fix this bug, we need to open and create the table files. Then we can rename the table files.
	SDBOpenTableFiles(sdbUserId_, sdbDbId_, sdbTableNumber_);

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 1) 
		SDBPrintStructure(sdbDbId_);		// print metadata structure

	if (ha_scaledb::mysqlInterfaceDebugLevel_ > 3) { // for debugging memory leak
		SDBDebugStart();			// synchronize threads printout	
		SDBPrintMemoryInfo(); 
		SDBDebugEnd();			// synchronize threads printout	
	}
	if (mysqlInterfaceDebugLevel_ > 4) {
		// print user lock status on the primary node
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("In ha_scaledb::create, print user locks imposed by primary node before sending DDL ");
		SDBDebugEnd();			// synchronize threads printout	
		SDBShowUserLockStatus(sdbUserId_);
	}
#endif

	if ( sqlCommand == SQLCOM_CREATE_TABLE ) 
		SDBCloseTable(sdbUserId_, sdbDbId_, pTableName, true, true);
	else	// do not commit for ALTER TABLE statement
		SDBCloseTable(sdbUserId_, sdbDbId_, pTableName, true, false);

	// pass the DDL statement to engine so that it can be propagated to other nodes.
	// We need to make sure it is a regular CREATE TABLE command because this method is called
	// when a user has ALTER TABLE or CREATE/DROP INDEX commands.
	// We also need to make sure that the scaledb hint is not found in the user query.

	if ((SDBNodeIsCluster() == true) && (errorNum == 0))
		if ((sqlCommand == SQLCOM_CREATE_TABLE) && (!(ddlFlag & SDBFLAG_DDL_SECOND_NODE)) ) {
			bool createDatabaseReady = true;

			// if this is the first user table in the database, we need to issue CREATE DATABASE stmt to other nodes.
			// We need to check this only for a regular CREATE TABLE statement, not for CREATE TABLE ... SELECT
			if (sdbTableNumber_ == SDB_FIRST_USER_TABLE_ID) {
				char* ddlCreateDB = SDBUtilAppendString("CREATE DATABASE IF NOT EXISTS ", dbFsName);
				createDatabaseReady = sendStmtToOtherNodes(sdbDbId_, ddlCreateDB, true, false);// need to ignore DB name for CREATE DATABASE stmt
				RELEASE_MEMORY(ddlCreateDB);
			}

			if ( createDatabaseReady ) {
				// Now we pass the CREATE TABLE statement to other nodes
				// need to send statement "SET SESSION STORAGE_ENGINE=SCALEDB" before CREATE TABLE
				bool createTableReady = true;
				createTableReady = sendStmtToOtherNodes(sdbDbId_, thd->query(), false, true); 

				if ( createTableReady == false )
					retCode = CREATE_TABLE_FAILED_IN_CLUSTER;
			}
			else retCode = CREATE_DATABASE_FAILED_IN_CLUSTER;
		}

	// TODO: To be enabled later for fast ALTER TABLE statement.
	// For ALTER TABLE statement, we create a scratch table to hold new table structure.
	// To speed up ALTER TABLE, we need to temporarily disable all indexes before inserts.
	//if (sqlCommand == SQLCOM_ALTER_TABLE) {
	//	pSdbMysqlTxn_->addScratchTableName( (char*) name );
	//	pSdbEngine->disableTableIndexes(sdbDbId, tblName);
	//}

	// CREATE TABLE: Primary node needs to release lockMetaInfo here after all nodes finish processing.
	// ALTER TABLE: Primary node needs to release lockMetaInfo in the last step: delete_table .
	if ( sqlCommand == SQLCOM_CREATE_TABLE )
		SDBCommit(sdbUserId_);

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 4) {
		// print user lock status on the primary node
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("In ha_scaledb::create, print user locks imposed by primary node after commit ");
		SDBDebugEnd();			// synchronize threads printout	
		SDBShowUserLockStatus(sdbUserId_);
	}
#endif

	RELEASE_MEMORY( pCreateTableStmt );
	errorNum = convertToMysqlErrorCode(retCode);
	DBUG_RETURN(errorNum);
}


// add columns to a table, part of create table
int ha_scaledb::has_overflow_fields(THD* thd, TABLE *table_arg){
	Field*	pField;
	enum_field_types fieldType;
	unsigned short sdbFieldSize;
	unsigned int sdbMaxDataLength; 
	unsigned int maxColumnLengthInBaseFile = SDBGetMaxColumnLengthInBaseFile();

	for (unsigned short i=0; i < (int) table_arg->s->fields; ++i) {
		pField = table_arg->field[i];
		fieldType = pField->type();
		switch ( fieldType ) {
		case MYSQL_TYPE_VARCHAR:  
		case MYSQL_TYPE_VAR_STRING: 
		case MYSQL_TYPE_TINY_BLOB:  // tiny	blob applies to to TEXT as well
			if( pField->field_length > maxColumnLengthInBaseFile )
				sdbFieldSize = maxColumnLengthInBaseFile;
			else
				sdbFieldSize = pField->field_length;
			sdbMaxDataLength = pField->field_length;
			
			if (sdbMaxDataLength > sdbFieldSize)
				return true; // this table will have overflows
			break;

		case MYSQL_TYPE_BLOB:	// These 3 data types apply to to TEXT as well
		case MYSQL_TYPE_MEDIUM_BLOB:  
		case MYSQL_TYPE_LONG_BLOB: 
			// We parse through the index keys, based on the participation of this field in the
			// keys we decide how much of the field we want to store as part of the record and 
			// how much in the overflow file. If field does not participate in any index then
			// we do not store anything as part of the record.
			sdbFieldSize = get_field_key_participation_length(thd, table_arg, pField);
			if( sdbFieldSize > maxColumnLengthInBaseFile )
				sdbFieldSize = maxColumnLengthInBaseFile; // can not exceed maxColumnLengthInBaseFile under any circumstances)

			sdbMaxDataLength = pField->field_length;
			if (sdbMaxDataLength > sdbFieldSize)
				return true; // this table will have overflows
			break;

		default:
			break;
		}

	}
	return false;
}


// add columns to a table, part of create table
int ha_scaledb::add_columns_to_table(THD* thd, TABLE *table_arg, unsigned short ddlFlag){

	unsigned int errorNum = 0;
	Field*	pField;
	enum_field_types fieldType;
	unsigned char sdbFieldType;
	unsigned short sdbFieldSize;
	unsigned int sdbMaxDataLength; 
	unsigned short sdbFieldId;
	unsigned int maxColumnLengthInBaseFile = SDBGetMaxColumnLengthInBaseFile();
	bool isAutoIncrField;
	Field* pAutoIncrField = NULL;
	const char* charsetName = NULL;
	if ( table_arg->s->found_next_number_field )	// does it have an auto_increment field?
		pAutoIncrField = table_arg->s->found_next_number_field[0];	// points to auto_increment field

	for (unsigned short i=0; i < (int) table_arg->s->fields; ++i) {
		isAutoIncrField = false;
		if ( pAutoIncrField ) {	// check to see if this table has an auto_increment column
			if ( pAutoIncrField->field_index == i )
				isAutoIncrField = true;
		}

		pField = table_arg->field[i];
		fieldType = pField->type();
		switch ( fieldType ) {
		case MYSQL_TYPE_SHORT:
			if (pField->flags & UNSIGNED_FLAG)
				sdbFieldType = ENGINE_TYPE_U_NUMBER;
			else
				sdbFieldType = ENGINE_TYPE_S_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_SHORT;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_INT24:
			if (pField->flags & UNSIGNED_FLAG)
				sdbFieldType = ENGINE_TYPE_U_NUMBER;
			else
				sdbFieldType = ENGINE_TYPE_S_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_MEDIUMINT;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_LONG:	
			if (pField->flags & UNSIGNED_FLAG)
				sdbFieldType = ENGINE_TYPE_U_NUMBER;
			else
				sdbFieldType = ENGINE_TYPE_S_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_INTEGER;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_LONGLONG:
			if (pField->flags & UNSIGNED_FLAG)
				sdbFieldType = ENGINE_TYPE_U_NUMBER;
			else
				sdbFieldType = ENGINE_TYPE_S_NUMBER;
			sdbFieldSize = ENGINE_TYPE_SIZE_OF_LONG;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_TINY:
			if (pField->flags & UNSIGNED_FLAG)
				sdbFieldType = ENGINE_TYPE_U_NUMBER;
			else
				sdbFieldType = ENGINE_TYPE_S_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_TINYINT;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_FLOAT:	// FLOAT is treated as a 4-byte number
			if (pField->flags & UNSIGNED_FLAG)
				sdbFieldType = ENGINE_TYPE_U_NUMBER;
			else
				sdbFieldType = ENGINE_TYPE_S_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_FLOAT;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_DOUBLE:	// DOUBLE is treated as a 8-byte number
			if (pField->flags & UNSIGNED_FLAG)
				sdbFieldType = ENGINE_TYPE_U_NUMBER;
			else
				sdbFieldType = ENGINE_TYPE_S_NUMBER;
			sdbFieldSize = ENGINE_TYPE_SIZE_OF_DOUBLE;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_BIT:	// BIT is treated as a byte array.  Its length is (M+7)/8
			sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			sdbFieldSize = pField->pack_length();
			//			sdbFieldSize = ((Field_bit*) pField)->pack_length();	// same effect as last statement (due to polymorphism)
			sdbMaxDataLength = 0;
			break;

			// In MySQL 5.1.42, MySQL treats SET as string during create table, insert, and select.
		case MYSQL_TYPE_SET:	// SET is treated as a non-negative integer.  Its length is up to 8 bytes
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = ((Field_set*) pField)->pack_length();
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_DATE:	// DATE is treated as a non-negative 3-byte integer
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_DATE;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_TIME:	// TIME is treated as a non-negative 3-byte integer
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_TIME;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_DATETIME:	// DATETIME is treated as a non-negative 8-byte integer
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_DATETIME;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_TIMESTAMP:	// TIMESTAMP is treated as a non-negative 4-byte integer
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_TIMESTAMP;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_YEAR:
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_TINYINT;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_NEWDECIMAL:	// treat decimal as a fixed-length byte array
			sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			sdbFieldSize = ((Field_new_decimal*) pField)->bin_size;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_ENUM:
			sdbFieldType = ENGINE_TYPE_U_NUMBER;

			// In MySQL 5.1.20-beta, MySQL treats ENUM as string during create table and select.
			// But it treats ENUM as INT during inserts.  This is wrong!  It happens to work now.
			// It appears to get fixed in mysql-5.1.42.
			sdbFieldSize = ((Field_enum*) pField)->pack_length();	// TBD: need validation
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_STRING:	// can be CHAR or BINARY data type
			// if the character set name contains the string "bin" we infer that this is a binary string
			charsetName = pField->charset()->name;
			charsetName = strstr(charsetName, "bin");
			if ( pField->binary() ){ 
				sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			}
			else if(charsetName){
				sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			}
			else{
				sdbFieldType = ENGINE_TYPE_STRING;
			}
			// do NOT use pField->field_length as it may be too big
			sdbFieldSize = pField->pack_length();	// exact size used in RAM
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_VARCHAR:  
		case MYSQL_TYPE_VAR_STRING: 
		case MYSQL_TYPE_TINY_BLOB:  // tiny	blob applies to to TEXT as well
//			if ( pField->binary() ) 
//				sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
//			else
//				sdbFieldType = ENGINE_TYPE_STRING;
//			Fix for 1001, we treat all the variable length blobs, varchars, and text as a 
//			binary array only, because they are padded by zero instead of any space if they
//			fall short in length. ENGINE_TYPE_STRING means that they would get padded by space
//			which is not desirable.
			sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;

			if( pField->field_length > maxColumnLengthInBaseFile )
				sdbFieldSize = maxColumnLengthInBaseFile;
			else
				sdbFieldSize = pField->field_length;
			sdbMaxDataLength = pField->field_length;
			break;

		case MYSQL_TYPE_BLOB:	// These 3 data types apply to to TEXT as well
		case MYSQL_TYPE_MEDIUM_BLOB:  
		case MYSQL_TYPE_LONG_BLOB: 
//			if ( pField->binary() ) 
//				sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
//			else
//				sdbFieldType = ENGINE_TYPE_STRING;
//			Fix for 1001, we treat all the variable length blobs, varchars, and text as a 
//			binary array only, because they are padded by zero instead of any space if they
//			fall short in length. ENGINE_TYPE_STRING means that they would get padded by space
//			which is not desirable.
			sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;

			// We parse through the index keys, based on the participation of this field in the
			// keys we decide how much of the field we want to store as part of the record and 
			// how much in the overflow file. If field does not participate in any index then
			// we do not store anything as part of the record.
			sdbFieldSize = get_field_key_participation_length(thd, table_arg, pField);
			if( sdbFieldSize > maxColumnLengthInBaseFile )
				sdbFieldSize = maxColumnLengthInBaseFile; // can not exceed maxColumnLengthInBaseFile under any circumstances)

			sdbMaxDataLength = pField->field_length;
			break;

		default:
			sdbFieldType = 0;
			SDBRollBack(sdbUserId_);
			SDBDeleteTableById(sdbUserId_, sdbDbId_, sdbTableNumber_);
#ifdef SDB_DEBUG_LIGHT
			SDBDebugStart();			// synchronize threads printout	
			SDBDebugPrintString("\0This data type is not supported yet.\0");
			SDBDebugEnd();			// synchronize threads printout	
#endif
			return HA_ERR_UNSUPPORTED;
			break;
		}

		sdbFieldId = SDBCreateField(sdbUserId_, sdbDbId_, sdbTableNumber_, (char*) pField->field_name, sdbFieldType, 
			sdbFieldSize, sdbMaxDataLength, NULL, isAutoIncrField, ddlFlag);
		if ( sdbFieldId == 0 ) {	// an error occurs
			SDBRollBack(sdbUserId_);
			SDBDeleteTableById(sdbUserId_, sdbDbId_, sdbTableNumber_);
#ifdef SDB_DEBUG_LIGHT
			SDBDebugStart();			// synchronize threads printout	
			SDBDebugPrintString("\0fails to create user column ");
			SDBDebugPrintString((char*) pField->field_name);
			SDBDebugPrintString("\0");
			SDBDebugEnd();			// synchronize threads printout	
#endif
			return HA_ERR_UNSUPPORTED;
		}
	}
	return errorNum;
}


// create the foreign keys for a table
int ha_scaledb::create_fks(THD* thd, TABLE *table_arg, char* tblName, SdbDynamicArray* fkInfoArray, char* pCreateTableStmt){

	unsigned int errorNum = 0;
	int numOfKeys = (int) table_arg->s->keys ;

	if ( thd->query_length() > 0 ) {

		// foreign key constraint syntax:
		// [CONSTRAINT [symbol]] FOREIGN KEY [index_name] (index_col_name, ...) 
		//		REFERENCES tbl_name (index_col_name,...)
		// MySQL creates a non-unique secondary index in the child table for a foreign key constraint

		char* pCurrConstraintClause = strstr( pCreateTableStmt, "constraint ");
		char* pCurrForeignKeyClause = strstr( pCreateTableStmt, "foreign key ");
		char* pConstraintName = NULL;
		while ( pCurrForeignKeyClause != NULL ) {  // foreign key clause exists
			pConstraintName = NULL;
			if (pCurrConstraintClause && pCurrForeignKeyClause) {
				if (pCurrForeignKeyClause - pCurrConstraintClause > 11)
					pConstraintName = pCurrConstraintClause + 11;	// there are 11 characters in "constraint "
			}

			MysqlForeignKey* pKeyI = new MysqlForeignKey();
			char* pOffset = pCurrForeignKeyClause + 12;  // there are 12 characters in "foreign key "
			if (pConstraintName)
				pKeyI->setForeignKeyName( pConstraintName );// use constraint symbol as foreign key constraint name
			else {
				pKeyI->setForeignKeyName( pOffset );		// use index_name as foreign key constraint name
				pOffset = pOffset + pKeyI->getForeignKeyNameLength();	// advance after index_name
			}

			char* pColumnNames = strstr( pOffset, "(" );	// points to column name

			if (!pColumnNames) {
				errorNum = HA_ERR_CANNOT_ADD_FOREIGN;
				return errorNum;
			}
			pColumnNames++;

			// set key number, index column names, number of keyfields
			int keyNum = pKeyI->setKeyNumber( table_arg->key_info, numOfKeys, pColumnNames );

			if (keyNum == -1){
				errorNum = HA_ERR_CANNOT_ADD_FOREIGN;
				return errorNum;
			}

			char* pTableName = strstr( pOffset, "references ") + 11;  // points to parent table name
			pKeyI->setParentTableName( pTableName );
			pOffset = pTableName + pKeyI->getParentTableNameLength();
			pColumnNames = strstr( pOffset, "(" ) + 1;   // points to column name
			pKeyI->setParentColumnNames( pColumnNames );

			// test case 109.sql, Make sure the parent table is open.  
			char* pParentTableName = pKeyI->getParentTableName();
			unsigned short parentTableId  = SDBGetTableNumberByName( sdbUserId_, sdbDbId_, pParentTableName );
			if ( parentTableId == 0 ) 
				parentTableId = SDBOpenTable(sdbUserId_, sdbDbId_, pParentTableName); 

			if ( parentTableId == 0 ) 
				return HA_ERR_CANNOT_ADD_FOREIGN;

			// save the info about this foreign key for use when creating the associated designator
	    	if (keyNum >= 0){
                SDBArrayPutPtr(fkInfoArray, keyNum+1, pKeyI);
				//fkInfoArray->useLocation(keyNum+1);
				//fkInfoArray->putPtrInArray(keyNum+1, pKeyI);
			}

			unsigned short retValue = SDBDefineForeignKey(sdbUserId_, sdbDbId_, tblName, pParentTableName, pKeyI->getForeignKeyName(),
				pKeyI->getIndexColumnNames(), pKeyI->getParentColumnNames() );


			if ( retValue > 0 ) {
				errorNum = HA_ERR_CANNOT_ADD_FOREIGN;
				return errorNum;
			}

			// Need to add a non-unique secondary index here based on the foreign key constraint

			pCurrConstraintClause = strstr( pOffset, "constraint ");
			pCurrForeignKeyClause = strstr( pOffset, "foreign key ");
		}	// while ( pCurrForeignKeyClause != NULL )

	}	// if ( thd->query_length() > 0 )

	return errorNum;
}


// Find if a given field is participating in indexes (is a keypart), and if yes for how much size?
int ha_scaledb::get_field_key_participation_length(THD* thd, TABLE *table_arg, Field * pField) {
	unsigned int errorNum = 0;
	unsigned int primaryKeyNum = table_arg->s->primary_key;
	KEY_PART_INFO* pKeyPart;
	int numOfKeys = (int) table_arg->s->keys ;
	int maxParticipationLength = 0;

	for (int i=0; i < numOfKeys; ++i ) {
		KEY* pKey = table_arg->key_info + i;
		int numOfKeyFields = (int) pKey->key_parts;
		for ( int i2=0; i2 < numOfKeyFields; ++i2) {
			pKeyPart = pKey->key_part + i2;
			if (SDBUtilCompareStrings(pKeyPart->field->field_name, pField->field_name, true))
				if (maxParticipationLength < pKeyPart->length)
					maxParticipationLength = pKeyPart->length;
		}
	}
	return maxParticipationLength;
}


// add indexes to a table, part of create table
int ha_scaledb::add_indexes_to_table(THD* thd, TABLE *table_arg, char* tblName, unsigned short ddlFlag, 
									 SdbDynamicArray* fkInfoArray, char* pCreateTableStmt) {
	unsigned int errorNum = 0;
	unsigned int primaryKeyNum = table_arg->s->primary_key;
	KEY_PART_INFO* pKeyPart;
	int numOfKeys = (int) table_arg->s->keys ;
	char* pTableFsName = SDBGetTableFileSystemNameByTableNumber(sdbDbId_, sdbTableNumber_);

	for (int i=0; i < numOfKeys; ++i ) {
		KEY* pKey = table_arg->key_info + i;
		char* keyNameInLower = SDBUtilGetStrInLower(pKey->name);
		char* designatorName = SDBUtilFindDesignatorName(pTableFsName, keyNameInLower, i);

		// MySQL does not set value properly in pKey->algorithm for HASH index.
		// We need to parse USING HASH clause in a CREATE TABLE statement.  The syntax is:
		//col_name column_definition
		// | [CONSTRAINT [symbol]] PRIMARY KEY [index_type] (index_col_name,...) [index_option] ...
		// | {INDEX|KEY} [index_name] [index_type] (index_col_name,...) [index_option] ...
		// | [CONSTRAINT [symbol]] UNIQUE [INDEX|KEY] [index_name] [index_type] (index_col_name,...) [index_option] ...
		bool isHashIndex = false;

		if ( (primaryKeyNum == 0) && (i == 0) ) {  // primary key specified

			if ( strstr( pCreateTableStmt, "using hash") ) {
				char* pPrimaryKeyClause = strstr( pCreateTableStmt, "primary key ");
				if (pPrimaryKeyClause) {
					char* pUsingHash = strstr( pPrimaryKeyClause+12, "using hash");
					if (pUsingHash - pPrimaryKeyClause == 12)	// PRIMARY KEY immediately followed by USING HASH
						isHashIndex = true;
				}
			}

			KEY* pKey = table_arg->key_info + i;
			int numOfKeyFields = (int) pKey->key_parts;
			char** keyFields = (char **) GET_MEMORY((numOfKeyFields + 1) * sizeof(char *));
			unsigned short* keySizes = (unsigned short*) GET_MEMORY((numOfKeyFields + 1) * sizeof(unsigned short));

			for ( int i2=0; i2 < numOfKeyFields; ++i2) {
				pKeyPart = pKey->key_part + i2;
				keyFields[i2] = (char*) pKeyPart->field->field_name ;
				keySizes[i2] = (unsigned short)pKeyPart->length;
			}
			keyFields[numOfKeyFields] = NULL;  // must end with NULL to signal the end of pointer list

			MysqlForeignKey* fKeyInfo = (MysqlForeignKey*)SDBArrayGetPtr(fkInfoArray, i+1);

			char** parentKeyColumns = fKeyInfo ? fKeyInfo->getParentColumnNames() : NULL;
			char* parent = SDBGetParentIndexByForeignFields(sdbDbId_, tblName, keyFields, parentKeyColumns);

			// a foreign key and we can't find the parent designator
			if (fKeyInfo && !parent){
				errorNum = HA_ERR_CANNOT_ADD_FOREIGN;
				return errorNum;
			}

			unsigned short retValue = SDBCreateIndex(sdbUserId_, sdbDbId_, sdbTableNumber_, designatorName, 
				keyFields, keySizes, true, false, parent, ddlFlag, 0, isHashIndex);
			// Need to free keyFields, but not the field names used by MySQL.
			RELEASE_MEMORY( keyFields );
			RELEASE_MEMORY( keySizes );
			if (retValue == 0) {	// an error occurs
				SDBRollBack(sdbUserId_);
				SDBDeleteTableById(sdbUserId_, sdbDbId_, sdbTableNumber_);	// cleans up table name and field name
				errorNum = HA_ERR_NO_SUCH_TABLE;
				return errorNum;
			}

		} else {  // add secondary index (including the foreign key)

			if ( keyNameInLower && strstr( pCreateTableStmt, "using hash") ) {
				if (pKey->name) {	// For now, we assume a user needs to specify index_name if he wants USING HASH
					char* pIndexName = strstr( pCreateTableStmt, keyNameInLower);
					unsigned short indexNameLen = SDBUtilGetStrLength(keyNameInLower);
					char* pUsingHash = strstr( pIndexName+indexNameLen+1, "using hash");
					if (pUsingHash - pIndexName == indexNameLen+1)	// index_name immediately followed by USING HASH
						isHashIndex = true;
				}
			}

			int numOfKeyFields = (int) pKey->key_parts;
			bool isUniqueIndex = (pKey->flags & HA_NOSAME) ? true : false;
			char** keyFields = (char **) GET_MEMORY((numOfKeyFields + 1)* sizeof(char *));
			unsigned short* keySizes = (unsigned short*) GET_MEMORY((numOfKeyFields + 1) * sizeof(unsigned short));
			for (int i2=0; i2 < numOfKeyFields; ++i2) {
				pKeyPart = pKey->key_part + i2;
				keyFields[i2] = (char*) pKeyPart->field->field_name ;
				keySizes[i2] = (unsigned short)pKeyPart->length;
			}
			keyFields[numOfKeyFields] = NULL;  // must end with NULL to signal the end of pointer list

			MysqlForeignKey* fKeyInfo = (MysqlForeignKey*)SDBArrayGetPtr(fkInfoArray, i+1);

			char** parentKeyColumns = fKeyInfo ? fKeyInfo->getParentColumnNames() : NULL;
			char* parent = SDBGetParentIndexByForeignFields(sdbDbId_, tblName, keyFields, parentKeyColumns);

			// a foreign key and we can't find the parent designator
			if (fKeyInfo && !parent){
				errorNum = HA_ERR_CANNOT_ADD_FOREIGN;
				return errorNum;
			}

			unsigned short retValue = SDBCreateIndex(sdbUserId_, sdbDbId_, sdbTableNumber_, designatorName,  
				keyFields, keySizes, false, !isUniqueIndex, parent, ddlFlag, i, isHashIndex);
			// Need to free keyFields, but not the field names used by MySQL.
			RELEASE_MEMORY( keyFields );
			RELEASE_MEMORY( keySizes );
			if (retValue == 0) {	// an error occurs as index number should be > 0
				SDBRollBack(sdbUserId_);
				SDBDeleteTableById(sdbUserId_, sdbDbId_, sdbTableNumber_);	// cleans up table name and field name
				errorNum = HA_ERR_WRONG_INDEX;
				return errorNum;
			}
		}

		RELEASE_MEMORY( keyNameInLower );
		RELEASE_MEMORY( designatorName );
	}   // for (int i=0; i < numOfKeys; ++i )

	return errorNum;
}


// delete a user table. 
// This method is outside the pair of external_lock calls.  Hence we need to set sdbTableNumber_.
// Note that the name is file system compliant (or file system safe for table file names).
// We cannot use table->s->table_name.str because it is not defined.
int ha_scaledb::delete_table(const char* name)
{
	DBUG_ENTER("ha_scaledb::delete_table");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::delete_table(name=");
		SDBDebugPrintString( (char *)name );
		SDBDebugPrintString(") ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	char dbFsName[METAINFO_MAX_IDENTIFIER_SIZE] = {0};
	char tblFsName[METAINFO_MAX_IDENTIFIER_SIZE] = {0};
	char pathName[METAINFO_MAX_IDENTIFIER_SIZE] = {0};

	char* pTableName = NULL;
	int errorNum = 0;
	unsigned short retCode = 0;
	bool bIsAlterTableStmt = false;
	THD* thd = ha_thd();
	placeSdbMysqlTxnInfo( thd );	
#ifdef SDB_DEBUG
	SDBLogSqlStmt(sdbUserId_, thd->query(), thd->query_id);	// inform engine to log user query for DDL
#endif

	unsigned int sqlCommand = thd_sql_command(thd);
	if (sqlCommand==SQLCOM_ALTER_TABLE || sqlCommand==SQLCOM_CREATE_INDEX || sqlCommand==SQLCOM_DROP_INDEX) 
		bIsAlterTableStmt = true;

	// First we fetch db and table names
	fetchIdentifierName(name, dbFsName, tblFsName, pathName); 

	// If the ddl statement has the key word defined in SCALEDB_HINT_PASS_DDL,
	// then we need to update memory metadata only.  (NOT the metadata on disk).
	unsigned short ddlFlag = 0;
	if (SDBNodeIsCluster() == true) {
		if ( strstr(thd->query(), SCALEDB_HINT_PASS_DDL) != NULL ) 
			ddlFlag |= SDBFLAG_DDL_SECOND_NODE;
	}

	// Secondary node needs to delete memory metadata only.  (NOT the metadata on disk).
	if (ddlFlag & SDBFLAG_DDL_SECOND_NODE) {

		// At this moment, the table should have been closed.  However, a table may be left open
		// when we process its child table.  Hence, we need to close it if it is still open.
		if (sdbTableNumber_) {
			pTableName = SDBGetTableNameByNumber(sdbUserId_, sdbDbId_, sdbTableNumber_);
			SDBCloseTable(sdbUserId_, sdbDbId_, pTableName, false, false);
		}

		// Because we implement close(), secondary node should exit early on all the situations. 
		SDBCommit(sdbUserId_);
		pSdbMysqlTxn_->setActiveTrn( false );	// mark the end of long implicit transaction
#ifdef SDB_DEBUG_LIGHT
		if (mysqlInterfaceDebugLevel_ > 4) {
			// print user lock status on the non-primary node
			SDBDebugStart();			// synchronize threads printout	
			SDBDebugPrintHeader("In ha_scaledb::delete_table, print user locks imposed by non-primary node ");
			SDBDebugEnd();			// synchronize threads printout	
			SDBShowUserLockStatus(sdbUserId_);
		}
#endif
		DBUG_RETURN(errorNum);
	}	

	// Open user database if it is not open yet.
	retCode = openUserDatabase(dbFsName, dbFsName, true, bIsAlterTableStmt, ddlFlag);
	if (retCode) 
		DBUG_RETURN( convertToMysqlErrorCode(retCode) );

	// DROP TABLE: Primary node needs to lock the metadata of the master dbms before opening a user database
	// DROP TABLE: Secondary nodes do NOT need to lock the metadata any more.
	// ALTER TABLE and equivalent: primary node imposed lockMetaInfo in ::create
	if ( (!(ddlFlag & SDBFLAG_DDL_SECOND_NODE)) && ((sqlCommand==SQLCOM_DROP_TABLE) || (sqlCommand==SQLCOM_DROP_DB)) ) {
		retCode = SDBLockMetaInfo(sdbUserId_, sdbDbId_);
		if (retCode)
			DBUG_RETURN( convertToMysqlErrorCode(retCode) );
	}

	sdbTableNumber_ = SDBGetTableNumberByFileSystemName(sdbUserId_, sdbDbId_, tblFsName);

	// may simplify the logic in the remaining code because only primary node and single node will proceed 
	// after this point.

	if ( (sdbTableNumber_ == 0) && (!virtualTableFlag_) ) {

		// TODO: This is a kludge.  The right way is to make sure all parent foreign tables are open when we open a table.
		if (ddlFlag & SDBFLAG_DDL_SECOND_NODE) {
			retCode = initializeDbTableId(dbFsName, tblFsName, true, true);
			if (retCode == TABLE_NAME_UNDEFINED)	// the table file has not been opened yet on a secondary node.
				DBUG_RETURN( 0 );
		}
		else 
			retCode = initializeDbTableId(dbFsName, tblFsName, true, false);
	}

	// Do nothing on virtual view table since it is just a mapping, not a real table
	if (virtualTableFlag_) 
		DBUG_RETURN(errorNum);

	// If a table name has special characters such as $ or + enclosed in back-ticks,
	// then MySQL encodes the user table name by replacing the special characters with ASCII code.
	// In our metadata, we save the original user-defined table name and the file-system-compliant name.
	pTableName = SDBGetTableNameByNumber(sdbUserId_, sdbDbId_, sdbTableNumber_);
	retCode = SDBCanTableBeDropped(sdbUserId_, sdbDbId_, pTableName);
	if (retCode == SUCCESS) {

		// For DROP TABLE and DROP DATABASE, we need to impose the exclusive table level lock.
		// For ALTER TABLE, we already imposed the exclusive table level lock in rnd_init. 
		if ( (sqlCommand == SQLCOM_DROP_TABLE) || (sqlCommand == SQLCOM_DROP_DB) ) {
			if (SDBLockTable(sdbUserId_, sdbDbId_, sdbTableNumber_, REFERENCE_LOCK_EXCLUSIVE) == false) {
				SDBCommit(sdbUserId_);	// release lockMetaInfo
				DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
			}
		}

		// The primary node needs to propagate the DDL to other nodes.
		bool dropTableReady = true;
		if ( SDBNodeIsCluster() == true ) {
			char* passedDDL = NULL;
			if ( (sqlCommand == SQLCOM_DROP_TABLE) || (sqlCommand == SQLCOM_DROP_DB) ) {
				// We cannot use user's drop table statement because we can process only one DROP TABLE statement
				// a time.  MySQL allows multiple table names specified in a DROP TABLE statement.
				// For DROP DATABASE statement, MySQL calls this method to drop individual table first.
				// Hence we need to generate a DROP TABLE statement in this case.
				char* pTableCsName = SDBGetTableCaseSensitiveNameByNumber(sdbUserId_, sdbDbId_, sdbTableNumber_);

				// Bug 1066, always enclose table name with backtick so that we can handle special character. 
				// If the table name has a backtick, we need to double the backtick character
				char* pDropTableStmtTemp = NULL;
				if ( strstr(pTableCsName, "`") ) {
					pTableCsName = SDBUtilDoubleSpecialCharInString(pTableCsName, '`');
					pDropTableStmtTemp = SDBUtilAppendString("drop table `", pTableCsName);
					RELEASE_MEMORY(pTableCsName);
				} else {
					pDropTableStmtTemp = SDBUtilAppendString("drop table `", pTableCsName);
				}
				char* pDropTableStmt = SDBUtilAppendStringFreeFirstParam(pDropTableStmtTemp, "` ");
				dropTableReady = sendStmtToOtherNodes(sdbDbId_, pDropTableStmt, false, false);
				RELEASE_MEMORY(pDropTableStmt);
			}
		}

		// pass the DDL statement to engine so that it can be propagated to other nodes
		// The user statement may be DROP TABLE, ALTER TABLE or CREATE/DROP INDEX,
		if (dropTableReady == true) {
			// remove the table name from lock table vector if it exists
			pSdbMysqlTxn_->removeLockTableName(pTableName );

			// single node solution: need to drop table and its in-memory metadata.
			// cluster solution: primary node needs to drop table and its in-memory metadata.  
			// cluster solution: secondary node: MySQL already closed the to-be-dropped table and deleted MySQL metadata for the table.
			retCode = SDBDeleteTable(sdbUserId_, sdbDbId_, pTableName, ddlFlag);
		}
		else		
			retCode = METAINFO_UNDEFINED_DATA_TABLE;
	}


	// For ALTER TABLE in cluster, this is the last MySQL interface method call.
	// The primary node now replicates DDL to other nodes.
	if ( bIsAlterTableStmt && (SDBNodeIsCluster() == true) && ( strstr(tblFsName, MYSQL_TEMP_TABLE_PREFIX2) ) ) {
		// if the table to be dropped is the second temp table, then we proceed to replicate to other nodes.
		// if not, then we are rolling back an ALTER TABLE statement by removing the first temp table; 
		// hence there is no need to send ALTER TABLE statements to other nodes
		bool bAlterTableReady = sendStmtToOtherNodes(sdbDbId_, thd->query(), false, false);
		if (!bAlterTableReady)
			retCode = ALTER_TABLE_FAILED_IN_CLUSTER;
	}

	// For both DROP TABLE and ALTER TABLE, primary node needs to release lockMetaInfo and table level lock
	SDBCommit(sdbUserId_);
	pSdbMysqlTxn_->setDdlFlag(0);	// Bug1039: reset the flag as a subsequent statement may check this flag
	pSdbMysqlTxn_->setActiveTrn( false );	// mark the end of long implicit transaction

	errorNum = convertToMysqlErrorCode( retCode );

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 1) {
		if (retCode == SUCCESS)
			SDBPrintStructure(sdbDbId_);		// print metadata structure
	}
#endif

#ifdef SDB_DEBUG
	if (mysqlInterfaceDebugLevel_ > 4) {
		// print user lock status on the primary node
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("In ha_scaledb::delete_table, print user locks imposed by primary node ");
		SDBDebugEnd();			// synchronize threads printout	
		SDBShowUserLockStatus(sdbUserId_);
	}
#endif

	DBUG_RETURN(errorNum);
}


// rename a user table.
// This method is outside the pair of external_lock calls.  Hence we need to set sdbTableNumber_.
// we need to rename all the file name as file name depends on table name
// Note that the name is file system compliant (or file system safe for table file names).
// We cannot use table->s->table_name.str because it is not defined.
int ha_scaledb::rename_table(const char* fromTable, const char* toTable) {
	DBUG_ENTER("ha_scaledb::rename_table");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::rename_table(fromTable=");
		SDBDebugPrintString( (char *)fromTable );
		SDBDebugPrintString(", toTable=");
		SDBDebugPrintString( (char *)toTable );
		SDBDebugPrintString(") ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	char pathName[METAINFO_MAX_IDENTIFIER_SIZE] = {0};
	char dbName[METAINFO_MAX_IDENTIFIER_SIZE] = {0};
	char fromDbName[METAINFO_MAX_IDENTIFIER_SIZE] = {0};
	char fromTblFsName[METAINFO_MAX_IDENTIFIER_SIZE] = {0};
	char toTblFsName[METAINFO_MAX_IDENTIFIER_SIZE] = {0};
	char* userFromTblName = NULL;

	// First we fetch db and table names
	fetchIdentifierName(fromTable, fromDbName, fromTblFsName, pathName); 
	fetchIdentifierName(toTable, dbName, toTblFsName, pathName);
	if (!SDBUtilCompareStrings(dbName, fromDbName, true) ){
		DBUG_RETURN(HA_ERR_UNSUPPORTED);
	}

	unsigned int errorNum = 0;
	unsigned short retCode = 0;
	bool bIsAlterTableStmt = false;
	THD* thd = ha_thd();
	placeSdbMysqlTxnInfo( thd );	
#ifdef SDB_DEBUG
	SDBLogSqlStmt(sdbUserId_, thd->query(), thd->query_id);	// inform engine to log user query for DDL
#endif

	unsigned int sqlCommand = thd_sql_command(thd);
	if (sqlCommand==SQLCOM_ALTER_TABLE || sqlCommand==SQLCOM_CREATE_INDEX || sqlCommand==SQLCOM_DROP_INDEX) 
		bIsAlterTableStmt = true;

	// For statement "ALTER TABLE t1 RENAME [TO] t2", need to set sqlCommand to SQLCOM_RENAME_TABLE.
	// If this is a true ALTER TABLE statement, then the flag SDBFLAG_ALTER_TABLE_CREATE must be turned on.
	// Also there is a temp table in the arguments.  If both condition fails, then it is actually a RENAME TABLE statement.
	if (sqlCommand==SQLCOM_ALTER_TABLE) {
		bool bExistsTempTable = false;
		if ( strstr(fromTblFsName, MYSQL_TEMP_TABLE_PREFIX) || strstr(toTblFsName, MYSQL_TEMP_TABLE_PREFIX) )
			bExistsTempTable = true;

		if ( (!bExistsTempTable) && (!(pSdbMysqlTxn_->getDdlFlag() & SDBFLAG_ALTER_TABLE_CREATE)) ) {	
			bIsAlterTableStmt = false;
			sqlCommand = SQLCOM_RENAME_TABLE;
		}
	}


	// If the ddl statement has the key word defined in SCALEDB_HINT_PASS_DDL,
	// then we need to update memory metadata only.  (NOT the metadata on disk).
	unsigned short ddlFlag = 0;
	if (SDBNodeIsCluster() == true) {
		if ( strstr(thd->query(), SCALEDB_HINT_PASS_DDL) != NULL ) 
			ddlFlag |= SDBFLAG_DDL_SECOND_NODE;
	}

	// If the ddl statement has the key word SCALEDB_HINT_PASS_DDL, then this node
	// is a non-primary node in cluster systems. We do nothing in the 2nd call for ALTER TABLE stmt.
	// Secondary node does nothing on all cases. 
	if (ddlFlag & SDBFLAG_DDL_SECOND_NODE) {

		// At this moment, the table should have been closed.  However, a table may be left open
		// when we process its child table.  Hence, we need to close it if it is still open.
		if (sdbTableNumber_) {
			userFromTblName = SDBGetTableNameByNumber(sdbUserId_, sdbDbId_, sdbTableNumber_);
			SDBCloseTable(sdbUserId_, sdbDbId_, userFromTblName, false, false);
		}

		// Because we implement close(), secondary node should exit early on all the situations. 
		SDBCommit(sdbUserId_);
		DBUG_RETURN(errorNum);
	}

	// Open user database if it is not open yet.
	retCode = openUserDatabase(fromDbName, fromDbName, true, bIsAlterTableStmt, ddlFlag);
	if (retCode) 
		DBUG_RETURN( convertToMysqlErrorCode(retCode) );

	// RENAME TABLE: Primary node needs to lock the metadata of the master dbms before opening a user database
	// RENAME TABLE: Secondary nodes do NOT need to lock the metadata any more.
	// ALTER TABLE: primary node already imposed lockMetaInfo in ::create
	if ( (!(ddlFlag & SDBFLAG_DDL_SECOND_NODE)) && (sqlCommand==SQLCOM_RENAME_TABLE) ) {
		retCode = SDBLockMetaInfo(sdbUserId_, sdbDbId_);
		if (retCode)
			DBUG_RETURN( convertToMysqlErrorCode(retCode) );
	}

	sdbTableNumber_ = SDBGetTableNumberByFileSystemName(sdbUserId_, sdbDbId_, fromTblFsName);

	// TODO: We can simplify the logic in the remaining code because only primary node and single node will
	// proceed the remaining code.

	if ( (sdbTableNumber_ == 0) && (!virtualTableFlag_) ) 
		errorNum = initializeDbTableId(fromDbName, fromTblFsName, true);

	// If a table name has special characters such as $ or + enclosed in back-ticks,
	// then MySQL encodes the user table name by replacing the special characters with ASCII code.
	// In our metadata, we save the original user-defined table name.
	userFromTblName = SDBGetTableNameByNumber(sdbUserId_, sdbDbId_, sdbTableNumber_);
	char* userToTblName = SDBUtilDecodeCharsInStrings(toTblFsName);	// change name if chars are encoded ????
	// This is a kludge as we cannot find the new table name unless we parse the SQL statement.  

	// The primary node needs to pass the DDL statement to engine so that it can be propagated to other nodes
	// We need to make sure it is a RENAME TABLE command because this method is also called
	// when a user has ALTER TABLE or CREATE/DROP INDEX commands.
	// We also need to make sure that the scaledb hint is not found in the user query.
	bool renameTableReady = true;

	if ( renameTableReady ) {

		// For RENAME TABLE, we need to impose the exclusive table level lock.
		// For ALTER TABLE, we already imposed the exclusive table level lock in rnd_init. 
		if ( sqlCommand == SQLCOM_RENAME_TABLE ) {
			if (SDBLockTable(sdbUserId_, sdbDbId_, sdbTableNumber_, REFERENCE_LOCK_EXCLUSIVE) == false) {
				SDBCommit(sdbUserId_);	// release lockMetaInfo
				DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
			}
		}

		// TODO: To be enabled later for fast ALTER TABLE statement.
		// For ALTER TABLE statement, we need to enable indexes on the scratch table before renaming it to the final table
		//if (sqlCommand == SQLCOM_ALTER_TABLE) {
		//	char* pScratchTableName = pSdbMysqlTxn_->getScratchTableName();
		//	if ( SDBGetUtilCompareStrings(fromTable, pScratchTableName, true) ) 
		//		pSdbEngine->enableTableIndexes(sdbDbId, userFromTblName);
		//}

		retCode = SDBRenameTable(sdbUserId_, sdbDbId_, userFromTblName, fromTblFsName, 
			userToTblName, toTblFsName, userToTblName, true, ddlFlag);
	}
	else retCode = METAINFO_UNDEFINED_DATA_TABLE;

	// RENAME TABLE: primary node needs to release lock on MetaInfo
	// ALTER TABLE: primary node releases lockMetaInfo in ::delete_table
	if ( (!(ddlFlag & SDBFLAG_DDL_SECOND_NODE))  && (sqlCommand==SQLCOM_RENAME_TABLE) ) {
		if ( (SDBNodeIsCluster() == true) && (retCode == SUCCESS) ) {
			renameTableReady = sendStmtToOtherNodes(sdbDbId_, thd->query(), false, false);
			if (!renameTableReady)
				retCode = METAINFO_UNDEFINED_DATA_TABLE;
		}

		SDBCommit(sdbUserId_);
	}

	// On single node, if the to-be-altered table is not a scaledb table, we need to commit here to release all locks
	if ( (sqlCommand==SQLCOM_ALTER_TABLE) && (SDBNodeIsCluster() == false) && (retCode == SUCCESS) 
		&& (pSdbMysqlTxn_->getAlterTableName()==NULL) )
		SDBCommit(sdbUserId_);


	errorNum = convertToMysqlErrorCode( retCode );

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 1) {
		SDBPrintStructure(sdbDbId_);		// print metadata structure
	}

	if (mysqlInterfaceDebugLevel_ > 4) {
		// print user lock status on the primary node
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("In ha_scaledb::rename_table, print user locks imposed by primary node ");
		SDBDebugEnd();			// synchronize threads printout	
		SDBShowUserLockStatus(sdbUserId_);
	}
#endif
	DBUG_RETURN(errorNum);
}


// -------------------------------------------------------
// this function should only be called if SDBNodeIsCluster() == true
// we do not check that again inside the function avoid duplicate check
// 
// this function will issue the DDL statement to all other nodes on the cluster
//
// Pass DDL SQL statement and its related DbId
// When bIgnoreDB is set to false, we should pass databasename when we set up a mysql connection.
// When bIgnoreDB is set to true, then we pass NULL for db name in setting up a mysql connection. 
// return true for success and false for failure
// -------------------------------------------------------
bool ha_scaledb::sqlStmt(unsigned short dbmsId, char* sqlStmt, bool bIgnoreDB, bool bEngineOption) {

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::sqlStmt(...) ");
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	unsigned int  nodesArray[METAINFO_SLM_MAX_NODES];
	unsigned int  portsArray[METAINFO_SLM_MAX_NODES];
	unsigned char numberOfNodes = SDBSetupClusterNodes(nodesArray, portsArray);

	if (!numberOfNodes){
		return true;
	}

	bool overallRc = true;
	int queryLength = SDBUtilGetStrLength(sqlStmt);
	char* pDbName = NULL;
	if (bIgnoreDB == false) 
		pDbName = SDBGetDatabaseNameByNumber(dbmsId);

	// loop through all non-primary nodes and execute the sqlStmt
	for (unsigned char i = 0; i < numberOfNodes; ++i){

		char ip[64];
        SDBUtilGetIpFromInet(&nodesArray[i], ip, sizeof(ip));
		int port = portsArray[i];
		char* password = SDBGetClusterPassword();
		char* user = SDBGetClusterUser();
		char* socket = NULL;

		// set up MySQL client connection
     	SdbMysqlClient* pSdbMysqlClient = new SdbMysqlClient(ip, user, password, pDbName, socket, port, mysqlInterfaceDebugLevel_);

		// Execute the statement on MySQL server on a non-primary node
		int rc = 0;
		rc = pSdbMysqlClient->executeQuery(sqlStmt, queryLength, bEngineOption);
		if (rc && overallRc) { // there was an error code, 0 is success, so we set the overall function to failure
#ifdef SDB_DEBUG_LIGHT
			if (mysqlInterfaceDebugLevel_) {
				SDBDebugStart();			// synchronize threads printout	
				SDBDebugPrintHeader("Replicate DDL (or the like) to ip=");
				SDBDebugPrintString(ip);
				SDBDebugPrintString("; Statement=");
				SDBDebugPrintString(sqlStmt);
				SDBDebugPrintHeader("MySQL C API mysql_real_query return code = ");
				SDBDebugPrintInt(rc);
				SDBDebugEnd();			// synchronize threads printout	
			}
#endif
			overallRc = false;
		}

		delete pSdbMysqlClient;		// remove the client connection
    }

	if (!overallRc){
		SDBTerminate(0, "cluster ddl failed");
	}

	return overallRc;
}


// tells if data copy to new table is needed during an alter
bool ha_scaledb::check_if_incompatible_data(HA_CREATE_INFO*	info, uint table_changes)
{
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::check_if_incompatible_data(...) ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	if (table_changes != IS_EQUAL_YES) {
		return COMPATIBLE_DATA_NO;
	}

	/* Check that auto_increment value was not changed */
	if ((info->used_fields & HA_CREATE_USED_AUTO) && info->auto_increment_value != 0) {
		return COMPATIBLE_DATA_NO;
	}

	/* Check that row format didn't change */
	if ((info->used_fields & HA_CREATE_USED_ROW_FORMAT) && get_row_type() != info->row_type) {
		return COMPATIBLE_DATA_NO;
	}
	return COMPATIBLE_DATA_YES;
}

// analyze the table
int ha_scaledb::analyze(THD* thd, HA_CHECK_OPT*	check_opt)
{
	DBUG_ENTER("ha_scaledb::analyze");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::analyze(...) ");
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif
	int errorNum = info(HA_STATUS_TIME | HA_STATUS_CONST | HA_STATUS_VARIABLE);
	DBUG_RETURN(errorNum);
}

// initialize the index
int ha_scaledb::index_init(uint	keynr, bool sorted)
{
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_init(...)");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

#if 0
	// check if there is already a query manager, if not create one
	KEY* pKey = table->key_info + keynr;
	char* pTableFsName = pMetaInfo_->getTableFileSystemNameByTableNumber(sdbTableNumber_);
	char* designatorName = SDBUtilFindDesignatorName(pTableFsName, pKey->name, keynr);
	virtualTableFlag_ = SDBGetUtilCompareStrings(table->s->table_name.str, SDB_VIRTUAL_VIEW, true, SDB_VIRTUAL_VIEW_PREFIX_BYTES);

	unsigned short queryMgrId1 = pSdbMysqlTxn_->findQueryManagerId(designatorName, (void*) this, (char*) NULL, 0);
	unsigned short queryMgrId2 = pSdbMysqlTxn_->findQueryManagerId(designatorName, (void*) this, (char*) NULL, 0);

	if ( queryMgrId1 && !queryMgrId2 ) {  
		// need to get a new one and save sdbQueryMgrId_ into MysqlTxn object
		sdbQueryMgrId_ = pSdbEngine->getQueryManager(sdbUserId_);
		pSdbMysqlTxn_->addQueryManagerId( this->pMetaInfo_, true, designatorName, (void*) this, (char*) NULL, 0, sdbQueryMgrId_); 
	}
	else {
		sdbQueryMgrId_ = queryMgrId2;
	}
	RELEASE_MEMORY( designatorName );
#endif

	sdbQueryMgrId_ = 0;
	//pQm_ = NULL;
	//SDBQueryCursorReset();
	active_index = keynr;
	return 0;
}

// end the index
int ha_scaledb::index_end(void)
{
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_end()");
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString( table->s->table_name.str );
		SDBDebugPrintString(", alias: ");
		SDBDebugPrintString( table->alias );
		outputHandleAndThd();
		SDBDebugPrintString(", Query Manager ID #");
		SDBDebugPrintInt(sdbQueryMgrId_);
		SDBDebugPrintString(" counter = ");
		SDBDebugPrintInt(readDebugCounter_);
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	active_index = MAX_KEY;
	return 0;
}

//
int ha_scaledb::index_read_last(uchar * buf, const uchar * key, uint key_len)
{
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_read_last(...) ");
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	return index_read(buf, key, key_len, HA_READ_PREFIX_LAST);
}


// return last key which actually resulted in an error (duplicate error)
unsigned short ha_scaledb::get_last_index_error_key()
{
	print_header_thread_info("MySQL Interface: executing ha_scaledb::get_last_index_error_key() ");

	unsigned short last_designator = SDBGetLastIndexError(sdbUserId_);
	unsigned short last_errkey = SDBGetLastIndexPositionInTable(sdbDbId_, last_designator);

	if (last_errkey == MAX_KEY)
	{
		int key= active_index != MAX_KEY ? active_index : 0;
		last_errkey = table->key_info ? ((KEY *)(table->key_info + key))->key_part->fieldnr - 1 : 0;
	}

	Field* pAutoIncrField = table->found_next_number_field;
	if (pAutoIncrField == table->field[last_errkey]){
		short mySqlFieldType = table->field[last_errkey]->type();
		short length = 1;
		switch (mySqlFieldType) {
			case MYSQL_TYPE_TINY:
				length = 1;
				break;
			case MYSQL_TYPE_SHORT:
				length = 2;
				break;
			case MYSQL_TYPE_INT24:	
				length = 3;
				break;
			case MYSQL_TYPE_LONG:
				length = 4;
				break;
			case MYSQL_TYPE_LONGLONG:
				length = 8;
				break;
			default:
				break;
		}

		unsigned short tableId = SDBGetTableNumberByName(sdbUserId_, sdbDbId_, table->s->table_name.str);
		char* dupeValue = SDBGetFileDataField(sdbUserId_, tableId, last_errkey+1);
		memcpy(table->field[last_errkey]->ptr, dupeValue, length);
	}
	return last_errkey;
}


//extra information passed by the handler
int ha_scaledb::extra( enum ha_extra_function operation )
{
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::extra(...) ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	switch(operation) {
		case HA_EXTRA_IGNORE_DUP_KEY:
			extraChecks_ |= SDB_EXTRA_DUP_IGNORE;
			break;
		case HA_EXTRA_WRITE_CAN_REPLACE:
			extraChecks_ |= SDB_EXTRA_DUP_REPLACE;
			break;
		case HA_EXTRA_WRITE_CANNOT_REPLACE:
			extraChecks_ &= ~SDB_EXTRA_DUP_REPLACE;
			break;
		case HA_EXTRA_NO_IGNORE_DUP_KEY:
			extraChecks_ &= ~(SDB_EXTRA_DUP_IGNORE | SDB_EXTRA_DUP_REPLACE);
	}
	return(0);
}

bool ha_scaledb::get_error_message(int error, String *buf){

	// this function is expensive (lots of string ops) but it is only for error message condition

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::get_error_message(...) ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	char detailed_error[1024] = {0};
	char keys[1024] = {0};
	char parentKeys[1024] = {0};

	unsigned short last_designator = SDBGetLastIndexError(sdbUserId_);

	if (!last_designator || !table || !table->key_info){
		return false;
	}

	unsigned short keyId = SDBGetIndexExternalId(sdbDbId_, last_designator);
	KEY* key = table->key_info + keyId;

	char* keyname = key->name;
	unsigned int parentDesignator = SDBGetParentIndex(sdbDbId_, last_designator);

	if (!parentDesignator) {
		return false;
	}

	char* foreignTableName = SDBGetTableNameByIndex(sdbDbId_, parentDesignator);
	if (!foreignTableName){
		foreignTableName = "";
	}

	for (unsigned int i = 0; i < key->key_parts; ++i){
		KEY_PART_INFO* kp = &key->key_part[i];
		strcat(keys, "`");
		strcat(keys, kp->field->field_name);
		strcat(keys, "`");

		if (i + 1 < key->key_parts){
			strcat(keys, ", ");
		}
	}

	unsigned short parentKeyFields = SDBGetNumberOfKeyFields(sdbDbId_, parentDesignator);
	for (unsigned int i = 0; i < parentKeyFields; ++i){
		strcat(parentKeys, "`");
		strcat(parentKeys, SDBGetKeyFieldNameInIndex(sdbDbId_, parentDesignator, i+1));
		strcat(parentKeys, "`");

		if (i + 1 < key->key_parts){
			strcat(parentKeys, ", ");
		}
	}

	sprintf(detailed_error, "`%s`.`%s`, CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s` (%s)", 
		table->s->db.str, table->s->table_name.str, keyname, keys, foreignTableName, parentKeys);

	buf->copy(detailed_error, strlen(detailed_error), &my_charset_latin1);
	return false;
}

// returns total records in the current table
// this is needed for select count(*) without full table scan
ha_rows ha_scaledb::records() {

	DBUG_ENTER("ha_scaledb::records");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::records() ");
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString( table->s->table_name.str );
		SDBDebugPrintString(", alias: ");
		SDBDebugPrintString( table->alias );
		outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

    if (pSdbMysqlTxn_->getScaledbDbId() == 0) {
        // not yet initialized .. This method may be called directly
        pSdbMysqlTxn_->setScaledbDbId(sdbDbId_);
    }

	int64 totalRows = 0;
	// If a seoncdary node is running ALTER TABLE, then we turn on ddlFlag, and return totalRows 0.
	if (SDBNodeIsCluster() == true) {
		if ( pSdbMysqlTxn_->getDdlFlag() & SDBFLAG_DDL_SECOND_NODE )
			DBUG_RETURN( (ha_rows)totalRows );
	}

	prepareIndexOrSequentialQueryManager();
	totalRows = SDBCountRef(sdbUserId_, sdbQueryMgrId_, sdbDbId_, sdbTableNumber_, ((THD*)ha_thd())->query_id );

	if (totalRows < 0) {
		// error condition
		totalRows = HA_POS_ERROR;
	}

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 3) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("total count from table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(": ");
		SDBDebugPrint8ByteUnsignedLong((uint64)totalRows);
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	DBUG_RETURN( (ha_rows)totalRows );
}


//  Return actual upper bound of number of records in the table.
//  TODO: venu; double check with Ron on how to estimate rows from index here
ha_rows ha_scaledb::estimate_rows_upper_bound() { 
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::estimate_rows_upper_bound() ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	return HA_POS_ERROR; 
}

// Return time for a scan of the table
double ha_scaledb::scan_time() {

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::scan_time() ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	unsigned long long seekLength = 1000;
	if (virtualTableFlag_)		// return a large number for virtual table scan because it should use index
		return (double) seekLength;

	// TODO: this should be stored in file handle; auto updated during dictionary change.
	// Compute disk seek length for normal tables.
	seekLength = SDBGetTableStats(sdbDbId_, sdbTableNumber_, SDB_STATS_INFO_SEEK_LENGTH);

	if (seekLength == 0)
		seekLength = 1;

	return (double) seekLength;
}

// Return read time of set of range of rows.
// TODO: The current version will make query processor choose index if an index exists. 
double ha_scaledb::read_time(uint inx, uint ranges, ha_rows rows) {

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::read_time(...) ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	double rowsFactor = 0;

#ifdef ENABLE_RANGE_COUNT
	// TODO: Ron and Venu need to coordinate on this.  include rows count to tell which index is faster.
	if ( stats.records > 0 )
		rowsFactor = ((double) rows) / ((double) stats.records);
#endif

	return ( ((double) ranges) + rowsFactor) ;
}


// TODO: need to comment out these 2 methods as it does not work well with foreign key constraint
//-- only myisam supports this.  Other storage engines do NOT support it.
// Do NOT remove the code yet as we may enable them in the future.
///*
//Disable indexes, making it persistent if requested.
//*/
//int ha_scaledb::disable_indexes(uint mode)
//{
//#ifdef SDB_DEBUG_LIGHT
//	if (mysqlInterfaceDebugLevel_) {
//		SDBDebugStart();			// synchronize threads printout	
//		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::disable_indexes(...) ");
//		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
//		SDBDebugEnd();			// synchronize threads printout	
//	}
//#endif
//
//	int errorNum = HA_ERR_WRONG_COMMAND;
//
//	// This is not a normal ALTER TABLE statement.  It is DISABLE KEYS
//	pSdbMysqlTxn_->setOrOpDdlFlag( (unsigned short)SDBFLAG_ALTER_TABLE_KEYS );
//
//	if (mode == HA_KEY_SWITCH_ALL || mode == HA_KEY_SWITCH_NONUNIQ_SAVE) {
//
//		// impose exclusive table level lock for ALTER TABLE DISABLE KEYS statement.
//		// MySQL will later issue commit command to release the lock.
//		if (SDBLockTable(sdbUserId_, sdbDbId_, sdbTableNumber_, REFERENCE_LOCK_EXCLUSIVE) == true)
//			errorNum = SDBDisableTableIndexes(sdbUserId_, sdbDbId_, table->s->table_name.str);
//		else
//			errorNum = HA_ERR_LOCK_WAIT_TIMEOUT;
//	}
//
//	return errorNum;
//}
//
//
///*
//Enable indexes, making it persistent if requested.
//*/
//int ha_scaledb::enable_indexes(uint mode)
//{
//#ifdef SDB_DEBUG_LIGHT
//	if (mysqlInterfaceDebugLevel_) {
//		SDBDebugStart();			// synchronize threads printout	
//		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::enable_indexes(...) ");
//		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
//		SDBDebugEnd();			// synchronize threads printout	
//	}
//#endif
//
//	int errorNum = HA_ERR_WRONG_COMMAND;
//
//	// This is not a normal ALTER TABLE statement.  It is ENABLE KEYS
//	pSdbMysqlTxn_->setOrOpDdlFlag( (unsigned short)SDBFLAG_ALTER_TABLE_KEYS );
//
//	// impose exclusive table level lock for ALTER TABLE ENABLE KEYS statement.
//	// MySQL will later issue commit command to release the lock.
//	if (SDBLockTable(sdbUserId_, sdbDbId_, sdbTableNumber_, REFERENCE_LOCK_EXCLUSIVE) == true)
//		errorNum = SDBEnableTableIndexes(sdbUserId_, sdbDbId_, table->s->table_name.str);
//	else
//		errorNum = HA_ERR_LOCK_WAIT_TIMEOUT;
//
//	return errorNum;
//}


// returns estimated records in range
ha_rows ha_scaledb::records_in_range(uint inx, key_range* min_key, key_range* max_key) {
	DBUG_ENTER("ha_scaledb::records_in_range");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::records_in_range(...) ");
		if (mysqlInterfaceDebugLevel_>1) outputHandleAndThd();
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

	ha_rows rangeRows = 1;

	// give low estimate for virtual view since we have to use its primary key
	if (virtualTableFlag_)
		DBUG_RETURN( rangeRows );

	if (!min_key && !max_key) {
		// return all rows count.   info method sets value stats.records.
		// And info method is usually called before records_in_range. 
		DBUG_RETURN( stats.records );
	}

	if (min_key && max_key) 
		bRangeQuery_ = true;	// set the flag when the range conditions are fully specified.

	// TODO: The following code is disabled until we have a new design in estimating record statistics using histogram
#ifdef ENABLE_RANGE_COUNT
	Field* pAutoIncrField = table->found_next_number_field;	// points to auto_increment field
	KEY* pKey = table->key_info + inx;
	unsigned int numOfKeyFields = pKey->key_parts;
	KEY_PART_INFO* pKeyPart = pKey->key_part;
	Field* pField = pKeyPart->field;

	key_range *minMaxKey = min_key ? min_key : max_key;		// because one of them may be NULL
	const unsigned char* keyOffset = minMaxKey->key;
	unsigned int keyLength = minMaxKey->length;
	unsigned int offsetLength = 0;
	unsigned int realVarLength = 0;
	unsigned int mysqlVarLengthBytes = 0;
	bool keyValueIsNull = false;

	if ( (keyOffset != NULL) && (keyLength >= 0) ) {
		//check if the key is NULL, first byte tells if it is NULL
		if (!(pField->flags & NOT_NULL_FLAG)) {
			offsetLength = 1;

			if (*keyOffset != 0) {
				keyValueIsNull = true;
			}
		}
	}
	char* pMinFieldValue = (!min_key || keyValueIsNull) ? NULL :(char*)min_key->key + offsetLength;
	char* pMaxFieldValue = (!max_key || keyValueIsNull) ? NULL :(char*)max_key->key + offsetLength;

	if ( (pAutoIncrField==pField) && (numOfKeyFields == 1) ) {

		// We estimate the number of records in the auto_increment index range
		int64 diffValue = 0;
		int64 minValue = 0;
		int64 maxValue = 0;

		switch ( pField->type() ){
			case MYSQL_TYPE_SHORT:
				if (pMinFieldValue )
					minValue = (int64) ( *((short*) pMinFieldValue) );
				if (pMaxFieldValue )
					maxValue = (int64) ( *((short*) pMaxFieldValue) );
				break;

			case MYSQL_TYPE_INT24:
#ifdef SDB_BIG_ENDIAN
				if (pMinFieldValue )
					minValue = (((int64) *(pMinFieldValue)) << 16)
					+ (((int64) *(pMinFieldValue+1)) << 8)
					+  ((int64) *(pMinFieldValue+2)) ;
				if (pMaxFieldValue )
					maxValue = (((int64) *(pMaxFieldValue)) << 16)
					+ (((int64) *(pMaxFieldValue+1)) << 8)
					+  ((int64) *(pMaxFieldValue+2)) ;
#else
				if (pMinFieldValue )
					minValue = (((int64) *(pMinFieldValue+2)) << 16)
					+ (((int64) *(pMinFieldValue+1)) <<  8)
					+  ((int64) *(pMinFieldValue)) ;
				if (pMaxFieldValue )
					maxValue = (((int64) *(pMaxFieldValue+2)) << 16)
					+ (((int64) *(pMaxFieldValue+1)) <<  8)
					+  ((int64) *(pMaxFieldValue)) ;
#endif
				break;

			case MYSQL_TYPE_LONG:
				if (pMinFieldValue )
					minValue = (int64) ( *((int*) pMinFieldValue) );
				if (pMaxFieldValue )
					maxValue = (int64) ( *((int*) pMaxFieldValue) );
				break;

			case MYSQL_TYPE_LONGLONG:
				if (pMinFieldValue )
					minValue = *((int64*) pMinFieldValue);
				if (pMaxFieldValue )
					maxValue = *((int64*) pMaxFieldValue);
				break;

			case MYSQL_TYPE_TINY:
				if (pMinFieldValue )
					minValue = (int64) (*pMinFieldValue);
				if (pMaxFieldValue )
					maxValue = (int64) (*pMaxFieldValue);
				break;

			default:
				break;
		}

		if ((min_key == NULL) && (max_key) ) 
			diffValue = maxValue;
		else if ( (min_key) && (max_key == NULL) )
			diffValue = stats.records -  minValue;
		else
			diffValue = maxValue - minValue;

		if (diffValue > (int64) stats.records)
			diffValue = stats.records;


		// adjust number of records by a factor if the index is an auto_increment column
		unsigned int sdbAutoIncScanFactor = SDB_AUTOINC_SCAN_FACTOR;
		if ( stats.mean_rec_length > 0 ) {
			sdbAutoIncScanFactor = METAINFO_BLOCK_SIZE / stats.mean_rec_length;

			if (sdbAutoIncScanFactor == 0)
				++sdbAutoIncScanFactor;
		}

		rangeRows = (uint64) (diffValue / sdbAutoIncScanFactor);
		if (diffValue % sdbAutoIncScanFactor > 0)
			++rangeRows;
	}
	else {	// for non-auto-increment index column

		sdbQueryMgrId_ = 0;
		prepareIndexQueryManager(inx, NULL, 0);	// also resets query

		int retValue = SUCCESS;
		bool bVarStringField = false;

		switch ( pField->type() ) {

			case MYSQL_TYPE_VARCHAR:	
			case MYSQL_TYPE_VAR_STRING:
				realVarLength = (unsigned int) *( (unsigned short*)(keyOffset+offsetLength));
				mysqlVarLengthBytes = 2;
				bVarStringField = true;
				break;

			case MYSQL_TYPE_TINY_BLOB:  
			case MYSQL_TYPE_BLOB:
			case MYSQL_TYPE_MEDIUM_BLOB:  
			case MYSQL_TYPE_LONG_BLOB: 
				realVarLength = (unsigned int) *( (unsigned short*)(keyOffset+offsetLength));
				mysqlVarLengthBytes = pKeyPart->store_length - pKeyPart->length;
				bVarStringField = true;
				break;

			default:
				break;
		}	// switch

		if ( pMaxFieldValue )
			pMaxFieldValue = pMaxFieldValue + mysqlVarLengthBytes;
		if ( bVarStringField ) 
			retValue = pQm_->defineQuery(sdbDbId_, sdbDesignatorId_,  
			(char*) pField->field_name, pMaxFieldValue, false, realVarLength, false);
		else 
			retValue = pQm_->defineQuery(sdbDbId_, sdbDesignatorId_, 
			(char*) pField->field_name, pMaxFieldValue);
		if (retValue == SUCCESS)  
			retValue = pQm_->prepareEstimateCount(0, false);  
		if (retValue != SUCCESS)
			DBUG_RETURN( rangeRows );

		if ( pMinFieldValue )
			pMinFieldValue = pMinFieldValue + mysqlVarLengthBytes;
		if ( bVarStringField ) 
			retValue = pQm_->defineQuery(sdbDbId_, sdbDesignatorId_,  
			(char*) pField->field_name, pMinFieldValue, false, realVarLength, false);
		else 
			retValue = pQm_->defineQuery(sdbDbId_, sdbDesignatorId_, 
			(char*) pField->field_name, pMinFieldValue);
		if (retValue == SUCCESS)  
			retValue = pQm_->prepareEstimateCount(0, true);  
		if (retValue != SUCCESS)
			DBUG_RETURN( rangeRows );

		rangeRows = pQm_->getEstimatedRowsCount();

		sdbQueryMgrId_ = 0;
		pQm_ = NULL;
	}	// end of non-auto-increment index column

	// the estimated number of rows has to be >= 1.  Otherwise, MySQL thinks it is an empty set.
	if (rangeRows < 1) 
		rangeRows = 1;

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 3) {
		SDBDebugStart();			// synchronize threads printout	
		SDBDebugPrintHeader("total range count estimate from table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(": ");
		SDBDebugPrint8ByteUnsignedLong((uint64)rangeRows);
		SDBDebugEnd();			// synchronize threads printout	
	}
#endif

#endif
	DBUG_RETURN( rangeRows );
}

// /////////////////////////////////////////////////////////////////////////
// Plugin information
//
////////////////////////////////////////////////////////////////////////////

struct st_mysql_storage_engine scaledb_storage_engine =
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static MYSQL_SYSVAR_STR(config_file, scaledb_config_file,
						PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						"ScaledDB Config File Path",
						NULL, NULL, NULL);

static struct st_mysql_sys_var* scaledb_system_variables[]= {
	MYSQL_SYSVAR(config_file),
	/* TODO: to be enabled later
	MYSQL_SYSVAR(data_directory),
	MYSQL_SYSVAR(log_directory),
	MYSQL_SYSVAR(buffer_size_index),
	MYSQL_SYSVAR(buffer_size_data),
	*/
	NULL
};

mysql_declare_plugin(scaledb)
{
	MYSQL_STORAGE_ENGINE_PLUGIN,
		&scaledb_storage_engine,
		"ScaleDB",
		"ScaleDB, Inc.",
		"ScaleDB is a transactional storage engine that runs on a cluster machine with multiple nodes.  No data partition needed.",
		PLUGIN_LICENSE_PROPRIETARY,
		scaledb_init_func,			/* Plugin Init		*/
		scaledb_done_func,			/* Plugin Deinit	*/
		0x0100	,					/* version 1.0		*/
		//scaledb_status_variables_export,	/* status variables	*/
		NULL,	/* status variables	*/
		scaledb_system_variables,			/* system variables	*/
		NULL								/* config options	*/
}
mysql_declare_plugin_end;

#endif // SDB_MYSQL
