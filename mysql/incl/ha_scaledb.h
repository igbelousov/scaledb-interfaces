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
 //  File Name: ha_scaledb.h
 //
 //  Description: ScaleDB handler.   ha_scaledb is a subclass of MySQL's handler class.
 //  An instance of handler is created for each table descriptor.
 //  A handler object is not shared by concurrent user threads between ::external_lock function calls.
 //  After one user thread finishes access, a table handler object may be reused by another user thread.
 //  Note that ScaleDB's header files must come before MySQL header file.
 //  This is because we have STL header files which must be declared before C header files.
 //
 //  Version: 1.0
 //
 //  Copyright: ScaleDB, Inc. 2007
 //
 //  History: 08/21/2007  RCH   coded.
 //
 */

#ifdef SDB_MYSQL

#include "mysql_foreign_key.h"
#include "mysql_txn.h"
#include "mysql_priv.h"           // this must come second
#include "mysql/plugin.h"         // this must come third
#define DEFAULT_CHAIN_LENGTH 512
#define ERRORNUM_INTERFACE_MYSQL 10000
#define MYSQL_INDEX_NUMBER_SIZE 4
#define EXTRA_SAVEPOINT_OFFSET 32	// need to use this offset to reach the actual user specified savepoint
#define SCALEDB_HINT_PREFIX		" /*SCALEDBHINT:"	// all hints sent to non-primary nodes must have same prefix
#define SCALEDB_HINT_PASS_DDL	" /*SCALEDBHINT: PASS DDL*/"
#define SCALEDB_HINT_CLOSEFILE	" /*SCALEDBHINT: CLOSEFILE*/"
#define SCALEDB_HINT_OPENFILE	" /*SCALEDBHINT: OPENFILE*/"
#define SCALEDB_HINT_INDEX_BTREE	"SCALEDBHINT: INDEX TYPE BTREE"
#define SCALEDB_HINT_INDEX_TRIE		"SCALEDBHINT: INDEX TYPE TRIE"
#define MYSQL_TEMP_TABLE_PREFIX "#sql"		// ususally the first temp table used in ALTER TABLE
#define MYSQL_TEMP_TABLE_PREFIX2 "#sql2"	// the second temp table used in ALTER TABLE statement
#define MYSQL_ENGINE_EQUAL_SCALEDB " engine=scaledb "	// 16 bytes
/*
 Version for file format.
 1 - Initial Version. That is, the version when the metafile was introduced.
 */

#define SCALEDB_VERSION 1

//#define ENABLE_RANGE_COUNT		// TODO: Ron, a temporary test 

#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

typedef struct st_scaledb_share {
	char *table_name;
	uint table_name_length, use_count;
	pthread_mutex_t mutex;
	THR_LOCK lock;
} SCALEDB_SHARE;

#define SDB_EXTRA_DUP_IGNORE	1   // ignore any duplicates
#define SDB_EXTRA_DUP_REPLACE	2   // replace any duplicates

class ha_scaledb: public handler
{
public:
	ha_scaledb(handlerton *hton, TABLE_SHARE *table_arg);
	~ha_scaledb();

	static unsigned char mysqlInterfaceDebugLevel_; // defines the debug level for Interface component
	// You need to set up debug level in parameter 'debug_string' in scaledb.ini
	// Any positive debug level means all printouts from level 1 up to the specified level.
	// level 0: no debug printout 
	// level 1: prints out the function call for every method in ha_scaledb class
	// level 2: print out metadata structure after create/delete/rename tables,
	//          and MySQL user thread id plus handle which are useful in multi-user testing
	// level 3: print out file access statistics and user activities of a session
	// level 4: print out memory usage
	// level 5: for debugging commit operation and user lock status
	// level 6: print out query search, index traversal, and its path

	int debugCounter_;

	const char *table_type() const {return "ScaleDB";}
	const char *index_type(uint inx) {return "BTREE";}
	const char **bas_ext() const;

	Table_flags table_flags() const {
		return (HA_REC_NOT_IN_SEQ |
				HA_NULL_IN_KEY |
				HA_CAN_INDEX_BLOBS |
				HA_CAN_SQL_HANDLER |

				// no need to set HA_PRIMARY_KEY_IN_READ_INDEX.  We do not maintain data file
				// in primary key sequence order.  We just append records to the end during inserts.
				//HA_PRIMARY_KEY_IN_READ_INDEX |			// Bug 532
				// TODO: Without primary key, we can still call position().
				//HA_PRIMARY_KEY_REQUIRED_FOR_POSITION |

				HA_BINLOG_ROW_CAPABLE |
				HA_BINLOG_STMT_CAPABLE |
				HA_CAN_GEOMETRY |
				HA_TABLE_SCAN_ON_INDEX |
				HA_FILE_BASED |
				HA_HAS_RECORDS
		);
	}

	ulong index_flags(uint idx, uint part, bool all_parts) const {
		return (HA_READ_NEXT |
				HA_READ_PREV |
				HA_READ_ORDER |
				HA_READ_RANGE |
				HA_KEYREAD_ONLY);
	}

	void print_header_thread_info(const char *msg);

	uint max_supported_keys() const {return MAX_KEY;}
	uint max_supported_key_length() const {return 3500;}

	// pass the value in configuration parameter max_column_length_in_base_file
	uint max_supported_key_part_length() const;

	int open(const char *name, int mode, uint test_if_locked); // required
	int close(void); // required

	// This method inserts a record with row data pointed by buf 
	int write_row(unsigned char* buf);
	// This method updates a record with both the old row data and the new row data specified 
	int update_row(const unsigned char* old_row, unsigned char* new_row);
	// This method deletes a record with row data pointed by buf 
	int delete_row(const unsigned char* buf);
	// This method deletes all records of a ScaleDB table.
	int delete_all_rows();

	// MySQL calls this method for every table it is going to use at the beginning of every statement.
	// Thus, if a table is touched for the first time, it implicitly starts a transaction.
	// Note that a table handler is used exclusively by a single user thread after the first ::external_lock call
	// of a SQL statement.  The user thread will release the table handler in the second ::external_lock call.
	int external_lock(THD *thd, int lock_type);
	// If a user issues LOCK TABLES statement, MySQL will call ::external_lock only once.
	// In this case, MySQL will call this method at the beginning of the statement.
	int start_stmt(THD* thd, thr_lock_type lock_type);

	ha_rows records_in_range(uint inx, key_range *min_key, key_range *max_key);

	// This method fetches a single row using next() method
	int fetchSingleRow(unsigned char* buf);
	// This method fetches a single row based on rowid position
	int fetchRowByPosition(unsigned char* buf, unsigned int pos);
	// This method fetches a single virtual row.  This is ScaleDB extension.
	int fetchVirtualRow( unsigned char* buf );
	//This method returns the first key value in index
	int index_first(uchar* buf);
	//This method returns the last key value in index
	int index_last(uchar* buf);
	// This method returns the next row matching key value according to internal cursor 
	int index_next(unsigned char* buf);
	// This method reads the next row matching the key value given as the parameter.
	int index_next_same(uchar* buf, const uchar* key, uint keylen);
	//This method returns the prev key value in index
	int index_prev(uchar* buf);

	// This method retrieves a record based on index/key 
	int index_read(uchar * buf, const uchar * key, uint key_len, enum ha_rkey_function find_flag);
	// This method retrieves a record based on index/key.  The index number is a parameter 
	int index_read_idx(uchar* buf, uint keynr, const uchar* key, uint key_len, enum ha_rkey_function find_flag);
	// The following functions works like index_read, but it find the last row with the current key value or prefix.
	int index_read_last(uchar * buf, const uchar * key, uint key_len);

	int index_init(uint index, bool sorted); // this method is optional
	int index_end(); // this method is optional

	// This method prepares for a table scan.
	int rnd_init(bool scan); ///< required
	// This method ends a full table scan	
	int rnd_end(); ///< required
	// This method returns the next record
	int rnd_next(uchar* buf); ///< required
	int rnd_pos(uchar* buf, uchar* pos); ///< required
	// This method is called after each call to rnd_next() if the data needs to be ordered.
	void position(const uchar *record); ///< required

	int info(uint); ///< required

	// create a user table.
	int create(const char* name, TABLE *form, HA_CREATE_INFO *create_info); ///< required
	int add_columns_to_table(THD* thd, TABLE *table_arg, unsigned short ddlFlag);
	int add_indexes_to_table(THD* thd, TABLE *table_arg, char* tblName, unsigned short ddlFlag,
			SdbDynamicArray* fkInfoArray, char* pCreateTableStmt);
	int create_fks(THD* thd, TABLE *table_arg, char* tblName, SdbDynamicArray* fkInfoArray, char* pCreateTableStmt,
			bool bIsAlterTableStmt);

	int get_field_key_participation_length(THD* thd, TABLE *table_arg, Field * pField);

	// delete a user table.
	int delete_table(const char* name);

	// rename a user table.
	int rename_table(const char* fromTable, const char* toTable);

	THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
			enum thr_lock_type lock_type); ///< required

	// tells if data copy to new table is needed during an alter    
	bool check_if_incompatible_data(HA_CREATE_INFO *info, uint table_changes);
	ha_rows estimate_rows_upper_bound();

	// return table scan time
	double scan_time();

	// return read time of set of range of rows
	double read_time(uint inx, uint ranges, ha_rows rows);

	// analyze table
	int analyze(THD* thd, HA_CHECK_OPT* check_opt);

	// this function is called at the end of the query and can be used to clear state for that query
	int reset();

	// this function is called to issue DDL statements to the other nodes on a cluster
	// This function is static because other static functions will call it.
	static int sqlStmt(unsigned short userId, unsigned short dbmsId, char* sqlStmt, bool bIgnoreDB=false, bool bEngineOption=false);

	// return last key which actually resulted in an error (duplicate error)
	unsigned short get_last_index_error_key();

	// extra info from handler
	int extra( enum ha_extra_function operation );

	// copy scaledb row data to mysql row buffers
	int copyRowToMySQLBuffer(unsigned char *buf);

	// Get the latest auto_increment value from the engine
	uint64 get_scaledb_autoincrement_value();

	// Update HA_CREATE_INFO object.  Used in SHOW CREATE TABLE
	void update_create_info(HA_CREATE_INFO* create_info);

	// For SHOW CREATE TABLE statement, these two methods recreate foreign key constraint clause
	// based on ScaleDB's metadata information, and then return it to MySQL 
	char* get_foreign_key_create_info();
	void free_foreign_key_create_info(char* str);

	// set detailed error message for fk constraint failures
	bool get_error_message(int error, String *buf);

	// give records count
	ha_rows records();

	// disable keys -- only myisam supports this.  Other storage engines do NOT support it.
	// need to comment out this method as it does not work well with foreign key constraint
	//int disable_indexes(uint mode);

	// enable keys -- only myisam supports this.  Other storage engines do NOT support it.
	// need to comment out this method as it does not work well with foreign key constraint
	//int enable_indexes(uint mode);


private:
	THR_LOCK_DATA lock; ///< MySQL lock
	SCALEDB_SHARE *share; ///< Shared lock info

	bool beginningOfScan_; // set to true only we begin scanning a table and have not fetched rows yet.
	bool deleteAllRows_; // set to true if delete_all_rows() method is called.
	unsigned short sdbDbId_; // DbId used by ScaleDB
	unsigned short sdbTableNumber_; // table number used by ScaleDB
	unsigned int sdbUserId_; // user id assigned by ScaleDB storage engine
	unsigned short sdbQueryMgrId_; // current query Manager ID
	MysqlTxn* pSdbMysqlTxn_; // pointer to MysqlTxn object
	//	String fieldBuf;			// the buffer to hold a field of a response record
	char* sdbDesignatorName_; // current designator name
	unsigned short sdbDesignatorId_; // current designator id
	unsigned int sdbRowIdInScan_; // RowId used in sequential table scan
	unsigned int extraChecks_; // Extra information from handler
	unsigned int readDebugCounter_; // counter for debugging
	unsigned int deleteRowCount_;
	int sqlCommand_; // SQL command defined in ::external_lock.  Use this variable to avoid repetitively calling
	// thd_sql_command() function.  Example: DATA LOAD command
	unsigned short sdbCommandType_; // specifies command to be passed to ScaleDB engine
	unsigned short numberOfFrontFixedColumns_; // the number of fixed length columns at the front of a record
	unsigned short frontFixedColumnsLength_; // the length of all fixed length columns at the front of a record
	// this value is 0 if the first column is has variable length

	bool releaseLocksAfterRead_; // flag to indicate whether we hold the locks after reading data
	bool virtualTableFlag_; // flag to show if it is a virtual table
	bool bRangeQuery_; // flag is set to true if range condition is specified.
	// This flag is set in ::records_in_range method

	unsigned short getOffsetByDesignator(unsigned short designator);
	// This method packs a MySQL row into ScaleDB engine row buffer 
	// When we prepare the old row value for update operation, 
	// rowBuf1 points to old MySQL row buffer and rowBuf2 points to new MySQL row buffer.
	// For all other cases, these 2 row buffers point to same row buffer
	unsigned short placeMysqlRowInEngineBuffer(unsigned char* rowBuf1, unsigned char* rowBuf2, unsigned short groupType,
			bool checkAutoIncField, bool &needToUpdateAutoIncrement);
	// This method unpacks a single ScaleDB column and saves it into MySQL buffer
	void placeEngineFieldInMysqlBuffer(unsigned char *destBuff, char *ptrToField, Field* pField);

	// This method saves a MySQL transaction information for a given user thread.
	// When isTransient is true (outside the pair of external_locks calls), we do NOT save information into ha_scaledb.
	// When it is false, we save the returned pointer (possible others) into ha_scaledb member variables.
	MysqlTxn* placeSdbMysqlTxnInfo(THD* thd, bool isTransient=false);
	bool addSdbKeyFields();

	// This method is outside of a user transaction.  It opens a user database and saves value in sdbDbId_.
	// It does not open individual table files.
	unsigned short openUserDatabase(char* pDbName, char *pDbFsName, bool bIsPrimaryNode, 
			bool bIsAlterTableStmt, unsigned short ddlFlag=0);

	// prepare index query
	int prepareIndexKeyQuery(const uchar* key, uint key_len, enum ha_rkey_function find_flag);

	// prepare index scan query manager
	void prepareIndexQueryManager(unsigned int indexNum, const uchar* key, uint key_len);

	// prepare query manager using any avaiable key
	void prepareFirstKeyQueryManager();

	// prepare query manager 
	void prepareIndexOrSequentialQueryManager();

	// output handle and MySQL user thread id
	void outputHandleAndThd();

	// initialize DB id and Table id.  Returns non-zero if there is an error
	unsigned short initializeDbTableId(char* pDbName=NULL, char* pTblName=NULL,
			bool isFileName=false, bool allowTableClosed=false);

	int has_overflow_fields(THD* thd, TABLE *table_arg);

};

#endif	// SDB_MYSQL
