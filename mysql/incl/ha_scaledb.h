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

#ifdef _MARIA_DB
#include <mysql_version.h>
#include "mysql_foreign_key.h"
#include "sql_time.h"
#include <typeinfo>
#if MYSQL_VERSION_ID >= 100014
#include "item_inetfunc.h"
#endif
#if MYSQL_VERSION_ID>=50515
#include "sql_class.h"
#include "sql_array.h"
#include "sql_select.h"
#elif MYSQL_VERSION_ID>50100
#include "mysql_priv.h"
#include <mysql/plugin.h>
#else
#include "../mysql_priv.h"
#endif
#include "mysql_txn.h"
#include "mysql/plugin.h"         // this must come third
#else //_MARIA_DB
#include "mysql_foreign_key.h"
#include "mysql_txn.h"
#include "mysql_priv.h"           // this must come second
#include "mysql/plugin.h"         // this must come third
#endif //_MARIA_DB

#define _ENABLE_READ_RANGE

#define DEFAULT_CHAIN_LENGTH 512
#define ERRORNUM_INTERFACE_MYSQL 10000
#define MYSQL_INDEX_NUMBER_SIZE 4
#define SDB_AUTOINC_SCAN_FACTOR 30
#define EXTRA_SAVEPOINT_OFFSET 32	// need to use this offset to reach the actual user specified savepoint
#define SCALEDB_HINT_PREFIX		" /*SCALEDBHINT:"	// all hints sent to non-primary nodes must have same prefix

#define SCALEDB_HINT_CLOSEFILE	" /*SCALEDBHINT: CLOSEFILE*/"
#define SCALEDB_HINT_OPENFILE	" /*SCALEDBHINT: OPENFILE*/"
#define SCALEDB_HINT_INDEX_BTREE	"SCALEDBHINT: INDEX TYPE BTREE"
#define SCALEDB_HINT_INDEX_TRIE		"SCALEDBHINT: INDEX TYPE TRIE"
#define SCALEDB_ADD_PARTITION	"add partition"
#define SCALEDB_DROP_PARTITION "drop partition"
#define SCALEDB_COALESCE_PARTITION "COALESCE PARTITION"
#define SCALEDB_MAX_PARTITIONS 2048
#define MYSQL_TEMP_TABLE_PREFIX "#sql"		// ususally the first temp table used in ALTER TABLE
#define MYSQL_TEMP_TABLE_PREFIX2 "#sql2"	// the second temp table used in ALTER TABLE statement
#define MYSQL_ENGINE_EQUAL_SCALEDB " engine=scaledb "	// 16 bytes
#define SDB_MAX_CONDITION_EXPRESSIONS_TO_PARSE 10
#define SCALEDB_DB_TYPE 39
/*
Version for file format.
1 - Initial Version. That is, the version when the metafile was introduced.
*/
#if MYSQL_VERSION_ID >= 100011  //only enable for montys branch
#define USE_GROUP_BY_HANDLER
#define SDB_USE_MDB_MRR			// Use MariaDB multi-range-read functionality to determine whether the WHERE clause includes conditions on indexed columns
								// Disable this to instead parse the WHERE condition string to try and determine this
#endif


/* bits in group by header info_flags */
#define GH_ORDER_BY             1       /* query contain an order by*/
#define ANALYTIC_FLAG_ASCENDING 2
#define ANALYTIC_FLAG_USES_COUNT 4
//#define GH_ANOTHER_FLAG                 8       /* add as required */


#define SCALEDB_VERSION 1
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
#define _MARIA_SDB_10
#define _USE_NEW_MARIADB_DISCOVERY
#include "sql_show.h"
bool parse_sql(THD *thd, Parser_state *parser_state,
               Object_creation_ctx *creation_ctx, bool do_pfs_digest=false);
#endif

#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

// table charset defines
#define SDB_CHARSET_UNDEFINED 0xFF
#define SDB_ASCII 0
#define SDB_LATIN1 1
#define SDB_UTF8 2

typedef struct st_scaledb_share {
	char *table_name;
	uint table_name_length, use_count;
	pthread_mutex_t mutex;
	THR_LOCK lock;
} SCALEDB_SHARE;

typedef struct SimpleCondContext{
	SimpleCondItem items_[SDB_MAX_CONDITION_EXPRESSIONS_TO_PARSE];
	int  itemsParsed_;
}SimpleCondContext;

#define SDB_EXTRA_DUP_IGNORE	1   // ignore any duplicates
#define SDB_EXTRA_DUP_REPLACE	2   // replace any duplicates

#define _HIDDEN_DIMENSION_TABLE

enum streaming_state
{
  ST_UNKNOWN=0,
  ST_TRUE=1,
  ST_FALSE=2
};
//added because we can't include pushdown_condition.h
#pragma pack(1)  //prevent padding of struct
struct GroupByAnalyticsHeader
{
	uint   cardinality;
	uint   limit;
	uint   info_flag;
	ushort thread_count;
	ushort numberColumns;
	ushort numberInOrderby;
	ushort offsetToAuxiliary;			// the offset to the auxilary field in the row
};
struct GroupByAnalyticsBody
{
		ushort field_offset;		//the position in table row
		ushort columnNumber;     //this is the column number
		ushort length;			//the length of data 
		ushort function;			//this is operation to perform
		ushort function_length;  //the column type	
		char  type;				//the column type
		char orderByPosition;
		char orderByDirection;
};


struct SelectAnalyticsHeader
{
	ushort numberColumns;
};

struct SelectAnalyticsBody1
{
		ushort numberFields;		//the number of fields in column	
		ushort function;	
};

	
struct SelectAnalyticsBody2
{
		ushort field_offset;			//the position in table row
		ushort columnNumber;                 //this is the column number
		ushort length;			//the length of data 
		ushort precision;
		ushort scale;
		ushort function;			//this is operation to perform
		ushort result_precision;
		ushort result_scale;
		char type;				//the column type
		char orderByPosition;
		char orderByDirection;
};
#pragma pack()
#define  PROCESS_COUNT_DISTINCT
enum function_type { FT_NONE=0, FT_MIN=1, FT_MAX=2, FT_SUM=3, FT_COUNT=4, FT_AVG=5, FT_COUNT_DISTINCT=6,  FT_STREAM_COUNT=17, FT_DATE=18, FT_HOUR=19, FT_MAX_CONCAT=20, FT_CHAR=21, FT_UNSUPPORTED=22 };


struct rangebounds
{
	public:
	void clear()
	{
		startRange=0;
		endRange=0;
		startSet=false;
		endSet=false;
		valid=false;
	}
	int startRange;
	int endRange;
	bool startSet;
	bool endSet;
	bool valid;
	bool isValid()
	{
		if(valid ==true && startSet==true && endSet==true && endRange>0)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
};

class ha_scaledb: public handler
{
public:
	ha_scaledb(handlerton *hton, TABLE_SHARE *table_arg);	
	~ha_scaledb();

	mutable streaming_state isStreamingTable_; //mutable needed because index_flags is a const function
	void setEndRange(key_range* end)
	{

		if(SDBIsStreamingTable(sdbDbId_, sdbTableNumber_))
		{
			 in_range_check_pushed_down = TRUE;
		}
		else
		{
			in_range_check_pushed_down = FALSE;
		}

		range_key_part= table->key_info[active_index].key_part;
		eq_range=false;
		if(end)
		{
			end_key_ = end;		
			eq_range_ = eq_range;
		}
		  set_end_range(end);
	}


	// convert mysql many types of fields to SDB 4 types of fields: numeric, char, varchar, blob
	static SDBFieldType  mysqlToSdbType_[MYSQL_TYPE_GEOMETRY+1];

	static unsigned char mysqlInterfaceDebugLevel_; // defines the debug level for Interface component
	// You need to set up debug level in parameter 'debug_string' in scaledb.ini
	// Any positive debug level means all printouts from level 1 up to the specified level.
	// level 0: no debug printout 
	// level 1: prints out the function call for every method in ha_scaledb class
	// level 2: print out metadata structure after create/delete/rename tables,
	//          and MySQL user thread id plus handle which are useful in multi-user testing
	// level 3: print every fetch record, print out file access statistics and user activities of a session 
	// level 4: print out memory usage
	// level 5: for debugging commit operation and user lock status
	// level 6: print out query search, index traversal, and its path

	int debugCounter_;
	char lastSDBError[1000];
	int lastSDBErrorLength;
	void saveSDBError(unsigned int userid)
	{
		//get the ast erro for this user
		lastSDBErrorLength=SDBGetErrorMessage(userid, lastSDBError, 1000);	
	}
	TABLE* temp_table;
	void setTempTable(TABLE* t)
	{
		temp_table=t;
	}
	unsigned short getLastSDBError(char* buff,int buffLength)
	{
		
		int len=lastSDBErrorLength;
		if (lastSDBErrorLength + 2 < buffLength)
		{
			memcpy(buff, lastSDBError, lastSDBErrorLength + 1);	// copy include the null
		}
		lastSDBErrorLength=0; //prevent message getting returned again
		return len;
	}
	const char *table_type() const {return "ScaleDB";}
	const char *index_type(uint inx) {return "BTREE";}
	const char **bas_ext() const;

	Table_flags table_flags() const {
		return (HA_REC_NOT_IN_SEQ |
			HA_NULL_IN_KEY |
			HA_CAN_INDEX_BLOBS |
			HA_CAN_SQL_HANDLER |

			//HA_PRIMARY_KEY_IN_READ_INDEX.  We do not maintain data file
			// in primary key sequence order.  We just append records to the end during inserts.
			//			HA_PRIMARY_KEY_IN_READ_INDEX |			// Bug 532
			// TODO: Without primary key, we can still call position().
			//			HA_PRIMARY_KEY_REQUIRED_FOR_POSITION |

			HA_BINLOG_ROW_CAPABLE |
			HA_BINLOG_STMT_CAPABLE |
			HA_CAN_GEOMETRY |
			HA_TABLE_SCAN_ON_INDEX |
			HA_FILE_BASED |
//			HA_STATS_RECORDS_IS_EXACT |
			HA_HAS_RECORDS 

			);
	}

	ulong index_flags(uint idx, uint part, bool all_parts) const;
	

	void print_header_thread_info(const char *msg);

#ifdef SDB_DEBUG
	void printTableId(char * cmd);
#else
	inline void printTableId(char * cmd) {}
#endif

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

    int iSstreamingRangeDeleteSupported(unsigned long long* delete_key, unsigned short* columnNumber, bool* delete_all);

	// This method deletes all records of a ScaleDB table.
	int delete_all_rows();
	bool getRangeKeys( unsigned char * string, unsigned int length, key_range* key_start, key_range* key_end );
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
	int fetchRowByPosition(unsigned char* buf, unsigned long long pos);
	// This method fetches a single virtual row.  This is ScaleDB extension.
	int fetchVirtualRow( unsigned char* buf );

	// This method wraps both kinds of rows fetching 
	inline int fetchRow(unsigned char* buf) 
	{

		// Fetch the row in either method 
		if (virtualTableFlag_) {
			return fetchVirtualRow(buf);
		}
		else {
			return fetchSingleRow(buf);
		}
	}

	inline void removeSdbUser()
	{
		SDBRemoveUserById(sdbUserId_);
#ifdef SDB_DEBUG
		sdbUserId_ = 0;					// set the value to 0 such that a bug can be detected
#endif
	}

#ifdef _ENABLE_READ_RANGE
	//This method reads the first key in the range - we use it to mark a preftech op
	virtual int read_range_first(const key_range *start_key, const key_range *end_key,bool eq_range, bool sorted) {
		// if valid then mark the range query op for prefetch 
		if(SDBIsStreamingTable(sdbDbId_, sdbTableNumber_))
		{
			 in_range_check_pushed_down = TRUE;
		}
		else
		{
			in_range_check_pushed_down = FALSE;
		}
		// save the end_key for prefetch setting
		if ( end_key ) {			
			end_key_ = end_key;		
			eq_range_ = eq_range;
		}
		// now call the original function which in its turn will call index_read 
		// in index_read we will prepare the range query for prefetch  
		return handler::read_range_first(start_key, end_key, eq_range, sorted);
	}
#endif // _ENABLE_READ_RANGE

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
	
	// this is to compile for mariadb interface
	double keyread_time(uint index, uint ranges, ha_rows rows) 
	{
		return read_time(index,ranges,rows);
	}

	void generateAnalyticsString();
	int getOrderByPosition(const char* col_name, const char* col_alias, function_type ft, bool order_by_field, bool& ascending);
	int numberInOrderBy();
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
#ifdef _MARIA_SDB_10
	static int scaledb_discover_table(handlerton *hton, THD* thd, TABLE_SHARE *share);
#endif
	int scaledb_discover(handlerton *hton, THD* thd, const char *db,
		const char *name,
		uchar **frmblob,
		size_t *frmlen);
	int scaledb_table_exists(handlerton *hton, THD* thd, const char *db,
		const char *name);


	int info(uint); ///< required

	int addOrderByToList(char* buf, int& pos, SelectAnalyticsHeader* sah,unsigned short dbid, unsigned short tabid);
	bool isInSelectList(SelectAnalyticsHeader* sah, char*  col_name, unsigned short dbid, unsigned short tabid, function_type ft);

	// create a user table.
	int parseTableOptions( THD *thd, HA_CREATE_INFO *create_info, bool& streamTable, bool& dimensionTable, unsigned long long&  dimensionSize, char** rangekey);

	char getCASType(enum_field_types mysql_type, int flags);
	int getSDBSize(enum_field_types fieldType, Field* field) ;
	int generateGroupConditionString(int cardinality, int thread_count, char* buf, int max_buf, unsigned short dbid, unsigned short tabid, char* select_buf);
	int generateSelectConditionString(char* buf, int max_buf, unsigned short dbid, unsigned short tabid);
	int generateOrderByConditionString(char* buf, int max_buf, unsigned short dbid, unsigned short tabid, char* select_buf);
	bool checkFunc(char* name, char* my_function);
	bool checkNestedFunc(char* name, char* my_func1, char* my_func2);
	Item* NestedFunc(enum_field_types& type, function_type& function, int& no_fields, char* name, Item::Type ft, Item *item, Item_sum* sum, char* buf, int& pos, SelectAnalyticsBody1* sab1,unsigned short dbid, unsigned short tabid, bool& contains_analytics_function );
#ifdef  PROCESS_COUNT_DISTINCT
	Item* multiArgumentFunction(function_type funct, enum_field_types& type, function_type& function, int& no_fields, char* name, Item::Type ft, Item *item, Item_sum* sum, char* buf, int& pos, SelectAnalyticsBody1* sab1,unsigned short dbid, unsigned short tabid, bool& contains_analytics_function );
#endif // PROCESS_COUNT_DISTINCT
	bool addSelectField(char* buf, int& pos, unsigned short dbid, unsigned short tabid, enum_field_types type, short function,  const char* col_name, bool& contains_analytics, short precison, short scale, int flag, short result_precision, short result_scale , char* alias_name, bool is_orderby_field);
#ifdef _HIDDEN_DIMENSION_TABLE // UTIL FUNC DECLERATION  
	char * getDimensionTableName(char* table_name, char* col_name, char* dimension_table_name);
	char * getDimensionTablePKName(char* table_name, char* col_name, char* dimension_pk_name);

	int create_dimension_table(TABLE *fact_table_arg, char * col_name, unsigned char col_type, unsigned short col_size, unsigned long long  hash_size,  unsigned short ddlFlag, unsigned short tableCharSet, SdbDynamicArray * fkInfoArray );
	int create_multi_dimension_table(TABLE *fact_table_arg, char* index_name, KEY_PART_INFO* hash_key, int key_parts, unsigned long long  hash_size,  unsigned short ddlFlag, unsigned short tableCharSet, SdbDynamicArray * fkInfoArray );
	int getSDBType(Field* pField, enum_field_types fieldType,  unsigned char& sdbFieldType, unsigned short& sdbMaxDataLength,  unsigned short& sdbFieldSize, unsigned short&  dataLength) ;

#endif // _HIDDEN_DIMENSION_TABLE 
	int init_from_sql_statement_string(TABLE *table_arg, THD *thd, bool write, const char *sql, size_t sql_length);
	int create(const char* name, TABLE *form, HA_CREATE_INFO *create_info); ///< required
	int add_columns_to_table(THD* thd, TABLE *table_arg, unsigned short ddlFlag,unsigned short tableCharSet, char* rangekey, int& number_streaming_keys, int& number_streaming_attributes,bool dimensionTable,SdbDynamicArray * fkInfoArray);
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

	int check( THD* thd, HA_CHECK_OPT* check_opt );

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
	int copyRowToRowBuffer(unsigned char *buf);


	// copy scaledb row data to mysql row buffers in an efficient way using pre build template 
	static void initMysqlTypes();
	void buildRowTemplate(TABLE* tab, unsigned char * buff,bool checkAutoIncField=true);

	// create keyTemplate - return false if no key is found 
	bool  buildKeyTemplate(SDBKeyTemplate & t,unsigned char* key, unsigned int key_len,unsigned int index,bool & isFullKey, bool & allNulls);

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
//	int disable_indexes(uint mode);

	// enable keys -- only myisam supports this.  Other storage engines do NOT support it.
	// need to comment out this method as it does not work well with foreign key constraint
	//int enable_indexes(uint mode);

#ifdef	SDB_PUSH_DOWN
	// Engine condition pushdown
	const COND* cond_push(const COND *cond);
	void cond_pop();
	void saveConditionToString(const COND *cond);
	bool conditionTreeToString(const COND *cond, unsigned char **start, unsigned int *place, unsigned short* DBID, unsigned short* TABID );
	bool conditionMultEqToString( unsigned char** pCondString, unsigned int* pCondOffset, const COND* pCondMultEq );
	bool conditionFunctionToString( unsigned char** pCondString, unsigned int* pItemOffset, Item_func* pFuncItem,
									Item* pComperandItem, unsigned int* pComperandDataOffset,
									unsigned short countArgs, int typeFunc, unsigned short* pDbId, unsigned short* pTableId );
	bool conditionFieldToString( unsigned char** pCondString, unsigned int* pItemOffset, Item* pFieldItem,
								 Item* pComperandItem, unsigned int* pComperandDataOffset,
								 unsigned short countArgs, int typeOperation, unsigned short* pDbId, unsigned short* pTableId );
	bool conditionConstantToString( unsigned char** pCondString, unsigned int* pItemOffset, Item* pConstItem, Item* pComperandItem, unsigned int* pComperandDataOffset );

	inline int checkConditionStringSize( unsigned char** pCondString, unsigned int* pStringLength, unsigned int additionalLength )
	{
		//	Check whether the condition string buffer needs to be extended
		unsigned int	allocatedLength	= condStringAllocatedLength_;

		if ( allocatedLength			< ( *pStringLength + additionalLength ) )
		{
			// Condition string buffer needs to be extended
			allocatedLength			   += condStringExtensionLength_;

			if ( allocatedLength		> condStringMaxLength_ )
			{
				// Cannot extend further
				return -1;
			}

			unsigned char*	temp		= ( unsigned char* ) realloc( *pCondString, allocatedLength );

			if ( !temp )
			{
				// realloc failed
				return -1;
			}

			*pCondString				= temp;
			condStringAllocatedLength_	= allocatedLength;

			// Extension succeeded
			return 1;
		}

		// Extension was unnecessary
		return 0;
	}
#endif

	// gives number of rows which are about to be inserted 
	// 0 rows means a lot 
	void start_bulk_insert(ha_rows rows);
	virtual void start_bulk_insert(ha_rows rows, uint flags);


	// This method is used both inside and outside of a user transaction.  
	// inside - It opens a user database and saves value in sdbDbId_.
	// outside- it just opens the database
	// It does not open individual table files.
	static unsigned short openUserDatabase(char* pDbName, char *pDbFsName, unsigned short & dbId, unsigned short nonTxnUserId, MysqlTxn* pSdbMysqlTxn);

	static bool lockDDL(unsigned short userId, unsigned short dbId, unsigned short tableId, unsigned short partitionId) {
		bool retVal = false;
		if ( SDBSessionLock(userId, dbId, tableId, partitionId, 0, REFERENCE_LOCK_EXCLUSIVE) ) {
			if ( tableId ) {
				if ( SDBSessionLock(userId, dbId, 0, 0, 0, REFERENCE_LOCK_EXCLUSIVE)) {
					retVal = true;
				}
			}else { // lock only database 
				retVal = true;
			}
		}
		return retVal;
	}

	static bool lockDML(unsigned short userId, unsigned short dbId, unsigned short tableId, unsigned short partitionId) {
		return SDBSessionLock(userId, dbId, tableId, partitionId, 0, DEFAULT_REFERENCE_LOCK_LEVEL);
	}

	static inline bool isAlterCommand(int sqlCommand) 
	{
		bool retVal =false;
		switch ( sqlCommand )
		{
		case SQLCOM_ALTER_TABLE:
		case SQLCOM_CREATE_INDEX:
		case SQLCOM_DROP_INDEX:
			retVal =true;
		}
		return retVal;
	}

	inline bool	isIndexedQuery()
	{
		return isIndexedQuery_;
	}
	inline void setIsIndexedQuery()
	{
		 isIndexedQuery_=true;
	}
	inline void	setQueryEvaluation()
	{
		isQueryEvaluation_			= true;
	}

	inline void	setRangeKeyEvaluation()
	{
		setQueryEvaluation();
		isRangeKeyEvaluation_		= true;
	}

	inline void	clearQueryEvaluation()
	{
		ha_index_or_rnd_end();

		isQueryEvaluation_			=
		isRangeKeyEvaluation_		= false;
	}

	inline void	clearRangeKeyEvaluation()
	{
		clearQueryEvaluation();
	}

	inline bool	isQueryEvaluation()
	{
		return isQueryEvaluation_;
	}

	inline bool isRangeKeyEvaluation()
	{
		return ( ( isQueryEvaluation_ && isRangeKeyEvaluation_ ) ? true : false );
	}

	inline bool isRangeDesignator()
	{
		if ( sdbDbId() && sdbTableNumber() && sdbDesignatorId() )
		{
			return ( ( sdbDesignatorId() == ( unsigned short ) SDBGetRangeKey( sdbDbId(), sdbTableNumber() ) ) ? true : false );
		}

		return false;
	}

	inline bool hasRangeDesignator()
	{
		if ( sdbDbId() && sdbTableNumber() )
		{
			return ( SDBGetRangeKey( sdbDbId(), sdbTableNumber() ) ? true : false );
		}

		return false;
	}

	inline bool isRangeRead( TABLE_LIST* pTableList )
	{
		// Evaluate the indexes referenced in the WHERE clause to determine whether the query can be executed as a streaming range read
		JOIN_TAB*		pJoinTab;
		TABLE*			pTable;
		char*			pszDbName;
		unsigned short	dbId;
		unsigned short	tableId;

		if ( !pTableList )
		{
			return false;
		}

		if ( !( pTable				= pTableList->table ) )
		{
			return false;
		}

		pszDbName					= SDBUtilDuplicateString( pTable->s->db.str );
		dbId						= SDBGetDatabaseNumberByName( sdbUserId_, pszDbName );

		FREE_MEMORY( pszDbName );

		tableId						= SDBGetTableNumberByName( sdbUserId_, dbId, pTable->s->table_name.str );

		if ( !tableId )
		{
			return false;
		}

		if ( !( SDBIsStreamingTable( dbId, tableId ) ) )
		{
			return false;
		}

		if ( !( pJoinTab				= pTable->reginfo.join_tab ) )
		{
			return false;
		}

		if ( !( pJoinTab->read_first_record ) )
		{
			return false;
		}

		setRangeKeyEvaluation();

		int				rc				= ( *pJoinTab->read_first_record )( pJoinTab );
		bool			isOk			= ( rc ? false : true );

		clearRangeKeyEvaluation();

		return isOk;
	}

	inline void clearIndexKeyRanges()
	{
		clearIndexKeyRangeStart();
		clearIndexKeyRangeEnd();
	}

	inline void clearIndexKeyRange( key_range* pKeyRange )
	{
		pKeyRange->key					= NULL;
		pKeyRange->flag					= ( ha_rkey_function ) 0;
		pKeyRange->length				= 0;
		pKeyRange->keypart_map			= 0;
	}

	inline void clearIndexKeyRangeStart()
	{
		indexKeyRangeStart_.key			= NULL;
		indexKeyRangeStart_.flag		= ( ha_rkey_function ) 0;
		indexKeyRangeStart_.length		= 0;
		indexKeyRangeStart_.keypart_map	= 0;
	}

	inline void clearIndexKeyRangeEnd()
	{
		indexKeyRangeEnd_.key			= NULL;
		indexKeyRangeEnd_.flag			= ( ha_rkey_function ) 0;
		indexKeyRangeEnd_.length		= 0;
		indexKeyRangeEnd_.keypart_map	= 0;
	}

	inline void copyIndexKeyRangeStart( const uchar* key, uint key_len, ha_rkey_function flag, key_part_map keypart_map )
	{
		if ( key_len )
		{
			indexKeyRangeStart_.key		= indexKeyRangeStartData_;
			memcpy( ( char* )( indexKeyRangeStart_.key ), ( char* ) key, key_len );
		}
		else
		{
			indexKeyRangeStart_.key		= NULL;
		}
		indexKeyRangeStart_.flag		= flag;
		indexKeyRangeStart_.length		= key_len;
		indexKeyRangeStart_.keypart_map	= keypart_map;
	}

	inline void copyIndexKeyRangeStart( const key_range* pKeyRange )
	{
		indexKeyRangeStart_.key			= indexKeyRangeStartData_;
		memcpy( ( char* )( indexKeyRangeStart_.key ), ( char* )( pKeyRange->key ), pKeyRange->length );
		indexKeyRangeStart_.flag		= pKeyRange->flag;
		indexKeyRangeStart_.length		= pKeyRange->length;
		indexKeyRangeStart_.keypart_map	= pKeyRange->keypart_map;
	}

	inline void copyIndexKeyRangeEnd( const uchar* key, uint key_len, ha_rkey_function flag, key_part_map keypart_map )
	{
		if ( key_len )
		{
			indexKeyRangeEnd_.key		= indexKeyRangeEndData_;
			memcpy( ( char* )( indexKeyRangeEnd_.key ), ( char* ) key, key_len );
		}
		else
		{
			indexKeyRangeEnd_.key		= NULL;
		}
		indexKeyRangeEnd_.flag			= ( ha_rkey_function ) flag;
		indexKeyRangeEnd_.length		= key_len;
		indexKeyRangeEnd_.keypart_map	= keypart_map;
	}

	inline void copyIndexKeyRangeEnd( const key_range* pKeyRange )
	{
		indexKeyRangeEnd_.key			= indexKeyRangeEndData_;
		memcpy( ( char* )( indexKeyRangeEnd_.key ), ( char* )( pKeyRange->key ), pKeyRange->length );
		indexKeyRangeEnd_.flag			= pKeyRange->flag;
		indexKeyRangeEnd_.length		= pKeyRange->length;
		indexKeyRangeEnd_.keypart_map	= pKeyRange->keypart_map;
	}
#define MAX_ANALYTICS_LITERAL_BUFFER 1000
        char analytics_literal[MAX_ANALYTICS_LITERAL_BUFFER]; 
        int literal_buffer_offset;
	unsigned short analyticsStringLength() {return analyticsStringLength_;}
	unsigned short analyticsSelectLength() {return analyticsSelectLength_;}
	void resetAnalyticsString()
	{
		analyticsSelectLength_=0;
		analyticsStringLength_=0;
	}
	unsigned char* conditionString() {return conditionString_;}
	unsigned int   conditionStringLength() {return conditionStringLength_;}

	unsigned short sdbDbId() {return sdbDbId_; }
	unsigned short sdbUserId() {return sdbUserId_; }
	unsigned short sdbTableNumber() {return sdbTableNumber_;}
	unsigned short sdbDesignatorId()	{ return ( unsigned short )( sdbDesignatorId_ & DESIGNATOR_NUMBER_MASK ); }

	unsigned char	indexKeyRangeStartData_[ 8 ];
	unsigned char	indexKeyRangeEndData_[ 8 ];
	key_range		indexKeyRangeStart_;
	key_range		indexKeyRangeEnd_;
	bool forceAnalytics_;
	rangebounds rangeBounds;
	void setConditionStringLength(unsigned int len) { conditionStringLength_=len;}
private:

	THR_LOCK_DATA lock; ///< MySQL lock

	SCALEDB_SHARE *share; ///< Shared lock info
	bool analytics_uses_count;
	bool beginningOfScan_; // set to true only we begin scanning a table and have not fetched rows yet.
	unsigned short sdbDbId_; // DbId used by ScaleDB
	unsigned short sdbTableNumber_; // table number used by ScaleDB
	unsigned short sdbPartitionId_;
	unsigned int sdbUserId_; // user id assigned by ScaleDB storage engine
	unsigned short sdbQueryMgrId_; // current query Manager ID
	MysqlTxn* pSdbMysqlTxn_; // pointer to MysqlTxn object
	//	String fieldBuf;			// the buffer to hold a field of a response record
	unsigned int sdbDesignatorId_; // current designator id (e.g number plus level)
	char sdbDesignatorName_[SDB_MAX_NAME_LENGTH];		// fixed size buffer
	bool sdbSequentialScan_; // is in middle of sequential scan 
	unsigned long long sdbRowIdInScan_; // RowId used in sequential table scan
	unsigned int extraChecks_; // Extra information from handler
	bool readJustKey_; // extra information if to read keys only ( index traversal is enough )
	unsigned int readDebugCounter_; // counter for debugging
	unsigned int deleteRowCount_;
	bool isStreamingDelete_;
	int sqlCommand_; // MySQL command defined in ::external_lock.  Use this variable to avoid repetitively calling
	// thd_sql_command() function.  Example: DATA LOAD command
	unsigned short sdbCommandType_; // ScaleDB command type which specifies command to be passed to ScaleDB engine
	bool starLookupTraversal_; // if true our traversal includes the where clauses and we need to set it only once 
	bool releaseLocksAfterRead_; // flag to indicate whether we hold the locks after reading data
	bool virtualTableFlag_; // flag to show if it is a virtual table

	// True if the current query is executed using an index traversal
	bool	isIndexedQuery_;
	bool	isQueryEvaluation_;
	bool	isRangeKeyEvaluation_;

	static const int SdbKeySearchDirectionTranslation[13][2];
	static const enum ha_rkey_function SdbKeySearchDirectionFirstLastTranslation[13];
	// store info about how to convert SDB row internal info to MySQL or other packed row foramt  
	SDBRowTemplate rowTemplate_;
	// store info about how to convert SDB row internal info to MySQL or other packed row foramt  
	SDBKeyTemplate keyTemplate_[2];

	//Stack of conditions 
	SdbConditionStack  * conditions_;
	
	// Condition Push Variables
	unsigned char* conditionString_;
	unsigned int   conditionStringLength_;
	unsigned int   condStringAllocatedLength_;
	unsigned int   condStringExtensionLength_;
	unsigned int   condStringMaxLength_;
	bool           pushCondition_;



	// Analytics Push Variables
	unsigned char* analyticsString_;
	unsigned short analyticsStringLength_;
	unsigned short analyticsSelectLength_;
	unsigned short analyticsStringAllocatedLength_;
	unsigned short analyticsStringExtensionLength_;
	unsigned short analyticsStringMaxLength_;

	// number of rows in bulk insert 
	ha_rows numOfBulkInsertRows_;
	ha_rows numOfInsertedRows_;

	SdbDynamicArray * sortedVarcharsFieldsForInsert_; // temp buffer to sort 

	const key_range *end_key_; // end of range prefetch op 
	bool  eq_range_;            // if the rage is equal    

	unsigned short getOffsetByDesignator(unsigned short designator);
	// This method packs a MySQL row into ScaleDB engine row buffer 
	// When we prepare the old row value for update operation, 
	// rowBuf1 points to old MySQL row buffer and rowBuf2 points to new MySQL row buffer.
	// For all other cases, these 2 row buffers point to same row buffer
	unsigned short placeMysqlRowInEngineBuffer(unsigned char* rowBuf1, unsigned char* rowBuf2, unsigned short groupType, bool updateBlobContent,
		bool checkAutoIncField);

	// This method unpacks a single ScaleDB column and saves it into MySQL buffer
	void placeEngineFieldInMysqlBuffer(unsigned char *destBuff, char *ptrToField, Field* pField);

	// This method saves a MySQL transaction information for a given user thread.
	// We save the returned pointer (possible others) into ha_scaledb member variables.
	MysqlTxn* placeSdbMysqlTxnInfo(THD* thd);

	// Handle the SdbQueryMgrId which is active on the handle 
	// The assumption: For a single handler there is a single active query manger in each moment 
	// The below methods set and unset the active query manger of the Handler
	void setSdbQueryMgrId();
	void unsetSdbQueryMgrId();
	void resetSdbQueryMgrId()  {unsetSdbQueryMgrId();setSdbQueryMgrId();}

	// debug method that shows the HA call
#ifdef SDB_DEBUG
	void debugHaSdb(char *funcName, const char* name1, const char* name2, TABLE *table_arg);
#endif

	// evaluate table scan - used for analytic queries
	int evaluateTableScan();

	// evaluate index key - used for analytic queries
	int evaluateIndexKey( const uchar* key, uint key_len, enum ha_rkey_function find_flag );

	// prepare index query
	int prepareIndexKeyQuery(const uchar* key, uint key_len, enum ha_rkey_function find_flag);

	// prepare index scan query manager
	void prepareIndexQueryManager(unsigned int indexNum);

	// prepare query manager using any avaiable key
	void prepareFirstKeyQueryManager();

	// output handle and MySQL user thread id
	void outputHandleAndThd();

	// initialize DB id and Table id.  Returns non-zero if there is an error
	unsigned short initializeDbTableId(char* pDbName=NULL, char* pTblName=NULL,
		bool isFileName=false, bool allowTableClosed=false);

	int has_overflow_fields(THD* thd, TABLE *table_arg);

	// check mysqls .frm file to see if it matches our copy. if not thers is
	// out of date and must be refreshed (using discover)
	int checkFrmCurrent(const char *path, uint userId, uint dbId, uint tableId );

	static unsigned short tableCharSet(HA_CREATE_INFO *create_info) 
	{
		unsigned short tableCharSet = SDB_CHARSET_UNDEFINED;
		// we support charset in either ascii, latin1, or utf8 only.
		if ((strncmp(create_info->default_table_charset->csname, "ascii", 5) == 0))  {
			tableCharSet = SDB_ASCII;
		} else if ((strncmp(create_info->default_table_charset->csname, "latin1", 6) == 0)) {
			tableCharSet = SDB_LATIN1;
		} else if ((strncmp(create_info->default_table_charset->csname, "utf8", 4) == 0)) {
			tableCharSet = SDB_UTF8;
		}
		return tableCharSet;
	}

	inline static void convertTimeStringToTimeConstant( THD* pMysqlThd, Item* pItem, const char* pString, unsigned int stringSize,
														int temporalDataType, unsigned char* pDataType, unsigned char* pDataValue )
	{
		MYSQL_TIME		myTime;
		long long		timestamp;
		ulonglong		datetime;
		unsigned int	uiError;

		// Convert the time string to a time value and then to a timestamp
		if ( convertStringToTime( pMysqlThd, pString, stringSize, temporalDataType, &myTime ) )
		{
			switch ( temporalDataType )
			{
				case MYSQL_TYPE_TIMESTAMP:
				{
					// Convert the time value  to a timestamp
					timestamp		= TIME_to_timestamp( pMysqlThd, &myTime, &uiError );

					if ( uiError )
					{
						// Timestamp conversion error
						timestamp	= 0;
					}

					*pDataType		= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_TIMESTAMP );
					*( ( long long* ) pDataValue ) = timestamp;
					break;
				}

				case MYSQL_TYPE_TIME:
				{
					// Convert the time value to its stored format
					timestamp		= convertTimeStructToTimeType( &myTime );

					*pDataType		= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_TIME );
					*( ( long long* ) pDataValue ) = timestamp;
					break;
				}

				case MYSQL_TYPE_DATETIME:
				{
					datetime		= TIME_to_ulonglong_datetime( &myTime );

					*pDataType		= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_DATETIME );
					*( ( ulonglong* ) pDataValue ) = datetime;
					break;
				}

				case MYSQL_TYPE_DATE:
				case MYSQL_TYPE_NEWDATE:
				{
					datetime		= convertTimeStructToNewdateType( &myTime );

					*pDataType		= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_DATE );
					*( ( ulonglong* ) pDataValue ) = datetime;
					break;
				}

				case MYSQL_TYPE_YEAR:
				{
					datetime		= convertYearToStoredFormat( ( ( Item_int* ) pItem )->val_int() );

					*pDataType		= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_YEAR );
					*( ( ulonglong* ) pDataValue ) = datetime;
					break;
				}
			}
		}
		else
		{
			// Time conversion error
			switch ( temporalDataType )
			{
				case MYSQL_TYPE_TIMESTAMP:
					*pDataType		= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_TIMESTAMP );
					*( ( long long* ) pDataValue ) = 0;
					break;
				case MYSQL_TYPE_TIME:
					*pDataType		= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_TIME );
					*( ( long long* ) pDataValue ) = 0;
					break;
				case MYSQL_TYPE_DATETIME:
					*pDataType		= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_DATETIME );
					*( ( ulonglong* ) pDataValue ) = 0;
					break;
				default:
					*pDataType		= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER );
					*( ( ulonglong* ) pDataValue ) = 0;
					break;
			}
		}
	}

	inline static bool convertStringToTime( THD* pMysqlThd, const char* pString, unsigned int stringSize, int temporalDataType, MYSQL_TIME* pMyTime )
	{
#ifdef	SDB_INTERNAL_CONVERT_STRING_TO_DATETIME
		if ( pMysqlThd->charset()->state		& MY_CS_NONASCII )
		{
			// Only Ascii is supported for now
			return false;
		}

		char*				pChar				= ( char* ) pString;
		unsigned int*		pTime				= &( pMyTime->year );
		unsigned int*		pHour				= &( pMyTime->hour );
		int					unit				= 0;
		unsigned int		place				= 0;
		bool				hasAmPm				= false;

		memset( pMyTime, 0, sizeof( MYSQL_TIME ) );

		// Skip leading whitespace
		while ( ( *pChar == ' ' )			   || ( *pChar == '\t' ) )
		{
			pChar++;
		}

		while ( place							< stringSize )
		{
			if ( ( *pChar >= '0' ) && ( *pChar <= '9' ) )
			{
				*pTime						   *= 10;
				*pTime						   += ( *pChar - '0' );
				pChar++;
				place++;
			}
			else 
			{
				// Not a digit: a delimiter
				if ( ( unit == 0 )			   && ( place == 2 ) )
				{
					if ( *pTime				   >= 70 )
					{
						*pTime				   += 1900;
					}
					else
					{
						*pTime				   += 2000;
					}
				}

				if ( ( unit >= 3 )			   && ( unit <= 5 ) )
				{
					// Skip whitespace
					do
					{
						pChar++;
						place++;
					}
					while ( ( *pChar == ' ' )  || ( *pChar == '\t' ) );

					// Check for AM/PM
					if ( ( *( pChar + 1 )	   == 'M' ) || ( *( pChar + 1 ) == 'm' ) )
					{
						switch ( *pChar )
						{
							case 'A':
							case 'a':
								if ( *pHour	   >= 12 )
								{
									( *pHour ) -= 12;
								}
								hasAmPm			= true;
								break;
							case 'P':
							case 'p':
								if ( *pHour		< 12 )
								{
									( *pHour ) += 12;
								}
								hasAmPm			= true;
						}
					}

					pChar--;
					place--;
				}

				if ( hasAmPm )
				{
					break;
				}

				if ( unit					   >= 5 )
				{
					// Disregard fractional seconds
					break;
				}

				// Move to the next time unit
				switch ( unit )
				{
					case 0:
						pTime					= &( pMyTime->month );
						break;
					case 1:
						pTime					= &( pMyTime->day );
						break;
					case 2:
						pTime					= &( pMyTime->hour );
						break;
					case 3:
						pTime					= &( pMyTime->minute );
						break;
					case 4:
						pTime					= &( pMyTime->second );
						break;
				}

				unit++;

				pChar++;
				place++;

				// Skip whitespace
				while ( ( *pChar == ' ' )	   || ( *pChar == '\t' ) )
				{
					pChar++;
					place++;
				}
			}
		}

		if ( pMyTime->year						< 1900 )
		{
			return false;
		}

		if ( !( pMyTime->month ) )
		{
			pMyTime->month						= 1;
		}
		
		if ( !( pMyTime->day ) )
		{
			pMyTime->day						= 1;
		}

		pMyTime->time_type						= MYSQL_TIMESTAMP_DATETIME;
#else
		unsigned long long	flags;
#ifdef	_MARIA_SDB_10
		bool				returnValue;
		MYSQL_TIME_STATUS	timeStatus;
#else	// MARIADB 5
		timestamp_type		returnValue;
		int					timeStatus;
#endif	// MARIADB 5

		switch ( temporalDataType )
		{
			case MYSQL_TYPE_YEAR:
				// No conversion necessary: MySQL has already converted the string year value to an int year value
				return true;
			case MYSQL_TYPE_TIME:
				flags							= pMysqlThd->variables.sql_mode & ( MODE_NO_ZERO_DATE | MODE_NO_ZERO_IN_DATE | MODE_INVALID_DATES );
				break;
			case MYSQL_TYPE_DATE:
			case MYSQL_TYPE_NEWDATE:
#ifdef	_MARIA_SDB_10
				flags							= sql_mode_for_dates( pMysqlThd );
#else	// MARIADB 5
				flags							= TIME_FUZZY_DATE | ( pMysqlThd->variables.sql_mode & ( MODE_NO_ZERO_DATE | MODE_INVALID_DATES ) ) | MODE_NO_ZERO_IN_DATE;
#endif	// MARIADB 5
				break;
			default:
				flags							= ( pMysqlThd->variables.sql_mode & MODE_NO_ZERO_DATE ) | MODE_NO_ZERO_IN_DATE;
		}

		// Convert the time string to a time value
		switch ( temporalDataType )
		{
			case MYSQL_TYPE_YEAR:
				memset( pMyTime, 0, sizeof( MYSQL_TIME ) );
				pMyTime->time_type				= MYSQL_TIMESTAMP_NONE;
				break;
			case MYSQL_TYPE_TIME:
				returnValue						= str_to_time    ( pMysqlThd->charset(), pString, stringSize, pMyTime, flags, &timeStatus );
				break;
			default:
				returnValue						= str_to_datetime( pMysqlThd->charset(), pString, stringSize, pMyTime, flags, &timeStatus );
		}

#ifdef	_MARIA_SDB_10
		if ( returnValue )
#else	// MARIADB 5
		if ( timeStatus )
#endif	// MARIADB 5
		{
			// Time conversion error
			return false;
		}
#endif

		return true;
	}
	
#ifdef	_MARIA_SDB_10
	inline static bool getTimeCachedResult( THD* pMysqlThd, Item* pItem, int temporalDataType, MYSQL_TIME* pMyTime )
	{
		return ( ( Item_cache_temporal* ) pItem )->get_date_result( pMyTime, sql_mode_for_dates( pMysqlThd ) );
	}
#endif	// MARIADB 10

	inline static void convertTimeToType( THD* pMysqlThd, Item* pItem, int temporalDataType, MYSQL_TIME* pMyTime, unsigned char* pDataType, unsigned char* pDataValue )
	{
		long long			timestamp;
		ulonglong			datetime;
		unsigned int		uiError;

		switch ( temporalDataType )
		{
			case MYSQL_TYPE_TIMESTAMP:
			{
				// Convert the time value  to a timestamp
				timestamp						= TIME_to_timestamp( pMysqlThd, pMyTime, &uiError );
				if ( uiError )
				{
					// Timestamp conversion error
					timestamp					= 0;
				}

				*pDataType						= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_TIMESTAMP );
				*( ( long long* ) pDataValue )	= timestamp;
				break;
			}
			case MYSQL_TYPE_TIME:
			{
				// Convert the time value to its stored format
				timestamp						= convertTimeStructToTimeType( pMyTime );

				*pDataType						= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_TIME );
				*( ( long long* ) pDataValue )	= timestamp;
				break;
			}

			case MYSQL_TYPE_DATETIME:
			{
				datetime						= TIME_to_ulonglong_datetime( pMyTime );

				*pDataType						= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_DATETIME );
				*( ( ulonglong* ) pDataValue )	= datetime;
				break;
			}

			case MYSQL_TYPE_DATE:
			case MYSQL_TYPE_NEWDATE:
			{
				datetime						= convertTimeStructToNewdateType( pMyTime );

				*pDataType						= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_DATE );
				*( ( ulonglong* ) pDataValue )	= datetime;
				break;
			}

			case MYSQL_TYPE_YEAR:
			{
				datetime						= convertYearToStoredFormat( ( ( Item_int* ) pItem )->val_int() );

				*pDataType						= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_YEAR );
				*( ( ulonglong* ) pDataValue )	= datetime;
				break;
			}
		}
	}

	inline static unsigned int convertTimeStructToDateType( MYSQL_TIME* pMyTime )
	{
		// Store the date value in 4 bytes
		unsigned int		dateValue			= ( pMyTime->year * 10000L ) + ( pMyTime->month * 100 ) + pMyTime->day;

		return dateValue;
	}

	inline static unsigned int convertTimeStructToNewdateType( MYSQL_TIME* pMyTime )
	{
		// Store the date value in 3 bytes
		unsigned int		dateValue			= ( pMyTime->year * 16 * 32 ) + ( pMyTime->month * 32 ) + pMyTime->day;

		return dateValue;
	}

	inline static unsigned char convertYearToStoredFormat( long long year )
	{
		// Store number of years since 1900
		return ( unsigned char )( year - 1900 );
	}

	inline static int convertTimeStructToTimeType( MYSQL_TIME* pMyTime )
	{
		// Store the time value in 3 bytes
		int					timeValue			= ( ( ( pMyTime->day *24L ) + pMyTime->hour ) * 10000L ) + ( ( pMyTime->minute * 100 ) + pMyTime->second );

		if ( pMyTime->neg )
		{
			timeValue							= -( timeValue );
		}

		return timeValue;
	}

	inline static void handleTimeConversionError( int temporalDataType, unsigned char* pDataType, unsigned char* pDataValue )
	{
		switch ( temporalDataType )
		{
			case MYSQL_TYPE_TIMESTAMP:
				*pDataType						= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_TIMESTAMP );
				*( ( long long* ) pDataValue )	= 0;
				break;
			case MYSQL_TYPE_TIME:
				*pDataType						= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_TIME );
				*( ( long long* ) pDataValue )	= 0;
				break;
			case MYSQL_TYPE_DATETIME:
				*pDataType						= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_DATETIME );
				*( ( ulonglong* ) pDataValue )	= 0;
				break;
			default:
				*pDataType						= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER );
				*( ( ulonglong* ) pDataValue )	= 0;
				break;
		}
	}
};

#ifdef USE_GROUP_BY_HANDLER
//disable the following to enable index lookup
//#define _USE_GROUPBY_SEQUENTIAL

class ha_scaledb_groupby: public group_by_handler
{
private:
	TABLE*					temp_table_;
	TABLE_LIST*				my_table_list_;
	JOIN_TAB*				pJoinTab_;
	bool					first_;

public:

	ha_scaledb_groupby(THD *thd_arg,
					   SELECT_LEX *select_lex_arg,
					   List<Item> *fields_arg,
					   TABLE_LIST *table_list_arg, ORDER *group_by_arg,
					   ORDER *order_by_arg, Item *where_arg,
					   Item *having_arg, handlerton *ht_arg)
		: group_by_handler(thd_arg, select_lex_arg,  fields_arg, table_list_arg, group_by_arg,   order_by_arg, where_arg,having_arg,ht_arg)
	{
		first_			= true;
		my_table_list_	= table_list_arg;

		temp_table_		= NULL;

		pJoinTab_		= my_table_list_->table->reginfo.join_tab;
	}
	
	virtual ~ha_scaledb_groupby() {}

	/*
	  Store pointer to temporary table and objects modified to point to
	  the temporary table.  This will happen during the optimize phase.

	  We provide new 'having' and 'order_by' elements here. The differ from the
	  original ones in that these are modified to point to fields in the
	  temporary table 'table'.

	  Return 1 if the storage handler cannot handle the GROUP BY after all,
	  in which case we have to give an error to the end user for the query.
	  This is becasue we can't revert back the old having and order_by elements.
	  */

	virtual bool init(TABLE *temporary_table, Item *having_arg,
					  ORDER *order_by_arg)
	{
		group_by_handler::init(temporary_table,having_arg,order_by_arg);
		temp_table_	= temporary_table;
		return 0;
	}

	/*
	  Functions to scan data. All these returns 0 if ok, error code in case
	  of error
	*/

	/*
	  Initialize group_by scan, prepare for next_row().
	  If this is a sub query with group by, this can be called many times for
	  a query.
	*/
	virtual int init_scan()
	{
#ifdef _USE_GROUPBY_SEQUENTIAL
		ha_scaledb* scaledb= (ha_scaledb*) (my_table_list_->table->file);
		int rc=scaledb->rnd_init(true);
		return rc;
#else
		ha_scaledb* scaledb= (ha_scaledb*) (my_table_list_->table->file);
	

		if ( scaledb->isIndexedQuery() )
		{

			int range_index=SDBGetRangeKey(scaledb->sdbDbId(), scaledb->sdbTableNumber()) ;
			int index_id=SDBGetIndexExternalId(scaledb->sdbDbId(), range_index);
			int rc=scaledb->ha_index_init(index_id, 1);  //need to patch in the range key
			return rc;
		}
		else
		{
			int rc=scaledb->rnd_init(true);
			return rc;
		}


#endif
	}

	/*
	  Return next group by result in table->record[0].
	  Return 0 if row found, HA_ERR_END_OF_FILE if last row and other error
	  number in case of fatal error.
	*/
	virtual int next_row()
	{
		ha_scaledb* scaledb= (ha_scaledb*) (my_table_list_->table->file);

#ifdef _USE_GROUPBY_SEQUENTIAL
		scaledb->setTempTable( temp_table_ );
		int rc			= scaledb->rnd_next( temp_table_->record[ 0 ]);
		return rc;
#else
		scaledb->setTempTable( temp_table_ );
		int rc	= 0;
	
#ifdef	SDB_USE_MDB_MRR
		if ( scaledb->isIndexedQuery() )
		{
			if ( first_ )
			{
				if ( scaledb->indexKeyRangeEnd_.key )
				{
					scaledb->setEndRange( &( scaledb->indexKeyRangeEnd_ ) );
				}
			
				rc		= scaledb->index_read( temp_table_->record[ 0 ],
											   ( const uchar* ) scaledb->indexKeyRangeStart_.key, scaledb->indexKeyRangeStart_.length, scaledb->indexKeyRangeStart_.flag );
				first_	= false;
			}
			else
			{
				rc		= scaledb->index_next( temp_table_->record[ 0 ] );
			}
		}
		else
		{
			if ( first_ )
			{
				first_	= false;
			}

			rc			= scaledb->rnd_next( temp_table_->record[ 0 ] );
		}
#else
		if ( scaledb->isIndexedQuery() )
		{
			if ( first_ )
			{
				if ( scaledb->indexKeyRangeEnd_.key )
				{
					scaledb->setEndRange( &( scaledb->indexKeyRangeEnd_ ) );
				}

				rc			= scaledb->index_read( temp_table_->record[ 0 ],
					( const uchar* ) scaledb->indexKeyRangeStart_.key, scaledb->indexKeyRangeStart_.length, scaledb->indexKeyRangeStart_.flag );
				first_		= false;
			}
			else
			{
				rc			= scaledb->index_next( temp_table_->record[ 0 ] );
			}
		}
		else
		{
			if ( first_ )
			{
				first_	= false;
			}

			rc			= scaledb->rnd_next( temp_table_->record[ 0 ] );
		}

#endif
	
		return rc;
#endif
	}

	/* End scanning */
	virtual int end_scan()
	{
		ha_scaledb* scaledb= (ha_scaledb*) (my_table_list_->table->file);

#ifdef _USE_GROUPBY_SEQUENTIAL
		int rc=scaledb->rnd_end();
#else
		int rc=0;
		if (scaledb->isIndexedQuery() )
		{
			rc=scaledb->index_end();
		}
		else
		{
			rc=scaledb->rnd_end();
		}
#endif
		
		scaledb->setTempTable(NULL);
		return rc;
	}

	/* Information for optimizer (used by EXPLAIN) */
	virtual int info(uint flag, ha_statistics *stats)
	{
	 return 0;
	}


	void print_error(int error, myf errflag)
	{

		ha_scaledb* scaledb= (ha_scaledb*) (my_table_list_->table->file);

		if(error==HA_ERR_GENERIC)
		{
			scaledb->lastSDBErrorLength=0; //make sure only latest error gets returned

			String str;
			bool temporary= scaledb->get_error_message(error, &str);
			if (!str.is_empty())
			{
				const char* engine= scaledb->table_type();
				my_error(ER_GET_ERRMSG, errflag, error, str.c_ptr(),engine);
				return;
			}
		}
		//fall through to defautlt handler

		my_error(ER_GET_ERRNO, MYF(0), error, hton_name(ht)->str);
	}
	};


#endif //USE_GROUP_BY_HANDLER

#endif	// SDB_MYSQL
