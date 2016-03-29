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


//when debugging results back to client put a breakpoint in here
// protocol.cc:905
//  DBUG_ENTER("Protocol::send_result_set_row");
//

//#define DISABLE_DISCOVER
#include <time.h>
#ifdef SDB_MYSQL

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif
#ifdef _MARIA_DB
#include "sql_table.h"
#include "sql_partition.h"
#include "discover.h"
#define hash_free my_hash_free
#define hash_init my_hash_init
#define hash_get_key my_hash_get_key
#define hash_search my_hash_search
#define hash_delete my_hash_delete
#endif

#include "../incl/sdb_mysql_client.h" // this should be included before ha_scaledb.h
#include "../incl/ha_scaledb.h"
#ifdef  __DEBUG_CLASS_CALLS
#include "../../../cengine/engine_util/incl/debug_class.h"
#endif
using namespace std;
#include <set>
#include <string>
#ifdef SDB_WINDOWS
#else
	#define stricmp	strcasecmp
#endif

#include <sys/stat.h>
#include <ctype.h>
#include "create_options.h"
#define SOURCE_HA_SCALEDB 63

#define SDB_DEBUG_LITE
#define ALLOW_STREAMING_ALTER

//#define SDB_SUPPORT_HANDLER_SOCKET_WRITE 
#define HANDLER_SOCKET_WRITE_BULK 0x4000
#define HANDLER_SOCKET_WRITE_MASK (HANDLER_SOCKET_WRITE_BULK -1)
#define HANDLER_SOCKET_WRITE_INSERTS_COUNT(globalInsertsCounter) (globalInsertsCounter & HANDLER_SOCKET_WRITE_MASK)
#define IS_HANDLER_SOCKET_WRITE_THREAD(thd) ( strcmp(thd->db,"handlersocket") == 0 )

#define STREAMING_TABLE_LOAD_BATCH_SIZE 1000

//to build so on windows need to enable this
//st_plugin_int *hton2plugin[MAX_HA];

handlerton *scaledb_hton=NULL;
unsigned char ha_scaledb::mysqlInterfaceDebugLevel_ = 0; // defines the debug level for Interface component
SDBFieldType  ha_scaledb::mysqlToSdbType_[MYSQL_TYPE_GEOMETRY+1] = {NO_TYPE, };

extern int unsigned statisticsLevel_;
const char* mysql_key_flag_strings[] = { "HA_READ_KEY_EXACT", "HA_READ_KEY_OR_NEXT",
        "HA_READ_KEY_OR_PREV", "HA_READ_AFTER_KEY", "HA_READ_BEFORE_KEY", "HA_READ_PREFIX",
        "HA_READ_PREFIX_LAST", "HA_READ_PREFIX_LAST_OR_PREV" };

#define DIMENSION_TABLE_PREFIX "sdb_dimension_"
// Translate MySQL direction to SDB direction X match_prefix
// SDB direction - controls the direction of the search
// Match_prefix  - marks if we limit the search to the given key prefix -
//     it goes with EQUAL and PREFIX modes.
const int ha_scaledb::SdbKeySearchDirectionTranslation[13][2] = {
  {SDB_KEY_SEARCH_DIRECTION_EQ,        1},  //HA_READ_KEY_EXACT,              /* Find first record else error */
  {SDB_KEY_SEARCH_DIRECTION_GE,        0},  //HA_READ_KEY_OR_NEXT,            /* Record or next record */
  {SDB_KEY_SEARCH_DIRECTION_LE,        0},  //HA_READ_KEY_OR_PREV,            /* Record or previous */
  {SDB_KEY_SEARCH_DIRECTION_GT,        0},  //HA_READ_AFTER_KEY,              /* Find next rec. after key-record */
  {SDB_KEY_SEARCH_DIRECTION_LT,        0},  //HA_READ_BEFORE_KEY,             /* Find next rec. before key-record */
  {SDB_KEY_SEARCH_DIRECTION_EQ,        1},  //HA_READ_PREFIX,                 /* Key which as same prefix */
  {SDB_KEY_SEARCH_DIRECTION_LE,        1},  //HA_READ_PREFIX_LAST,            /* Last key with the same prefix */
  {SDB_KEY_SEARCH_DIRECTION_LE,        1},  //HA_READ_PREFIX_LAST_OR_PREV,    /* Last or prev key with the same prefix */
  {SDB_KEY_SEARCH_DIRECTION_NONE,      0},  //HA_READ_MBR_CONTAIN,
  {SDB_KEY_SEARCH_DIRECTION_NONE,      0},  //HA_READ_MBR_INTERSECT,
  {SDB_KEY_SEARCH_DIRECTION_NONE,      0},  //HA_READ_MBR_WITHIN,
  {SDB_KEY_SEARCH_DIRECTION_NONE,      0},  //HA_READ_MBR_DISJOINT,
  {SDB_KEY_SEARCH_DIRECTION_NONE,      0}   //HA_READ_MBR_EQUAL
};	

// Translate MySQL direction to MySQL direction which SDB USES
// needed when no key is given and we replace the search "> SMALLEST" to ">= EVERYTHING" and  "< BIGGEST" to "<= EVERYTHING" and 
const enum ha_rkey_function ha_scaledb::SdbKeySearchDirectionFirstLastTranslation[13] =  {
	HA_READ_KEY_EXACT,            
	HA_READ_KEY_OR_NEXT,          
	HA_READ_KEY_OR_PREV,          
	HA_READ_KEY_OR_NEXT,    // SDB replace HA_READ_AFTER_KEY at the beginning of index         
	HA_READ_KEY_OR_PREV,    // SDB replace HA_READ_BEFORE_KEY  at the end  of index         
	HA_READ_PREFIX,               
	HA_READ_PREFIX_LAST,          
	HA_READ_PREFIX_LAST_OR_PREV,  
	HA_READ_MBR_CONTAIN,
	HA_READ_MBR_INTERSECT,
	HA_READ_MBR_WITHIN,
	HA_READ_MBR_DISJOINT,
	HA_READ_MBR_EQUAL
};


/* variables for ScaleDB configuration parameters */
#ifdef SDB_DEBUG
	static int externalLockCounter_ = 0;
#endif

static my_bool scaledb_system_variable_force_sdb_analytics = TRUE;

static char* scaledb_config_file = NULL;
static char* scaledb_data_directory = NULL;
static char* scaledb_log_directory = NULL;
static my_bool scaledb_log_dir_append_host = FALSE;
static unsigned int scaledb_buffer_size_index;
static unsigned int scaledb_buffer_size_data;
static unsigned int scaledb_max_file_handles;
static my_bool scaledb_aio_flag = TRUE;
static unsigned int scaledb_max_column_length_in_base_file;
static unsigned int scaledb_dead_lock_milliseconds;
static my_bool scaledb_cluster = FALSE;
static char* scaledb_debug_string = NULL;

static int scaledb_init_func();
static handler *scaledb_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root);

static void scaledb_kill_query(handlerton *hton, THD* thd, enum thd_kill_levels level);
static int scaledb_close_connection(handlerton *hton, THD* thd);
static int scaledb_commit(handlerton *hton, THD* thd, bool all); // inside handlerton
static int scaledb_rollback(handlerton *hton, THD* thd, bool all); // inside handlerton
static void scaledb_drop_database(handlerton* hton, char* path);
static bool scaledb_show_status(handlerton *hton, THD* thd, stat_print_fn* stat_print,	enum ha_stat_type stat_type);

static int scaledb_savepoint_set(handlerton *hton, THD *thd, void *sv);
static int scaledb_savepoint_rollback(handlerton *hton, THD* thd, void *sv);
static int scaledb_savepoint_release(handlerton *hton, THD* thd, void *sv);

static void start_new_stmt_trx(THD *thd, handlerton *hton);
static void rollback_last_stmt_trx(THD *thd, handlerton *hton);


static int scaledb_discover_table(handlerton *hton, THD* thd, TABLE_SHARE *share);
static int scaledb_discover_table_existence(handlerton *hton, const char *db, const char *table_name);
static int scaledb_discover_table_names(handlerton *hton, LEX_STRING *db, MY_DIR *dirp, handlerton::discovered_list *result);



#ifdef NEW_GROUPBY_HANDLER
static group_by_handler *scaledb_create_group_by_handler(THD *thd,Query* query);

/*
  Internal representation of the frm blob is:

  ver	  4 bytes
  orglen  4 bytes
  complen 4 bytes
*/

#define BLOB_HEADER 12


/*
  packfrm is a method used to compress the frm file for storage in a
  handler. This method was developed for the NDB handler and has been moved
  here to serve also other uses.

  SYNOPSIS
    packfrm()
    data                    Data reference to frm file data.
    len                     Length of frm file data
    out:pack_data           Reference to the pointer to the packed frm data
    out:pack_len            Length of packed frm file data

  NOTES
    data is replaced with compressed content

  RETURN VALUES
    0                       Success
    >0                      Failure
*/

int packfrm(const uchar *data, size_t len,
            uchar **pack_data, size_t *pack_len)
{
  int error;
  size_t org_len, comp_len, blob_len;
  uchar *blob;
  DBUG_ENTER("packfrm");
  DBUG_PRINT("enter", ("data: 0x%lx  len: %lu", (long) data, (ulong) len));

  error= 1;
  org_len= len;
  if (my_compress((uchar*)data, &org_len, &comp_len))
    goto err;

  DBUG_PRINT("info", ("org_len: %lu  comp_len: %lu", (ulong) org_len,
                      (ulong) comp_len));
  DBUG_DUMP("compressed", data, org_len);

  error= 2;
  blob_len= BLOB_HEADER + org_len;
  if (!(blob= (uchar*) my_malloc(blob_len,MYF(MY_WME))))
    goto err;

  /* Store compressed blob in machine independent format */
  int4store(blob, 1);
  int4store(blob+4, (uint32) len);
  int4store(blob+8, (uint32) org_len);          /* compressed length */

  /* Copy frm data into blob, already in machine independent format */
  memcpy(blob+BLOB_HEADER, data, org_len);

  *pack_data= blob;
  *pack_len=  blob_len;
  error= 0;

  DBUG_PRINT("exit", ("pack_data: 0x%lx  pack_len: %lu",
                      (long) *pack_data, (ulong) *pack_len));
err:
  DBUG_RETURN(error);

}

/*
  unpackfrm is a method used to decompress the frm file received from a
  handler. This method was developed for the NDB handler and has been moved
  here to serve also other uses for other clustered storage engines.

  SYNOPSIS
    unpackfrm()
    pack_data               Data reference to packed frm file data
    out:unpack_data         Reference to the pointer to the unpacked frm data
    out:unpack_len          Length of unpacked frm file data

  RETURN VALUES¨
    0                       Success
    >0                      Failure
*/

int unpackfrm(uchar **unpack_data, size_t *unpack_len,
              const uchar *pack_data)
{
   uchar *data;
   size_t complen, orglen;
   ulong ver;
   DBUG_ENTER("unpackfrm");
   DBUG_PRINT("enter", ("pack_data: 0x%lx", (long) pack_data));

   ver=         uint4korr(pack_data);
   orglen=      uint4korr(pack_data+4);
   complen=     uint4korr(pack_data+8);

   DBUG_PRINT("blob",("ver: %lu  complen: %lu  orglen: %lu",
                      ver, (ulong) complen, (ulong) orglen));
   DBUG_DUMP("blob->data", pack_data + BLOB_HEADER, complen);

   if (ver != 1)
     DBUG_RETURN(1);
   if (!(data= (uchar*)my_malloc(MY_MAX(orglen, complen), MYF(MY_WME))))
     DBUG_RETURN(2);
   memcpy(data, pack_data + BLOB_HEADER, complen);

   if (my_uncompress(data, complen, &orglen))
   {
     my_free(data);
     DBUG_RETURN(3);
   }

   *unpack_data= data;
   *unpack_len=  orglen;

   DBUG_PRINT("exit", ("frmdata: 0x%lx  len: %lu", (long) *unpack_data,
                       (ulong) *unpack_len));
   DBUG_RETURN(0);
}
#else
static group_by_handler *scaledb_create_group_by_handler(THD *thd,
									   SELECT_LEX *select_lex,
                                       List<Item> *fields,
                                       TABLE_LIST *table_list, ORDER *group_by,
                                       ORDER *order_by, Item *where,
                                       Item *having );


#endif

static int scaledb_discover(handlerton *hton, THD* thd, const char *db,
	                   const char *name,
	                   uchar **frmblob,
	                   size_t *frmlen);
static int scaledb_table_exists(handlerton *hton,  const char *db,
                                 const char *name);

int saveFrmData(const char* name, unsigned short userId, unsigned short dbId, unsigned short tableId);
int updateFrmData(const char* name, unsigned int userId, unsigned short dbId);

/* Variables for scaledb share methods */
static HASH scaledb_open_tables; ///< Hash used to track the number of open tables; variable for scaledb share methods
pthread_mutex_t scaledb_mutex; ///< This is the mutex used to init the hash; variable for scaledb share methods
#define MIN_STREAMING_TABLE_HASH_SIZE 10
#define MAX_STREAMING_TABLE_HASH_SIZE 100000

//the following code was taken directly from mysql.
//the readpar and writepar are identical to readfrm and writefrm except
//the extension was changed from .frm to .par
//i could have used the readfrm functions however i would have had
//to change the reg_ext to .par before call and .frm after which
//is really dangerous. To avoid this i have added these new functions, and changed the extension
//these are very generic funtions, it is very unlikely that any changes to mysql will cause
//an issue with these functions
#include <my_dir.h>    // For MY_STAT
#ifdef _WIN32
//added the following from mariadb because the mysql codewas causing a failure
//after we moved to mariadb
//
int sdb_my_win_fstat(File fd, struct _stati64 *buf)
{
  int crt_fd;
  int retval;
  HANDLE hFile, hDup;

  DBUG_ENTER("my_win_fstat");

  hFile= my_get_osfhandle(fd);
  if(!DuplicateHandle( GetCurrentProcess(), hFile, GetCurrentProcess(), 
    &hDup ,0,FALSE,DUPLICATE_SAME_ACCESS))
  {
    my_osmaperr(GetLastError());
    DBUG_RETURN(-1);
  }
  if ((crt_fd= _open_osfhandle((intptr_t)hDup,0)) < 0)
    DBUG_RETURN(-1);

  retval= _fstati64(crt_fd, buf);
  if(retval == 0)
  {
    /* File size returned by stat is not accurate (may be outdated), fix it*/
    GetFileSizeEx(hDup, (PLARGE_INTEGER) (&(buf->st_size)));
  }
  _close(crt_fd);
  DBUG_RETURN(retval);
}
#endif //_WIN32
int sdb_my_fstat(File Filedes, MY_STAT *stat_area,
             myf MyFlags __attribute__((unused)))
{
  DBUG_ENTER("my_fstat");
  DBUG_PRINT("my",("fd: %d  MyFlags: %d", Filedes, MyFlags));
#ifdef _WIN32
  DBUG_RETURN(sdb_my_win_fstat(Filedes, stat_area));
#else
  DBUG_RETURN(fstat(Filedes, (struct stat *) stat_area));
#endif
}

int writepar(const char *name, const uchar *frmdata, size_t len)
{
  File file;
  char	 index_file[FN_REFLEN];
  int error;
  DBUG_ENTER("writefrm");
  DBUG_PRINT("enter",("name: '%s' len: %lu ",name, (ulong) len));

  error= 0;
  if ((file=my_create(fn_format(index_file,name,"",".par",
                      MY_UNPACK_FILENAME|MY_APPEND_EXT),
		      CREATE_MODE,O_RDWR | O_TRUNC,MYF(MY_WME))) >= 0)
  {
    if (my_write(file, frmdata, len,MYF(MY_WME | MY_NABP)))
      error= 2;
    void(my_close(file,MYF(0)));
  }
  DBUG_RETURN(error);
} /* writefrm */

int readpar(const char *name, uchar **frmdata, size_t *len)
{
  int    error;
  char	 index_file[FN_REFLEN];
  File	 file;
  size_t read_len;
  uchar *read_data;
  MY_STAT state;  
  DBUG_ENTER("readfrm");
  DBUG_PRINT("enter",("name: '%s'",name));
  
  *frmdata= NULL;      // In case of errors
  *len= 0;
  error= 1;
  if ((file=my_open(fn_format(index_file,name,"",".par",
                              MY_UNPACK_FILENAME|MY_APPEND_EXT),
		    O_RDONLY | O_SHARE,
		    MYF(0))) < 0)  
    goto err_end; 
  
  // Get length of file
  error= 2;
  if (sdb_my_fstat(file, &state, MYF(0)))
    goto err;
  read_len= state.st_size;  

  // Read whole frm file
  error= 3;
  read_data= 0;                                 // Nothing to free


   error= 3;
  if (!(read_data= (uchar*)my_malloc(read_len, MYF(MY_WME))))
    goto err;
  if (mysql_file_read(file, read_data, read_len, MYF(MY_NABP)))
  {
    my_free(read_data);
    goto err;
  }


  // Setup return data
  *frmdata= (uchar*) read_data;
  *len= read_len;
  error= 0;
  
 err:
  if (file > 0)
    void(my_close(file,MYF(MY_WME)));
  
 err_end:		      /* Here when no file */
  DBUG_RETURN (error);
} /* readfrm */
////////////////////////////////////////////////////////



/*
Current session lock behaviour

Close:
has an exclusive lock on table (tid), releasing at end


Open:
has a shared metalock on table 0 [from a new user uid], then will take a shared lock on (tid), releasing table 0 shared lock [for uid]

create:
will have an exclusive lock on table 0, which will get released after create

rename_table:
delete_table:
takes and exclusive table lock on table 0, then releases at end

 
external_lock:
has a shared lock on table (tid) 
*/
//this lock can be used to protect the opentable.
//just take the lock then noone can mess with the metadata while you open the table
//the lock will get released when function completes
//which will be after you have taken your shared lock.
class SessionSharedMetaLock
{
	unsigned int userIdforOpen;
	unsigned int dbid;
	bool ok;
	bool remove_user;
public:
	SessionSharedMetaLock(unsigned short db)
	{
		userIdforOpen = SDBGetNewUserId();
		remove_user=true;
		dbid=db;
		ok=true;
	}
	SessionSharedMetaLock(unsigned short db, unsigned short uid)
	{
		userIdforOpen = uid;
		remove_user=false;
		dbid=db;
		ok=true;
	}

	SessionSharedMetaLock()
	{
		userIdforOpen = SDBGetNewUserId();
		remove_user=true;
		dbid=0;
	}

	void set(unsigned short db)
	{
		dbid=db;
	}
	~SessionSharedMetaLock()
	{
		if(ok)
		{
			SDBCommit(userIdforOpen,true);
		}
		else
		{
			SDBRollBack(userIdforOpen,NULL,0,true);
		}
		if(remove_user)
		{
			SDBRemoveUserById(userIdforOpen);
		}
	}

	bool lock()
	{
				if (!ha_scaledb::lockDML(userIdforOpen, dbid, 0, 0))
				{
					ok=false;
					return false;
				}
				else
				{
					ok=true;
					return true;
				}		
	}

};


class SessionExclusiveMetaLock
{
	unsigned int userIdforOpen;
	unsigned int dbid;
	bool ok;
public:
	SessionExclusiveMetaLock(unsigned short db, unsigned int uid)
	{
		userIdforOpen = uid;
		dbid=db;
		ok=true;
	}


	~SessionExclusiveMetaLock()
	{
		if(ok)
		{
			SDBCommit(userIdforOpen,true);
		}
		else
		{
			SDBRollBack(userIdforOpen,NULL,0,true);
		}
	}

	bool lock()
	{
			    SDBCommit(userIdforOpen,true); //release any previously held locks

				if (!ha_scaledb::lockDDL(userIdforOpen, dbid, 0, 0))
				{
					ok=false;
					return false;
				}
				else
				{
					ok=true;
					return true;
				}
	
	}

};



class DDLInProgress
{
	unsigned int m_uid;
	bool is_complete;
public:
	DDLInProgress( unsigned int uid)
	{
		m_uid = uid;

		is_complete=false;
		//we are starting a DDL operation, so lets set a flag and make sure that the flag is not released until the end
		SDBStartDDL(m_uid);
	}


	~DDLInProgress()
	{
		
		if(!is_complete)
		{
			complete();
		}
		
	}
	void setComplete()
	{
			is_complete=true;
	}

	void complete()
	{
			bool b=SDBCheckEndDDL(m_uid);
			if(b==true)
			{
				//this indicates that the ddl is still in progress (has not been committed or rolled back which is GOOD)
				//all is ok
			}
			else
			{
#ifdef SDB_DEBUG
				//data must have got commited before the end, something is wrong.
				SDBTerminateEngine(0, "Commit or Rollback called while DDL in progress", __FILE__,__LINE__);
#endif
			}
			is_complete=true;
	}
	

};

// convert ScaleDB error code to MySQL error code.
// This method can be used as a centralized breakpoint to see what error code returned to MySQL.
static int convertToMysqlErrorCode(int scaledbErrorCode) {
	int mysqlErrorCode = 0;

	switch (scaledbErrorCode) {
	case SUCCESS:
		mysqlErrorCode = 0;
		break;

	case DATA_EXISTS:
	case METAINFO_DUPLICATE_FOREIGN_KEY_CONSTRAINT:
		mysqlErrorCode = HA_ERR_FOUND_DUPP_KEY;
		break;

	case WRONG_PARENT_KEY:
		mysqlErrorCode = HA_ERR_NO_REFERENCED_ROW;
		break;

	case DEAD_LOCK:
//	case 100:
	case LOCK_TABLE_FAILED:

		// the below is taken from - http://bugs.mysql.com/bug.php?id=32416
		//HA_ERR_LOCK_DEADLOCK rolls back the entire transaction.
		//HA_ERR_LOCK_TABLE_FULL rolls back the entire transaction.
		//
		//Starting from 5.0.13, HA_ERR_LOCK_WAIT_TIMEOUT lets MySQL just roll back the
		//latest SQL statement in a lock wait timeout. Previously, we rolled back the whole transaction.
		//
		//All other errors just roll back the statement.

		mysqlErrorCode = HA_ERR_LOCK_DEADLOCK;		// up to Nov. 2012 it used to be HA_ERR_LOCK_WAIT_TIMEOUT;
		break;

	case DATA_EXISTS_FATAL_ERROR:
		mysqlErrorCode = HA_ERR_UNSUPPORTED;
		break;

	case METAINFO_ATTEMPT_DROP_REFERENCED_TABLE:
	case ATTEMPT_TO_DELETE_KEY_WITH_SUBORDINATES:
		mysqlErrorCode = HA_ERR_ROW_IS_REFERENCED;
		break;

	case KEY_DOES_NOT_EXIST:
	case QUERY_END:
	case STOP_TRAVERSAL:
	case QUERY_KEY_DOES_NOT_EXIST:
		/* MySQL does not allow return value HA_ERR_KEY_NOT_FOUND */
		//mysqlErrorCode = HA_ERR_KEY_NOT_FOUND;
		mysqlErrorCode = HA_ERR_END_OF_FILE;
		break;

	case METAINFO_WRONG_PARENT_DESIGNATOR_NAME:
	case METAINFO_WRONG_FOREIGN_FIELD_NAME:
	case METAINFO_WRONG_FOREIGN_TABLE_NAME:
		mysqlErrorCode = HA_ERR_CANNOT_ADD_FOREIGN;
		break;

	case METAINFO_FAIL_TO_CREATE_INDEX:
		mysqlErrorCode = HA_ERR_WRONG_INDEX;
		break;

	case METAINFO_MISSING_FOREIGN_KEY_CONSTRAINT:
		mysqlErrorCode = HA_ERR_DROP_INDEX_FK;
		break;

	case METAINFO_DELETE_ROW_BY_ROW: // tell MySQL to call different method
		mysqlErrorCode = HA_ERR_WRONG_COMMAND;
		break;

	case  METAINFO_ROW_SIZE_TOO_LARGE:
		mysqlErrorCode = HA_ERR_TO_BIG_ROW;
		break;

	case UPDATE_REJECTED:			// the update by the user is not supported
		mysqlErrorCode = HA_ERR_RECORD_IS_THE_SAME; // row not actually updated: new values same as the old values
		break;

	case WRONG_AUTO_INCR_VALUE:
		mysqlErrorCode = HA_ERR_INDEX_COL_TOO_LONG;    /* In ScaleDB - user provided wrong auto incr value to a streaming table --> Index column length exceeds limit */
		break;

	case METAINFO_UNDEFINED_DATA_TABLE:
	case TABLE_NAME_UNDEFINED:
		{
			mysqlErrorCode = HA_ERR_NO_SUCH_TABLE;
			break;
		}
	case QUERY_ABORTED:
		{
			mysqlErrorCode = HA_ERR_GENERIC;
			break;
		}
	case DECIMAL_OVERFLOW_IN_ANALYTICS:
		{
			mysqlErrorCode = HA_ERR_GENERIC;
			break;
		}
	case UNSUPPORTED_ANALYTICS_QUERY:
		{
			mysqlErrorCode = HA_ERR_GENERIC;
			break;
		}
	case SDB_NO_DISK_SPACE:
		mysqlErrorCode = HA_ERR_DISK_FULL;
		break;

	case SDB_INTERNAL_ERROR:
		mysqlErrorCode = HA_ERR_INTERNAL_ERROR;
		break;
		
	default:
		mysqlErrorCode = HA_ERR_GENERIC;
		break;
	}

	return mysqlErrorCode;
}


// -----------------------------------------------------------------------------------------
//Start a large batch of insert rows - API FOR MARIA DB 5.5 and down 
//SYNOPSIS
//  start_bulk_insert()
//  rows                  Number of rows to insert
//RETURN VALUE
//  NONE
//DESCRIPTION
//  rows == 0 means we will probably insert many rows via insert-select 
// -----------------------------------------------------------------------------------------
void ha_scaledb::start_bulk_insert(ha_rows rows)
{
    if(rows==0)
	{
		//insert from sleect, we dont no the number of rows so set to max, and will call bulk_end after last row inserted
		rows=SDB_ULLONG_MAX;
	}
	numOfBulkInsertRows_ = rows;
	numOfInsertedRows_ = 0;
}

// -----------------------------------------------------------------------------------------
//Start a large batch of insert rows  - API FOR MARIA DB 10 and up
//SYNOPSIS
//  start_bulk_insert()
//  rows                  Number of rows to insert
//  flag                  mode of batch 
// -----------------------------------------------------------------------------------------
void ha_scaledb::start_bulk_insert(ha_rows rows, uint flags) 
{
	start_bulk_insert(rows);
}


// -----------------------------------------------------------------------------------------
//End a large batch of insert rows  - API FOR MARIA DB 10 and up
//SYNOPSIS
//  send_bulk_insert()
// -----------------------------------------------------------------------------------------
int ha_scaledb::end_bulk_insert() 
{
	if(numOfBulkInsertRows_==SDB_ULLONG_MAX)
	{
		int rc= SDBFlushStreamingInsert(sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_); //will get called after every bulk insert
                numOfBulkInsertRows_=1;
                return rc;
	}
	else
	{
		return 0;
	}
}


// -----------------------------------------------------------------------------------------
//	Partition name is seen as #P#pX (where X is a number) at the suffix of the name -
//	Remove the suffix and return X
// -----------------------------------------------------------------------------------------
unsigned short getAndRemovePartitionName(char *tblFsName, char* partitionName){


	int length = SDBUtilGetStrLength(tblFsName);
	int pLen = 0;
	bool tmpPartition = false;
	if (SDBUtilStrstrCaseInsensitive(tblFsName, "#TMP#")) {
		length -= 5;
		tmpPartition = true;
	}
	while (--length){ // start with position of last char in string


		if (tblFsName[length] == '#'){

			// remove the partition name from the string
			tblFsName[length - 2] = '\0';

			if (length <=2 ){
				SDBTerminateEngine(10, "Wrong partition name", __FILE__,__LINE__);
			}
			break;
		} else {
			partitionName[pLen] = tblFsName[length];
			pLen++;
		}
	}
	if (pLen) {
		if (tmpPartition) {
			partitionName[pLen++] = 't';
		}
		partitionName[pLen] = '\0';
		SDBUtilReverseString(partitionName);

	}


	return 0;
}

// -----------------------------------------------------------------------------------------
// find the DB name, table name, and path name from a fully qualified character array.
// An example of name is "./test/TableName".
// dbName is 'test', tableName is 'TableName', pathName is './'
// Note that Windows system uses backslash to separate DB name from table name.
// ----------------------------------------------------------------------------------------
void fetchIdentifierName(const char* name, char* dbName, char* tblName, char* pathName) {
	int i, j;
	int nameLength = (int)strlen(name);

	for (i = nameLength; i >= 0; --i) {
		if ((name[i] == '/') || (name[i] == '\\'))
			break;
	}
	int tblStartPos = i + 1;
	for (j = 0; tblStartPos + j <= nameLength; ++j)
		tblName[j] = name[tblStartPos + j];

	tblName[j] = '\0';

	int dbStartPos = 0;
	if (tblStartPos > 1) { // DB name is also specified
		for (i = tblStartPos - 2; i >= 0; --i) {
			if ((name[i] == '/') || (name[i] == '\\'))
				break;
		}
		dbStartPos = i + 1;
		for (j = 0; dbStartPos + j < tblStartPos - 1; ++j)
			dbName[j] = name[dbStartPos + j];
		dbName[j] = '\0';
	}

	for (j = 0; j < dbStartPos; ++j) {
		if (name[j] >= 'A' && name[j] <= 'Z')
			pathName[j] = name[j] - 'A' + 'a'; // use lower case characters only
		else
			pathName[j] = name[j];
	}
	pathName[j] = '\0';
}

/* convert LF to space, remove extra separators; 
   convert to lower case letters if bLowerCase is true.  
   Do NOT conver to lower case letters if bLowerCase is false.
 */
void convertSeparatorLowerCase(char* toQuery, char* fromQuery, bool bLowerCase, unsigned int length)
{
	unsigned int i = 0;
	unsigned int j = 0;

	while (fromQuery[i] != '\0') {
		if (i >= length){
			break;
		}
		if (fromQuery[i] == '`') { // need to handle the quote character of an identifier

			do { // need to copy the entire identifier name without skipping blanks until next backtick
				if (bLowerCase && (fromQuery[i] >= 'A') && (fromQuery[i] <= 'Z'))
					toQuery[j++] = fromQuery[i++] - 'A' + 'a';
				else
					toQuery[j++] = fromQuery[i++];
			} while (fromQuery[i] != '`');

			toQuery[j++] = fromQuery[i++];

		} else {

			if ((fromQuery[i] == ' ') || (fromQuery[i] == '\t') || (fromQuery[i] == '\f')
			        || (fromQuery[i] == '\n') || (fromQuery[i] == '\r')) {

				while ((fromQuery[i + 1] == ' ') || (fromQuery[i + 1] == '\t') || (fromQuery[i + 1]
				        == '\f') || (fromQuery[i + 1] == '\n') || (fromQuery[i + 1] == '\r'))
					i = i + 1; // remove the extra separator

				toQuery[j] = ' ';
			} else if (bLowerCase && (fromQuery[i] >= 'A') && (fromQuery[i] <= 'Z')) {
				toQuery[j] = fromQuery[i] - 'A' + 'a';
			} else {
				toQuery[j] = fromQuery[i];
			}

			++i;
			++j;
		}
	}

	if (i == 0)
		toQuery[0] = '\0';
	else
		toQuery[j] = '\0'; // set NULL after last byte
}


static uchar* scaledb_get_key(SCALEDB_SHARE *share, size_t* length, my_bool not_used __attribute__((unused))) {
#ifdef SDB_DEBUG_LIGHT

	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing scaledb_get_key(...) ");
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	*length = share->table_name_length;
	return (uchar*) share->table_name;
}

struct ha_table_option_struct
{
  const char *strparam;
  ulonglong ullparam;
  uint enumparam;
  bool boolparam;
};



ha_create_table_option scaledb_table_option_list[]=
{

  /*
    one boolean option, the valid values are YES/NO, ON/OFF, 1/0.
    The default is 1, that is true, yes, on.
  */
  HA_TOPTION_BOOL("STREAMING", boolparam, 1),
  HA_TOPTION_STRING("TABLE_TYPE", strparam),
  HA_TOPTION_END
};


ha_create_table_option scaledb_field_option_list[]=
{

  /*
    one boolean option, the valid values are YES/NO, ON/OFF, 1/0.
    The default is 1, that is true, yes, on.
  */
  HA_TOPTION_BOOL("DUMMY", boolparam, 1),

 
 

  HA_TOPTION_END
};

ha_create_table_option scaledb_index_option_list[]=
{

  /*
    one boolean option, the valid values are YES/NO, ON/OFF, 1/0.
    The default is 1, that is true, yes, on.
  */

  HA_TOPTION_STRING("RANGE_KEY", strparam),
  HA_TOPTION_STRING("RANGE_INTERVAL", strparam),
  HA_TOPTION_STRING("RANGE_START", strparam),
  HA_TOPTION_BOOL("HASH_KEY", boolparam, 0),
  HA_TOPTION_NUMBER("HASH_SIZE", ullparam, UINT_MAX32, 0, UINT_MAX32, 1),
  HA_TOPTION_END
};


/* This function is executed only once when MySQL server process comes up.
 */

static int scaledb_init_func(void *p) {
	DBUG_ENTER("scaledb_init_func");


	scaledb_hton = (handlerton *) p;
	void(pthread_mutex_init(&scaledb_mutex, MY_MUTEX_INIT_FAST));
	(void) hash_init(&scaledb_open_tables, system_charset_info, 32, 0, 0,
	        (hash_get_key) scaledb_get_key, 0, 0);

	scaledb_hton->state = SHOW_OPTION_YES;
	scaledb_hton->db_type = (legacy_db_type)SCALEDB_DB_TYPE;

	// The following are pointers to functions.
	// Once we uncommet a pointer, then we need to implement the corresponding function.
	scaledb_hton->close_connection = scaledb_close_connection;
	scaledb_hton->kill_query = scaledb_kill_query;
	scaledb_hton->savepoint_offset = 0;
	scaledb_hton->savepoint_set = scaledb_savepoint_set;
	scaledb_hton->savepoint_rollback = scaledb_savepoint_rollback;
	scaledb_hton->savepoint_release = scaledb_savepoint_release;

	scaledb_hton->commit = scaledb_commit;
	scaledb_hton->rollback = scaledb_rollback;

	scaledb_hton->discover_table = scaledb_discover_table;
	scaledb_hton->discover_table_existence=scaledb_table_exists;

	scaledb_hton->create_group_by=scaledb_create_group_by_handler;

// the discover_table_existence is disabled because it is redundant (and the current implmentation is unreliable). For scaledb really need to do a full discovery to be sure that
// the tableexists or not. The table_existence is designed for a fast way of determining if a table exists, however i would have to map it to the
// discover table function to be sure it wrks, because of this i will leave it disabled and force mariadb to do a full disscovery.
//---	scaledb_hton->discover_table_existence= scaledb_discover_table_existence;

// this function is currently not enabled, and i need to enable to support accurate SHOW TABLES call. 
	scaledb_hton->discover_table_names= scaledb_discover_table_names;



	scaledb_hton->table_options= scaledb_table_option_list;
    scaledb_hton->field_options= scaledb_field_option_list;
	scaledb_hton->index_options=scaledb_index_option_list;
	scaledb_hton->create = scaledb_create_handler;
	scaledb_hton->drop_database = scaledb_drop_database;

	scaledb_hton->show_status=scaledb_show_status;

	// bug #88, truncate should call delete_all_rows
	//scaledb_hton->flags = HTON_CAN_RECREATE;
	scaledb_hton->flags = HTON_TEMPORARY_NOT_SUPPORTED | HA_CAN_PARTITION;

	if (SDBGlobalInit(scaledb_config_file, 0)) {
		sql_print_error("________________________________________________");
		sql_print_error("ScaleDB: Failed to initialize the storage engine");
		sql_print_error("________________________________________________");
		DBUG_RETURN(1);
	}

	ha_scaledb::mysqlInterfaceDebugLevel_ = SDBGetDebugLevel();
#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintString("\nMySQL Interface: executing scaledb_init_func()");
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	// Need to perform this step immediately after we instantiate ScaleDB storage engine
	unsigned int userIdforOpen = SDBGetNewUserId();
	if (SDBOpenMasterDbAndRecoverLog(userIdforOpen) != SUCCESS){
		sql_print_error("________________________________________________");
		sql_print_error("ScaleDB: Failed to open a database table");
		sql_print_error("Check the configuration file and directories for the database");
		sql_print_error("________________________________________________");
		DBUG_RETURN(1);

	}
	SDBRemoveUserById(userIdforOpen);

	// Get all ScaleDB Configuration Parameters
	scaledb_data_directory = SDBGetDataDirectory();
	scaledb_log_directory = SDBGetLogDirectory();
	scaledb_buffer_size_index = SDBGetBufferSizeIndex();
	scaledb_buffer_size_data = SDBGetBufferSizeData();
	scaledb_max_file_handles = SDBGetMaxFileHandles();
	scaledb_aio_flag = true;
	scaledb_max_column_length_in_base_file = 256;
	scaledb_dead_lock_milliseconds = SDBGetDeadlockMilliseconds();
	scaledb_debug_string = SDBGetDebugString();


	scaledb_system_variable_force_sdb_analytics=SDBForceAnalyticsEngine();


	DBUG_RETURN(0);
}

static int scaledb_done_func(void *p) {
	DBUG_ENTER("scaledb_done_func");

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintString("\nMySQL Interface: executing scaledb_done_func()");
		SDBDebugEnd(); // synchronize threads printout
	}
#endif
	sql_print_information("ScaleDB: %s","Start Shutdown"); 

	int error = 0;
	if (!SDBStorageEngineIsInited())
		DBUG_RETURN(0);

	if (scaledb_open_tables.records)
		error = 1;

	hash_free(&scaledb_open_tables);
	SDBGlobalEnd();

	pthread_mutex_destroy(&scaledb_mutex);

	sql_print_information("ScaleDB: %s","End Shutdown"); 

	DBUG_RETURN(0);
}


static int scaledb_table_exists(handlerton *hton, const char *db,
                                 const char *name)
{
	//check if db exists
	DBUG_ENTER("scaledb_table_exists");



#ifdef SDB_DEBUG
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing scaledb_table_exists(table name: ");
		SDBDebugPrintString((char*) name);
		SDBDebugPrintString(" )");
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	unsigned short userIdforOpen, dbId=0, tableId;
	int errorCode = 0;
	int retCode;



	userIdforOpen = SDBGetNewUserId();

	// First we put db and table information into metadata memory
	//  fetchIdentifierName(name, dbFsName, tblFsName, pathName);

	retCode = ha_scaledb::openUserDatabase((char *)db,(char *) db, dbId,userIdforOpen,NULL);
	if(dbId==0)
	{
		errorCode = 0; //no database, so table can't exist
	}
	else
	{

		//need shared metalock
		SessionSharedMetaLock ot(dbId);
		if(ot.lock()==false)
		{
			errorCode = 0; //could not lock so assume table is NOT there
		}



		char to[512];
		uint n=tablename_to_filename(name, to, sizeof(to));
		tableId = SDBFetchTableIdByTableFsName(userIdforOpen, dbId, (char *)to);

		if(tableId)
		{	
			errorCode = 1; //table exists
		}
		else
		{
			errorCode = 0;
		}
	}
	SDBRemoveUserById(userIdforOpen);


	DBUG_RETURN(errorCode);


}


COND *make_cond_for_table( THD *thd, Item *cond, table_map table,
						   table_map used_table,
						   int join_tab_idx_arg,
						   bool exclude_expensive_cond,
						   bool retain_ref_cond );
#ifdef NEW_GROUPBY_HANDLER
static group_by_handler *scaledb_create_group_by_handler(THD *thd,Query* query)
{
    TABLE_LIST *table_list=query->from;
    Item *pWhere= query->where;
    List<Item> *fields=query->select;
#else
static group_by_handler *scaledb_create_group_by_handler(THD *thd,
									   SELECT_LEX *select_lex,
                                       List<Item> *fields,
                                       TABLE_LIST *table_list, ORDER *group_by,
                                       ORDER *order_by, Item *pWhere,
                                       Item *having)
{
#endif //NEW_GROUPBY_HANDLER

	char*		pszDisableSdbHandler1	= SDBUtilFindComment( thd->query(), "disable_sdb_group_by_handler" );
	char*		pszDisableSdbHandler2	= SDBUtilFindComment( thd->query(), "disable_sdb_analytics" ); //add both options

	if ( pszDisableSdbHandler1 || pszDisableSdbHandler2)
	{
		// Do not create a ScaleDB group-by handler
		return NULL;
	}

	ha_scaledb* scaledb					= (ha_scaledb*) (table_list->table->file);
	scaledb->isStreamingHashIndex_=false;
	scaledb->optimizeWhere_=true;

	const COND* cond					= scaledb->cond_push( pWhere );
	bool		is_streaming			= false;

		
	// Evaluate the indexes referenced in the WHERE clause to determine whether the query can be executed as a streaming range read
	bool		isRangeReadQuery		= scaledb->isRangeRead( table_list, &is_streaming );


	if(is_streaming==true)
	{
		//this flag is only applicable to streaming tables
		//if (dynamic) system variable disables force_sdb analytics thehn ignore config variable.
		scaledb->forceAnalytics_=  scaledb_system_variable_force_sdb_analytics; 
	}
	else
	{
		scaledb->forceAnalytics_=false;
	}



	bool isHashIndex= (isRangeReadQuery==false && scaledb->active_index>0 && is_streaming);

	
    bool analytic_string_ok=true;
	analytic_string_ok=scaledb->generateAnalyticsString(fields);	 //new method to process analytic queries


	//if no where clause then still support streaming table because will do a sequential scan over implicit range index
	// so if cond==NULL, then use groupby handler

	//if there is a where clause but was not extracted then cannot use analytics (because storage node wont be able to filter results)
	bool condition_ok=true;

	if (cond!=NULL && scaledb->conditionStringLength()==0)
	{
		condition_ok=false;
	}

	// if cond==NULL && original_query_contains_condition==false, then mysql has optimized out the where clause, so dont use analytics
	// do not use the groupby handler if mysql optimizes out the where clause
	bool		doCreateGroupByHandler	= ( (isRangeReadQuery || (cond==NULL && scaledb->original_query_contains_condition==false))  && (scaledb->analyticsStringLength() && scaledb->analyticsSelectLength() && condition_ok && analytic_string_ok)  );

	if ( !doCreateGroupByHandler )
	{
		// Do not create the group-by handler if the analytics string is empty, or there are no aggregate functions (analyticsSelectLength)
		scaledb->resetAnalyticsString();
		return NULL;
	}
	else
	{
		if (isHashIndex)
		{
			scaledb->isStreamingHashIndex_=true;
		}

		if (scaledb->optimizeWhere_ &&  scaledb->isIndexedQuery() && !( scaledb->isUserTimeIndexQuery() ) && scaledb->rangeBounds.isValid() && cond != NULL )
		{
			//---------------------------------------------------------------------------------------------------
			// Prepared query is an indexed query, and the index is not the user time index, and the condition
			//	tree contains a valid system range key subclause and these range key values will be used
			//---------------------------------------------------------------------------------------------------
			// Remove the system range key subclause from the WHERE clause:
			//	Move forward the part of the string after the system range key subclause to fill the gap
			//---------------------------------------------------------------------------------------------------
			// Note that if the index selected by the optimizer is the user time index, the part of the
			//	condition tree that pertains to the system range index must be evaluated because the start and
			//	end key values passed by the optimizer apply to the user time index, NOT the system range index
			//---------------------------------------------------------------------------------------------------
			unsigned char*	pCond			= scaledb->conditionString();
			char*			bool_true_node	= scaledb->conditionResultNode_.getPointer();

			unsigned int	len_to_remove	= scaledb->rangeBounds.endRange - scaledb->rangeBounds.startRange;
			unsigned char*	start_pos		= pCond + scaledb->rangeBounds.startRange;
			unsigned char*	end_pos			= pCond + scaledb->rangeBounds.endRange;
			unsigned int	move_len		= scaledb->conditionStringLength() - scaledb->rangeBounds.endRange;

			if( (scaledb->conditionStringLength() - len_to_remove) == 0 || (scaledb->conditionStringLength() - len_to_remove) == LOGIC_OP_NODE_LENGTH)
			{
				//if after removing the range tree the condition string is empty then dont add the bool node, just send an empty condition tree
				scaledb->setConditionStringLength(0);
			}
			else
			{
				// Note: To avoid array overflow the bool node length must be < the condition string pulled out (which is currently the case since the bool node is only 4 bytes and
				//		 the range will be much longer than that).
				unsigned int	diff_len	= len_to_remove - LOGIC_OP_NODE_LENGTH;

				scaledb->setConditionStringLength( scaledb->conditionStringLength() - diff_len );
				memmove( start_pos + LOGIC_OP_NODE_LENGTH, end_pos, move_len );	// MOVE the remainder of condition first, leave space for the boolean node
				memcpy ( start_pos, bool_true_node, LOGIC_OP_NODE_LENGTH );		// insert the boolean node

				if ( scaledb->rangeBounds.rangeParent )
				{
					if ( scaledb->rangeBounds.childCount > 1 )
					{
						// Boolean node replaces multiple range key conditions: Reduce the parent operator's child count
						*( unsigned short* )( pCond + scaledb->rangeBounds.rangeParent - diff_len )	-= ( scaledb->rangeBounds.childCount - 1 );
					}
				}
			}
		}
		SDBLogAnalyticsSqlStmt(scaledb->sdbUserId() , thd->query(), thd->query_id); 
		
		scaledb->forceAnalytics_=false;
#ifdef NEW_GROUPBY_HANDLER
		query->group_by=NULL;
		return new  ha_scaledb_groupby( thd, query, scaledb_hton );
#else
		return new  ha_scaledb_groupby( thd, select_lex, fields, table_list, group_by, order_by, pWhere, having, scaledb_hton );
#endif
	}
}


static int scaledb_discover_table_names(handlerton *hton, LEX_STRING *db, MY_DIR *dirp, handlerton::discovered_list *result)
{
	unsigned int userId = SDBGetNewUserId();
	void* p= SDBallocTableNamesArray(userId);
	int n = SDBgetTableNamesByDbmsName( userId, p, db->str);	// return number of tables (n) and set table names for this database name (placed in the array p).

	for(int i=0;i<=n;i++)
	{
		char* name= SDBgetNextTable(userId, i,p);
		if(name!=NULL)
		{


			char tname[SAFE_NAME_LEN + 1];
			size_t tlen= filename_to_tablename(name, tname, sizeof(tname), false); 
			//because scaledb storeas all names as filenames (eg stores  @0023sql1 instead of  #sql1   we
			//need to convert back to real table name from the faile table name 


			if(strncmp(tname,DIMENSION_TABLE_PREFIX,strlen(DIMENSION_TABLE_PREFIX))==0)
			{
				//hidden dimension table so ignore
			}
			else
			{
				result->add_table(tname,tlen);
			}
		}
	}

	SDBFreeTableNamesArray(userId, p);
	SDBRemoveUserById(userId);
	return 0;
}
static int scaledb_discover_table(handlerton *hton, THD* thd, TABLE_SHARE *share)
{
	DBUG_ENTER("scaledb_discover_table");

	size_t frm_len=0;
	uchar* frm_blob;


	int rc=scaledb_discover(hton,  thd, share->db.str, share->table_name.str,&frm_blob,&frm_len);
	if(rc==SUCCESS)
	{
		rc= share->init_from_binary_frm_image(thd, 1,frm_blob, frm_len);
		my_errno=rc;
		DBUG_RETURN(my_errno);
	}
	else
	{
		DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
	}

}

static int scaledb_discover(handlerton *hton, THD* thd, const char *db,
	           const char *name,
	           uchar **frmblob,
	           size_t *frmlen) {

	//check if db exists
        DBUG_ENTER("scaledb_discover");
		
#ifdef DISABLE_DISCOVER
		DBUG_RETURN(1);
#endif

#ifdef SDB_DEBUG
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing ha_discover(table name: ");
		SDBDebugPrintString((char*) name);
		SDBDebugPrintString(" )");
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	unsigned short dbId=0, tableId;
	unsigned int userIdforOpen, frmLength = 0, parLength=0;
	char * frmData;
	char * parData=NULL;
	*frmlen = 0;
	*frmblob = NULL;
	int errorCode = -1; // init to failure
	int retCode;

	userIdforOpen = SDBGetNewUserId();

	retCode = ha_scaledb::openUserDatabase((char *)db,(char *) db, dbId,userIdforOpen,NULL);
	if (retCode){
       	SDBRemoveUserById(userIdforOpen);
        DBUG_RETURN(convertToMysqlErrorCode(retCode));
	}

	SessionSharedMetaLock ot(dbId);
	if(ot.lock()==false)
        {
         	SDBRemoveUserById(userIdforOpen);
	        DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
	}

	if(!retCode) {		
		tableId = SDBOpenTable(userIdforOpen, dbId, (char *)name, 0, true);
		SDBCommit(userIdforOpen, false);
		if(tableId)
		{
			if (ha_scaledb::lockDML(userIdforOpen, dbId, tableId, 0)) 
			{
				SDBCommit(userIdforOpen, false);	// the commit is needed as the open got the table at level 2 and the getFrm asks for a table lock at level 1.

				//get the frm
				frmData = SDBGetFrmData(userIdforOpen, dbId, tableId, &frmLength, &parData, &parLength);
				if (!frmData) {
					errorCode = 1;
				}
				else{
					errorCode = 0;
					DBUG_PRINT("info",
						("setFrm data: 0x%lx  len: %lu", (long) frmData,
						(ulong) frmLength));
					*frmlen = frmLength;
					*frmblob = (uchar *)my_memdup(frmData, frmLength, MYF(0));
					//if the database does not exist then create it


					  MY_STAT stat_info;
					  char dirname[FN_REFLEN];
#ifdef SDB_WINDOWS
					  strcpy(dirname,".\\");
#else
					  strcpy(dirname,"./");
#endif		
					  strcat(dirname,db);

					  if (!my_stat(dirname,&stat_info,MYF(0)))
					  {
						  //directory does not exist
						  if (my_mkdir(dirname, 0777, MYF(MY_WME)))
						  {
							  //can't create directory, so fail
							  errorCode = 1;
						  }
					  }

					//now copy the par file into place, if it exists
					if(parLength>0)
					{
						char path[FN_REFLEN + 1];
						build_table_filename(path, sizeof(path) - 1, db, name, "", 0);
						writepar(path, (uchar*)parData, parLength); //actually write the par file.
					}
				}
			}
		}
	}
	else {
		errorCode = convertToMysqlErrorCode(retCode);
	}

	if (!errorCode) {
		SDBCommit(userIdforOpen,true);
	}
	SDBRemoveUserById(userIdforOpen);

	DBUG_RETURN(errorCode);
}

/** @brief
 Skeleton of simple lock controls. The "share" it creates is a structure we will
 pass to each scaledb handler. Do you have to have one of these? Well, you have
 pieces that are used for locking, and they are needed to function.
 */
static SCALEDB_SHARE *get_share(const char *table_name, TABLE *table) {

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
	length = (uint) strlen(table_name);

	if (!(share = (SCALEDB_SHARE*) hash_search(&scaledb_open_tables, (uchar*) table_name, length))) {
		if (!(share = (SCALEDB_SHARE *) my_multi_malloc(MYF(MY_WME | MY_ZEROFILL), &share,
		        sizeof(*share), &tmp_name, length + 1, NullS))) {
			pthread_mutex_unlock(&scaledb_mutex);
			return NULL;
		}

		share->use_count = 0;
		share->table_name_length = length;
		share->table_name = tmp_name;
		strmov(share->table_name, table_name);
		if (my_hash_insert(&scaledb_open_tables, (uchar*) share))
			goto error;
		thr_lock_init(&share->lock);
		pthread_mutex_init(&share->mutex, MY_MUTEX_INIT_FAST);
	}
	share->use_count++;
	pthread_mutex_unlock(&scaledb_mutex);

	return share;

error: pthread_mutex_destroy(&share->mutex);
#ifdef _MARIA_DB
	my_free((uchar *) share);
#else
	my_free((uchar *) share, MYF(0));
#endif

	return NULL;
}

/** @brief
 Free lock controls. We call this whenever we close a table. If the table had
 the last reference to the share, then we free memory associated with it.
 */
static int free_share(SCALEDB_SHARE *share) {
#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing free_share(...) ");
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	pthread_mutex_lock(&scaledb_mutex);
	if (!--share->use_count) {
		hash_delete(&scaledb_open_tables, (uchar*) share);
		thr_lock_delete(&share->lock);
		pthread_mutex_destroy(&share->mutex);
#ifdef _MARIA_DB
	my_free((uchar *) share);
#else
	my_free((uchar *) share, MYF(0));
#endif
	}
	pthread_mutex_unlock(&scaledb_mutex);

	return 0;
}

/*****************************************************************//**
Cancel any pending lock request associated with the current THD. */
static void scaledb_kill_query(

        handlerton*	hton,	    /* in:  handlerton */
		THD*	thd,	    /* in: MySQL thread being killed */
        enum thd_kill_levels level) /* in: kill level */
{
	DBUG_ENTER("scaledb_kill_query");
		MysqlTxn* pMysqlTxn = (MysqlTxn *) *thd_ha_data(thd, hton);
	


	if (pMysqlTxn != NULL) {
		// we assume that we get the user ID that is related to the handle
		unsigned short userId = pMysqlTxn->getScaleDbUserId();
		
	
		SDBKillQueryByUserId(userId);

	
	}

	DBUG_VOID_RETURN;
}


/* We close user connection when he logs off.
 We need to free MysqlTxn object for this user session.
 */
static int scaledb_close_connection(handlerton *hton, THD* thd) {
	DBUG_ENTER("scaledb_close_connection");

	MysqlTxn* pMysqlTxn = (MysqlTxn *) *thd_ha_data(thd, hton);
	
#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing scaledb_close_connection( ");
		SDBDebugPrintString("hton=");
		SDBDebugPrint8ByteUnsignedLong((unsigned long long) hton);
		SDBDebugPrintString(") pMysqlTxn=");
		SDBDebugPrint8ByteUnsignedLong((unsigned long long) pMysqlTxn);
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	if (pMysqlTxn != NULL) {
		// we assume that we get the user ID that is related to the lost connection
		unsigned short userId = pMysqlTxn->getScaleDbUserId();
		
		// rollback if the connection was killed in a middle of a transaction 
		// done inside SDBRemoveUserById 
		SDBRemoveUserById(userId);

		delete pMysqlTxn;
		pMysqlTxn = NULL;
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
	if (userTxn != NULL) {
		userId = userTxn->getScaleDbUserId();
		isActiveTran = userTxn->getActiveTxn();
	}

	// make sure it is not in automcommit mode.
	DBUG_ASSERT(thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN) || thd->in_sub_stmt);

	/* must happen inside of a transaction */
	DBUG_ASSERT(isActiveTran);

	// We use the memory address of sv where the argument sv is not the user-defined savepoint name.
	// According to Sergei Golubchik of MySQL, savepoint name is not exposed to storage engine.
	//char *pSavepointName;
	//pSavepointName = static_cast<char *>(sv);
	//SDBSetSavePoint(userId, pSavepointName);

	SDBSetSavePoint(userId, (char *)&sv, 8);		// we use the address as the savepoint name

	DBUG_RETURN(0);
}

/*
 rollback to a specified savepoint.
 The user's savepoint name is saved at sv + savepoint_offset
 */
static int scaledb_savepoint_rollback(handlerton *hton, THD *thd, void *sv)
{
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
	if (userTxn != NULL) {
		userId = userTxn->getScaleDbUserId();
		isActiveTran = userTxn->getActiveTxn();
	}

	// make sure it is not in automcommit mode.
	DBUG_ASSERT(thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN) || thd->in_sub_stmt);

	/* must happen inside of a transaction */
	DBUG_ASSERT(isActiveTran);

	//char *pSavepointName;
	//pSavepointName = static_cast<char *>(sv);
	//SDBRollBack(userId, pSavepointName, false);

	SDBRollBack(userId, (char *)&sv, 8, false);		// we use the address as the savepoint name

	DBUG_RETURN(errorNum);
}


/*
 Shows the engine statistics to the user 
 */



static bool scaledb_show_status(handlerton *hton, THD* thd, stat_print_fn* stat_print,	enum ha_stat_type stat_type)
{
	unsigned int userId;
	bool res;

	MysqlTxn* userTxn = (MysqlTxn *) *thd_ha_data(thd, hton);
	if (userTxn != NULL) {
		userId = userTxn->getScaleDbUserId();	
	}

	switch (stat_type) {
	case HA_ENGINE_STATUS:
		{
		char* str="There Was a Problem with This Request. Please Try Again or Contact ScaleDB Support";


		if (statisticsLevel_ == 1) {

			// turn off the monitoring clock if it is on (Public Service - in case the user forgot)

			str= SDBstat_getMonitorStatistics();
		}

		else if (statisticsLevel_ == 2) {

			// Deal with the monitoring clock
		}

		else {

			str="Invalid Statistics Level Requested. Valid Levels are 1 and 2. Please Refer to ScaleDB Documentation for Complete Information.";
		}


		res = stat_print( thd, "ScaleDB",
						  ( uint ) strlen( "ScaleDB" ),
						  STRING_WITH_LEN( "" ), str, ( uint ) strlen( str ) );

		return res;
		}
	case HA_ENGINE_MUTEX:
		return SUCCESS;
	default:
		return(FALSE);
	}
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
	if (userTxn != NULL) {
		userId = userTxn->getScaleDbUserId();
		isActiveTran = userTxn->getActiveTxn();
	}

	// make sure it is not in automcommit mode.
	DBUG_ASSERT(thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN) || thd->in_sub_stmt);

	/* must happen inside of a transaction */
	DBUG_ASSERT(isActiveTran);

	//just return 0 since we rollback savepoint during rollback it-self
	//so to satisfy mysql we will return 0;

	//char *pSavepointName;
	//pSavepointName = static_cast<char *>(sv);
	//SDBRemoveSavePoint(userId, pSavepointName);

	SDBRemoveSavePoint(userId, (char *)&sv, 8);	// we use the address as the savepoint name

	DBUG_RETURN(errorNum);
}

/*
 Commits a transaction or ends an SQL statement

 MySQL query processor calls this method directly if there is an implied commit.
 For example, START TRANSACTION, BEGIN WORK
 */
static int scaledb_commit(handlerton *hton, THD* thd, bool all) /* all=true if it is a real commit, all=false if it is an end of statement */
{
	DBUG_ENTER("scaledb_commit");
	MysqlTxn* userTxn = (MysqlTxn *) *thd_ha_data(thd, hton);
	unsigned int userId = userTxn->getScaleDbUserId();

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing scaledb_commit(hton=");
		SDBDebugPrint8ByteUnsignedLong((uint64) hton);
		SDBDebugPrintString(", thd=");
		SDBDebugPrint8ByteUnsignedLong((uint64) thd);
		SDBDebugPrintString(", all=");
		SDBDebugPrintString(all ? "true" : "false");
		SDBDebugPrintString(")");
		SDBDebugPrintString("; ScaleDbUserId:");
		SDBDebugPrintInt(userId);
		SDBDebugPrintString("; Query:");
		SDBDebugPrintString(thd->query());
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	unsigned short retCode = 0;
	unsigned short mySqlRetCode = 0;

	unsigned int sqlCommand = thd_sql_command(thd);

#ifdef SDB_SUPPORT_HANDLER_SOCKET_WRITE
	// on handler socket writes - 
	if ( IS_HANDLER_SOCKET_WRITE_THREAD(thd)) {
		// if not at the end of a bulk
		if (HANDLER_SOCKET_WRITE_INSERTS_COUNT(userTxn->getGlobalInsertsCount())) {
			// skip commits (make commit only after bulk - inside write_row)
			DBUG_RETURN(0); // do nothing
		}
	}
#endif

	if (sqlCommand == SQLCOM_LOCK_TABLES)
		DBUG_RETURN(0); // do nothing if it is a LOCK TABLES statement.

	if (all || (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN ))) {
		// should see how innobase_commit sets the condition for reference.
		// we need to commit txn under either of these two conditions:
		// (a) A user issues commit statement
		// (b) a user issues a single SQL statement with autocommit=1
		// In a single update statement after LOCK TABLES, MySQL may skip external_lock() 
		// and call this method directly.
#ifdef SDB_DEBUG_LIGHT
		SDBLogSqlStmt(userId, thd->query(), thd->query_id); 
#endif
		if (ha_scaledb::isAlterCommand(sqlCommand)  && (userTxn->getDdlFlag() & SDBFLAG_ALTER_TABLE_CREATE))  {
			// ensure that commit is called inside processing ALTER TABLE
			// For ALTER TABLE, primary node needs to commit the rows copied from the original table to the new table ?but should not return errors?
			retCode = SDBCommit(userId, true);
		} else {
			// For all other cases, we should perform the normal commit.
			// This includes UNLOCK TABLE statement to release locks.
			// This also includes the case that, when auto_commit==0, an additional commit method is called before ALTER TABLE is executed.		
			retCode = SDBCommit(userId, false);
			if (retCode){
				mySqlRetCode = HA_ERR_GENERIC;
			}

			//	need to free table locks after commit
	

			// After calling commit(), lockCount_ should be 0 except ALTER TABLE, CREATE/DROP INDEX.
			userTxn->lockCount_ = 0;
			if (userTxn->getDdlFlag() & SDBFLAG_ALTER_TABLE_KEYS)
				userTxn->setDdlFlag(0); // reset the flag as we may check this flag in next SQL statement
		}

		userTxn->setActiveTrn(false);
	} // else   TBD: mark it as the end of a statement.  Do we need logic here?

	

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugFlush();
	}
#endif

	DBUG_RETURN(mySqlRetCode);
}

/*
Rollbacks a transaction or the latest SQL statement
*/
static int scaledb_rollback(handlerton *hton, THD* thd, bool all) {

		unsigned int userId;
		unsigned int sqlCommand;

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

		MysqlTxn* userTxn = (MysqlTxn *) *thd_ha_data(thd, hton);

		if (userTxn != NULL) {
			// all = 1; rollback complete transaction
			// all = 0; rollback the current statement - i.e. it is Trans for current statement
			// in both cases the Txn ends in the rollback 
			userTxn->setActiveTrn(false);

			if (all) 
			{

				userId = userTxn->getScaleDbUserId();
#ifdef SDB_DEBUG_LIGHT			
			SDBLogSqlStmt(userId, "ROLLBACK", thd->query_id); 
#endif
				SDBRollBack(userId, NULL, 0, false);
				sqlCommand = thd_sql_command(thd);
				// After calling commit(), lockCount_ should be 0 except ALTER TABLE, CREATE/DROP INDEX.
				// In a single update statement after LOCK TABLES, MySQL may skip external_lock() 
				// and call this method directly.
				if (!ha_scaledb::isAlterCommand(sqlCommand)) {
					userTxn->lockCount_ = 0;
				}
			}
			else
			{
				rollback_last_stmt_trx(thd, hton);
			}
			
		} // else   TBD: issue an internal error message

#ifdef SDB_DEBUG_LIGHT
		if (ha_scaledb::mysqlInterfaceDebugLevel_) {
			SDBDebugFlush();
		}
#endif

		DBUG_RETURN(0);
}

static handler* scaledb_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root) {
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

static const char *ha_scaledb_exts[] = { NullS };

// find the DB name from the specified path name where the database name is the last directory of the path.  
// For example, dbName is 'test' in the pathName "....\test\".
// Note that Windows system uses backslash to separate the directory name.
// This method allocated memory for char* dbName.  The calling method needs to release memory.
char* fetchDatabaseName(char* pathName) {
	int i;
	for (i = (int) strlen(pathName) - 2; i >= 0; --i) {
		if ((pathName[i] == '/') || (pathName[i] == '\\'))
			break;
	}
	int dbStartPos = i + 1;
	int dbNameLen = (int) strlen(pathName) - 1 - dbStartPos;
	char* dbName = (char*) ALLOCATE_MEMORY(dbNameLen + 1, SOURCE_HA_SCALEDB, __LINE__);
	memcpy(dbName, pathName + dbStartPos, dbNameLen);
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
	THD* thd = current_thd;
	unsigned int userId = 0;
	// Fetch database name from the path
	char* pDbName = fetchDatabaseName(path);

	// If the ddl statement has the key word SCALEDB_HINT_PASS_DDL,
	// then we need to update memory metadata only.  (NOT the metadata on disk).
	unsigned short ddlFlag = SDBFLAG_DDL_META_TABLES;
	
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

		SDBDCloseDbms(0, 0, pDbName);
		return;
	} 
	
	userId = userTxn->getScaleDbUserId();


	unsigned short dbId = 0;
	int errorNum = ha_scaledb::openUserDatabase(pDbName, pDbName,dbId,userId,NULL); 
	if (!errorNum) {
		retValue = SDBDropDbms(userId, dbId, ddlFlag, pDbName);
	}

	FREE_MEMORY(pDbName);
}



/********************************************************
*	
*	From Here The Handler functions section begins 
*
********************************************************/
void ha_scaledb::setSdbQueryMgrId() {
	// get a new query manger id if the previous query ended 
	if (!sdbQueryMgrId_ || !SDBIsValidQueryManagerId(sdbQueryMgrId_, sdbUserId_)) 
	{ 
		sdbQueryMgrId_ = SDBGetQueryManagerId(sdbUserId_);
 	    sdbDesignatorId_ = 0;
		sdbSequentialScan_ = false;
		eq_range_ = true; // unless stated otherwise the defualt is equal range/ point lookup 
		end_key_ = NULL;  // the default is eq_range ==> no end key 
		
	} 

	// assertion check on the validity of the current sdbQueryMgrId_ 
#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) 
	{
		THD* thd = ha_thd();
		MysqlTxn* pMysqlTxn = (MysqlTxn *) *thd_ha_data(thd, ht);
		if ( pSdbMysqlTxn_->getScaleDbUserId() != sdbUserId_ && sdbQueryMgrId_ > 0 ) 
		{
			SDBTerminate(0, "SdbQueryMgrId: New user uses the handler but the query id is active ");
		}
	}
#endif
}


void ha_scaledb::unsetSdbQueryMgrId() {
	if ( sdbQueryMgrId_ ) 
	{
		SDBFreeQueryManager(sdbUserId_,sdbQueryMgrId_);
		sdbQueryMgrId_ = 0;
		sdbDesignatorId_ = 0;
		sdbSequentialScan_ = false;
		beginningOfScan_ = false;
		numOfBulkInsertRows_ = 1;
		numOfInsertedRows_ = 0;
	}
}

void ha_scaledb::print_header_thread_info(const char *msg) {

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintString("\n");
		SDBDebugPrintString(msg);
		if (ha_scaledb::mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif
}

#ifdef SDB_DEBUG
void ha_scaledb::printTableId(char * cmd)
{
		SDBDebugStart();
		SDBDebugPrintString("\n Thd [");
		SDBDebugPrint8ByteUnsignedLong((long)ha_thd());
		SDBDebugPrintString("], From [");
		SDBDebugPrintString(cmd);
		SDBDebugPrintString("], Table ID [");
		SDBDebugPrintInt(sdbTableNumber_);
		SDBDebugPrintString("], Table name [");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString("].");
		SDBDebugEnd();
}
#endif

// constructor
ha_scaledb::ha_scaledb(handlerton *hton, TABLE_SHARE *table_arg) :
	handler(hton, table_arg) {

#ifdef __DEBUG_CLASS_CALLS
	DebugClass::countClassConstructor("ha_scaledb");
#endif

#ifdef SDB_DEBUG_LIGHT
	if (ha_scaledb::mysqlInterfaceDebugLevel_) {
		print_header_thread_info("MySQL Interface: executing ha_scaledb::ha_scaledb(...) ");
	}
#endif
	is_delayed_insert=false;
	original_query_contains_condition=false;
	isStreamingTable_=ST_UNKNOWN;
	isStreamingHashIndex_=false;
	indexKeyRangeStart_.key	= indexKeyRangeStartData_;
	indexKeyRangeEnd_.key	= &( indexKeyRangeEndData_[ 0 ] );
	temp_table=NULL;
	lastSDBErrorLength =0;
	debugCounter_ = 0;
	deleteRowCount_ = 0;
	isStreamingDelete_ = false;

	isIndexedQuery_			=
	isUserTimeIndexQuery_	=
	isQueryEvaluation_		=
	isRangeKeyEvaluation_	= false;

	sdbDbId_ = 0;
	sdbTableNumber_ = 0;
	dimension_list.clear();
	parent_list.clear();
	sdbPartitionId_ = 0;
	sdbDesignatorId_ = 0;
	sdbQueryMgrId_ = 0;
	//pQm_ = NULL;
	pSdbMysqlTxn_ = NULL;
	beginningOfScan_ = false;
	sdbRowIdInScan_ = 0;
	extraChecks_ = 0;
	readJustKey_ = false;
	sdbCommandType_ = 0;
	releaseLocksAfterRead_ = false;
	virtualTableFlag_ = false;
	starLookupTraversal_ = false;
	numOfBulkInsertRows_ = 1;
    numOfInsertedRows_ = 0;
	conditionStringLength_			= 0;
	condStringExtensionLength_		=																// Must be a power of 2
	condStringAllocatedLength_		= 8192;															// Must be a power of 2
	condStringMaxLength_			= 67108864;
	analyticsStringLength_			= 0;
	analyticsSelectLength_          = 0;
	analyticsStringExtensionLength_	=																// Must be a power of 2
	analyticsStringAllocatedLength_	= 2048;															// Must be a power of 2
	analyticsStringMaxLength_		= analyticsStringAllocatedLength_ * 8;
	forceAnalytics_=false;
	rowTemplate_.fieldArray_		= SDBArrayInit( 10, 5, sizeof( SDBFieldTemplate ),   true ); 
	keyTemplate_[ 0 ].keyArray_		= SDBArrayInit( 10, 5, sizeof( SDBKeyPartTemplate ), true );
	keyTemplate_[ 1 ].keyArray_		= SDBArrayInit( 10, 5, sizeof( SDBKeyPartTemplate ), true );
	sortedVarcharsFieldsForInsert_	= SDBArrayInit( 10, 5, sizeof( unsigned long long ), false );
	conditions_ = SDBConditionStackInit(10);

	conditionString_				= ( unsigned char* ) malloc( condStringAllocatedLength_			* sizeof( unsigned char ) );
	analyticsString_				= ( unsigned char* ) malloc( analyticsStringAllocatedLength_	* sizeof( unsigned char ) );
	pushCondition_					= false;

	// init static array the first time we use the interface - float must have a length  
	if (mysqlToSdbType_[MYSQL_TYPE_FLOAT] != SDB_NUM)
	{
		ha_scaledb::initMysqlTypes();
	}
}

ha_scaledb::~ha_scaledb() {
#ifdef __DEBUG_CLASS_CALLS
	DebugClass::countClassDestructor("ha_scaledb");
#endif
	SDBArrayFree(rowTemplate_.fieldArray_); 
	SDBArrayFree(keyTemplate_[0].keyArray_);
	SDBArrayFree(keyTemplate_[1].keyArray_);

	SDBConditionStackFree(conditions_);
}

// output handle and MySQL user thread id
void ha_scaledb::outputHandleAndThd() {
	SDBDebugPrintString(", handler=");
	SDBDebugPrint8ByteUnsignedLong((unsigned long long) this);
	SDBDebugPrintString(", thd=");
	THD* thd = ha_thd();
	SDBDebugPrint8ByteUnsignedLong((unsigned long long) thd);
	if (thd) {
		SDBDebugPrintString(", Query:");
		SDBDebugPrintString(thd->query());
	}

}

ulong ha_scaledb::index_flags(uint idx, uint part, bool all_parts) const
	{

		ulong flags;
		
		//if the table is loaded from FRM (ie. already exists), then need to lookup streaming state by opening the table.
		//if the table is in the middle of create then the streaming state will be determined by the flag isStreamingTable_
		//which gets set uring the parse (of create table options) note* this happens before the 'important index_flag calls happen (to determine what index to use)
		//but after a lot of other calls.
		//because of this the ST_UNKNOWN state is used to prevent repeated calls to check the streaming state

		//two important cases
		//1) table exists, then the call will return TRUE or FALSE, dependig on wether the table is a streaming table

		//2) the table is being created, will initially return FALSE will will then return TRUE or FALSE,
		// depending on if the table is really a streaming table.


		if(this->isStreamingTable_==ST_UNKNOWN)
		{

			bool b=false;
			unsigned short dbid	=sdbDbId_;
			unsigned short tid=sdbTableNumber_;
			THD* thd = ha_thd();
			unsigned int sqlCommand = thd_sql_command(thd);

			if( (sdbDbId_==0 || sdbTableNumber_==0)   && sqlCommand!=SQLCOM_CREATE_TABLE) //if we are in middle of create, then don't try to open,
			{																			   //isStreamingTable_ will get set during parse.
			
				unsigned short userId = SDBGetNewUserId();
				int retCode = ha_scaledb::openUserDatabase(table_share->db.str,table_share->db.str, dbid,userId,NULL);
				SessionSharedMetaLock ot(dbid);
					if(ot.lock()==true)
					{
						//a shared might be needed here SessionSharedMetaLock ot(dbid);
						//before open, what is someone is dropping the table?
						//but we might be in the middle of a create 
						if(!retCode)
						{		
							char* name=table_share->table_name.str;
							char to[512];
							uint n=tablename_to_filename(name, to, sizeof(to));
							tid = SDBOpenTable(userId, dbid, to, 0, true);
							SDBCommit(userId, false);
						}

					}
				SDBRemoveUserById(userId);
			}

			if(dbid!=0 && tid !=0) 
			{
				b=SDBIsStreamingTable(dbid, tid); //table is open so can call.
			}
			
			if(b==true) 
			{
				isStreamingTable_=ST_TRUE;
			}
			else
			{
				isStreamingTable_=ST_FALSE;
			}
		}
		if(isStreamingTable_==ST_TRUE)
		{
			// to skip MIN/MAX  optimization we remove the order from streaming tables 
			// As a result we also remove loose-index-scans but since on streaming tables we apply exact keys only - it does not matter at this point
			// we keep the range reads and the point reads
			//also order by ASC wont do an sort on normal indexes, but if a streaming table then we will force it by using the following flags.
		
			flags = (HA_READ_NEXT |               HA_READ_RANGE | HA_KEYREAD_ONLY);
		}
		else
		{
			// for general case: support all operations 
			flags = (HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER | HA_READ_RANGE | HA_KEYREAD_ONLY);
		}
		
		return flags ;

		
	}

// initialize DB id and Table id.  Returns 0 if there is an error
unsigned short ha_scaledb::initializeDbTableId(char* pDbName, char* pTblName, bool isFileName,
        bool allowTableClosed) {
	unsigned short retValue = SUCCESS;
	if (!pDbName)
		pDbName = table->s->db.str;
	if (!pTblName)
		pTblName = table->s->table_name.str;

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::initializeDbTableId(pDbName=");
		SDBDebugPrintString(pDbName);
		SDBDebugPrintString(", pTblName=");
		SDBDebugPrintString(pTblName);
		SDBDebugPrintString(")");
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	this->sdbDbId_ = SDBGetDatabaseNumberByName(sdbUserId_, pDbName);
	if (this->sdbDbId_ == 0) {
		retValue = DATABASE_NAME_UNDEFINED;
		SDBTerminate(IDENTIFIER_INTERFACE + ERRORNUM_INTERFACE_MYSQL + 5,
				"Database definition is out of sync between MySQL and ScaleDB engine.\0" );
	}

	virtualTableFlag_ = SDBTableIsVirtual(pTblName);
	if (!virtualTableFlag_) {
		this->sdbTableNumber_ = SDBGetTableNumberByName(sdbUserId_, sdbDbId_, pTblName);
//		printTableId("ha_scaledb::initializeDbTableId(open):#1");
		if (this->sdbTableNumber_ == 0) {
			if (isFileName) {
				// We always try TableFsName again.  For some rare cases, we do not have tableName, but we have TableFsName. 
				sdbTableNumber_ = SDBGetTableNumberByFileSystemName(sdbUserId_, sdbDbId_, pTblName);
//				printTableId("ha_scaledb::initializeDbTableId(open):#2");
				if (sdbTableNumber_ == 0) {
					if (allowTableClosed)
						return TABLE_NAME_UNDEFINED;
					else {
						// Try one more time by opening the table
						sdbTableNumber_ = SDBOpenTable(sdbUserId_, sdbDbId_, pTblName, sdbPartitionId_, true); // bug137
//						printTableId("ha_scaledb::initializeDbTableId(open):#3");
						if (this->sdbTableNumber_ == 0) 
							retValue = TABLE_NAME_UNDEFINED;
					}
				}
			}

			if (this->sdbTableNumber_ == 0) {
				retValue = TABLE_NAME_UNDEFINED;
				SDBTerminate(IDENTIFIER_INTERFACE + ERRORNUM_INTERFACE_MYSQL + 6, // ERROR - 16010006
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
		SDBDebugPrintHeader(
		        "MySQL Interface: executing ha_scaledb::max_supported_key_part_length() ");
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif

	return SDBGetMaxKeyLength();	// scaledb_max_column_length_in_base_file;
}

void start_new_stmt_trx(THD *thd, handlerton *hton) {
	MysqlTxn* userTxn = (MysqlTxn *) *thd_ha_data(thd, hton);

	if (userTxn) {
		SDBSetStmtId( userTxn->getScaleDbUserId() );		// set the current LSN as an id for this statement
	}
}

void rollback_last_stmt_trx(THD *thd, handlerton *hton) {
	MysqlTxn* userTxn = (MysqlTxn *) *thd_ha_data(thd, hton);
	uint64 lastStmtId;
	unsigned int userId;

	if (userTxn) {
		userId = userTxn->getScaleDbUserId();
#ifdef SDB_DEBUG_LIGHT			
			SDBLogSqlStmt(userId, "ROLLBACK", thd->query_id); 
#endif
		lastStmtId = SDBGetStmtId( userId );
		SDBRollBackToSavePointId( userId, lastStmtId, false);

//		userTxn->setLastStmtSavePointId(previousSavePointId); // after the rollback the previous savepoint is the next one to use
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
int ha_scaledb::external_lock(THD* thd, /* handle to the user thread */
							  int lock_type) /* lock type */
{
	DBUG_ENTER("ha_scaledb::external_lock");
	DBUG_PRINT("enter", ("lock_type: %d", lock_type));

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
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
		default:
			SDBDebugPrintString("Lock Type ");
			SDBDebugPrintInt(lock_type);
			break;
		}
		SDBDebugPrintString(")");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif
#ifdef SDB_DEBUG
	++externalLockCounter_;
	//if (externalLockCounter_ == 0x000033ed){
	//	mysqlInterfaceDebugLevel_  = 1;
	//}
#endif
	
	bool is_streaming_table=SDBIsStreamingTable(sdbDbId_, sdbTableNumber_);
	//this code is used to determine if mysql has optimized out the where clause.
	if(lock_type==F_RDLCK)
	{
		SELECT_LEX*  lex=  (((THD*) ha_thd())->lex)->select_lex.parent_lex->current_select;
		if(lex->where!=NULL)
		{
			original_query_contains_condition=true;
		}
		else
		{
			original_query_contains_condition=false;
		}
	}
	int retValue = 0;

	placeSdbMysqlTxnInfo(thd);

	this->isIndexedQuery_		= false;
	this->isUserTimeIndexQuery_	= false;

	this->pushCondition_		= true;

	this->sqlCommand_ = thd_sql_command(thd);
	
	switch (sqlCommand_) {
	case SQLCOM_SELECT:
	case SQLCOM_HA_READ:
		sdbCommandType_ = SDB_COMMAND_SELECT;
		break;
	case SQLCOM_INSERT:
	case SQLCOM_INSERT_SELECT:
		{
			sdbCommandType_ = SDB_COMMAND_INSERT;
			break;
		}
	case SQLCOM_REPLACE_SELECT:
	case SQLCOM_REPLACE:
		{
			if(lock_type==F_WRLCK && is_streaming_table == true )
			{
				//if we are selecting from a scaledb streaming  table into an innodb table then OK, but if 
				//we are inserting into a streaming table then we fail (the lock check determines this)
				int err=ER_GET_ERRNO;
				my_error(err, MYF(ME_JUST_WARNING), HA_ERR_GENERIC, " REPLACE is not supported for Streaming Tables.");
				SDBRollBack(sdbUserId_, NULL, 0, true);
				DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
			}
			sdbCommandType_ = SDB_COMMAND_INSERT;
			break;
		}


	case SQLCOM_DELETE:
		sdbCommandType_ = SDB_COMMAND_DELETE;
		break;
	case SQLCOM_UPDATE:
		sdbCommandType_ = SDB_COMMAND_UPDATE;
		break;
	case SQLCOM_LOAD:
			sdbCommandType_ = SDB_COMMAND_LOAD;
			if(lock_type==F_UNLCK)
			{
				//table load has finished. add whatever you need todo at end here
				numOfInsertedRows_=0;
			}
			break;
	case SQLCOM_CREATE_TABLE:
		sdbCommandType_ = SDB_COMMAND_CREATE_TABLE;
		break;
	case SQLCOM_ALTER_TABLE:
	case SQLCOM_CREATE_INDEX:
	case SQLCOM_DROP_INDEX:
		sdbCommandType_ = SDB_COMMAND_ALTER_TABLE;
		break;	
	case SQLCOM_DELETE_MULTI:
	{
		sdbCommandType_ = SDB_COMMAND_MULTI_DELETE;
		break;	
	}
	case SQLCOM_UPDATE_MULTI:
	{
		sdbCommandType_ = SDB_COMMAND_MULTI_UPDATE;
		break;	
	}
	default:
		sdbCommandType_ = 0;
		break;
	}
#ifdef ALLOW_STREAMING_ALTER
	if(is_streaming_table == true &&
		( sdbCommandType_ == SDB_COMMAND_UPDATE
		|| sdbCommandType_ == SDB_COMMAND_MULTI_DELETE))
	{
		DBUG_RETURN(HA_ERR_WRONG_COMMAND);
	}
#else
	if(is_streaming_table == true &&
		( sdbCommandType_ == SDB_COMMAND_UPDATE
		|| sdbCommandType_ == SDB_COMMAND_ALTER_TABLE
		|| sdbCommandType_ == SDB_COMMAND_MULTI_DELETE))
	{
		DBUG_RETURN(HA_ERR_WRONG_COMMAND);
	}
#endif

	isStreamingHashIndex_=false;

	if(lock_type == F_RDLCK && sdbCommandType_ == SDB_COMMAND_SELECT && is_streaming_table==true)
	{
		char* s_force_analytics=SDBUtilFindComment(thd->query(), "force_sdb_analytics") ;
		if(s_force_analytics!=NULL){forceAnalytics_=true;}
		else {forceAnalytics_=false;} //reset
	}
	// fetch sdbTableNumber_ if it is 0 (which means a new table handler).
	if ((sdbTableNumber_ == 0) && (!virtualTableFlag_))
		retValue = initializeDbTableId();

	// need to check sdbDbId_ in MysqlTxn object because the same table handler may be used by a new user session.
	if (pSdbMysqlTxn_->getScaledbDbId() == 0)
		pSdbMysqlTxn_->setScaledbDbId(sdbDbId_);

	// determine if reads will be referenced again in the same SQL statement
	if (lock_type == F_WRLCK) {
		releaseLocksAfterRead_ = false;
	} else {
		releaseLocksAfterRead_ = true;
	}

	// lock the table if the user has an explicit lock tables statement
	if (sqlCommand_ == SQLCOM_UNLOCK_TABLES) { // now the user has an explicit UNLOCK TABLES statement

		//InnoDB does NOT commit when table lock released. 
		//however  we are holding table locks
		int ret=SDBCommit(sdbUserId_,true); //release any previously held lock so i can get an exclusive
		DBUG_RETURN(0);
	}

	if (lock_type != F_UNLCK) {
#ifdef SDB_DEBUG_LIGHT
		SDBLogSqlStmt(sdbUserId_, thd->query(), thd->query_id); // inform engine to log user query for DML
#endif
		bool bAutocommit = false;
		bool all_tx = false;

		// lock the table if the user has an explicit lock tables statement	
		if ((sqlCommand_ == SQLCOM_LOCK_TABLES )) { // now the user has an explicit LOCK TABLES statement

			//do not allow table locks if we are in autocommit mode
			//this is to support the mariadb documentation
			// https://mariadb.com/kb/en/mariadb/lock-tables-and-unlock-tables/
			// also because the 'explicit' commit will not get called into the engine in autocommit mode and so
			// there is no way to rollback the table locks on commit
			if (thd_in_lock_tables(thd)) {

				bool inTransaction =(thd_test_options(thd, OPTION_NOT_AUTOCOMMIT )  ? true : false );
		//		if (inTransaction)
                               if(true)
				{
					unsigned char tableLockLevel = REFERENCE_READ_ONLY; // table level read lock
					if (lock_type > F_RDLCK)
						tableLockLevel = REFERENCE_LOCK_EXCLUSIVE; // table level write lock

					bool bGetLock = SDBLockTable(sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_, tableLockLevel);

					if (bGetLock == false) // failed to lock the table
						DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));

				}		

				// Need to register with MySQL for the beginning of a transaction so that we will be called
				// to perform commit/rollback statements issued by end users.  LOCK TABLES implies a transaction.
				bool all=thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);
				trans_register_ha(thd, all, ht);

			}
		} else { // the normal case
			if (pSdbMysqlTxn_->lockCount_ == 0) {
				//  when new command is not bulk insert ( select  etc.) it can read invalid rows. In this case
				//  wait until the index thread insert the rows to the indexes before starting the new statement 				
				if(	!(sdbCommandType_ == SDB_COMMAND_INSERT || sdbCommandType_ == SDB_COMMAND_LOAD)  )  
				{
					SDBUSerSQLStatmentEnd(sdbUserId_);	
				}
		
				// clear the bulk variables for new statement 
				if ( numOfInsertedRows_ > 0 ) {
					numOfBulkInsertRows_ = 1;
					numOfInsertedRows_ = 0;
				}
				

				pSdbMysqlTxn_->txnIsolationLevel_ = thd->variables.tx_isolation;

#ifdef _MARIA_DB
				bAutocommit =(thd_test_options(thd, OPTION_NOT_AUTOCOMMIT)  ? false : true );
			        all_tx = thd_test_options(thd, (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN | OPTION_TABLE_LOCK)); //TBD ??
#else
				bAutocommit = ( test( thd->options & OPTION_NOT_AUTOCOMMIT ) ? false : true );
				all_tx = test(thd->options & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN
				        | OPTION_TABLE_LOCK)); //TBD ??
#endif
				// issue startTransaction() only for the very first statement
				if (pSdbMysqlTxn_->getActiveTxn() == false) {

					bool in_transaction=false;
					if(bAutocommit ==true && all_tx==true || bAutocommit==false)
					{
						//there are 2 cases when we are in a transaction. IF autocommit is OFF or if autcommit is ON but we issued a START TRANSACTION
						in_transaction=true;
					}
					

					sdbUserId_ = pSdbMysqlTxn_->getScaleDbUserId();
					SDBStartTransaction( sdbUserId_, !in_transaction );
					long txnId = SDBGetTransactionIdForUser(sdbUserId_);
					pSdbMysqlTxn_->setScaleDbTxnId(txnId);
					pSdbMysqlTxn_->setActiveTrn(true);
					if ( !sdbQueryMgrId_) {
						setSdbQueryMgrId();
					}
#ifdef SDB_DEBUG
					int n=SDBgetNumberOfActiveQMIds(sdbUserId_);
#endif
				}
			}


			// we need to impose a shared session lock in order to protect the memory metadata to be used in DML. 
			// For all DML commands the  session lock is DEFAULT_REFERENCE_LOCK_LEVEL 
			assert(sdbUserId_!=0);
		    assert(sdbTableNumber_!=0);
			if(sdbCommandType_ == SDB_COMMAND_ALTER_TABLE)
			{
				SDBCommit(sdbUserId_,true); //release any previously held lock so i can get an exclusive
				if (!lockDDL(sdbUserId_, sdbDbId_, sdbTableNumber_, 0)){
				//untill mariadb supports generic errors, just return lock failed
#ifdef _MARIADB_FIXED_MDEV_8453
					SDBSetErrorMessage( sdbUserId_, TABLE_IN_USE_ON_ANOTHER_NODE, " table cannot be dropped because it is in use or is open on another MySql node." );
					SDBRollBack(sdbUserId_, NULL, 0, true);
					DBUG_RETURN(convertToMysqlErrorCode(TABLE_IN_USE_ON_ANOTHER_NODE));

#else
					//lets push a warning without using the mysql interface
					int err=ER_GET_ERRNO;
					my_error(err, MYF(ME_JUST_WARNING), HA_ERR_GENERIC, " table cannot be dropped because it is in use or is open on another MySql node.");
					SDBRollBack(sdbUserId_, NULL, 0, true);

					DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
#endif

				}


			}else
			{

				if (!lockDML(sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_)) {
					SDBRollBack(sdbUserId_, NULL, 0, true);
					DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
				}
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

		// this is primary node
		if ((sdbCommandType_ == SDB_COMMAND_ALTER_TABLE) && (strstr(table->s->table_name.str,MYSQL_TEMP_TABLE_PREFIX) == NULL)) {
			// We can find the alter-table name in the first external_lock on the regular table name
			char to[512];
			uint n=tablename_to_filename( table->s->table_name.str, to, sizeof(to));
			pSdbMysqlTxn_->addAlterTableName(to);
		}

		/*
		else if (txn->stmt == 0) {  //TBD: if there is no savepoint defined
		txn->stmt= txn->new_savepoint();	// to add savepoint
		trans_register_ha(thd, false, ht);
		}
		*/
	} else { // (lock_type == F_UNLCK), need to release locks
	
		// free the query manger of the current select
#ifdef SDB_DEBUG
		int n1=SDBgetNumberOfActiveQMIds(sdbUserId_);
#endif
		unsetSdbQueryMgrId();
#ifdef SDB_DEBUG
		int n2=SDBgetNumberOfActiveQMIds(sdbUserId_);
#endif

		if (pSdbMysqlTxn_->lockCount_ > 0) {

			// one less table in the transaction 
			pSdbMysqlTxn_->lockCount_ -= 1;

			//  if release all locks - only when take at least one 
			if (pSdbMysqlTxn_->lockCount_ == 0) 
			{ 

#ifdef SDB_DEBUG
				// When a user execute a query, he assigns a query manager to the query. The query manager maintains the state of the query.
				// When the query is done the query manager needs to be released.
				// The code that tests SDBCheckAllQueryManagersAreFree - needs to show 0 indicating no uncompleted queries at this point.
				// SDBGetQueryManagerId - gets a query manager id for this user.
				// SDBFreeQueryManager - releases the query manager id for the user.

				SDBCheckAllQueryManagersAreFree(sdbUserId_);

#endif
				if (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) 
				{   
					// in auto_commit mode
					// MySQL query processor does NOT execute autocommit for pure SELECT transactions.
					// We should commit for SELECT statement in order for ScaleDB engine to release the right locks.
					if (sqlCommand_ == SQLCOM_SELECT || sqlCommand_ == SQLCOM_HA_READ) {

						SDBCommit(sdbUserId_, false); // call engine to commit directly
					}	

					if (sdbCommandType_ == SDB_COMMAND_ALTER_TABLE) {

						SDBCommit(sdbUserId_, true); // call engine to commit directly
					}		

				}	
			}
		}
		

		
		if ((sdbCommandType_ == SDB_COMMAND_ALTER_TABLE) && (strstr(table->s->table_name.str,
		        MYSQL_TEMP_TABLE_PREFIX) == NULL)) {
			// We can find the alter-table name in the first external_lock on the regular table name
			pSdbMysqlTxn_->removeAlterTableName();
		}
		//if alter has finished then we need to release the session lock
		//this lock might have already got release when the alter completes (but if mysql decides we dont support the alter then then need todo here)
		if(sdbCommandType_ == SDB_COMMAND_ALTER_TABLE)
		{
				SDBCommit(sdbUserId_,true);
			
		}
		sdbCommandType_ = 0;	// Reset command type

		


	} 

	DBUG_RETURN(0);
}

// If a user issues LOCK TABLES statement, MySQL will call ::external_lock only once to release lock.
// In this case, MySQL will call this method at the beginning of each SQL statement after LOCK TABLES statement.
int ha_scaledb::start_stmt(THD* thd, thr_lock_type lock_type) {
	DBUG_ENTER("ha_scaledb::start_stmt");
	DBUG_PRINT("enter", ("lock_type: %d", lock_type));

	this->sqlCommand_ = thd_sql_command(thd);
	//  when command is not bulk insert ( select  etc.) it can read invalid rows. In this case
	//  wait until the index thread insert the rows to the indexes before starting the new statement 
	if (  !(sdbCommandType_ == SDB_COMMAND_INSERT || sdbCommandType_ == SDB_COMMAND_LOAD)  )  
	{
		SDBUSerSQLStatmentEnd(sdbUserId_);	
	}
	// clear the bulk variables for new statement 
	if ( numOfInsertedRows_ > 0 ) {
		numOfBulkInsertRows_ = 1;
		numOfInsertedRows_ = 0;
	} 
	
	print_header_thread_info("MySQL Interface: executing ha_scaledb::start_stmt()");

	int retValue = 0;
	placeSdbMysqlTxnInfo(thd);

	
	switch (sqlCommand_) {
	case SQLCOM_SELECT:
	case SQLCOM_HA_READ:
		sdbCommandType_ = SDB_COMMAND_SELECT;
		break;
	case SQLCOM_INSERT:
	case SQLCOM_INSERT_SELECT:
		sdbCommandType_ = SDB_COMMAND_INSERT;
		break;
	case SQLCOM_DELETE:
		sdbCommandType_ = SDB_COMMAND_DELETE;
		break;
	case SQLCOM_UPDATE:
		sdbCommandType_ = SDB_COMMAND_UPDATE;
		break;
	case SQLCOM_LOAD:
		sdbCommandType_ = SDB_COMMAND_LOAD;
		break;
	case SQLCOM_CREATE_TABLE:
		sdbCommandType_ = SDB_COMMAND_CREATE_TABLE;
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

	if (sdbCommandType_ == SDB_COMMAND_ALTER_TABLE) {
		if (strstr(table->s->table_name.str, MYSQL_TEMP_TABLE_PREFIX) == NULL) {
			// We can find the alter-table name (same logic used in the first external_lock when there is no LOCK TABLES)
			char to[512];
			uint n=tablename_to_filename( table->s->table_name.str, to, sizeof(to));
			pSdbMysqlTxn_->addAlterTableName(to); // must be the regular table name, not temporary table
		}
	}

	bool all_tx = false;
	if (pSdbMysqlTxn_->lockCount_ == 0) {
		pSdbMysqlTxn_->txnIsolationLevel_ = thd->variables.tx_isolation;

		if ((sdbTableNumber_ == 0) && (!virtualTableFlag_))
			retValue = initializeDbTableId();

		sdbUserId_ = pSdbMysqlTxn_->getScaleDbUserId();
		pSdbMysqlTxn_->setScaledbDbId(sdbDbId_);
		long txnId = SDBGetTransactionIdForUser(sdbUserId_);
		pSdbMysqlTxn_->setScaleDbTxnId(txnId);
		pSdbMysqlTxn_->setActiveTrn(true);
	}

	pSdbMysqlTxn_->lockCount_ += 1;

	// Need to register with MySQL for the beginning of a transaction so that we will be called
	// to perform commit/rollback statements issued by end users
	trans_register_ha(thd, false, ht);

	all_tx = (thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN) > 0) ? true : false;
	if (all_tx)
		trans_register_ha(thd, true, ht);
	else
		start_new_stmt_trx(thd, ht);

	DBUG_RETURN(0);
}

const char **ha_scaledb::bas_ext() const {
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

// Debug the MySQL calls
#ifdef SDB_DEBUG
void ha_scaledb::debugHaSdb(char *funcName, const char* name1, const char* name2, TABLE *table_arg){

	bool showSql = false;
	THD* thd = ha_thd();
	char *sqlQuery = thd->query();
	unsigned long long sqlQueryId = thd->query_id;
	unsigned int sqlCommand = thd_sql_command(thd);
	char* pDbName;
	char* pTableName;
	unsigned short numberOfPartitions = 0;


	if (table_arg){
		pDbName = table_arg->s->db.str; // points to user-defined database name
		char to[512];
		uint n=tablename_to_filename( table_arg->s->table_name.str, to, sizeof(to));
		pTableName=to;

		if (table_arg->part_info) {
#ifdef _MARIA_DB
			numberOfPartitions = table_arg->part_info->num_parts;
#else
			numberOfPartitions = table_arg->part_info->no_parts;
#endif

		}
	}

	if (showSql){
		SDBDebugPrintNewLine();
		SDBDebugPrint8ByteUnsignedLong(sqlQueryId);
		SDBDebugPrintString(" - ");
		SDBDebugPrintString(sqlQuery);
	}
}
#endif


// This method is outside of a user transaction.  It opens a user database and saves value in sdbDbId_.
// It does not open individual table files.
unsigned short ha_scaledb::openUserDatabase(char* pDbName, char *pDbFsName, unsigned short & dbId, unsigned short nonTxnUserId, MysqlTxn* pSdbMysqlTxn) {

	unsigned short retCode = 0;

	// Because this method can be called outside of a normal user transaction,
	// hence may use a different user id to open database and table in order to avoid commit issue.
	unsigned int userIdforOpen = nonTxnUserId ? nonTxnUserId :  pSdbMysqlTxn->getScaleDbUserId() ;

	// if user transaction and no dbid (!sdbDbId_  && !nonTxnUserId) OR
	// if non-transactional user (|| nonTxnUserId) the sdbDbId_ should be replaced because it belongs to the txn 
	if (!dbId  ||  nonTxnUserId) {
		// try to find previousily created table with the same name
		// set sdbDbId_ locally for this API (sdbDbId_ is update from Txn in each API call)
			dbId = SDBGetDatabaseNumberByName(userIdforOpen, (char *)pDbName);
//CAS_EXCEPTION
			if(dbId==0) 
			{
				return HA_ERR_GENERIC;
			}

		if (!dbId) {
			// open the database -  
			dbId = SDBOpenDatabaseByName(userIdforOpen, (char *)pDbName, (char *)pDbFsName);
			if(!dbId) {		
				retCode = TABLE_NAME_UNDEFINED;
			}			
		}		
		// inside txn  
		if (pSdbMysqlTxn) 
		{
			// set the new dbid for the transaction 
			pSdbMysqlTxn->setScaledbDbId(dbId);			
		}
	}

	return retCode;
}


static int cmpr_frms(unsigned char * frm1, unsigned char * frm2, int len)
{

	if( (frm1 == NULL) ^ (frm2 == NULL))
		return 1;
	return memcmp(frm1, frm2, len);
}


int ha_scaledb::checkFrmCurrent(const char *name, uint userIdforOpen, uint dbId, uint tableId)
{

#ifdef DISABLE_DISCOVER
		return(1);
#endif


#ifdef SDB_DEBUG_LIGHT

	if (mysqlInterfaceDebugLevel_) {
		print_header_thread_info("MySQL Interface: executing ha_scaledb::checkFrmCurrent");
		SDBDebugStart();
		SDBDebugPrintHeader("ha_scaledb::checFrmCurrent name = ");
		SDBDebugPrintString((char *) name);
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif  

	unsigned char *frmBlob;
	unsigned char *frmData;
	unsigned int frmLen;
	size_t  dataLen;
	int retCode = 0;
// Build the name of the .frm file that we want to use to
//   update our copy, since we altered the table..


#ifdef SDB_WINDOWS
	// Windows
	char frmFilePathName[80];
	strcpy(frmFilePathName, ".\\" );
	strcat(frmFilePathName, SDBGetDatabaseNameByNumber(sdbDbId_));
	strcat(frmFilePathName, "\\" );
	strcat(frmFilePathName, name);
#else
	// Linux
	char frmFilePathName[80];
	strcpy(frmFilePathName, "./" );
	strcat(frmFilePathName, SDBGetDatabaseNameByNumber(sdbDbId_));
	strcat(frmFilePathName, "/" );
	strcat(frmFilePathName, name);
#endif

	retCode = readfrm(frmFilePathName,(const uchar**) &frmData, &dataLen);

	if(retCode) {
//		my_free(frmData, MYF(0));
		return 0;
	}
	frmBlob = (unsigned char *)SDBGetFrmData( userIdforOpen, dbId, tableId, &frmLen, NULL,NULL);
	if(!frmBlob ) {
		assert(frmData);
#ifdef _MARIA_DB
             	my_free(frmData);
#else
                my_free(frmData, MYF(0));
#endif

		return 0;
	}

	retCode = 0;
	if( dataLen != frmLen || cmpr_frms( frmData, frmBlob, frmLen))
	        retCode = HA_ERR_TABLE_DEF_CHANGED;

	assert(frmData);
#ifdef _MARIA_DB
             	my_free(frmData);
#else
                my_free(frmData, MYF(0));
#endif

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
		SDBDebugPrintString((char *) name);
		SDBDebugFlush();
		SDBDebugEnd();
	}
#endif
#ifdef OPEN_CLOSE_TABLE_DEBUG_
	SDBDebugStart();
	SDBDebugPrintHeader("\nMySQL Interface: start executing ha_scaledb::open(void) ");
	SDBDebugPrintString(", table=");
	SDBDebugPrintString(table->s->table_name.str);
	SDBDebugEnd();
#endif
	int errorNum = HA_ERR_NO_SUCH_TABLE; // init to general fail 
	unsigned short retCode = 0;
	char dbFsName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 }; // database name that is compliant with file system
	char tblFsName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 }; // table name that is compliant with file system
	char pathName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };
	char fullName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };
	fetchIdentifierName(name, dbFsName, tblFsName, pathName);

	char* pTblFsName = &tblFsName[0]; // points to the beginning of tblFsName

	THD* thd = ha_thd();
	placeSdbMysqlTxnInfo(thd);
	unsigned int sqlCommand = thd_sql_command(thd);
	stats.block_size = METAINFO_BLOCK_SIZE; // index block size
	bool bIsAlterTableStmt = false;
	int tid=0;
	unsigned int frmLength = 0;
	char * frmData;
	uchar *pack_data= NULL;
	size_t  pack_length;

#ifdef SDB_DEBUG
	if (mysqlInterfaceDebugLevel_ > 4)
		SDBShowAllUsersLockStatus();
#endif

	if (ha_scaledb::isAlterCommand(sqlCommand) ) {
			bIsAlterTableStmt = true;
	}

	virtualTableFlag_ = SDBTableIsVirtual(pTblFsName);
	if (virtualTableFlag_) {
		pTblFsName = pTblFsName + SDB_VIRTUAL_VIEW_PREFIX_BYTES;
		char* pUnderscores = strstr(pTblFsName, "___");
		*pUnderscores = 0; //  make pTblFsName stop at the beginning of "___"
	}

	// Because MySQL calls this method outside of a normal user transaction,
	// hence we use a different user id to open database and table in order to avoid commit issue.
     unsigned int userIdforOpen = SDBGetNewUserId();
	 SessionSharedMetaLock ot;
	// First, we open the user database by retrieving the metadata information.
	retCode = ha_scaledb::openUserDatabase(table->s->db.str, dbFsName, sdbDbId_,userIdforOpen,NULL);
	if (retCode) {
		errorNum = convertToMysqlErrorCode(retCode);
        saveSDBError(userIdforOpen);
		goto open_fail;
	}

	//check for partition name and copy it.
	if (SDBUtilStrstrCaseInsensitive(pTblFsName, "#P#")) {
		char partitionName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };
		getAndRemovePartitionName(pTblFsName, partitionName);

		sdbPartitionId_ = SDBGetPartitionId(userIdforOpen, sdbDbId_, partitionName, pTblFsName);
	}

	//the fullname is needed by readfrm functions. includes path and table name without partition info
	//this must happen afeter the partition info has been stripped out
	strcat(fullName, pathName );
	strcat(fullName, dbFsName);	
#ifdef SDB_WINDOWS
	// Windows
	strcat(fullName, "\\" );
#else	// Linux
	strcat(fullName, "/" );
#endif
	strcat(fullName, tblFsName);

	ot.set(sdbDbId_);
	if(ot.lock()==false)
        {
       	SDBRemoveUserById(userIdforOpen);
	    DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
	}

	// after getting the upated version - open the table
	sdbTableNumber_ = SDBOpenTable(userIdforOpen, sdbDbId_, pTblFsName, sdbPartitionId_, false);
 
//added for debugging
//	int no_shards=SDBNumberShards(sdbDbId_,sdbTableNumber_);
//
	if ( !sdbTableNumber_ ) {
		goto open_fail;
	}

	SDBCommit(userIdforOpen, true);  //
       assert(sdbTableNumber_!=0);
	if (!ha_scaledb::lockDML(userIdforOpen, sdbDbId_, sdbTableNumber_, 0)) {
		errorNum = convertToMysqlErrorCode(LOCK_TABLE_FAILED);
		goto open_fail;
	}

	// on real open-table's - get the FRM
	if( !bIsAlterTableStmt ) {		
		SDBCommit(userIdforOpen, false);	// the commit is needed as the open got the table at level 2 and the getFrm asks for a table lock at level 1.

		//get the frm
		frmData = SDBGetFrmData(userIdforOpen, sdbDbId_, sdbTableNumber_, &frmLength,NULL,NULL);
		if (frmData==NULL)
		{
			//no frm metatable missing, maybe old meta data.
			goto open_fail;	
		}

		// update the FRM version from local env 
		packfrm((uchar *)frmData, frmLength, &pack_data, &pack_length);
		SDBCommit(userIdforOpen, false);

		uchar *data= NULL, *pack_data_local= NULL;
		size_t length, pack_length_local;

		if (readfrm(fullName, (const uchar**)&data, &length) ||

			packfrm(data, length, &pack_data_local, &pack_length_local))
		{

			my_free(data);
			my_free(pack_data_local);

			goto open_fail;
		}
		else {
			//now compare 
			info( HA_STATUS_NO_LOCK | HA_STATUS_VARIABLE | HA_STATUS_CONST );
			if(pack_length_local != pack_length || memcmp(pack_data_local,pack_data,  pack_length_local)!=0)
			{
				errorNum =HA_ERR_TABLE_DEF_CHANGED;	
				goto open_fail;
			}
		}
 	}	
	
	// set info variables 
	if ( (errorNum = info(HA_STATUS_VARIABLE | HA_STATUS_CONST)) )
	{
		goto open_fail;
	}
	
	//  lock the table for MySQL
	if (!(share = get_share(name, table))) {
		goto open_fail;
	}
	thr_lock_data_init(&share->lock, &lock, NULL);
	
	//open_sucess:
	errorNum = 0;
	SDBCommit(userIdforOpen, false);
	SDBRemoveUserById(userIdforOpen);
	DBUG_RETURN(errorNum);	//leave shared session lock on table

open_fail:
	// We can commit without problem for this new user
	SDBCommit(userIdforOpen, true);
	SDBRemoveUserById(userIdforOpen);	
#ifdef OPEN_CLOSE_TABLE_DEBUG_
	SDBDebugStart();
	SDBDebugPrintHeader("\nMySQL Interface: end executing ha_scaledb::open(void) ");
	SDBDebugPrintString(", table=");
	SDBDebugPrintString(table->s->table_name.str);
	SDBDebugPrintString(", errorNum=");
	SDBDebugPrintInt(errorNum);
	SDBDebugEnd();
#endif
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
	THD* thd = current_thd; // need to use this function in order to get the correct user query in a multi-user environment
	// another possibility is to use ha_thd() to fetch current user thread.  may try this one later.

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::close(void) ");
		SDBDebugPrintString(", handler=");
		SDBDebugPrint8ByteUnsignedLong((unsigned long long) this);
		SDBDebugPrintString(", thd=");
		SDBDebugPrint8ByteUnsignedLong((unsigned long long) thd);
		if (thd) {
			SDBDebugPrintString(", Query:");
			SDBDebugPrintString(thd->query());
		}
		SDBDebugEnd();
	}
#endif

	int errorCode = 0;
	bool bGetNewUserId = false;
	bool needToRemoveFromScaledbCache = false;
	bool needToCommit = false;
	bool flushTables =false;
	unsigned short ddlFlag = 0;
	if (virtualTableFlag_) { // do nothing for virtual table
		sdbTableNumber_ = 0;
		DBUG_RETURN(free_share(share));
	}

	unsigned int sqlCommand;
	if (thd) {
		// thd is defined.  In this case, a user executes a DDL.
		placeSdbMysqlTxnInfo(thd);

		sqlCommand = thd_sql_command(thd);

		if (ha_scaledb::isAlterCommand(sqlCommand) ) {
			// filter statement such as  ALTER TABLE t1 DISABLE KEYS;
			// on the above case - don't close the table i.e needToRemoveFromScaledbCache = false;
			if (pSdbMysqlTxn_->getDdlFlag() & SDBFLAG_ALTER_TABLE_CREATE) {
				// For regular ALTER TABLE, primary node will commit in delete_table method which is at very end of processing.
				needToRemoveFromScaledbCache = true;
			}
		} else if ( sqlCommand == SQLCOM_DROP_TABLE || sqlCommand == SQLCOM_DROP_DB || sqlCommand == SQLCOM_RENAME_TABLE || sqlCommand == SQLCOM_ALTER_TABLESPACE) {
			needToRemoveFromScaledbCache = true;
		} else if (sqlCommand == SQLCOM_FLUSH || sqlCommand == SQLCOM_CREATE_TABLE)  { // test case 206.sql
			needToRemoveFromScaledbCache = true;
			needToCommit = true;
			if(sqlCommand == SQLCOM_FLUSH) {
				flushTables=true;
			}
		}
	}
	else { // thd is NOT defined.  A system thread closes the opened tables.  Bug 969
		sdbUserId_ = SDBGetNewUserId(); // avoid using system user ID
		bGetNewUserId = true;
		needToRemoveFromScaledbCache = true;
		needToCommit = false;	// should not commit if a user has pending updates.
	}

	// MySQL may call this method multiple times on same table if a table is referenced more than
	// one time in a previous SQL statement.  Example: INSERT INTO tbug261 SELECT  a + 1 , b FROM tbug261;
	// Need to call SDBCloseTable instead of SDBCloseFile so that ScaleDB engine tolerates multiple calls of this method.
	// Also MySQL instantiates a table handler object if another user thread is holding a handler object for the same table.
	// MySQL remembers how many handler objects it creates for a given table.  It will call this method one time
	// for each instantiated table handler.  Hence we remove table information from metainfo for DDL/FLUSH statements only
	// or this method is called from a system thread (such as mysqladmin shutdown command).
	if (needToRemoveFromScaledbCache) 
	{
	
		SDBCommit(sdbUserId_,true); // release previous locks - in partiular older session lock on this table

		SessionExclusiveMetaLock ot(sdbDbId_,sdbUserId_);
		if(ot.lock()==false)
		{

			DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
		}	

		//now going to release ALL the session locks for this table on this DB node.
		//if there is only 1 DB node or all the session open for the table are on this DB node, then the close will
		// be successfull.
		// if there is a session lock for table from another DB node then the close will fail
		
		//note* irrespective of what we do, mysql will close the table.
		//if the close is part of a table drop then we will also check if any other session has table open, and if they have
		//we will return an error then.

		releaseAllUsersSessionLocks(sdbUserId_, sdbDbId_, sdbTableNumber_) ; 

		char to[512];
		uint n=tablename_to_filename( table->s->table_name.str, to, sizeof(to));

		// exclusive lock of the table (before the lock)
         assert(sdbTableNumber_!=0);
		if (!lockDDL(sdbUserId_, sdbDbId_, sdbTableNumber_, 0)) {	
     		errorCode = SDBCloseTable(sdbUserId_, sdbDbId_, to, sdbPartitionId_, true,needToCommit, flushTables, true);
			SDBRollBack(sdbUserId_, NULL, 0, true);
			DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));			
		}

		if(flushTables)
		{
			errorCode = SDBCloseTableCascade(true,sdbUserId_, sdbDbId_, to, sdbPartitionId_, true,needToCommit, flushTables, true);
		}
		else
		{
			errorCode = SDBCloseTable(sdbUserId_, sdbDbId_, to, sdbPartitionId_, true,needToCommit, flushTables, true);
		}		

		if (errorCode){
			if (bGetNewUserId){
				SDBRemoveUserById(sdbUserId_);
			}
			DBUG_RETURN(convertToMysqlErrorCode(errorCode));
		}

		
		SDBCommit(sdbUserId_,true); //table closed, so release session lock
	}

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 1) {
		SDBDebugStart(); // synchronize threads printout
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

		SDBDebugEnd(); // synchronize threads printout
	}
#endif

#ifdef SDB_DEBUG
	if (mysqlInterfaceDebugLevel_ > 4) {
		// print user lock status on the primary node
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("In ha_scaledb::close, print user locks after SDBCloseTable");
		SDBDebugEnd(); // synchronize threads printout
		SDBShowUserLockStatus(sdbUserId_);
	}
#endif

	// set table number to 0 since this table handler is closed.
	//	printTableId("ha_scaledb::close");
	sdbTableNumber_ = 0;
	if (bGetNewUserId){
		SDBRemoveUserById(sdbUserId_);
	}
	DBUG_RETURN(free_share(share));
}

// This method saves a MySQL transaction information for a given user thread.
// When isTransient is true (outside the pair of external_locks calls), we do NOT save information into ha_scaledb.
// When it is false, we save the returned pointer (possible others) into ha_scaledb member variables.
MysqlTxn* ha_scaledb::placeSdbMysqlTxnInfo(THD* thd) {

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::placeSdbMysqlTxnInfo(thd=");
		SDBDebugPrint8ByteUnsignedLong((uint64) thd);
		SDBDebugPrintString(", isTransient=");
		if (mysqlInterfaceDebugLevel_ > 1) {
			SDBDebugPrintString(", handler=");
			SDBDebugPrint8ByteUnsignedLong((uint64) this);
		}
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	MysqlTxn* pMysqlTxn = (MysqlTxn *) *thd_ha_data(thd, ht);
	if (pMysqlTxn == NULL) { // a new user session
		pMysqlTxn = new MysqlTxn();
#ifdef SDB_DEBUG_LIGHT
		if (ha_scaledb::mysqlInterfaceDebugLevel_) {
			SDBDebugStart(); // synchronize threads printout
			SDBDebugPrintHeader("MySQL Interface: new MysqlTxn");
			SDBDebugPrintString(", thd=");
			SDBDebugPrint8ByteUnsignedLong((uint64) thd);
			SDBDebugPrintString(", pMysqlTxn=");
			SDBDebugPrint8ByteUnsignedLong((uint64) pMysqlTxn);
			SDBDebugEnd(); // synchronize threads printout
		}
#endif
		pMysqlTxn->setMysqlThreadId(thd->thread_id);
		unsigned int scaleDbUserId = SDBGetNewUserId();
		pMysqlTxn->setScaleDbUserId(scaleDbUserId);
		pMysqlTxn->initGlobalInsertsCount();

		// save the new pointer into the per-connection place
		*thd_ha_data(thd, ht) = pMysqlTxn;
	}


	// save into member variables 
	this->pSdbMysqlTxn_ = pMysqlTxn;
	this->sdbUserId_ = pMysqlTxn->getScaleDbUserId();


	return pMysqlTxn;
}

unsigned short ha_scaledb::placeMysqlRowInEngineBuffer(unsigned char* rowBuf1,	unsigned char* rowBuf2, unsigned short groupType, 
	bool checkAutoIncField, bool updateBlobContent) {
	
	// reset SDB row buffers 
	SDBResetRow(sdbUserId_, sdbDbId_, sdbPartitionId_, sdbTableNumber_, groupType);
	
	// build template at the begin of scan 
	buildRowTemplate(table, rowBuf1, checkAutoIncField );

	// prepare row - in case of overflow blocks (blob + varchar) might be  getting from cache we need to enter + exit engine 
	if ( rowTemplate_.numOfOvf_ )  {
		return SDBPrepareRowByTemplateEnterExit(sdbUserId_, rowBuf1, rowTemplate_,sdbDbId_,groupType);
	}
	else {
		return SDBPrepareRowByTemplate(sdbUserId_, rowBuf1, rowTemplate_,sdbDbId_,groupType);
	}
}
	

/* 
 write_row() inserts a row.
 Parameter buf is a byte array of data in MySQL format with a size of table->s->reclength.
 You can use the field information to extract the data from the native byte array type.
 table->s->fields: gives the number of column in the current open table.
 */
int ha_scaledb::write_row(unsigned char* buf) {
	unsigned short retValue;

	DBUG_ENTER("ha_scaledb::write_row");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::write_row(...) on table ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(" ");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}

	// print out the integer key value
	//SDBDebugStart(); // synchronize threads printout
	//SDBDebugPrintString("\nid = ");
	//int intValue = *( (int*)table->field[0]->ptr );  // get first column value which is an integer
	//SDBDebugPrintInt(intValue);
	//SDBDebugEnd(); // synchronize threads printout
#endif

	
#ifdef SDB_SUPPORT_HANDLER_SOCKET_WRITE
	// on handler socket writes - fake  bulk insert 
	if ( IS_HANDLER_SOCKET_WRITE_THREAD(ha_thd())) {
		// maintain global counter + set the bulk variables 
		numOfBulkInsertRows_ = HANDLER_SOCKET_WRITE_BULK;
		numOfInsertedRows_ =  HANDLER_SOCKET_WRITE_INSERTS_COUNT(pSdbMysqlTxn_->incGlobalInsertsCount());
	}
#endif

// if load from file then dont know how many rows in batch so set to 0.
// might be able to improve later because we do know when the batch ends. (ha_end_batch_insert)
	if (sqlCommand_==SQLCOM_LOAD)
	{	
		numOfInsertedRows_=0;
	}

	
	int errorNum = 0;
	ha_statistic_increment(&SSV::ha_write_count);

	my_bitmap_map* org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);

	retValue = placeMysqlRowInEngineBuffer((unsigned char *) buf, (unsigned char *) buf, 0, true, true);

	if (retValue == 0) {
		retValue = SDBInsertRowAPI(sdbUserId_, is_delayed_insert, sdbDbId_, sdbTableNumber_, sdbPartitionId_,numOfBulkInsertRows_,numOfInsertedRows_++,
		        ((THD*) ha_thd())->query_id, sdbCommandType_);
	}
	
	// if insert row with auto increment  
	if (retValue == 0 &&table->found_next_number_field) { 
		// update stats 
		stats.auto_increment_value = SDBGetAutoIncrValue(sdbDbId_, sdbTableNumber_);
		// update for the use of: "select from last_insert_id()" 
		((THD*) ha_thd())->first_successful_insert_id_in_cur_stmt= stats.auto_increment_value;
		// mysql_insert() uses this for protocol return value 
		// binary log use this value to replicate the inserted rows with Autoinc values 
		table->next_number_field->store(SDBGetActualAutoIncrValue(sdbUserId_,sdbDbId_, sdbTableNumber_), 1);
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

	char* warn=SDBGetLastWarningMessage(sdbUserId_);

	if(warn)
	{
		//lets push a warning
		 my_error(ER_WRONG_VALUE, MYF(ME_JUST_WARNING), "", warn);
	}
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: ha_scaledb::write_row(...) returning ");
		SDBDebugPrintInt(errorNum);
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	dbug_tmp_restore_column_map(table->read_set, org_bitmap);
	DBUG_RETURN(errorNum);
}


int ha_scaledb::check( THD* thd, HA_CHECK_OPT* check_opt )
{
	DBUG_ENTER( "ha_scaledb::check" );

	info( HA_STATUS_NO_LOCK | HA_STATUS_TIME | HA_STATUS_VARIABLE | HA_STATUS_CONST );

	DBUG_RETURN( HA_ADMIN_OK );
}

// Update HA_CREATE_INFO object.  Used in SHOW CREATE TABLE and ALTER TABLE ... enable keys
void ha_scaledb::update_create_info(HA_CREATE_INFO* create_info) {
	if(table==NULL) 
	{
		return;
	}
	print_header_thread_info("MySQL Interface: executing ha_scaledb::update_create_info(...)");

	THD* thd = ha_thd();
	placeSdbMysqlTxnInfo(thd);

	unsigned short retValue = 0;
	// need to set Dbid and TableId in case SHOW CREATE TABLE is the first statement in a user session
	if ((sdbTableNumber_ == 0) && (!virtualTableFlag_))
		retValue = initializeDbTableId();

	info( HA_STATUS_CONST );

	// need to adjust auto_increment_value to the next integer
	if (!(create_info->used_fields & HA_CREATE_USED_AUTO)) {
		info( HA_STATUS_AUTO);
		create_info->auto_increment_value = stats.auto_increment_value;
	}
}

// For SHOW CREATE TABLE statement, the method recreates foreign key constraint clause
// based on ScaleDB's metadata information, and then return it to MySQL 
char* ha_scaledb::get_foreign_key_create_info() {
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader(
		        "MySQL Interface: executing ha_scaledb::get_foreign_key_create_info() on table ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(" ");
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	THD* thd = ha_thd();
	placeSdbMysqlTxnInfo(thd);

	return SDBGetForeignKeyClause(sdbUserId_, sdbDbId_, sdbTableNumber_);
}

// This method frees the string containing foreign key clause
void ha_scaledb::free_foreign_key_create_info(char* str) {
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader(
		        "MySQL Interface: executing ha_scaledb::free_foreign_key_create_info(...) on table ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(" ");
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	if (str) {
		FREE_MEMORY(str);
	}
}

// This method updates a record with both the old row data and the new row data specified 
int ha_scaledb::update_row(const unsigned char* old_row, unsigned char* new_row) {
	DBUG_ENTER("ha_scaledb::update_row");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::update_row(...) on table ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(" ");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	int errorNum = 0;
	ha_statistic_increment(&SSV::ha_update_count);

	/* If we have a timestamp column, update it to the current time */

	my_bitmap_map* org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);

	unsigned int retValue = 0;
	// place old row into ScaleDB buffer 1 for comparison
	retValue = placeMysqlRowInEngineBuffer((unsigned char*) old_row, new_row, 2, false, true);

	if (retValue == 0) // place new row into ScaleDB buffer 2 for comparison
		retValue = placeMysqlRowInEngineBuffer(new_row, new_row, 1, false, true);

	if (retValue > 0) {
		dbug_tmp_restore_column_map(table->read_set, org_bitmap);
		DBUG_RETURN(convertToMysqlErrorCode(retValue));
	}
	if (table->part_info) {
		char partitionName[METAINFO_MAX_IDENTIFIER_SIZE]  = { 0 };
		char tblFsName[METAINFO_MAX_IDENTIFIER_SIZE]  = { 0 };
		char dbFsName[METAINFO_MAX_IDENTIFIER_SIZE]  = { 0 };
		char pathName[METAINFO_MAX_IDENTIFIER_SIZE]  = { 0 };
		char* name = SDBUtilDuplicateString(this->share->table_name);
		fetchIdentifierName(name, dbFsName, tblFsName, pathName);
		getAndRemovePartitionName(tblFsName, partitionName);
		sdbPartitionId_ = SDBGetPartitionId(sdbUserId_, sdbDbId_, partitionName, tblFsName);
		FREE_MEMORY(name);
	}

	THD* thd = ha_thd();
	uint64 queryId = thd->query_id;
	retValue = SDBUpdateRowAPI(sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_, sdbRowIdInScan_, queryId);
	sdbRowIdInScan_ = SDBGetUserRowId(sdbUserId_);	// after update, the row may get a new ID

	if (retValue == DATA_EXISTS) { // bug 1203
		// For UPDATE IGNORE statement, we need to change the error code so that MySQL does not send any more records to engine
		char* pUpdateKeyword = SDBUtilStrstrCaseInsensitive(thd->query(), (char*) "update ");
		char* pIgnoreKeyword = SDBUtilStrstrCaseInsensitive(thd->query(), (char*) "ignore ");
		if (pUpdateKeyword && pIgnoreKeyword && (pUpdateKeyword < pIgnoreKeyword))
			retValue = DATA_EXISTS_FATAL_ERROR;
	}

	errorNum = convertToMysqlErrorCode(retValue);

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		if (errorNum) {
			SDBDebugStart(); // synchronize threads printout
			SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::update_row mysqlErrorCode ");
			SDBDebugPrintInt(errorNum);
			SDBDebugPrintString(" and scaledbErrorCode ");
			SDBDebugPrintInt(retValue);
			SDBDebugEnd(); // synchronize threads printout
		}
	}
#endif

	dbug_tmp_restore_column_map(table->read_set, org_bitmap);
	DBUG_RETURN(errorNum);
}

bool ha_scaledb::setDeleteKey( long long* delete_key )
{
	bool ok=true;
	if(end_key_!=NULL)
	{

		switch(end_key_->length)
		{
		case 1: {	*delete_key	= *(char*)end_key_->key; break; }
		case 2: {	*delete_key	= *(short*)end_key_->key; break; }
		case 4: {	*delete_key	= *(int*)end_key_->key; break; }
		case 8: {	*delete_key	= *(long long*)end_key_->key; break; }
		default:
			{
				//something is wrong.
				ok=false;
				break;
			}
		}
	}
	else
	{
		ok=false;
	}
	return ok;
}

int ha_scaledb::isStreamingRangeDeleteSupported( long long* delete_key, unsigned short* columnNumber, bool* delete_all )
{
		unsigned int retValue = 0;
		// this function passes through the key information (value and traversal direction) and expects the delete to complete.
		//	the query and delete will terminate after the first row is fetched.
		//  the parse tree is analysed to find the key and to only allow  ctime <9 type queries
		
		LEX* parse_tree=((THD*) ha_thd())->lex;
		Item* item= parse_tree->select_lex.where;
		*delete_all=true;
		
		if(item!=NULL && (item->type()==Item::FUNC_ITEM  || item->type()==Item::COND_ITEM))
		{
			*delete_all=false;
			//contains a where clause

			Item_func *func = ((Item_func *)item);



			if (func->functype()== Item_func::LT_FUNC)
			{
				//less than delete whare clause

				//must be a simple where clause , 3 nodes, with < 'column_name' and 'literal value'

				//			  <
				//			  /\
				//			9	ctime
				char* col_name=NULL;

		
				int node=0;
				bool ok=true;
				while( ok==true )
				{
					item=item->next;
					if(item==NULL) {break;}
					if(item->max_length==0) {continue;}
					node++;
					if(node==1)
					{
						switch(item->type())
						{
						case Item::FUNC_ITEM:
							{
								Item_func* func= (Item_func*)item;

								switch(func->functype())
								{
									 case Item_func::UNKNOWN_FUNC:
										 {
											 char* func_name = (char*) func->func_name();
											 if(stricmp("date_add_interval",func_name)==0)
											 {
												 ok=setDeleteKey(delete_key);
												 //skip through the function arguments
												 while(item=item->next)
												 {
													 if(item->next->type()==Item::FIELD_ITEM)
													 {
														 break;
													 }
												 }
											
											 }
											 else
											 {
												 ok=false;
											 }
											 break;
										 }
								case Item_func::NOW_FUNC:
									{
										ok=setDeleteKey(delete_key);
										break; //ok
									}
								case Item_func::GUSERVAR_FUNC:
									{
										switch ( ( ( Item_func_get_user_var* ) item )->result_type() )
										{
										case STRING_RESULT:
											{
												ok=setDeleteKey(delete_key);
												break;
											}
										default:
											{
												ok=false;
												break;
											}
										}
										break;
									}
								default:
									{
											//
											ok=false;
											break;
									}
								}







									break;
							}
						case Item::STRING_ITEM:
							{
								 ok=setDeleteKey(delete_key);
								 if(ok==false)
								 {

									 MYSQL_TIME pMyTime;
									 unsigned long long	flags=0;;
									 THD*			pMysqlThd	= ha_thd();
									 MYSQL_TIME_STATUS	timeStatus;
									 String* ss= item->val_str();
									 int rc		= str_to_datetime( pMysqlThd->charset(), ss->ptr(), ss->length(), &pMyTime, flags, &timeStatus );
									 my_bool b=validate_timestamp_range(&pMyTime);
									 if(b==false || rc!=0)
									 {
										 char buf[1000];
										 sprintf(buf,"- Unsupported WHERE clause type for a streaming delete.  '%s' is NOT a valid timestamp value.",ss->ptr());
										 SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, buf );
										 retValue = HA_ERR_GENERIC;
										 return retValue;
									 }
								 }
								break;
							}
						default:
							{
								ok=false;
								break;
							}	
						}
					}
					else if(node==2)
					{
						col_name=item->name;
			
						*columnNumber = SDBGetColumnNumberByName(sdbDbId_, sdbTableNumber_, col_name);
						if(*columnNumber==0) {ok=false;}
					}
					else
					{
						//should only be 2 branch nodes
						ok=false;
						break;
					}


				}
				if(ok==false)
				{
					SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- Unsupported WHERE clause type for a streaming delete." );
					retValue = HA_ERR_GENERIC;
					return retValue;
				}


			}
			else
			{
				//only col < literal are supported
				SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- Unsupported WHERE clause type for a streaming delete." );
				retValue = HA_ERR_GENERIC;
				return retValue;
			}

		}
		return retValue;
}

// This method deletes a record with row data pointed by buf 
int ha_scaledb::delete_row(const unsigned char* buf) {
	DBUG_ENTER("ha_scaledb::delete_row");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		print_header_thread_info("MySQL Interface: executing ha_scaledb::delete_row(...)");
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::delete_row(...)");
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	int errorNum = 0;
	ha_statistic_increment(&SSV::ha_delete_count);

	my_bitmap_map* org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);

	unsigned int retValue = 0;
	retValue = placeMysqlRowInEngineBuffer((unsigned char*) buf, (unsigned char*) buf, 0, false, true);



	if (retValue == 0) {
		retValue = SDBDeleteRowAPI(sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_, sdbRowIdInScan_, ((THD*) ha_thd())->query_id);
	}

	if (retValue != SUCCESS && retValue != ATTEMPT_TO_DELETE_KEY_WITH_SUBORDINATES) {
		THD* thd = ha_thd();
		scaledb_rollback(ht, thd, true);
	}

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		if (errorNum) {
			SDBDebugStart(); // synchronize threads printout
			SDBDebugPrintHeader("MySQL Interface: ha_scaledb::delete_row failed");
			SDBDebugEnd(); // synchronize threads printout
		} else {
			++deleteRowCount_;
			SDBDebugStart(); // synchronize threads printout
			SDBDebugPrintHeader(
			        "MySQL Interface: ha_scaledb::delete_row succeeded. deleteRowCount_=");
			SDBDebugPrintInt(deleteRowCount_);
			SDBDebugEnd(); // synchronize threads printout
		}
	}
#endif

	dbug_tmp_restore_column_map(table->read_set, org_bitmap);
	errorNum = convertToMysqlErrorCode(retValue);
	DBUG_RETURN(errorNum);
}

// This method deletes all records of a ScaleDB table.
// On a cluster system, it involves 4 steps:
// 1. the primary node needs to impose a table-level-write-lock.
// 2. the primary node sends the TRUNCATE TABLE statement with a hint so that all the non-primary nodes will close the table files.
// 3. the primary node deletes all the table files and then re-open the table files on its node.
// 4. the primary node sends the TRUNCATE TABLE statement with open file hint so that all the non-primary nodes will open the table files.
int ha_scaledb::delete_all_rows() {
	DBUG_ENTER("ha_scaledb::delete_all_rows");
	print_header_thread_info("MySQL Interface: executing ha_scaledb::delete_all_rows() ");

	int errorNum = 0;

	sdbPartitionId_ = 0;

	char* tblName = SDBGetTableNameByNumber(sdbUserId_, sdbDbId_, sdbTableNumber_);
	if (table->part_info) {
		char partitionName[METAINFO_MAX_IDENTIFIER_SIZE]  = { 0 };
		char tblFsName[METAINFO_MAX_IDENTIFIER_SIZE]  = { 0 };
		char dbFsName[METAINFO_MAX_IDENTIFIER_SIZE]  = { 0 };
		char pathName[METAINFO_MAX_IDENTIFIER_SIZE]  = { 0 };
		char* name = SDBUtilDuplicateString(table->s->path.str);
		fetchIdentifierName(name, dbFsName, tblFsName, pathName);
		getAndRemovePartitionName(tblFsName, partitionName);
		sdbPartitionId_ = SDBGetPartitionId(sdbUserId_, sdbDbId_, partitionName, tblFsName);
		FREE_MEMORY(name);
	}

	SessionExclusiveMetaLock ot(sdbDbId_,sdbUserId_);
	if(ot.lock()==false)
	{
		DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
	}

	//for truncate, because we might be dropping the table files we need an exclusive lock
       assert(sdbTableNumber_!=0);
	if (!lockDML(sdbUserId_, sdbDbId_, sdbTableNumber_, 0)){
		SDBRollBack(sdbUserId_, NULL, 0, false);
		DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
	}

	int retValue = SDBCanTableBeTruncated(sdbUserId_, sdbDbId_, tblName,0);
	if (retValue == SUCCESS) {
		THD* thd = ha_thd();
		sqlCommand_ = thd_sql_command(thd);

		

		//all ok 


		if (sqlCommand_ == SQLCOM_TRUNCATE || SDBIsStreamingTable(sdbDbId_, sdbTableNumber_)) {
			unsigned short stmtFlag = 0;						
			retValue = SDBTruncateTable(sdbUserId_, sdbDbId_, tblName, sdbPartitionId_, stmtFlag);			
		} else {
			// not TRUNCATE statement, need to tell MySQL to issue delete_row calls 1 by 1
			retValue = METAINFO_DELETE_ROW_BY_ROW;
		}
	}
	errorNum = convertToMysqlErrorCode(retValue);
	DBUG_RETURN(errorNum);
}

// This method fetches a single row using row position
int ha_scaledb::fetchRowByPosition(unsigned char* buf, unsigned long long pos) {

	DBUG_ENTER("ha_scaledb::fetchRowByPosition");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader(
		        "MySQL Interface: executing ha_scaledb::fetchRowByPosition from table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(", alias: ");
#ifdef _MARIA_DB
		SDBDebugPrintString((char *) table->alias.c_ptr());
#else
		SDBDebugPrintString((char *) table->alias);
#endif
		SDBDebugPrintString(", position: ");
		SDBDebugPrint8ByteUnsignedLong(pos);
		SDBDebugPrintString(", using Query Manager ID: ");
		SDBDebugPrintInt(sdbQueryMgrId_);
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	int errorNum = 0;

	int retValue = (int) SDBGetSeqRowByPosition(sdbUserId_, sdbQueryMgrId_, pos);

	if (retValue == SUCCESS)
		retValue = copyRowToRowBuffer(buf);

	if (retValue != SUCCESS)
		table->status = STATUS_NOT_FOUND;

	errorNum = convertToMysqlErrorCode(retValue);
	DBUG_RETURN(errorNum);
}

// This method fetches a single row using next() method
int ha_scaledb::fetchSingleRow(unsigned char* buf) {

	DBUG_ENTER("ha_scaledb::fetchSingleRow");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::fetchSingleRow from table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(", alias: ");
#ifdef _MARIA_DB
		SDBDebugPrintString((char *) table->alias.c_ptr());
#else
		SDBDebugPrintString((char *) table->alias);
#endif
		SDBDebugPrintString(", using Query Manager ID: ");
		SDBDebugPrintInt(sdbQueryMgrId_);
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}

	++debugCounter_;
#endif

	retryFetch: int retValue = 0;

	if (active_index == MAX_KEY)
		retValue = SDBQueryCursorNextSequential(sdbUserId_, sdbQueryMgrId_, buf, &rowTemplate_, sdbCommandType_);
	else
	{



		
			if(sdbCommandType_ == SDB_COMMAND_DELETE && SDBIsStreamingTable(sdbDbId_, sdbTableNumber_) && SDBIsDimensionTable(sdbDbId_, sdbTableNumber_)==false)
			{
				unsigned short	columnNumber	= 0;
				bool			delete_all		= false;
				long long		delete_key		= 0;

				retValue						= isStreamingRangeDeleteSupported( &delete_key, &columnNumber, &delete_all );

				if(retValue==SUCCESS) 
				{
					retValue = SDBStreamingDelete(sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_, delete_key, columnNumber, delete_all,((THD*) ha_thd())->query_id);
					isStreamingDelete_ = true;
					
				}
				
			}


		if ( isStreamingDelete_ && SDBIsStreamingTable(sdbDbId_, sdbTableNumber_) )
		{
			//have already deleted all rows in range so bail.
			isStreamingDelete_ = false;
			retValue =QUERY_END;
		}
		else
		{
			retValue = SDBQueryCursorNext(sdbUserId_, sdbQueryMgrId_, buf, &rowTemplate_,sdbCommandType_);
		}

	}
	if (retValue != SUCCESS ) {
		// no row
		in_range_check_pushed_down = FALSE; //reset
		table->status = STATUS_NOT_FOUND;
		if(retValue==QUERY_ABORTED)
		{
			SDBSetErrorMessage( sdbUserId_, QUERY_ABORTED, " - Insufficient resources to complete the query (or query cancelled by user)." );
		}
		if(retValue==DECIMAL_OVERFLOW_IN_ANALYTICS)
		{
			SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- Conversion to Decimal Failed. Maximum precision exceeded." );
		}
		if(retValue==UNSUPPORTED_ANALYTICS_QUERY)
		{
			SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- This analytics query is not supported." );
		}
		DBUG_RETURN(convertToMysqlErrorCode(retValue));
	}

	if (!temp_table){
		// temp_table describes the projection list with analytics.
		// with analytics, buff is set with the projected data inside SDBQueryCursorNextSequential
		// Without analytics - we map scaledb structure to mysql structure.
	retValue = copyRowToRowBuffer(buf);
	}

	
	if (retValue == ROW_RETRY_FETCH) {
		goto retryFetch;
	}

	// Assertion that MariaDB has in its code and we need to apply, for non-streaming tables only
	else if (retValue == SUCCESS && !stats.records)
	{
		stats.records				= ( ha_rows )	SDBGetTableStats( sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_, SDB_STATS_INFO_FILE_RECORDS );

		if ( !stats.records )
		{
			if ( SDBIsStreamingTable( sdbDbId_, sdbTableNumber_ ) )
			{
				// Streaming table: the statistics are committed during the next checkpoint
				SDBDebugPrintString( "\n Streaming table statistics will be saved during the next checkpoint " );
			}
			else
			{
				// Non-streaming table
				SDBTerminate(0, "ScaleDB internal error:  stats records is zero differs from actual scan which returns a row - forbidden by MariaDB ");
			}
		}
		else
		{
			SDBDebugPrintString("\n stats records were saved in the last minute  ");
		}
	}


#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 2 /* && (debugCounter_ < 100) */) {
		// print out every fetched record.  May set a limit using the above condition
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::fetchSingleRow from table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(", alias: ");
#ifdef _MARIA_DB
		SDBDebugPrintString((char *) table->alias.c_ptr());
#else
		SDBDebugPrintString((char *) table->alias);
#endif
		SDBDebugPrintString(", using Query Manager ID: ");
		SDBDebugPrintInt(sdbQueryMgrId_);
		SDBDebugPrintHeader("ha_scaledb::fetchSingleRow fetches this row: ");
		SDBDebugPrintHexByteArray((char*) buf, 0, table->s->reclength);
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	int errorNum = convertToMysqlErrorCode(retValue);
	DBUG_RETURN(errorNum);
}


//
//  initMysqlTypes inits mysqlToSdbType_ static array 
//  the array converts MySQL column type to SDB column type
//
void ha_scaledb::initMysqlTypes(){
	for (int type=0;type<= MYSQL_TYPE_GEOMETRY; type++) 
	{
		switch (type) {
			// the case statement should be listed based on the decending use frequency
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_FLOAT:
		case MYSQL_TYPE_SHORT:
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_LONGLONG:
		case MYSQL_TYPE_DOUBLE:
		case MYSQL_TYPE_TINY:
		case MYSQL_TYPE_YEAR:
			mysqlToSdbType_[type] = SDB_NUM;
			break;

		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_NEWDECIMAL: // we treat decimal as a string
		case MYSQL_TYPE_BIT: // copy its length in memory
		case MYSQL_TYPE_SET: // copy its length in memory
			mysqlToSdbType_[type] = SDB_CHAR;
			break;

		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
			mysqlToSdbType_[type] = SDB_VAR_CHAR;
			break;

		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
			mysqlToSdbType_[type] = SDB_BLOB;
			break;
		
		case MYSQL_TYPE_GEOMETRY:
			mysqlToSdbType_[type] = SDB_GEOMETRY;
			break;


		default:
			mysqlToSdbType_[type] = NO_TYPE;	
		} // switch
	}
}


//  This method builds a template from a MySQL string key format
//	Each RowField template includes:
//		1.  field number in SDB row 
//		2a. mark as null in bitmap  
//		2b. offset to mark in bitmap void ha_scaledb::buildRowTemplate(unsigned char * buff) {
//		3.  offset in output row 	int n_fields = (int)table->s->fields; /* number of columns */
//		4a. where to copy real SDB  data size - NULL if no copy is needed 	int n_copied_fields =0;
//		4b. how many bytes the SDB data size is composed of  	Field*		field;
//		5. weather to copy ptr only or whole data 	SDBFieldTemplate ft; 
void ha_scaledb::buildRowTemplate(TABLE* tab, unsigned char * buff,bool checkAutoIncField) {

//if we are using the groupby handler the temp table will be used
	
	int n_fields = (int)tab->s->fields; /* number of columns */
	int n_copied_fields =0;
	Field*		field;
	SDBFieldTemplate ft; 
	rowTemplate_.numOfblobs_ = 0;
	rowTemplate_.numOfOvf_ =0;
	rowTemplate_.autoIncfieldNum_ = 0;
	Field* pAutoIncrField = tab->found_next_number_field; // points to auto_increment field
		
	for (int i = 0; i < n_fields; i++) {
		field = tab->field[i];
		if (sdbCommandType_ !=  SDB_COMMAND_SELECT  || // On update+delete+insert+load:	copy ALL fields
			bitmap_is_set(tab->read_set, i)		|| // On select:					fields needed for select 
			bitmap_is_set(tab->write_set, i))		   // On select:					fields needed for insert-select  
		{
			ft.fieldNum_ = i + 1; // field ID should start with 1 in ScaleDB engine; it starts with 0 in MySQL
			ft.nullByte_ = field->null_ptr;
			ft.nullBit_  = field->null_bit;
			ft.offset_   = field->offset(tab->record[0]);
			ft.length_   = field->pack_length();
			ft.type_     = mysqlToSdbType_[field->type()];

			if ( ft.type_ <= SDB_NUM )
			{
				ft.varLengthBytes_	=
				ft.varLengthOffset_	= 0;
				ft.copyDataMethod_	= SDB_CPY_DATA;
				// if auto increment field 
				if ( checkAutoIncField && field == pAutoIncrField ) {
					// with no value i.e either null- value or all-zeros 
					if ((ft.nullByte_ && ( *ft.nullByte_ & ft.nullBit_)) || 
						(( (ha_thd()->variables.sql_mode & MODE_NO_AUTO_VALUE_ON_ZERO) == 0 ) && SDBUtilAreAllBytesZero(buff+ft.offset_,ft.length_)) ) {
						rowTemplate_.autoIncfieldNum_ = ft.fieldNum_;
					}
				}
			}
			else if ( ft.type_ == SDB_VAR_CHAR )
			{
				// Real length of the value precedes the value
				ft.varLengthBytes_	= ( ( ( Field_varstring* ) field )->length_bytes );
				ft.varLengthOffset_	= ft.offset_;
				ft.offset_		   += ft.varLengthBytes_;
				ft.copyDataMethod_	= SDB_CPY_DATA;
				// varchar may be treated as a blob
				if ( ft.length_ > SMALL_VAR_FIELD_SIZE )
				{
					rowTemplate_.numOfblobs_ ++;
				}
				else
				{
					rowTemplate_.numOfOvf_ ++;
				}
			}
			else	// SDB_BLOB || SDB_GEOMETRY
			{
				// Real length of the value precedes the value
				ft.varLengthBytes_	= ( ( Field_blob* ) field )->pack_length_no_ptr();
				ft.varLengthOffset_	= ft.offset_;
				ft.offset_		   += ft.varLengthBytes_;
				ft.copyDataMethod_	= SDB_CPY_PTR;
				rowTemplate_.numOfblobs_ ++;
			}

			SDBArrayPutFieldTemplate(rowTemplate_.fieldArray_,n_copied_fields,ft);
			n_copied_fields++;
		}
	}
	// mark end with no type - we reuse the same field templates 
	ft.type_ = NO_TYPE;
	SDBArrayPutFieldTemplate(rowTemplate_.fieldArray_,n_copied_fields,ft);
	rowTemplate_.numOfOvf_ += rowTemplate_.numOfblobs_;
}

//
//    buildKeyTemplate from MySQL string key format
//	
//	Each KeyField template includes:
//	-Offset
//	-Length
//	-Type
//	-IsNull
//	
//	The string format for storing a key field in MySQL is the following:
//
//	1. If the column can be NULL, then in the first byte we put 1 if the
//	field value is NULL, 0 otherwise.
//
//	2. If the column is of a BLOB type (it must be a column prefix field
//	in this case), then we put the length of the data in the field to the
//	next 2 bytes, in the little-endian format. If the field is SQL NULL,
//	then these 2 bytes are set to 0. Note that the length of data in the
//	field is <= column prefix length.
//
//	3. In a column prefix field, prefix_len next bytes are reserved for
//	data. In a normal field the max field length next bytes are reserved
//	for data. For a VARCHAR(n) the max field length is n. If the stored
//	value is the SQL NULL then these data bytes are set to 0.
//
//	4. We always use a 2 byte length for a true >= 5.0.3 VARCHAR. Note that
//	in the MySQL row format, the length is stored in 1 or 2 bytes,
//	depending on the maximum allowed length. But in the MySQL key value
//	format, the length always takes 2 bytes.
//
//	We have to zero-fill the buffer so that MySQL is able to use a
//	simple memcmp to compare two key values to determine if they are
//	equal. MySQL does this to compare contents of two 'ref' values. 
//
bool ha_scaledb::buildKeyTemplate(SDBKeyTemplate & t,unsigned char * key, unsigned int key_len,unsigned int index,bool & isFullKey, bool & allNulls) {

	// fail if no index exists
	if ( index == MAX_KEY ) {
		return false;
	}

	// now we prepare query by assigning key values to the corresponding key fields
	unsigned int keyOffset = 0; // points to key value (may include additional bytes for NULL or variable string.
	KEY* pKey = table->s->key_info + index; // find out which index to use
	KEY_PART_INFO* pKeyPart = pKey->key_part;
	SDBKeyPartTemplate kpt; 
	static unsigned char EMPTY =0x0;
	bool useKey = false;
	//unsigned int numofKeyParts = 0;
	isFullKey = true;
	allNulls = true;

	t.arrayLength_ =0;
	t.keyLength_ = key_len;

	// reset the lookup travsersal flag 
	starLookupTraversal_ = false;

	for ( unsigned int i = 0; i < pKey->user_defined_key_parts; i++, pKeyPart++ )

	{
		kpt.type_     = mysqlToSdbType_[pKeyPart->field->type()];
		kpt.allValues_ = false;
		
		if( keyOffset < key_len) 
		{
			t.arrayLength_++;
			bool isNull = false;

			if (pKeyPart->null_bit && key[keyOffset++] ) 
			{
				isNull = true;
			} 
			else
			{
				allNulls = false;
			}

			if(kpt.type_ < SDB_VAR_CHAR) 
			{
				kpt.length_   = pKeyPart->field->pack_length();
			}
			else // BLOB + VARCHR 
			{
				kpt.length_   =  *(unsigned short *)(key+keyOffset);
				keyOffset +=2;
				// sometimes MysQL pads varchar key with nulls - trim  nulls from varchar key prefix 

			}
		
			kpt.ptr_ = isNull ? NULL : key+ keyOffset;
			keyOffset +=pKeyPart->length;	
			// mark using key
			useKey = true;
		}
		else
		{
			isFullKey = false;
			kpt.ptr_ = NULL;
			kpt.allValues_ = true;
			kpt.length_ = 0;
			if (starLookupTraversal_) 
			{
				t.arrayLength_++;
			}
			
		}
		
		if (kpt.ptr_ == NULL && !SDBConditionStackIsEmpty(conditions_) )
		{
			// simple condition is only a condition of AND ...AND ... 
			SimpleCondition cond = SDBConditionStackTop(conditions_);
			int condItemOfKey =-1;
			// find a single condition item for this key 
			for (int j =0;j<cond.numOfItems_;j++)
			{
				if ( ((Field *)cond.item_[j].field_)->field_index == pKeyPart->field->field_index ) 
				{
					// first matching condition  
					if ( condItemOfKey == -1 ) {
						condItemOfKey = j;
					}
					// more then one - ignore because it is a range 
					else {
						condItemOfKey = -1;
						break;
					}
				}
			}

			if ( condItemOfKey >= 0 ) 
			{		
				// found the additional keys in a where caluse which is completely parsed - use a star look up travesal 
				if ( !starLookupTraversal_) {
					starLookupTraversal_ = true;
					t.arrayLength_ = i+1;
					useKey = true;
				}
				kpt.allValues_ = false;
				kpt.ptr_ = (unsigned char *)cond.item_[condItemOfKey].value(); 
				kpt.length_ = cond.item_[condItemOfKey].size_;
				
			}
		}

		SDBArrayPutKeyPartTemplate(t.keyArray_,i,kpt);
	}
	// mark end with no type - we reuse the same field templates 
	kpt.type_ = NO_TYPE;
	SDBArrayPutKeyPartTemplate(t.keyArray_,t.arrayLength_,kpt);
	return (useKey || !key);
}




// copy scaledb engine row to mysql row.. should be called after fetch
// The method builds a template for MYSQL buffer according to the row fields and the content of the buffer 
// Then it passes the template and the buffer to SDB engine - which fills the buffer acording to the engine 
//  This method builds a template from a MySQL string key format
//	Each RowField template includes:
//		1. field number in SDB row 
//		2. place to mark if NULL
//		3. offset in MYSQL row 	
//		4. place to copy real SDB  data size - NULL if no copy is needed 
//		5. flag  if to copy data / ptr to data 
int ha_scaledb::copyRowToRowBuffer(unsigned char* buf) {
	unsigned short retValue = 0;
	int errorNum = 0;
	
	// init MySQL buffer  
	my_bitmap_map* org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);
	memset(buf, 0, table->s->null_bytes);

	// choose one of the for API options: row_instance/data_instance X with blob/ without blob 
	// more eficent the doing the if-else inside 
	if ( rowTemplate_.numOfblobs_ == 0) {
		if (active_index < MAX_KEY) {
			errorNum =  SDBQueryCursorCopyToRowBufferWithRowTemplate(sdbUserId_,sdbQueryMgrId_,sdbDesignatorId_,buf,rowTemplate_,sdbRowIdInScan_);
		}
		else {
			errorNum =  SDBQueryCursorCopyToRowBufferWithRowTemplate(sdbUserId_,sdbQueryMgrId_,buf,rowTemplate_,sdbRowIdInScan_);
		}
	}	
	else {
		if (active_index < MAX_KEY) {
			errorNum =  SDBQueryCursorCopyToRowBufferWithRowTemplateWithEngine(sdbUserId_,sdbQueryMgrId_,sdbDesignatorId_,buf,rowTemplate_,sdbRowIdInScan_);
		}
		else {
			errorNum = SDBQueryCursorCopyToRowBufferWithRowTemplateWithEngine(sdbUserId_,sdbQueryMgrId_,buf,rowTemplate_,sdbRowIdInScan_);
		}    
	}

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart();
		SDBDebugPrintString("ha_scaledb::copyRowToRowBuffer: buffer[");
		SDBDebugPrintHexByteArray((char *)buf, 0, 20);
		SDBDebugPrintString("]\n");
		SDBDebugEnd();
	}
#endif

	// reste MySQL buffer 
	table->status = 0;
	dbug_tmp_restore_column_map(table->write_set, org_bitmap);
	return errorNum;
}



// -------------------------------------------------------------------------------------------
//	Place a ScaleDB field in a MySQL buffer
// -------------------------------------------------------------------------------------------
void ha_scaledb::placeEngineFieldInMysqlBuffer( unsigned char* destBuff, char* ptrToField, Field* pField )
{
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
		*destBuff = *ptrToField; // Copy 1 byte only
		break;

	case MYSQL_TYPE_NEWDECIMAL: // we treat decimal as a string
		memcpy(destBuff, ptrToField, ((Field_new_decimal*) pField)->bin_size);
		break;

	case MYSQL_TYPE_BIT: // copy its length in memory
		memcpy(destBuff, ptrToField, ((Field_bit*) pField)->pack_length());
		break;

	case MYSQL_TYPE_SET: // copy its length in memory
		memcpy(destBuff, ptrToField, ((Field_set*) pField)->pack_length());
		break;

	case MYSQL_TYPE_YEAR:
		*destBuff = *ptrToField; // Copy 1 byte only
		break;

	case MYSQL_TYPE_ENUM: // copy 1 or 2 bytes
		memcpy(destBuff, ptrToField, ((Field_enum*) pField)->pack_length());
		break;

	case MYSQL_TYPE_STRING:
		memcpy(destBuff, ptrToField, ((Field_bit*) pField)->pack_length());
		break;

	case MYSQL_TYPE_GEOMETRY:
	case MYSQL_TYPE_VARCHAR:
	case MYSQL_TYPE_VAR_STRING:
	case MYSQL_TYPE_TINY_BLOB:
	case MYSQL_TYPE_BLOB:
	case MYSQL_TYPE_MEDIUM_BLOB:
	case MYSQL_TYPE_LONG_BLOB:
		// Length and pointer to the value have already been copied into destBuff
		break;

	default:
#ifdef SDB_DEBUG_LIGHT
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("These data types are not supported yet.");
		SDBDebugEnd(); // synchronize threads printout
#endif
		break;
	} // switch

}



// This method fetches a single virtual row using nextByDesignator() method
int ha_scaledb::fetchVirtualRow(unsigned char* buf) {
	DBUG_ENTER("ha_scaledb::fetchVirtualRow");

	print_header_thread_info("MySQL Interface: executing ha_scaledb::fetchVirtualRow(...) ");

	int errorNum = 0;
	errorNum = SDBQueryCursorNext(sdbUserId_, sdbQueryMgrId_,NULL,NULL, sdbCommandType_); // always use the Multi-Table Index to fetch record

	if (errorNum != SUCCESS) {
		// no row
		table->status = STATUS_NOT_FOUND;
		errorNum = convertToMysqlErrorCode(errorNum);
		DBUG_RETURN(errorNum);
	}
	char to[512];
	uint n=tablename_to_filename( table->s->table_name.str, to, sizeof(to));
	char* pCurrDesignator = to + SDB_VIRTUAL_VIEW_PREFIX_BYTES;
	unsigned int designatorid= SDBGetIndexNumberByName(sdbDbId_, pCurrDesignator);

	char *dataPtr;
	unsigned short numberOfFields;
	unsigned short numberOfFieldsInParents;
	unsigned short sdbRowOffset;
	unsigned short designatorFetched;
	unsigned short offset;
	Field* pField;
	unsigned char* pFieldBuf; // pointer to the buffer location holding the field value

	// Now we start from level 1 and go down

	unsigned short totalLevel = SDBGetIndexLevel(designatorid);
	for (int currLevel = 0; currLevel < totalLevel; ++currLevel) {

		designatorFetched = SDBGetParentIndex(sdbDbId_, designatorid, currLevel + 1);

		if (designatorFetched == 0) { // no more designator founds
			errorNum = HA_ERR_END_OF_FILE;
			table->status = STATUS_NOT_FOUND;
			break;
		}
		dataPtr = SDBQueryCursorGetDataByIndex(sdbQueryMgrId_, designatorFetched);

		offset = getOffsetByDesignator(designatorFetched); // position of designated data in buff

		numberOfFields = SDBGetNumberOfFieldsInTableByIndex(sdbDbId_, designatorFetched);
		numberOfFieldsInParents = SDBGetNumberOfFieldsInParentTableByIndex(sdbDbId_,
		        designatorFetched);

		for (unsigned short i = 0; i < numberOfFields; ++i) {
			sdbRowOffset = SDBGetTableColumnPositionByIndex(sdbDbId_, designatorFetched, i + 1);
			pField = table->field[numberOfFieldsInParents + i];

			pFieldBuf = buf + (pField->ptr - table->record[0]);
			placeEngineFieldInMysqlBuffer(pFieldBuf, dataPtr + sdbRowOffset, pField);
		}

	}

	DBUG_RETURN(errorNum);
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
	for (unsigned short i = 1; i < level; ++i) {
		offset += SDBGetTableRowLengthByIndex(sdbDbId_, SDBGetParentIndex(sdbDbId_, designator, i));
	}
	return offset;
}


// prepare primary or first-key query manager
void ha_scaledb::prepareFirstKeyQueryManager() {
	char* pDesignatorName = NULL;

	if (virtualTableFlag_) {
		char to[512];
		uint n=tablename_to_filename( table->s->table_name.str, to, sizeof(to));
		pDesignatorName = to + SDB_VIRTUAL_VIEW_PREFIX_BYTES;
		sdbDesignatorId_  = SDBGetIndexNumberByName(sdbDbId_, pDesignatorName);
		active_index = 0; // has to be primary key for virtual view
	} 
	else 
	{
		SdbDynamicArray* pDesigArray = SDBGetTableDesignators(sdbDbId_, sdbTableNumber_);
		unsigned short i = SDBArrayGetNextElementPosition(pDesigArray, 0); // set to position of first element
		// if there are indexes use the first 
		if ( i > 0 ) {
			SDBArrayGet(pDesigArray, i, (unsigned char *) &sdbDesignatorId_); // get designator number from pDesigArray
			// Find MySQL index number and force full table scan to use the first available index 
			active_index = (unsigned int) SDBGetIndexExternalId(sdbDbId_, sdbDesignatorId_);
			// add level 
			sdbDesignatorId_ |= (SDBGetIndexLevel(sdbDbId_, sdbDesignatorId_) << DESIGNATOR_LEVEL_OFFSET);
		} 
		// otherwise use sequntail scan
		else 
		{
			active_index = MAX_KEY;
		}
	}
	// the index is used by defualt with no lookup 
	starLookupTraversal_ = false; 
}


// prepare query manager for index reads
void ha_scaledb::prepareIndexQueryManager(unsigned int indexNum) {
	char* pDesignatorName = NULL;

	if (!sdbQueryMgrId_) 
	{
		SDBTerminate(0, "ScaleDB internal error:  query manager must exist");
	}

	KEY* pKey = table->key_info + indexNum;

	if ( virtualTableFlag_ )
	{
		char to[512];
		uint n=tablename_to_filename( table->s->table_name.str, to, sizeof(to));
		pDesignatorName = SDBUtilGetStrInLower( to + SDB_VIRTUAL_VIEW_PREFIX_BYTES );
	}
	else
	{
		char* pTableName	= SDBGetTableNameByNumber( sdbUserId_, sdbDbId_, sdbTableNumber_ );
		pDesignatorName		= SDBUtilFindDesignatorName( pTableName, pKey->name, indexNum, true, sdbDesignatorName_, SDB_MAX_NAME_LENGTH );
	}

	sdbDesignatorId_		= SDBGetIndexNumberByName(sdbDbId_, pDesignatorName);

	if ( !( isRangeKeyEvaluation() ) && conditionStringLength_ )
	{
		// Perparation of a query with a WHERE clause
		isStreamingHashIndex_	= SDBIsStreamingHashKey( sdbDbId_, sdbDesignatorId_ );
	}

	// the index is used by default with no lookup
	starLookupTraversal_ = false ;

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: designator: ");
		if (pDesignatorName)
			SDBDebugPrintString(pDesignatorName);
		else
			SDBTerminate(IDENTIFIER_INTERFACE + ERRORNUM_INTERFACE_MYSQL + 11,
			"The designator name should NOT be NULL!!"); // ERROR - 16010011

		SDBDebugPrintString(" table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugEnd(); // synchronize threads printout
	}
	if (mysqlInterfaceDebugLevel_ > 5) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader(" QueryManagerId= ");
		SDBDebugPrintInt(sdbQueryMgrId_);
		SDBDebugPrintString(" \n");
		SDBDebugEnd(); // synchronize threads printout
	}
#endif
}

// Evaluate table definition for a subsequent table scan
int ha_scaledb::evaluateTableScan()
{
	// Clear the start key and end key values that apply only to an indexed query
	clearIndexKeyRanges();

	if ( !( hasRangeDesignator() ) )
	{
		// Not a streaming table with a range key
		return HA_ERR_END_OF_FILE;
	}

	// This query will not use an index
	isIndexedQuery_									=
	isUserTimeIndexQuery_							= false;

	return 0;
}

// Evaluate and assign key values for a subsequent indexed query
int ha_scaledb::evaluateIndexKey( const uchar* key, uint key_len, enum ha_rkey_function find_flag )
{
	// Initialize the start key and end key values
	clearIndexKeyRanges();

	if ( !( hasRangeDesignator() ) )
	{
		// Not a streaming table with a range key
		return HA_ERR_END_OF_FILE;
	}

	int		retValue								= 0;
	bool	isFullKey;
	bool	allNulls;
	bool	isFullKeyEnd;
	bool	buildKeySucceed;
	enum	ha_rkey_function sdb_find_flag			= find_flag;
	bool	isRangeKey, isDateKey;
	
	isIndexedQuery_									=
	isUserTimeIndexQuery_							= false;

	isRangeKey										= isRangeDesignator();
	isDateKey										= isDateDesignator();

	if ( !( isRangeKeyEvaluation()				   && ( isRangeKey || isDateKey ) ) )
	{
		// Query will not use a range index
		return HA_ERR_END_OF_FILE;
	}

	// Try to build the start key template
	if ( !( buildKeySucceed							= buildKeyTemplate( this->keyTemplate_[ 0 ], ( unsigned char* ) key, key_len, active_index, isFullKey, allNulls ) ) )
	{
		// Sequential scan
		return retValue;
	}

	// This query will use an index
	isIndexedQuery_									= true;

	if ( !isRangeKey							   && isDateKey )
	{
		// Query will use the user time index
		setIsUserTimeIndexQuery();
	}

	// Copy the start key value
	copyIndexKeyRangeStart( key, key_len, find_flag, 1 );

	// Handle the end key
	if ( releaseLocksAfterRead_ )
	{
		if ( SDBIsNonUniqueIndex( sdbDbId_, sdbDesignatorId_ )  || !eq_range_ )
		{
			// Try to build the end key template
			if ( !eq_range_ )
			{
				buildKeySucceed						= buildKeyTemplate( this->keyTemplate_[ 1 ], ( unsigned char* ) end_key_->key, end_key_->length,
																		active_index, isFullKeyEnd, allNulls );
			}
			else
			{
				buildKeySucceed						= false;
			}

			if ( buildKeySucceed				   && !allNulls )
			{
				// define the range
				if ( end_key_ )
				{
					// Copy the end key value
					copyIndexKeyRangeEnd( end_key_ );
				}
			}
		}
	}

//if mariadb decides to do a range read, but there was a condition string  but no key was extracted
//then bail, and do a table scan, because the row wont get found (we expect a range key to include the index)	
	if(buildKeySucceed==false&& conditionStringLength_>0 && key==NULL)
	{
		optimizeWhere_=false;
	}
	
	return retValue;
}

// Prepare query by assigning key values to the corresponding key fields
int ha_scaledb::prepareIndexKeyQuery(const uchar* key, uint key_len, enum ha_rkey_function find_flag) 
{
	int retValue = 0;
	bool isDistinct = true; // The MysQL query  mode is distinct	
	bool isFullKey;
	bool allNulls;
	bool isFullKeyEnd;
	bool buildKeySucceed;
	enum ha_rkey_function  sdb_find_flag = find_flag;
	bool use_prefetch = false;

	isIndexedQuery_			=
	isUserTimeIndexQuery_	= false;

	// 1. build the key template 
	if ( ! (buildKeySucceed = buildKeyTemplate(this->keyTemplate_[0],(unsigned char *)key,key_len,active_index,isFullKey,allNulls)) )
	{
		// prepare for sequential scan
		active_index				= MAX_KEY;

		char* pTableName			= SDBGetTableNameByNumber               ( sdbUserId_, sdbDbId_, sdbTableNumber_ );
		char* pTableFsName			= SDBGetTableFileSystemNameByTableNumber( sdbDbId_, sdbTableNumber_ );

		retValue					= ( int ) SDBPrepareSequentialScan( sdbUserId_, sdbQueryMgrId_, sdbDbId_, sdbPartitionId_, pTableName, ( ( THD* ) ha_thd() )->query_id,
																		releaseLocksAfterRead_, rowTemplate_,
																		conditionString_, conditionStringLength_, analyticsString_, analyticsStringLength_ );

		if ( conditionStringLength_ )
		{
			conditionStringLength_	= 0;
		}

		if ( analyticsStringLength_ )
		{
			analyticsStringLength_	= 0;
		}

		forceAnalytics_				= false;

		return retValue;
	}

	// This query will use an index
	isIndexedQuery_					= true;

	if ( !( isRangeDesignator() )  && isDateDesignator() )
	{
		// Query will use the user time index
		setIsUserTimeIndexQuery();
	}


	// 2a. null key is a special case - replace > NULL with >= *   
	if ( keyTemplate_[0].arrayLength_== 0 ) {
		sdb_find_flag = SdbKeySearchDirectionFirstLastTranslation[sdb_find_flag];
		SDBQueryCursorDefineQueryAllValues(sdbQueryMgrId_, sdbDbId_, sdbDesignatorId_, false);
		isDistinct = false;
	} 
	// 2b. set the key values into the query 
	else if ( SDBDefineQueryByTemplate(sdbQueryMgrId_, sdbDbId_, sdbDesignatorId_, (char *)key, this->keyTemplate_[0]) != SUCCESS) 
	{
		SDBTerminate(0, "ScaleDB internal error: wrong designator name!");
	}

	// 3. set the cursor direction
	if ( !starLookupTraversal_) {
		SDBQueryCursorSetFlags(sdbQueryMgrId_, sdbDesignatorId_, isDistinct , (SDB_KEY_SEARCH_DIRECTION)SdbKeySearchDirectionTranslation[sdb_find_flag][0], SdbKeySearchDirectionTranslation[sdb_find_flag][1], true,readJustKey_);
	}
	else { // on starLookupTraversal_ we use EXACT MATCH without prefix match 
		SDBQueryCursorSetFlags(sdbQueryMgrId_, sdbDesignatorId_, isDistinct , SDB_KEY_SEARCH_DIRECTION_EQ, false, true, readJustKey_);
	}

	// 4a. set the prefetch 
	if (releaseLocksAfterRead_)
	{
		// if not equal range on unique index, i.e. a result set with one row,
		//	we assume index_init set active_index before read_range_first is called 
		if ( SDBIsNonUniqueIndex(sdbDbId_, sdbDesignatorId_)  || !eq_range_ )
		{
			unsigned char direction = ( unsigned char ) SdbKeySearchDirectionTranslation[ find_flag ][ 0 ];
			unsigned char keyIndex  = 0;

			// build end_key template 
			if ( ! eq_range_) {
				buildKeySucceed =  buildKeyTemplate(this->keyTemplate_[1],(unsigned char *)end_key_->key,end_key_->length,active_index,isFullKeyEnd,allNulls);
				direction = ( unsigned char ) SdbKeySearchDirectionTranslation[ end_key_->flag ][ 0 ];
				keyIndex = 1;
			}

			// if search for nulls range (i.e != NULL) ignore the prefetch - for now - speical case - add later 
			if ( buildKeySucceed  && !allNulls )
			{
				// define the range 
				SDBDefineQueryRangeKey( sdbQueryMgrId_, sdbDbId_, sdbDesignatorId_, this->keyTemplate_[ keyIndex ], ( end_key_ ? direction : 0 ) );	// direction is meaningful here
				//	only if there is an end key
				use_prefetch  = true;
			}
		}
	}

	// 4b. streaming table corrections
	if ( SDBIsStreamingTable(sdbDbId_, sdbTableNumber_) )
	{
		// streaming table indices do not use prefetch - only trie indices do
		use_prefetch  = false;

		if (active_index == table->s->primary_key ) {
			// on streaming table the streaming_key is the single key_part that is indexed 
			// therefore the  key is always full 
			isFullKey = true;
		}
	}



	if(sdbCommandType_ == SDB_COMMAND_DELETE &&  SDBIsStreamingTable(sdbDbId_, sdbTableNumber_) && SDBIsDimensionTable(sdbDbId_, sdbTableNumber_)==false)
	{
		long long		delete_key;
		unsigned short	columnNumber	= 0;
		bool			delete_all		= false;

		isIndexedQuery_			=
		isUserTimeIndexQuery_	= false;

		retValue				= isStreamingRangeDeleteSupported( &delete_key, &columnNumber, &delete_all );

		if (retValue!=SUCCESS)
		{
			//this type of delete not supported so fail
			return retValue;
		}
	}

	
	// 4c. set the query cursor 
	retValue					= SDBPrepareQuery( sdbUserId_, sdbQueryMgrId_, sdbPartitionId_, ( ( THD* ) ha_thd() )->query_id,
												   releaseLocksAfterRead_, sdbCommandType_,
												   find_flag == HA_READ_KEY_EXACT && isFullKey && !use_prefetch,
												   rowTemplate_, use_prefetch, conditionString_, conditionStringLength_, analyticsString_, analyticsStringLength_ );


	if (temp_table){
		// build template representing the result set - if the result set is defined by a tmp table
		// i.e. with the group by handler.
		buildRowTemplate(temp_table, temp_table->record[0]);
	}

	if (forceAnalytics_==true && analyticsStringLength_==0)
	{
		conditionStringLength_	= 0;
		analyticsStringLength_	= 0;
		forceAnalytics_			= false;
		isIndexedQuery_			= false;
		isUserTimeIndexQuery_	= false;
		if(!SDBErrorExists(sdbUserId_))
		{
			//if we don't already have an error then add this error, otherwise we return the  current error (which is more relevant) 
			SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- force_sdb_analytics was set but not used." );
		}
		retValue = HA_ERR_GENERIC;
		return retValue;
	}

	if ( conditionStringLength_ )
	{
		conditionStringLength_	= 0;
	}

	if ( analyticsStringLength_ )
	{
		analyticsStringLength_	= 0;
	}

	forceAnalytics_				= false;

	if ( retValue )
	{
		isIndexedQuery_			=
		isUserTimeIndexQuery_	= false;
	}

	return retValue;
}

// This method retrieves a record based on index/key 
int ha_scaledb::index_read(uchar* buf, const uchar* key, uint key_len,
        enum ha_rkey_function find_flag) {

	DBUG_ENTER("ha_scaledb::index_read");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_read(...) on table ");
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(", alias: ");
#ifdef _MARIA_DB
		SDBDebugPrintString( table->alias.c_ptr());
#else
		SDBDebugPrintString( table->alias);
#endif
		outputHandleAndThd();
		SDBDebugPrintString(", Query Manager ID #");
		SDBDebugPrintInt(sdbQueryMgrId_);
		SDBDebugPrintString(", Index read flag ");
		SDBDebugPrintString(mysql_key_flag_strings[find_flag]);
		SDBDebugPrintString(", key_len ");
		SDBDebugPrintInt(key_len);
		SDBDebugPrintString(", key ");
		SDBDebugPrintHexByteArray((char*) key, 0, key_len);
		SDBDebugEnd(); // synchronize threads printout

		// initialize the debugging counter
		readDebugCounter_ = 1;
	}
#endif
	int retValue = SUCCESS;

	//if it is a streaming hash index and not a point lookup then FAIL it (we dont support)
	if ( (isStreamingHashIndex_==true) && (find_flag!=HA_READ_KEY_EXACT) )
	{
		SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- unsupported streaming hash index query." );

		conditionStringLength_	= 0;
		analyticsStringLength_	= 0;
		forceAnalytics_			= false;
		isIndexedQuery_			= false;
		isUserTimeIndexQuery_	= false;
		retValue = HA_ERR_GENERIC;
		retValue = convertToMysqlErrorCode(retValue);
		DBUG_RETURN(retValue);
	}


	ha_statistic_increment(&SSV::ha_read_key_count);

	// build template at the begin of scan 
	buildRowTemplate(table, buf);

	if ( isQueryEvaluation() )
	{
		retValue	= evaluateIndexKey( key, key_len, find_flag );

		DBUG_RETURN( retValue );
	}

	retValue = prepareIndexKeyQuery(key, key_len, find_flag);

	if (retValue == 0) {	
		retValue = fetchRow(buf);
	} else {
		retValue = convertToMysqlErrorCode(retValue);
		table->status = STATUS_NOT_FOUND;
	}

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 1) {
		SDBDebugStart(); // synchronize threads printout

		if (!retValue) {
			SDBDebugPrintHeader("ha_scaledb::index_read returned a valid row");
		} else {
			SDBDebugPrintHeader("ha_scaledb::index_read returned code: ");
			SDBDebugPrintInt(retValue);
		}

		SDBDebugFlush();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	DBUG_RETURN(retValue);
}

// this function is called at the end of the query and can be used to clear state for that query
int ha_scaledb::reset() {
	print_header_thread_info("MySQL Interface: executing ha_scaledb::reset()");
	if ( SDBIsValidQueryManagerId(sdbQueryMgrId_, sdbUserId_) ) {
		SDBFreeQueryManagerBuffers(sdbQueryMgrId_);
		unsetSdbQueryMgrId();
		// init the conditions stack 
		SDBConditionStackClearAll(conditions_);
	}
	return 0;
}

// This method retrieves a record based on index/key.  The index number is a parameter 
int ha_scaledb::index_read_idx(uchar* buf, uint keynr, const uchar* key, uint key_len,
	enum ha_rkey_function find_flag) {
		DBUG_ENTER("ha_scaledb::index_read_idx");

		print_header_thread_info("MySQL Interface: executing ha_scaledb::index_read_idx(...) ");

		active_index = keynr;
		DBUG_RETURN(index_read(buf, key, key_len, find_flag));
}


#ifdef SDB_PUSH_DOWN
/*
Condition pushdown API is used to add additional suffix fields to an index search 
SDB perfom loose scan with keys i.e composite key (A,B,C) = ('FIELD1',*,3) where * is the star operator i.e. EVERYTHING
Currently we parse only tuples of AND expressions which include equal op i.e X = C1 AND Y = C2 AND ....
*/
bool   parse_op_for_sdb_index(Item_func::Functype op, Field * f, Item * value, SimpleCondition * context ) 
{

	// if the condition is too composite  - ignore it 
	if ( context->numOfItems_ == SDB_MAX_CONDITION_EXPRESSIONS_TO_PARSE )
	{
		return false;
	}
	switch (value->type())
	{
	case Item::INT_ITEM:
		(context->item_[context->numOfItems_]).value_ = (char *)&(((Item_int*) value)->value);
		(context->item_[context->numOfItems_]).size_ = f->pack_length();
		(context->item_[context->numOfItems_++]).field_ = f;
		break;

	case Item::REAL_ITEM:
		(context->item_[context->numOfItems_]).value_ =  (char *)&(((Item_float*) value)->value);
		(context->item_[context->numOfItems_]).size_ = f->pack_length();
		(context->item_[context->numOfItems_++]).field_ = f;
		break;

	case Item::STRING_ITEM:		
#if MYSQL_VERSION_ID < 100014
		(context->item_[context->numOfItems_]).value_ =(char *)((Item_string *) value)->str_value.c_ptr();
		(context->item_[context->numOfItems_]).size_ = ((Item_string *) value)->str_value.length();		
#else
		(context->item_[context->numOfItems_]).value_ =(char *)((Item *) value)->val_str()->c_ptr();
		(context->item_[context->numOfItems_]).size_ = ((Item *) value)->val_str()->length();	
#endif
		(context->item_[context->numOfItems_++]).field_ = f;
		break;

 case Item::DECIMAL_ITEM:	
		(context->item_[context->numOfItems_]).value_decimal_ = ((Item_decimal*) value)->val_real();
		(context->item_[context->numOfItems_]).value_ = NULL;
		(context->item_[context->numOfItems_]).size_ = f->pack_length();
		(context->item_[context->numOfItems_++]).field_ = f;
		break;

	default:
		// two fields are not parsed 
		return false; 
	}

	return true;
}

bool parse_cond_for_sdb_index(COND *cond, SimpleCondition & context )
{
	if (cond->type() == Item::COND_ITEM)
	{
		if (((Item_cond*) cond)->functype() == Item_func::COND_AND_FUNC || ((Item_cond*) cond)->functype() == Item_func::COND_OR_FUNC)
		{
			/*AND or OR LIST */
			List_iterator<Item> li(*((Item_cond*) cond)->argument_list());
			Item *item;
			while ((item=li++))
			{
				if ( ! parse_cond_for_sdb_index(item,context))
				{
					return  false;
				}
			}
		}
		else
		{
			return false;
		}
	}
	else if (cond->type() == Item::FUNC_ITEM)
	{
		// primitive expression 
		if ((((Item_func*) cond)->functype()) == Item_func::EQ_FUNC ) {

			Item *left_item=	((Item_func*) cond)->arguments()[0];
			Item *right_item= ((Item_func*) cond)->arguments()[1];

			if ( left_item->type() == Item::FIELD_ITEM ) // only expression of type "FIELD = X" where X is a constant are parsed 
			{
				if (! parse_op_for_sdb_index(((Item_func*) cond)->functype(),((Item_field *) left_item)->field,right_item,&context))
				{
					return false;
				}
			}
		}
		else 
		{
			// >,<, etc. are not parsed
			return false;
		}
	}
	else
	{
		return false;
	}

	return true;
}


//--------------------------------------------------------------------------------------------------
//	Recursively postorder traverse condition tree, writing condition to string
//--------------------------------------------------------------------------------------------------
bool ha_scaledb::conditionTreeToString( const COND* cond, unsigned char** buffer, unsigned int* nodeOffset, unsigned short* DBID, unsigned short* TABID, TABLE_LIST* table_list )
{
	int				tableNum						= 0;
	Item*			pComperandItem;
	unsigned char*	pComperandData;
	unsigned int	offsetComperandData;
	int				resultExtension;

	switch (cond->type())		//check what kind of node this is. Options: logical operator, comparison operator...
	{
		case Item::COND_ITEM:	// This node is a logical operator (AND, OR...)- call recursively on children (postorder tree traversal)
		{
			List_iterator<Item> li(*((Item_cond*) cond)->argument_list());
			Item *item;
			unsigned int			childCount		= 0;
			unsigned short			offsetChild1	= *nodeOffset;

			while ( ( item							= li++ ) )
			{
				// iterate through children
				if ( ++childCount					> 0xffff )
				{
					// up to 65535 children
					return false;
				}

				if ( !( conditionTreeToString( item, buffer, nodeOffset, DBID, TABID, table_list ) ) )
				{
					// stop process
					return false;
				}
			}

			// Extend the condition string if necessary
			resultExtension							= checkConditionStringSize( buffer, nodeOffset, LOGIC_OP_NODE_LENGTH );

			if ( resultExtension					< 0 )
			{
				// Extension failed
				return false;
			}

			// All children visited. Write current node info to buffer
			*( unsigned short* )( *buffer + *nodeOffset + LOGIC_OP_OFFSET_CHILDCOUNT )	= ( unsigned short )( childCount );	// # of children

			bool	and_cond						= false;

			switch ( ( ( Item_cond* ) cond )->functype() )
			{
				case Item_func::COND_AND_FUNC:
				{
					*( *buffer + *nodeOffset + LOGIC_OP_OFFSET_OPERATION )	= SDB_PUSHDOWN_OPERATOR_AND;	// & for AND
					and_cond						= true;

					if ( rangeBounds.columnId )
					{
						if ( !( rangeBounds.endSet ) )
						{
#ifdef	SDB_DEBUG
							if ( !( rangeBounds.startSet ) )
							{
								SDBTerminateEngine( 0, "Invalid range key bounds", __FILE__, __LINE__ );
							}
#endif

							rangeBounds.valid		=
							rangeBounds.endSet		= true;
							rangeBounds.endRange	= *nodeOffset;
						}

						if ( rangeBounds.valid	   && ( offsetChild1 <= rangeBounds.startRange ) )
						{
#ifdef	SDB_DEBUG
							if ( rangeBounds.rangeParent )
							{
								SDBTerminateEngine( 0, "Invalid range key bounds", __FILE__, __LINE__ );
							}
#endif

							rangeBounds.rangeParent	= *nodeOffset;
						}
					}

					break;
				}
				case Item_func::COND_OR_FUNC:
					*( *buffer + *nodeOffset + LOGIC_OP_OFFSET_OPERATION )	= SDB_PUSHDOWN_OPERATOR_OR;		// | for OR
					break;
				case Item_func::XOR_FUNC:
					*( *buffer + *nodeOffset + LOGIC_OP_OFFSET_OPERATION )	= SDB_PUSHDOWN_OPERATOR_XOR;	// X for XOR
					break;
				default:
					return false;		// Only parse AND and OR logical operators
			}

			*( bool* )( *buffer + *nodeOffset + LOGIC_OP_OFFSET_IS_NEGATED )	= false;						// Operator result should not be negated

			*nodeOffset							   += LOGIC_OP_NODE_LENGTH;



			break;
		}

		case Item::FUNC_ITEM:		//	This node is a comparison operator such as =, >, etc.
		{
			Field*			pField					= NULL;
			unsigned short	columnId				= 0;
			int				countArgs				= ( ( Item_func* ) cond )->argument_count();
			int				typeFunc				= ( ( Item_func* ) cond )->functype();
#ifdef SDB_DEBUG
			bool			isNull;
			bool			isBetween;
#endif
			bool			isIn;
			bool			isNegated;
			bool			isSupportedOperator;

			switch ( typeFunc )
			{
				case Item_func::ISNULL_FUNC:
					if ( countArgs				   == 1 )
					{
#ifdef SDB_DEBUG
						isNull						=
#endif
						isSupportedOperator			= true;
					}
					else
					{
#ifdef SDB_DEBUG
						isNull						=
#endif
						isSupportedOperator			= false;
					}
					isNegated						=
#ifdef SDB_DEBUG
					isBetween						=
#endif
					isIn							= false;
					break;

				case Item_func::ISNOTNULL_FUNC:
					if ( countArgs				   == 1 )
					{
#ifdef SDB_DEBUG
						isNull						=
#endif
						isNegated					=
						isSupportedOperator			= true;
						typeFunc					= Item_func::ISNULL_FUNC;
					}
					else
					{
#ifdef SDB_DEBUG
						isNull						=
#endif
						isNegated					=
						isSupportedOperator			= false;
					}
#ifdef SDB_DEBUG
					isBetween						=
#endif
					isIn							= false;
					break;

				case Item_func::BETWEEN:
					if ( countArgs				   == 3 )
					{
#ifdef SDB_DEBUG
						isBetween					= true;
#endif
						isNegated					= ( ( Item_func_opt_neg* ) cond )->negated;			// Meaningful only for BETWEEN and IN
						isSupportedOperator			= true;
					}
					else
					{
#ifdef SDB_DEBUG
						isBetween					=
#endif
						isNegated					=
						isSupportedOperator			= false;
					}
#ifdef SDB_DEBUG
					isNull							=
#endif
					isIn							= false;
					break;

				case Item_func::IN_FUNC:
					isIn							= true;
					isNegated						= ( ( Item_func_opt_neg* ) cond )->negated;			// Meaningful only for BETWEEN and IN
					isSupportedOperator				= true;
#ifdef SDB_DEBUG
					isNull							=
					isBetween						= false;
#endif
					break;

				case Item_func::NOT_FUNC:
				{
					if ( countArgs				   == 1 )
					{
						Item*		pSubItem		= ( ( Item_func* ) cond )->arguments()[ 0 ];
						Item::Type	typeItem		= pSubItem->type();
						int			_typeFunc		= ( ( Item_func* ) pSubItem )->functype();

						if ( ( typeItem			   == Item::FUNC_ITEM ) && ( _typeFunc == Item_func::LIKE_FUNC ) )
						{
							isNegated				= true;
							isSupportedOperator		= true;
							cond					= pSubItem;

							// Update the count and the function type
							countArgs				= ( ( Item_func* ) cond )->argument_count();
							typeFunc				= ( ( Item_func* ) cond )->functype();
						}
						else
						{
							isNegated				= false;
							isSupportedOperator		= false;

						}
					}
					else
					{
						isNegated					= false;
						isSupportedOperator			= false;
					}

#ifdef SDB_DEBUG
					isNull							=
					isBetween						=
#endif
					isIn							= false;
					break;
				}
				case Item_func::LIKE_FUNC:
				{
#ifdef SDB_DEBUG
					isNull							=
					isBetween						=
#endif
					isIn							=
					isNegated						= false;
					isSupportedOperator				= true;
					break;
				}

				case Item_func::MULT_EQUAL_FUNC:
					return conditionMultEqToString( buffer, nodeOffset, cond, table_list );

				default:
#ifdef SDB_DEBUG
					isNull							=
					isBetween						=
#endif
					isIn							=
					isNegated						= false;
					isSupportedOperator				= ( ( countArgs == 2 ) ? true : false );
			}

			if ( !isSupportedOperator )
			{
				return false;
			}

			pComperandItem							= NULL;
			pComperandData							= NULL;
			offsetComperandData						= 0;

			for ( int i = 0; i < countArgs; i++ )
			{
				Item*		pSubItem				= ( ( Item_func* ) cond )->arguments()[ i ];
				Item::Type	typeItem				= pSubItem->type();

				if ( !i )
				{
					offsetComperandData				= *nodeOffset;
					pComperandData					= *buffer + offsetComperandData;
				}

				switch ( typeItem )
				{
					case Item::FIELD_ITEM:
					{
						// Visit field item
						pField						= ( ( Item_field* ) pSubItem )->field;

						if ( pField->flags			& 256 )
						{
							return false;			//enum type--our check for satisfiability doesn't work for enums
						}

#ifdef SDB_DEBUG
						if ( isBetween			   || isIn )
						{
							if ( i )
							{
								// Field operand should be first
								SDBTerminateEngine( -1, "Unexpected ordering of operands", __FILE__, __LINE__ );
							}
						}
#endif

						if ( typeFunc			   == Item_func::UNKNOWN_FUNC )
						{
							// Determine the function type from the function name
							switch ( *( ( Item_func_bit* ) cond )->func_name() )
							{
								case '&':
									typeFunc		= Item_func::COND_AND_FUNC;
									break;
								case '|':
									typeFunc		= Item_func::COND_OR_FUNC;
									break;
								case '^':
									typeFunc		= Item_func::XOR_FUNC;
									break;
								default:
									return false;
							}
						}

						// Append the column item to the condition string
						columnId					= conditionFieldToString( buffer, nodeOffset, pSubItem, pComperandItem, &offsetComperandData,
																			  countArgs, typeFunc, DBID, TABID, table_list );

						if ( !columnId )
						{
							return false;
						}

						// end visit field item
						break;
					}

#if	MYSQL_VERSION_ID >= 100014
					case Item::FUNC_ITEM:
					{
						// Visit function item

						// Append the function item to the condition string
						if ( !( conditionFunctionToString( buffer, nodeOffset, ( Item_func* ) pSubItem, pComperandItem, &offsetComperandData,
														   countArgs, typeFunc, DBID, TABID, table_list ) ) )
						{
							return false;
						}

						// End visit function item
						break;
					}
#endif

					default:
					{
						// Visit user data item
						//	Append the constant value item to the condition string
						if ( !( conditionConstantToString( buffer, nodeOffset, pSubItem, pComperandItem, &offsetComperandData, DBID, TABID, table_list ) ) )
						{
							return false;
						}

						// end visit user data item
					}
				}

				if ( !i )
				{
					pComperandItem					= pSubItem;
					pComperandData					= *buffer + offsetComperandData;
				}
			}

			// Extend the condition string if necessary
			resultExtension							= checkConditionStringSize( buffer, nodeOffset, COMP_OP_NODE_LENGTH );

			if ( resultExtension					< 0 )
			{
				// Extension failed
				return false;
			}
			if ( resultExtension )
			{
				// Condition string was extended
				if ( offsetComperandData )
				{
					pComperandData					= *buffer + offsetComperandData;
				}
			}

			//Children visited. Write current node info to buffer
			*( unsigned short* )( *buffer + *nodeOffset + COMP_OP_OFFSET_CHILDCOUNT )	= ( unsigned short )( countArgs );			// # of children

			bool			between_end				= false;
			unsigned char	opType;

			switch ( typeFunc )
			{
				case Item_func::EQ_FUNC:
					opType							= ( unsigned char )( SDB_PUSHDOWN_OPERATOR_EQ );
					break;
				case Item_func::LE_FUNC:
					opType							= ( unsigned char )( SDB_PUSHDOWN_OPERATOR_LE );
					break;
				case Item_func::GE_FUNC:
					opType							= ( unsigned char )( SDB_PUSHDOWN_OPERATOR_GE );
					break;
				case Item_func::LT_FUNC:
					opType							= ( unsigned char )( SDB_PUSHDOWN_OPERATOR_LT );
					break;
				case Item_func::GT_FUNC:
					opType							= ( unsigned char )( SDB_PUSHDOWN_OPERATOR_GT );
					break;
				case Item_func::NE_FUNC:
					opType							= ( unsigned char )( SDB_PUSHDOWN_OPERATOR_NE );
					break;
				case Item_func::ISNULL_FUNC:
					opType							= ( unsigned char )( SDB_PUSHDOWN_OPERATOR_ISNULL );
					break;
				case Item_func::BETWEEN:
					between_end						= true;
					opType							= ( unsigned char )( SDB_PUSHDOWN_OPERATOR_BETWEEN );
					break;
				case Item_func::IN_FUNC:
					opType							= ( unsigned char )( SDB_PUSHDOWN_OPERATOR_IN );
					break;
				case Item_func::LIKE_FUNC:
					opType							= ( unsigned char )( SDB_PUSHDOWN_OPERATOR_LIKE );
					break;
				case Item_func::COND_AND_FUNC:
					opType							= SDB_PUSHDOWN_OPERATOR_BITWISE_AND;
					break;
				case Item_func::COND_OR_FUNC:
					opType							= SDB_PUSHDOWN_OPERATOR_BITWISE_OR;
					break;
				case Item_func::XOR_FUNC:
					opType							= SDB_PUSHDOWN_OPERATOR_BITWISE_XOR;
					break;
				default:
					opType							= ( unsigned char )( SDB_PUSHDOWN_NONE );										// Unsupported operator
					return false;
			}

			*( *buffer + *nodeOffset + COMP_OP_OFFSET_OPERATION )			= opType;
			*( bool* )( *buffer + *nodeOffset + COMP_OP_OFFSET_IS_NEGATED )	= isNegated;											// Operator result should [not] be negated

			*nodeOffset							   += COMP_OP_NODE_LENGTH;

			// end visit operator
			if ( rangeBounds.endSet )
			{
				if ( between_end )
				{
					if ( ( rangeBounds.columnId	   == columnId ) && ( rangeBounds.opType == opType ) )
					{
						//have already found the end range, can't be multiple end ranges so fail.
						rangeBounds.valid			= false;
					}
				}
			}
			else
			{
				if ( rangeBounds.startSet )
				{
					if ( rangeBounds.columnId	   == columnId )
					{
						rangeBounds.opType			= opType;

						if ( between_end )
						{
							//must be the end of range.
							rangeBounds.valid		=
							rangeBounds.endSet		= true;
							rangeBounds.endRange	= *nodeOffset;
						}
					}
				}
			}

			break;
		}

		default:
		{
			return false;	//Only parse operator nodes
		}
	}

	return true;
}


//--------------------------------------------------------------------------------------------------
//	Add a converted MULT_EQUAL condition item to a condition string
//--------------------------------------------------------------------------------------------------
bool ha_scaledb::conditionMultEqToString( unsigned char** pCondString, unsigned int* pCondOffset, const COND* pCondMultEq, TABLE_LIST* table_list )
{
	// MULT_EQUAL is a list of items in which the first item is a constant and the remaining items are fields
	unsigned short				dbId			= 0;
	unsigned short				tableId			= 0;
	unsigned short				countEqs		= 0;
	Item_equal*					pItemEqual		= ( Item_equal* ) pCondMultEq;
	Item*						pItem			= pItemEqual->get_const();
	Item_equal_fields_iterator	it( *pItemEqual );
	Item*						pFieldItem;
	Item*						pConstItem;
	unsigned int				offsetFieldData;
	unsigned int				offsetConstData;
	int							resultExtension;

	if ( !pItem )
	{
		pItem									= it++;
	}

	// First item is always the constant item
	pConstItem									= pItem;
	offsetConstData								= *pCondOffset;

	// Convert the list into FIELD1 = CONSTANT  OR  FIELD2 = CONSTANT  OR ...
	while ( ( pItem								= it++ ) )
	{
		// Add an EQ comparison with one constant operand and one field operand in postorder
		pFieldItem								= pItem;
		offsetFieldData							= *pCondOffset;

		// Add the field operand
		if ( !( conditionFieldToString( pCondString, pCondOffset, pFieldItem, NULL, NULL, 2, Item_func::EQ_FUNC, &dbId, &tableId, table_list ) ) )
		{
			return false;
		}

		//	Add the constant operand
		offsetConstData							= *pCondOffset;
		if ( !( conditionConstantToString( pCondString, pCondOffset, pConstItem, pFieldItem, &offsetFieldData, &dbId, &tableId, table_list ) ) )
		{
			return false;
		}

		// Extend the condition string if necessary
		resultExtension							= checkConditionStringSize( pCondString, pCondOffset, COMP_OP_NODE_LENGTH );

		if ( resultExtension					< 0 )
		{
			// Extension failed
			return false;
		}
		
		// Add the EQ operator
		*( unsigned short* )( *pCondString + *pCondOffset + COMP_OP_OFFSET_CHILDCOUNT )		= ( unsigned short )( 2 );							// # of children
		*( *pCondString + *pCondOffset + COMP_OP_OFFSET_OPERATION )							= ( unsigned char  )( SDB_PUSHDOWN_OPERATOR_EQ );	// Operator =
		*( bool* )( *pCondString + *pCondOffset + COMP_OP_OFFSET_IS_NEGATED )				= false;											// Result should not be negated
		*pCondOffset																	   += COMP_OP_NODE_LENGTH;
		countEqs++;
	}

	if ( countEqs								> 1 )
	{
		// Extend the condition string if necessary
		resultExtension							= checkConditionStringSize( pCondString, pCondOffset, LOGIC_OP_NODE_LENGTH );

		if ( resultExtension					< 0 )
		{
			// Extension failed
			return false;
		}

		// Add an OR operator
		*( unsigned short* )( *pCondString + *pCondOffset + LOGIC_OP_OFFSET_CHILDCOUNT )	= ( unsigned short )( countEqs );					// # of children
		*( *pCondString + *pCondOffset + LOGIC_OP_OFFSET_OPERATION )						= SDB_PUSHDOWN_OPERATOR_OR;							// Operator OR
		*( bool* )( *pCondString + *pCondOffset + LOGIC_OP_OFFSET_IS_NEGATED )				= false;											// Result should not be negated
		*pCondOffset																	   += LOGIC_OP_NODE_LENGTH;
	}

	return true;
}


bool ha_scaledb::conditionFunctionToString( unsigned char** pCondString, unsigned int* pItemOffset, Item_func* pFuncItem,
											Item* pComperandItem, unsigned int* pComperandDataOffset,
											unsigned short countArgs, int typeOperation,
											unsigned short* pDbId, unsigned short* pTableId, TABLE_LIST* table_list )
{
	if ( pFuncItem->functype()	   == Item_func::GUSERVAR_FUNC )
	{
		// Handle user-defined variables in the condition
		return conditionFunctionItemToString( pCondString, pItemOffset, pFuncItem, pComperandItem, pComperandDataOffset, pDbId, pTableId, table_list );
	}

	unsigned short	countFuncParams	= pFuncItem->argument_count();
	Item*			pFuncParam;
	unsigned char	typeFuncItem;
	unsigned char	typeResult;
	int				typeFuncParam;
	int				typeField;
	int				resultExtension;

	if ( countFuncParams		   != 1 )
	{
		// Only single operand functions are supported for now
		return false;
	}

	// Determine function type
#if	defined( SDB_WINDOWS ) || defined( __GXX_RTTI )										// typeid() is supported
	if ( typeid( *pFuncItem )	   == typeid( Item_func_inet_aton ) )
	{
		typeFuncItem				= SDB_PUSHDOWN_FUNCTION_INET_ATON;
		typeResult					= SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER;
	}
	else if ( typeid( *pFuncItem ) == typeid( Item_func_inet_ntoa ) )
	{
		typeFuncItem				= SDB_PUSHDOWN_FUNCTION_INET_NTOA;
		typeResult					= SDB_PUSHDOWN_LITERAL_DATA_TYPE_CHAR;
	}
	else if ( typeid( *pFuncItem ) == typeid( Item_func_inet6_aton ) )
	{
		typeFuncItem				= SDB_PUSHDOWN_FUNCTION_INET6_ATON;
		typeResult					= SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER;
	}
	else if ( typeid( *pFuncItem ) == typeid( Item_func_inet6_ntoa ) )
	{
		typeFuncItem				= SDB_PUSHDOWN_FUNCTION_INET6_NTOA;
		typeResult					= SDB_PUSHDOWN_LITERAL_DATA_TYPE_CHAR;
	}
	else if ( typeid( *pFuncItem ) == typeid( Item_func_is_ipv4 ) )
	{
		typeFuncItem				= SDB_PUSHDOWN_FUNCTION_IS_IPV4;
		typeResult					= SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER;
	}
	else if ( typeid( *pFuncItem ) == typeid( Item_func_is_ipv6 ) )
	{
		typeFuncItem				= SDB_PUSHDOWN_FUNCTION_IS_IPV6;
		typeResult					= SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER;
	}
	else if ( typeid( *pFuncItem ) == typeid( Item_func_is_ipv4_compat ) )
	{
		typeFuncItem				= SDB_PUSHDOWN_FUNCTION_IS_IPV4_COMPAT;
		typeResult					= SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER;
	}
	else if ( typeid( *pFuncItem ) == typeid( Item_func_is_ipv4_mapped ) )
	{
		typeFuncItem				= SDB_PUSHDOWN_FUNCTION_IS_IPV4_MAPPED;
		typeResult					= SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER;
	}
	else
	{
		// Not a supported function type
		return false;
	}
#else	// typeid() is not supported
	const char*	pFuncName			= pFuncItem->func_name();

	if ( *( pFuncName++ )		   != 'i' )
	{
		// Not a supported function type
		return false;
	}

	if ( 0						   == strncmp( pFuncName, "net", 3 ) )
	{
		// INET...
		pFuncName				   += 3;

		if ( *pFuncName			   == '_' )
		{
			// INET_...
			pFuncName++;

			if ( 0				   == strcmp( pFuncName, "aton" ) )
			{
				typeFuncItem		= SDB_PUSHDOWN_FUNCTION_INET_ATON;
				typeResult			= SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER;
			}
			else if ( 0			   == strcmp( pFuncName, "ntoa" ) )
			{
				typeFuncItem		= SDB_PUSHDOWN_FUNCTION_INET_NTOA;
				typeResult			= SDB_PUSHDOWN_LITERAL_DATA_TYPE_CHAR;
			}
			else
			{
				// Not a supported function type
				return false;
			}
		}
		else if ( 0				   == strncmp( pFuncName, "6_", 2 ) )
		{
			// INET6_...
			pFuncName			   += 2;

			if ( 0				   == strcmp( pFuncName, "aton" ) )
			{
				typeFuncItem		= SDB_PUSHDOWN_FUNCTION_INET6_ATON;
				typeResult			= SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER;
			}
			else if ( 0			   == strcmp( pFuncName, "ntoa" ) )
			{
				typeFuncItem		= SDB_PUSHDOWN_FUNCTION_INET6_NTOA;
				typeResult			= SDB_PUSHDOWN_LITERAL_DATA_TYPE_CHAR;
			}
			else
			{
				// Not a supported function type
				return false;
			}
		}
		else
		{
			// Not a supported function type
			return false;
		}
	}
	else if ( 0					   == strncmp( pFuncName, "s_ipv", 5 ) )
	{
		// IS_IPV...
		pFuncName				   += 5;

		if ( 0					   == strcmp( pFuncName, "4" ) )
		{
			typeFuncItem			= SDB_PUSHDOWN_FUNCTION_IS_IPV4;
			typeResult				= SDB_PUSHDOWN_LITERAL_DATA_TYPE_BIT;
		}
		else if ( 0				   == strncmp( pFuncName, "4_", 2 ) )
		{
			// IS_IPV4_...
			pFuncName			   += 2;

			if ( 0				   == strcmp( pFuncName, "compat" ) )
			{
				typeFuncItem		= SDB_PUSHDOWN_FUNCTION_IS_IPV4_COMPAT;
				typeResult			= SDB_PUSHDOWN_LITERAL_DATA_TYPE_BIT;
			}
			else if ( 0			   == strcmp( pFuncName, "mapped" ) )
			{
				typeFuncItem		= SDB_PUSHDOWN_FUNCTION_IS_IPV4_MAPPED;
				typeResult			= SDB_PUSHDOWN_LITERAL_DATA_TYPE_BIT;
			}
			else
			{
				// Not a supported function type
				return false;
			}
		}
		else if ( 0				   == strcmp( pFuncName, "6" ) )
		{
			typeFuncItem			= SDB_PUSHDOWN_FUNCTION_IS_IPV6;
			typeResult				= SDB_PUSHDOWN_LITERAL_DATA_TYPE_BIT;
		}
	}
	else
	{
		// Not a supported function type
		return false;
	}
#endif	// typeid() is not supported

	// Only single operand functions are supported for now
	pFuncParam						= pFuncItem->arguments()[ 0 ];
	typeFuncParam					= pFuncParam->type();

	if ( typeFuncParam			   != Item::FIELD_ITEM )
	{
		// Only field parameters are supported for now
		return false;
	}

	typeField						= ( ( ( Item_field* ) pFuncParam )->field )->type();

	// Only string parameter types are supported for now
	switch ( typeField )
	{
		case MYSQL_TYPE_STRING:
			break;
		default:
			return false;
	}

	// Extend the condition string if necessary
	resultExtension					= checkConditionStringSize( pCondString, pItemOffset, FUNC_OP_NODE_LENGTH );

	if ( resultExtension			< 0 )
	{
		// Extension failed
		return false;
	}

	// Append the parameter field item to the condition string
	if ( !( conditionFieldToString( pCondString, pItemOffset, pFuncParam, pComperandItem, pComperandDataOffset, countArgs, typeOperation, pDbId, pTableId, table_list ) ) )
	{
		return false;
	}

	// Child visited
	//	Append the function item to the condition string
	*( unsigned short* )( *pCondString + *pItemOffset + FUNC_OP_OFFSET_CHILDCOUNT )	= ( unsigned short )( countFuncParams );	// # of function parameters
	*( *pCondString + *pItemOffset + FUNC_OP_OFFSET_TYPE )							= typeFuncItem;								// function type
	*( *pCondString + *pItemOffset + FUNC_OP_OFFSET_RESULT_TYPE )					= ( unsigned char ) typeResult;				// function result type

	*pComperandDataOffset															= *pItemOffset;
	*pItemOffset																   += FUNC_OP_NODE_LENGTH;

	return true;
}


//--------------------------------------------------------------------------------------------------
//	Add a table column field item to a condition string
//--------------------------------------------------------------------------------------------------
unsigned short ha_scaledb::conditionFieldToString( unsigned char** pCondString, unsigned int* pItemOffset, Item* pFieldItem,
												   Item* pComperandItem, unsigned int* pComperandDataOffset,
												   unsigned short countArgs, int typeFunc, unsigned short* pDbId, unsigned short* pTableId, TABLE_LIST* table_list)
{
	unsigned char*	pComperandData	= ( pComperandDataOffset ? ( *pCondString + *pComperandDataOffset ) : NULL );
	unsigned char*	pColumnData;
	int				resultExtension;
	int				fieldType;
#ifdef	SDB_DEBUG
	bool			isBetween;
#endif
	bool			isIn;
	bool			isLike;
	bool			doConvertUserTimestamp;

	// Extend the condition string if necessary
	resultExtension					= checkConditionStringSize( pCondString, pItemOffset, ROW_DATA_NODE_LENGTH );

	if ( resultExtension			< 0 )
	{
		// Extension failed
		return 0;
	}
	if ( resultExtension )
	{
		// Condition string was extended
		if ( pComperandDataOffset )
		{
			pComperandData			= *pCondString + *pComperandDataOffset;
		}
	}

	switch ( typeFunc )
	{
		case Item_func::BETWEEN:
#ifdef	SDB_DEBUG
			isBetween				= true;
#endif
			isIn					= false;
			isLike					= false;
			break;
		case Item_func::IN_FUNC:
#ifdef	SDB_DEBUG
			isBetween				= false;
#endif
			isIn					= true;
			isLike					= false;
			break;
		case Item_func::LIKE_FUNC:
#ifdef	SDB_DEBUG
			isBetween				= false;
#endif
			isIn					= false;
			isLike					= true;
			break;
		default:
#ifdef	SDB_DEBUG
			isBetween				=
#endif
			isIn					=
			isLike					= false;
	}

	if ( isIn )
	{
		*( unsigned short* )( *pCondString + *pItemOffset + ROW_DATA_OFFSET_CHILDCOUNT )	= ( unsigned short )( countArgs );	// # of children of the IN operator
	}
	else
	{
		*( unsigned short* )( *pCondString + *pItemOffset + ROW_DATA_OFFSET_CHILDCOUNT )	= ( unsigned short )( 0 );			// # of children of the row data
	}

	pColumnData						= *pCondString + *pItemOffset;

	// Fetch the database, table and column metadata
	char*			databaseName	= SDBUtilDuplicateString( ( char* )( ( Item_ident* ) pFieldItem )->db_name );
	unsigned short	dbId			= SDBGetDatabaseNumberByName( sdbUserId_, databaseName );

	FREE_MEMORY( databaseName );

	if ( ( *pDbId )				   && ( *pDbId != dbId ) )
	{
		// We do not support a WHERE clause on multiple databases
		return 0;
	}

	unsigned short	tableId			= getTableIDfromNameOrAlias(table_list, sdbUserId_, dbId, ( ( ( Item_ident* ) pFieldItem )->table_name ) );

	if ( !tableId )
	{
		return 0;				// This seems to happen when the table is renamed
	}

	if ( ( *pTableId )			   && ( *pTableId != tableId ) )
	{
		// We do not support a WHERE clause on multiple tables
		return 0;
	}

	unsigned short	columnId		= SDBGetColumnNumberByName( dbId, tableId, ( ( ( Item_ident* ) pFieldItem )->field_name ) );

	if ( !columnId )
	{
		return 0;
	}

	if ( SDBGetRangeKeyFieldID( sdbDbId(), sdbTableNumber() ) == columnId )
	{
		//this is the start of a range specification
		if ( !( rangeBounds.startSet ) )
		{
#ifdef	SDB_DEBUG
			if ( rangeBounds.endRange || rangeBounds.valid )
			{
				SDBTerminateEngine( 0, "Invalid range key bounds", __FILE__, __LINE__ );
			}
#endif

			rangeBounds.startSet		= true;
			rangeBounds.columnId		= columnId;
			rangeBounds.childCount++;

			if ( pComperandItem )
			{
				rangeBounds.startRange	= *pComperandDataOffset;
			}
			else
			{
				rangeBounds.startRange	= *pItemOffset;
			}
		}
		else
		{
			if ( rangeBounds.endSet )
			{
				//have already found the end range, can't be multiple end ranges so fail.
				rangeBounds.valid		= false;
			}
			else
			{
				rangeBounds.childCount++;
			}
		}
	}
	else if ( rangeBounds.startSet && !( rangeBounds.endSet ) )
	{
		// A field that is not the range key column marks the end of a range specification
		rangeBounds.valid				=
		rangeBounds.endSet				= true;

		if ( pComperandItem )
		{
			rangeBounds.endRange		= *pComperandDataOffset;
		}
		else
		{
			rangeBounds.endRange		= *pItemOffset;
		}
	}
#ifdef	SDB_DEBUG
	else if ( ( rangeBounds.startSet && rangeBounds.endSet && !( rangeBounds.endRange ) ) )
	{
		// Start range and end range have been found, but the end of the range has not been set yet
		SDBTerminateEngine( 0, "Invalid range key bounds", __FILE__, __LINE__ );
	}
#endif

	unsigned short	columnSize		= SDBGetColumnSizeByNumber( dbId, tableId, columnId );
	unsigned char	columnType		= SDBGetColumnTypeByNumber( dbId, tableId, columnId );
	unsigned short	columnOffset	= SDBGetColumnOffsetByNumber( dbId, tableId, columnId );

	fieldType						= ( ( ( Item_field* ) pFieldItem )->field )->type();

	doConvertUserTimestamp			= false;

	switch ( fieldType )
	{
		case MYSQL_TYPE_TINY:
		case MYSQL_TYPE_SHORT:
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_LONGLONG:
			if ( ( ( ( Item_field* ) pFieldItem )->field )->flags	& UNSIGNED_FLAG )
			{
				*( *pCondString + *pItemOffset + ROW_DATA_OFFSET_ROW_TYPE )	=  SDB_PUSHDOWN_COLUMN_DATA_TYPE_UNSIGNED_INTEGER;	// unsigned int (row)
			}
			else
			{
				*( *pCondString + *pItemOffset + ROW_DATA_OFFSET_ROW_TYPE )	=  SDB_PUSHDOWN_COLUMN_DATA_TYPE_SIGNED_INTEGER;	//   signed int (row)
			}
			break;

		case MYSQL_TYPE_BIT:
			*( *pCondString + *pItemOffset + ROW_DATA_OFFSET_ROW_TYPE )		= SDB_PUSHDOWN_COLUMN_DATA_TYPE_BIT;				// bit (row)
			break;

		case MYSQL_TYPE_STRING:
			if ( columnType		   == SDB_BLOB )
			{
				*( *pCondString + *pItemOffset + ROW_DATA_OFFSET_ROW_TYPE )	= SDB_PUSHDOWN_COLUMN_DATA_TYPE_BINARY;				// binary string (row)
			}
			else
			{
				*( *pCondString + *pItemOffset + ROW_DATA_OFFSET_ROW_TYPE )	= SDB_PUSHDOWN_COLUMN_DATA_TYPE_CHAR;				// char string (row)
			}
			break;

		case MYSQL_TYPE_DOUBLE:
		case MYSQL_TYPE_FLOAT:
			*( *pCondString + *pItemOffset + ROW_DATA_OFFSET_ROW_TYPE )		= SDB_PUSHDOWN_COLUMN_DATA_TYPE_FLOAT;				// float (row)
			break;

		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_NEWDECIMAL:
			*( *pCondString + *pItemOffset + ROW_DATA_OFFSET_ROW_TYPE )		= SDB_PUSHDOWN_COLUMN_DATA_TYPE_DECIMAL;			// binary decimal (row)
			break;

		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_NEWDATE:
			*( *pCondString + *pItemOffset + ROW_DATA_OFFSET_ROW_TYPE )		= SDB_PUSHDOWN_COLUMN_DATA_TYPE_DATE;				// date (row)
			break;

		case MYSQL_TYPE_TIME:
			*( *pCondString + *pItemOffset + ROW_DATA_OFFSET_ROW_TYPE )		= SDB_PUSHDOWN_COLUMN_DATA_TYPE_TIME;				// time (row)
			break;

		case MYSQL_TYPE_DATETIME:
			*( *pCondString + *pItemOffset + ROW_DATA_OFFSET_ROW_TYPE )		= SDB_PUSHDOWN_COLUMN_DATA_TYPE_DATETIME;			// datetime (row)
			break;

		case MYSQL_TYPE_YEAR:
			*( *pCondString + *pItemOffset + ROW_DATA_OFFSET_ROW_TYPE )		= SDB_PUSHDOWN_COLUMN_DATA_TYPE_YEAR;				// year (row)
			break;

		case MYSQL_TYPE_TIMESTAMP:
			if ( SDBIsUserTimeKey( dbId, tableId, columnId ) )
			{
#ifdef	SDB_USER_TIME_CONVERT_TO_INTERVAL
				doConvertUserTimestamp										= true;
#endif
				*( *pCondString + *pItemOffset + ROW_DATA_OFFSET_ROW_TYPE )	= SDB_PUSHDOWN_COLUMN_DATA_TYPE_USER_TIMESTAMP;		// user timestamp (row)
			}
			else
			{
				*( *pCondString + *pItemOffset + ROW_DATA_OFFSET_ROW_TYPE )	= SDB_PUSHDOWN_COLUMN_DATA_TYPE_TIMESTAMP;			// timestamp (row)
			}
			break;

		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
#ifdef	VAR_STREAM
			if ( SDBIsStreamingTable( dbId, tableId ) )
			{
				*( *pCondString + *pItemOffset + ROW_DATA_OFFSET_ROW_TYPE )	= SDB_PUSHDOWN_COLUMN_DATA_TYPE_VARCHAR;			// varchar string (row)
				break;
			}
#endif
			// Fall through ...

		default:
			*( *pCondString + *pItemOffset + ROW_DATA_OFFSET_ROW_TYPE )		= SDB_PUSHDOWN_NONE;								// unknown (row)
			return 0;
	}

	*pDbId																					= dbId;
	*pTableId																				= tableId;

	// Write field (row) data info to string
	*( unsigned short* )( *pCondString + *pItemOffset + ROW_DATA_OFFSET_DATABASE_NUMBER )	= dbId;
	*( unsigned short* )( *pCondString + *pItemOffset + ROW_DATA_OFFSET_TABLE_NUMBER )		= tableId;
	*( unsigned short* )( *pCondString + *pItemOffset + ROW_DATA_OFFSET_COLUMN_NUMBER )		= columnId;
	*( unsigned short* )( *pCondString + *pItemOffset + ROW_DATA_OFFSET_COLUMN_OFFSET )		= columnOffset;
	*( unsigned short* )( *pCondString + *pItemOffset + ROW_DATA_OFFSET_COLUMN_SIZE )		= columnSize;

	switch ( fieldType )
	{
		case MYSQL_TYPE_STRING:
#ifdef	VAR_STREAM
		case MYSQL_TYPE_VAR_STRING:
#endif
		case MYSQL_TYPE_VARCHAR:
			break;

		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_NEWDECIMAL:
			if ( isIn )
			{
				if ( columnSize		> sizeof( long long ) )
				{
					return 0;
				}
			}
			else if ( columnSize	> 65535 )
			{
				return 0;
			}
			if ( isLike )
			{
				// We support only character data types with LIKE
				return 0;
			}
			break;

		default:
			if ( columnSize			> sizeof( long long ) )
			{
				return 0;
			}
			if ( isLike )
			{
				// We support only character data types with LIKE
				return 0;
			}
	}

	// Patch the comperand entry
	switch ( fieldType )
	{
		case MYSQL_TYPE_TINY:
		case MYSQL_TYPE_SHORT:
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_LONGLONG:
			if ( pComperandItem )
			{
				Item_field*			pField			= ( pFieldItem->with_field ? ( ( Item_field* ) pFieldItem ) : NULL );
				Item_field*			pComperandField	= ( pComperandItem->with_field ? ( ( Item_field* ) pComperandItem ) : NULL );
				unsigned char*		pType			= ( unsigned char*  )( pComperandData + USER_DATA_OFFSET_DATA_TYPE );
				unsigned char*		pValue			= ( unsigned char*  )( pComperandData + USER_DATA_OFFSET_USER_DATA );
				unsigned short*		pSize			= ( unsigned short* )( pComperandData + USER_DATA_OFFSET_DATA_SIZE );
				long long			intValue;

				if ( pComperandData				   && ( *pSize != sizeof( long long ) ) )
				{
					// Comperand of non-numeric size
					//	Move the field's data to accommodate an 8-byte constant
					int				moveSize		= *pSize - sizeof( long long );

					memmove( *pCondString + *pItemOffset - moveSize, *pCondString + *pItemOffset, ROW_DATA_NODE_LENGTH );
					( *pItemOffset )			   -= moveSize;
					*pSize							= sizeof( long long );
				}

				if ( pComperandItem->type()		   == Item::CACHE_ITEM )
				{
					intValue						= ( long long ) ( ( Item_cache_int* ) pComperandItem )->val_int();
				}
				else
				{
					intValue						= ( long long ) ( ( Item_int* ) pComperandItem )->val_int();
				}

				if ( pComperandField			   && ( ( ( Item_field* ) pComperandItem )->field )->flags	& UNSIGNED_FLAG )
				{
					unsigned
					long long		uintValue		= ( unsigned long long ) intValue;

#ifdef	SDB_PUSHDOWN_CHECK_INTEGER_LIMITS
					if ( pComperandData )
					{
						checkUIntLimits( *pCondString + *pItemOffset, uintValue );
					}
#endif

					*pType							= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER );
					*( unsigned
					   long long* ) pValue			= uintValue;
				}
				else if ( pComperandData		   && ( *pType != SDB_PUSHDOWN_LITERAL_DATA_TYPE_SIGNED_INTEGER ) )
				{
					switch ( *pType )
					{
					case SDB_PUSHDOWN_LITERAL_DATA_TYPE_FLOAT:
					case SDB_PUSHDOWN_LITERAL_DATA_TYPE_DECIMAL:
						intValue					= ( long long ) ( ( Item_decimal* ) pComperandItem )->val_int();
						break;
					}

#ifdef	SDB_PUSHDOWN_CHECK_INTEGER_LIMITS
					checkIntLimits( *pCondString + *pItemOffset, intValue );
#endif

					*pType							= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_SIGNED_INTEGER );
					*( long long* ) pValue			= intValue;
				}
			}
			break;

		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_NEWDECIMAL:
			if ( pComperandItem )
			{
				// Convert the comperand to a decimal of the same precision and scale as the field, and deposit the converted value into the condition string
				int					diffSize;

				if ( !( convertDecimalToPrecisionAndScale( pFieldItem, *pCondString + *pItemOffset, pComperandItem, pComperandData, &diffSize ) ) )
				{
					return 0;
				}

				if ( diffSize )
				{
					// Current entry was moved in the condition string to accommodate the constant's modified size
					( *pItemOffset )		   -= diffSize;
				}
			}
			break;

		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_NEWDATE:
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_YEAR:
		{
			if ( pComperandItem )
			{
				if ( pComperandItem->type()		   == Item::INT_ITEM )
				{
					switch ( fieldType )
					{
						case MYSQL_TYPE_TIMESTAMP:
							*( pComperandData + USER_DATA_OFFSET_DATA_TYPE )	= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_TIMESTAMP );
							break;
						case MYSQL_TYPE_DATETIME:
							*( pComperandData + USER_DATA_OFFSET_DATA_TYPE )	= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_DATETIME );
							break;
						case MYSQL_TYPE_TIME:
							*( pComperandData + USER_DATA_OFFSET_DATA_TYPE )	= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_TIME );
							break;
						default:
							if ( fieldType		   == MYSQL_TYPE_YEAR )
							{
								// Convert the comperand year value to its stored format
								long long	lValue	= ( ( Item_int* ) pComperandItem )->value;

								*( ( unsigned long long* )( pComperandData + USER_DATA_OFFSET_USER_DATA ) )	= ( unsigned long long ) convertYearToStoredFormat( lValue,0 );
							}
							*( pComperandData + USER_DATA_OFFSET_DATA_TYPE )	= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_YEAR );
					}
				}
				else
				{
					// Minimum valid time string length is 10: '1970-01-01'
					THD*			pMysqlThd		= ha_thd();
					int				dataType		= ( ( ( Item_field* ) pFieldItem )->field )->type();
					const char*		pString;
					unsigned int	stringSize;
					int				diffSize;
					bool			isFromString;

					if ( Item::CACHE_ITEM		   == pComperandItem->type() )
					{
						// Time string was copied to the comperand's data
						MYSQL_TIME	myTime;

						// Get the cached value as a time value
						getTimeCachedResult( pMysqlThd, pComperandItem, dataType, &myTime );

						// Convert the time value to the field's data type, and deposit into the condition string
						if ( !( convertTimeToType( pMysqlThd, pFieldItem, dataType, &myTime,
												   pComperandData + USER_DATA_OFFSET_DATA_TYPE,
												   pComperandData + USER_DATA_OFFSET_USER_DATA,
												   doConvertUserTimestamp, dbId, tableId ) ) )
						{
							// Time conversion error
							return false;
						}

						isFromString				= false;

						diffSize					= *( unsigned short* )( pComperandData + USER_DATA_OFFSET_DATA_SIZE ) - 8;
					}
					else if ( Item::FUNC_ITEM	   == pComperandItem->type() )
					{
						switch ( ( ( Item_func* ) pComperandItem )->functype() )
						{
							case Item_func::GUSERVAR_FUNC:
								switch ( ( ( Item_func_get_user_var* ) pFieldItem )->result_type() )
								{
									case STRING_RESULT:
										// Time string was copied to the comperand's data
										pString			=  ( const char*     )( pComperandData + USER_DATA_OFFSET_USER_DATA );
										stringSize		= *( unsigned short* )( pComperandData + USER_DATA_OFFSET_DATA_SIZE );
										isFromString	= false;
										diffSize		= ( ( int ) stringSize ) - 8;

										// Convert the time string to a time value, and deposit into the condition string
										if ( !( convertTimeStringToTimeConstant( pMysqlThd, pComperandItem, pString, stringSize, dataType,
																				 pComperandData + USER_DATA_OFFSET_DATA_TYPE,
																				 pComperandData + USER_DATA_OFFSET_USER_DATA,
																				 doConvertUserTimestamp, dbId, tableId ) ) )
										{
											// Time conversion error
											return false;
										}
										break;

									case INT_RESULT:
										// No further conversion is necessary
										isFromString	= false;
										diffSize		= 0;
										break;

									default:
										return 0;
								}
								break;

							default:
								return 0;
						}
					}
					else
					{
#if MYSQL_VERSION_ID < 100014
						pString						= ( ( ( Item_int* ) pComperandItem )->str_value ).ptr();
						stringSize					= ( ( ( Item_int* ) pComperandItem )->str_value ).length();
#else
						pString						= ( ( ( Item* ) pComperandItem )->val_str() )->ptr();
						stringSize					= ( ( ( Item* ) pComperandItem )->val_str() )->length();
#endif
						isFromString				= true;
						diffSize					= ( ( int ) stringSize ) - 8;
					}

					if ( diffSize )
					{
						// Move the current entry to its new position in the condition string
						memmove( *pCondString + *pItemOffset - diffSize, *pCondString + *pItemOffset, ROW_DATA_NODE_LENGTH );
						( *pItemOffset )		   -= diffSize;
					}

					if ( isFromString )
					{
						// Convert the time string to a time value, and deposit into the condition string
						if ( !( convertTimeStringToTimeConstant( pMysqlThd, pComperandItem, pString, stringSize, dataType,
																 pComperandData + USER_DATA_OFFSET_DATA_TYPE,
																 pComperandData + USER_DATA_OFFSET_USER_DATA,
																 doConvertUserTimestamp, dbId, tableId ) ) )
						{
							// Time conversion error
							return false;
						}
					}

					*( unsigned short* )( pComperandData + USER_DATA_OFFSET_DATA_SIZE )	= ( unsigned short )( 8 );
				}
			}
			break;
		}

		case MYSQL_TYPE_STRING:
#ifdef	VAR_STREAM
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_VARCHAR:
#endif
			if ( pComperandItem )
			{
				unsigned char			pFieldEntry[ ROW_DATA_NODE_LENGTH ];

				// Save a copy of the field entry created above
				memcpy( pFieldEntry, *pCondString + *pItemOffset, ROW_DATA_NODE_LENGTH );

				switch ( pComperandItem->type() )
				{
					case Item::INT_ITEM:
					{
						const uint		maxStringSize( 65535 );	// Maximum length of a string
						THD*			pMysqlThd	= ha_thd();
						unsigned int	diffSize	= maxStringSize - sizeof( long long );
						long long		intValue	= ( ( Item_int* ) pComperandItem )->value;

						*pItemOffset				= *pComperandDataOffset;

						// Extend the condition string if necessary
						resultExtension				= checkConditionStringSize( pCondString, pItemOffset, diffSize );

						if ( resultExtension		< 0 )
						{
							// Extension failed
							return 0;
						}
						if ( resultExtension )
						{
							// Condition string was extended
							if ( pComperandDataOffset )
							{
								pComperandData		= *pCondString + *pComperandDataOffset;
							}
						}

						String			stringValue( ( char* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA ), maxStringSize, pMysqlThd->charset() );
						const char*		pString;
						unsigned int	stringSize;

						// Convert the integer value to a string
						( ( Item_int* ) pComperandItem )->val_str( &stringValue );
						pString						= stringValue.ptr();
						stringSize					= stringValue.length();

						if ( stringSize				> maxStringSize )
						{
							return 0;
						}

						if ( columnType			   == SDB_BLOB )
						{
							*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )					= ( unsigned char  )
																											  ( SDB_PUSHDOWN_LITERAL_DATA_TYPE_BINARY );
						}
						else
						{
							*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )					= ( unsigned char  )
																											  ( SDB_PUSHDOWN_LITERAL_DATA_TYPE_CHAR );
						}
						*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )	= ( unsigned short )( stringSize );
						*pItemOffset																	   += ( unsigned int   )( USER_DATA_OFFSET_USER_DATA + stringSize );
						break;
					}

					case Item::CACHE_ITEM:
					{
						int				cacheType	= ( ( Item_cache_decimal* ) pComperandItem )->field_type();

						switch ( cacheType )
						{
#if	MYSQL_VERSION_ID >= 100014
						case MYSQL_TYPE_VAR_STRING:
						case MYSQL_TYPE_STRING:
						{
							// Handle BINARY values used with INET functions
							const char*	pString		= ( ( ( Item* ) pComperandItem )->val_str() )->ptr();
							uint		stringSize	= ( ( ( Item* ) pComperandItem )->val_str() )->length();

							if ( ( columnSize			!= stringSize ) &&													// not an IPV6 function
								 ( ( columnSize != 16 ) || ( stringSize != 10 ) ) )											// not an IPV4 function
							{
								return 0;
							}
						}
#endif

						default:
							// Other types are currently unsupported
							return 0;
						}
					}

					default:
						if ( columnType			   == SDB_BLOB )
						{
							*( pComperandData + USER_DATA_OFFSET_DATA_TYPE )								= ( unsigned char  )
																											  ( SDB_PUSHDOWN_LITERAL_DATA_TYPE_BINARY );
						}
						else
						{
							// Other types are currently unsupported
							return 0;
						}
				}

				// Restore the field entry to its new position following the comperand entry
				memcpy( *pCondString + *pItemOffset, pFieldEntry, ROW_DATA_NODE_LENGTH );
			}
			break;

		case MYSQL_TYPE_FLOAT:
		case MYSQL_TYPE_DOUBLE:
			if ( pComperandItem )
			{
				// Use the floating point representation of the value
				double				dValue			= ( ( Item_decimal* ) pComperandItem )->val_real();

				*( pComperandData + USER_DATA_OFFSET_DATA_TYPE )						= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_FLOAT );
				*( double* )( pComperandData + USER_DATA_OFFSET_USER_DATA )				= dValue;
			}
			break;
	}

	*pItemOffset				   += ROW_DATA_NODE_LENGTH;

	return columnId;
}


//--------------------------------------------------------------------------------------------------
//	Add a constant value to a condition string
//--------------------------------------------------------------------------------------------------
bool ha_scaledb::conditionConstantToString( unsigned char** pCondString, unsigned int* pItemOffset, Item* pConstItem, Item* pComperandItem, unsigned int* pComperandDataOffset,
											unsigned short* pDbId, unsigned short* pTableId,TABLE_LIST* table_list )
{
	unsigned char*	pComperandData	= ( pComperandDataOffset ? ( *pCondString + *pComperandDataOffset ) : NULL );
	int				resultExtension;
	bool			doConvertUserTimestamp;

	// Extend the condition string if necessary
	resultExtension					= checkConditionStringSize( pCondString, pItemOffset, USER_DATA_OFFSET_USER_DATA + 8 );

	if ( resultExtension			< 0 )
	{
		// Extension failed
		return false;
	}
	if ( resultExtension )
	{
		// Condition string was extended
		if ( pComperandDataOffset )
		{
			pComperandData			= *pCondString + *pComperandDataOffset;
		}
	}

	doConvertUserTimestamp			= false;

	*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_CHILDCOUNT )	= ( unsigned short )( 0 ); // # of children

	switch ( pConstItem->type() )
	{
		case Item::INT_ITEM:
		{
			Item_field*		pField				= ( pConstItem->with_field ? ( ( Item_field* ) pConstItem )  : NULL );
			Item_field*		pComperandField;
			int				typeComperand;
			bool			isFunctionComperand;

			if ( pComperandItem )
			{
				switch ( *( pComperandData + FUNC_OP_OFFSET_TYPE ) )
				{
					casePushdownFunction:												// ALL SUPPORTED PUSHDOWN FUNCTION TYPES
						pComperandField			= NULL;
						typeComperand			= ( int )( *( pComperandData + FUNC_OP_OFFSET_RESULT_TYPE ) );
						isFunctionComperand		= true;
						break;

					default:
						pComperandField			= ( ( pComperandItem && pComperandItem->with_field )   ? ( ( Item_field* ) pComperandItem ) : NULL );
						typeComperand			= ( pComperandItem ? pComperandItem->type() : -1 );
						isFunctionComperand		= false;
				}
			}
			else
			{
				pComperandField					= NULL;
				typeComperand					= 0;
				isFunctionComperand				= false;
			}

			if ( typeComperand				   == Item::FUNC_ITEM )
			{
				// Function result is not available during query preparation
				return false;
			}

			typeComperand						= ( pComperandField ? ( pComperandField->field )->type() : -1 );

			bool			isUnsignedComperand	= ( isFunctionComperand ? true : ( ( ( typeComperand  >= 0 ) && pComperandField ) ?
													( ( pComperandField->field->flags & UNSIGNED_FLAG )  ? true : false  ) : false ) );
			long long		intValue			= ( ( Item_int* ) pConstItem )->value;

			switch ( typeComperand )
			{
				case MYSQL_TYPE_STRING:
				{
					const uint		maxStringSize( 65535 );	// Maximum length of a string
					THD*			pMysqlThd	= ha_thd();
					Item_field*		pField		= ( Item_field* ) pConstItem;

					// Extend the condition string if necessary
					resultExtension				= checkConditionStringSize( pCondString, pItemOffset, USER_DATA_OFFSET_USER_DATA + maxStringSize );

					if ( resultExtension		< 0 )
					{
						// Extension failed
						return false;
					}
					if ( resultExtension )
					{
						// Condition string was extended
						if ( pComperandDataOffset )
						{
							pComperandData		= *pCondString + *pComperandDataOffset;
						}
					}

					String			stringValue( ( char* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA ), maxStringSize, pMysqlThd->charset() );
					const char*		pString;
					unsigned int	stringSize;

					// Convert the integer value to a string
					( ( Item_int* ) pConstItem )->val_str( &stringValue );
					pString						= stringValue.ptr();
					stringSize					= stringValue.length();

					if ( stringSize				> maxStringSize )
					{
						return false;
					}

					if ( *( pComperandData + USER_DATA_OFFSET_DATA_TYPE )										   == SDB_PUSHDOWN_COLUMN_DATA_TYPE_BINARY )
					{
						*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )								= ( unsigned char  )
																													  ( SDB_PUSHDOWN_LITERAL_DATA_TYPE_BINARY );
					}
					else
					{
						*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )								= ( unsigned char  )
																													  ( SDB_PUSHDOWN_LITERAL_DATA_TYPE_CHAR );
					}
					*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )				= ( unsigned short )( stringSize );
					*pItemOffset																				   += ( unsigned int   )( USER_DATA_OFFSET_USER_DATA + stringSize );
					break;
				}

				case MYSQL_TYPE_BIT:
					*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )									= ( unsigned char  )
																													  ( SDB_PUSHDOWN_LITERAL_DATA_TYPE_BIT );
					*( ( unsigned long long* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA ) )		= ( unsigned long long ) intValue;
					*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )				= ( unsigned short )( 8 );		// size in bytes of int
					*pItemOffset																				   += USER_DATA_OFFSET_USER_DATA + 8;
					break;

				case MYSQL_TYPE_TIMESTAMP:
					if ( *( *pCondString + *pComperandDataOffset + ROW_DATA_OFFSET_ROW_TYPE )					   == SDB_PUSHDOWN_COLUMN_DATA_TYPE_USER_TIMESTAMP )
					{
						// User timestamp
#ifdef	SDB_USER_TIME_CONVERT_TO_INTERVAL
						doConvertUserTimestamp																		= true;
#endif

						// Adjust the user time value according to the table's time interval type
						unsigned short	dbId																		= *( unsigned short* )
																													  ( pComperandData + ROW_DATA_OFFSET_DATABASE_NUMBER );
						unsigned short	tableId																		= *( unsigned short* )
																													  ( pComperandData + ROW_DATA_OFFSET_TABLE_NUMBER );

						intValue																					= SDBTruncateUserTimeValue( dbId, tableId, intValue );
					}
					*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )									= ( unsigned char  )
																													  ( SDB_PUSHDOWN_LITERAL_DATA_TYPE_TIMESTAMP );
					*( ( long long*    )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA ) )				= intValue;
					*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )				= ( unsigned short )( 8 );		// size in bytes of int
					*pItemOffset																				   += USER_DATA_OFFSET_USER_DATA + 8;
					break;

				case MYSQL_TYPE_YEAR:
					*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )									= ( unsigned char  )
																													  ( SDB_PUSHDOWN_LITERAL_DATA_TYPE_YEAR );
					*( ( unsigned long long* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA ) )		= ( unsigned long long ) convertYearToStoredFormat( intValue,pComperandField->field->field_length );
					*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )				= ( unsigned short )( 8 );		// size in bytes of int
					*pItemOffset																				   += USER_DATA_OFFSET_USER_DATA + 8;
					break;

				case MYSQL_TYPE_NEWDECIMAL:
				{
					// Convert the constant to a decimal of the same precision and scale as the comperand, and deposit the converted value into the condition string
					if ( !( convertDecimalToPrecisionAndScale( pComperandItem, pComperandData, pConstItem, *pCondString + *pItemOffset, NULL ) ) )
					{
						return false;
					}

					( *pItemOffset )																			   += ( USER_DATA_OFFSET_USER_DATA +
																														*( unsigned short* )
																														 ( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE ) );
					break;
				}

				case MYSQL_TYPE_FLOAT:
				case MYSQL_TYPE_DOUBLE:
				{
					// Use the floating point representation of the value
					double			dValue		= ( ( Item_decimal* ) pConstItem )->val_real();

					*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )									= ( unsigned char  )
																													  ( SDB_PUSHDOWN_LITERAL_DATA_TYPE_FLOAT );
					*( ( double* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA ) )					= dValue;
					*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )				= ( unsigned short )( 8 );		// max size in bytes of double
					( *pItemOffset )																			   += USER_DATA_OFFSET_USER_DATA + 8;
					break;
				}

				default:
					if ( isUnsignedComperand )
					{
						unsigned
						long long	uintValue	= ( unsigned long long ) intValue;

#ifdef	SDB_PUSHDOWN_CHECK_INTEGER_LIMITS
						if ( pComperandData )
						{
							checkUIntLimits( pComperandData, uintValue );
						}
#endif

						*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )								= ( unsigned char )
																													  ( SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER );
						*( ( unsigned long long* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA ) )	= uintValue;
					}
					else
					{
#ifdef	SDB_PUSHDOWN_CHECK_INTEGER_LIMITS
						if ( pComperandData )
						{
							checkIntLimits( pComperandData, intValue );
						}
#endif

						*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )								= ( unsigned char  )
																													  ( SDB_PUSHDOWN_LITERAL_DATA_TYPE_SIGNED_INTEGER );
						*( ( long long* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA ) )				= intValue;
					}
					*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )				= ( unsigned short )( 8 );		// size in bytes of int
					*pItemOffset																				   += USER_DATA_OFFSET_USER_DATA + 8;
			}

			break;
		}

		case Item::VARBIN_ITEM:
		{
			Item_field*		pField				= ( pConstItem->with_field ? ( ( Item_field* ) pConstItem )  : NULL );
			Item_field*		pComperandField		= ( ( pComperandItem && pComperandItem->with_field )   ? ( ( Item_field* ) pComperandItem ) : NULL );
			int				typeComperand		= ( pComperandItem ? pComperandItem->type() : -1 );

			typeComperand						= ( pComperandField ? ( pComperandField->field )->type() : -1 );

			long long		intValue			= ( ( Item_hex_hybrid* ) pConstItem )->val_int();
			
			*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )											= ( unsigned char  )
																													  ( SDB_PUSHDOWN_LITERAL_DATA_TYPE_BIT );
			*( ( unsigned long long* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA ) )				= ( unsigned long long ) intValue;
			*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )						= ( unsigned short )( 8 );		// size in bytes of numeric type
			*pItemOffset																						   += USER_DATA_OFFSET_USER_DATA + 8;
			break;
		}

		case Item::DECIMAL_ITEM:
		{
			if ( pComperandItem )
			{
				switch ( *( pComperandData + ROW_DATA_OFFSET_ROW_TYPE ) )
				{
					case SDB_PUSHDOWN_COLUMN_DATA_TYPE_FLOAT:
					{
						// Use the floating point representation of the value
						double		dValue		= ( ( Item_decimal* ) pConstItem )->val_real();

						*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )								= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_FLOAT );
						*( double* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA )					= dValue;
						*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )			= ( unsigned short )( 8 );		// max size in bytes of float
						( *pItemOffset )																		   += USER_DATA_OFFSET_USER_DATA + 8;
						break;
					}

					case SDB_PUSHDOWN_COLUMN_DATA_TYPE_UNSIGNED_INTEGER:
					{
						// Use the integer representation of the value
						long long intValue		= ( ( Item_decimal* ) pConstItem )->val_int();

						*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )								= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER );
						*( ( unsigned long long* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA ) )	= ( unsigned long long ) intValue;
						*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )			= ( unsigned short )( 8 );		// max size in bytes of unsigned int
						( *pItemOffset )																		   += USER_DATA_OFFSET_USER_DATA + 8;
						break;
					}

					case SDB_PUSHDOWN_COLUMN_DATA_TYPE_SIGNED_INTEGER:
					{
						// Use the integer representation of the value
						long long intValue		= ( ( Item_decimal* ) pConstItem )->val_int();

						*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )								= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_SIGNED_INTEGER );
						*( ( long long* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA ) )				= intValue;
						*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )			= ( unsigned short )( 8 );		// max size in bytes of int
						( *pItemOffset )																		   += USER_DATA_OFFSET_USER_DATA + 8;
						break;
					}

					default:
						// Convert the constant to a decimal of the same precision and scale as the comperand, and deposit the converted value into the condition string
						if ( !( convertDecimalToPrecisionAndScale( pComperandItem, pComperandData, pConstItem, *pCondString + *pItemOffset, NULL ) ) )
						{
							return false;
						}

						( *pItemOffset )																		   += ( USER_DATA_OFFSET_USER_DATA +
																														*( unsigned short* )
																														 ( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE ) );
				}
			}
			else
			{
				// Convert the decimal to its binary representation
				my_decimal			dValue;
				my_decimal*			pValue		= ( my_decimal* )( ( Item_decimal* ) pConstItem )->val_decimal( &dValue );
				unsigned char*		pType		= ( unsigned char*  )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE );
				unsigned short*		pSize		= ( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE );
				unsigned char*		pBinary		= ( unsigned char*  )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA );
				int					iPrecision	= pValue->precision();
				int					iScale		= pValue->frac;

				memset( pBinary, '\0', sizeof( long long ) );

				int					retValue	= my_decimal2binary( E_DEC_FATAL_ERROR & ~E_DEC_OVERFLOW,
																	 pValue, pBinary, iPrecision, iScale );

				if ( retValue )
				{
					return false;
				}

				iPrecision					   += iScale;

				if ( iPrecision					< sizeof( long long ) )
				{
					iPrecision					= sizeof( long long );
				}

				*pType							= ( unsigned char  ) SDB_PUSHDOWN_LITERAL_DATA_TYPE_DECIMAL;
				*pSize							= ( unsigned short ) iPrecision;

				( *pItemOffset )			   += ( USER_DATA_OFFSET_USER_DATA + iPrecision );
			}

			break;
		}

		case Item::REAL_ITEM:
		{
			double					dValue		=  ( ( Item_decimal* ) pConstItem )->val_real();

			*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )						= ( unsigned char )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_FLOAT );
			*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )	= ( unsigned short )( 8 );	// size in bytes of double
			*( double* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA )			= dValue;
			( *pItemOffset )																   += USER_DATA_OFFSET_USER_DATA + 8;
			break;
		}

		case Item::FUNC_ITEM:
		{
			if ( !( conditionFunctionItemToString( pCondString, pItemOffset, pConstItem, pComperandItem, pComperandDataOffset, pDbId, pTableId, table_list ) ) )
			{
				return false;
			}

			break;
		}

#if MYSQL_VERSION_ID >= 100108
		case Item::DATE_ITEM:  //added because new 10.1.8 returns this type for test dt_date_and_time
#endif 
		case Item::STRING_ITEM:
		{
			int		temporalDataType;
			bool	isBinaryData;
			bool	isFunctionComperand;

			if ( pComperandItem )
			{
				unsigned char	typeFunction	= *( pComperandData + FUNC_OP_OFFSET_TYPE );

				switch ( *( pComperandData + FUNC_OP_OFFSET_TYPE ) )
				{
					casePushdownFunction:												// ALL SUPPORTED PUSHDOWN FUNCTION TYPES
						isFunctionComperand		= true;
						break;
					default:
						isFunctionComperand		= false;
				}

				if ( isFunctionComperand )
				{
					temporalDataType			= 0;
					isBinaryData				= false;
				}
				else
				{
					temporalDataType			= ( ( ( Item_field* ) pComperandItem )->field )->type();

					switch ( temporalDataType )
					{
						case MYSQL_TYPE_TIMESTAMP:
						case MYSQL_TYPE_TIME:
						case MYSQL_TYPE_DATETIME:
						case MYSQL_TYPE_DATE:
						case MYSQL_TYPE_NEWDATE:
						case MYSQL_TYPE_YEAR:
							isBinaryData		= false;
							break;
						default:
							temporalDataType	= 0;
							isBinaryData		= ( ( *( pComperandData + USER_DATA_OFFSET_DATA_TYPE ) == SDB_PUSHDOWN_COLUMN_DATA_TYPE_BINARY ) ? true : false );
					}
				}
			}
			else
			{
				temporalDataType				= 0;
				isBinaryData					=
				isFunctionComperand				= false;
			}

			if ( temporalDataType )
			{
				Item_field*		pField			= ( Item_field* ) pConstItem;
#if MYSQL_VERSION_ID < 100014
				const char*		pString			= ( ( ( Item_int* ) pConstItem )->str_value ).ptr();
				unsigned int	stringSize		= ( ( ( Item_int* ) pConstItem )->str_value ).length();
#else
				const char*		pString			= ( ( ( Item* ) pConstItem )->val_str() )->ptr();
				unsigned int	stringSize		= ( ( ( Item* ) pConstItem )->val_str() )->length();
#endif
				THD*			pMysqlThd		= ha_thd();
				MYSQL_TIME		myTime;

				// Convert the time string to a time value
				if ( convertStringToTime( pMysqlThd, pString, stringSize, temporalDataType, &myTime ) )
				{
					unsigned short	dbId;
					unsigned short	tableId;

					if ( *( *pCondString + *pComperandDataOffset + ROW_DATA_OFFSET_ROW_TYPE )					   == SDB_PUSHDOWN_COLUMN_DATA_TYPE_USER_TIMESTAMP )
					{
						// User timestamp
#ifdef	SDB_USER_TIME_CONVERT_TO_INTERVAL
						doConvertUserTimestamp																		= true;
#endif
						dbId																						= *( unsigned short* )
																													   ( pComperandData + ROW_DATA_OFFSET_DATABASE_NUMBER );
						tableId																						= *( unsigned short* )
																													   ( pComperandData + ROW_DATA_OFFSET_TABLE_NUMBER );
					}
					else
					{
						dbId																						=
						tableId																						= 0;
					}

					// Convert the time value to the field's data type
					if ( !( convertTimeToType( pMysqlThd, pConstItem, temporalDataType, &myTime,
											   *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE,
											   *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA,
											   doConvertUserTimestamp, dbId, tableId ) ) )
					{
						// Time conversion error
						return false;
					}
				}
				else
				{
					// Time conversion error
					SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- invalid temporal value in WHERE clause." );
					return false;
				}

				*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )	= ( unsigned short )( 8 );
				( *pItemOffset )																   += USER_DATA_OFFSET_USER_DATA + 8;
			}
			else
			{
#if MYSQL_VERSION_ID < 100014
				unsigned int stringSize = (((Item_int *)pConstItem)->str_value).length();	//length of user data
#else
				unsigned int stringSize = (((Item *)pConstItem)->val_str())->length();		//length of user data
#endif

				if ( stringSize					> 65535 )
				{
					// 65535 max string length since size is stored as 2 bytes
					return false;
				}

				if ( isBinaryData )
				{
					*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )					= ( unsigned char  )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_BINARY );
				}
				else
				{
					*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )					= ( unsigned char  )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_CHAR );
				}
				*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )	= ( unsigned short )( stringSize );

				// Extend the condition string if necessary
				resultExtension					= checkConditionStringSize( pCondString, pItemOffset, USER_DATA_OFFSET_USER_DATA + stringSize );

				if ( resultExtension			< 0 )
				{
					// Extension failed
					return false;
				}
				if ( resultExtension )
				{
					// Condition string was extended
					if ( pComperandDataOffset )
					{
						pComperandData			= *pCondString + *pComperandDataOffset;
					}
				}
#if MYSQL_VERSION_ID < 100014
				memcpy(*pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA, ((*pConstItem).str_value).ptr(), stringSize);	//copy user data to string
#else
				memcpy(*pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA, ((*pConstItem).val_str())->ptr(), stringSize);	//copy user data to string
#endif
				(*pItemOffset) += USER_DATA_OFFSET_USER_DATA + stringSize;		//increment the length of condition string accordingly
			}

			break;
		}

		case Item::CACHE_ITEM:
		{
			int						cacheType	= ( ( Item_cache_decimal* ) pConstItem )->field_type();

			switch ( cacheType )
			{
#if	MYSQL_VERSION_ID >= 100014
			case MYSQL_TYPE_VAR_STRING:
			case MYSQL_TYPE_VARCHAR:
			case MYSQL_TYPE_STRING:
			{
				// Handle temporal cached values and BINARY values used with INET functions
				if ( ( ( Item* ) pConstItem )->val_str() == NULL )
				{
					// Item with no string value is unsupported
					return false;
				}

				const char*			pString		= ( ( ( Item* ) pConstItem )->val_str() )->ptr();
				unsigned int		stringSize	= ( ( ( Item* ) pConstItem )->val_str() )->length();

				if ( pComperandItem )
				{
					int				dataType	= ( ( ( Item_field* ) pComperandItem )->field )->type();
					bool			isTemporal;
					bool			isBinary;
					bool			isInt;

					switch ( dataType )
					{
						case MYSQL_TYPE_TIMESTAMP:
						case MYSQL_TYPE_DATE:
						case MYSQL_TYPE_NEWDATE:
						case MYSQL_TYPE_DATETIME:
							isTemporal			= true;
							isBinary			=
							isInt				= false;
							break;
						case MYSQL_TYPE_TIME:
						case MYSQL_TYPE_YEAR:
							isBinary			=
							isTemporal			= false;
							isInt				= true;
							break;
						case MYSQL_TYPE_STRING:
							isBinary			= ( ( *( pComperandData + USER_DATA_OFFSET_DATA_TYPE ) == SDB_PUSHDOWN_COLUMN_DATA_TYPE_BINARY ) ? true : false );
							isTemporal			=
							isInt				= false;
							break;
						default:
							isTemporal			=
							isBinary			=
							isInt				= false;
					}

					if ( isTemporal )
					{
						THD*		pMysqlThd	= ha_thd();
						MYSQL_TIME	myTime;
						unsigned
						short		dbId;
						unsigned
						short		tableId;

						if ( *( *pCondString + *pComperandDataOffset + ROW_DATA_OFFSET_ROW_TYPE )  == SDB_PUSHDOWN_COLUMN_DATA_TYPE_USER_TIMESTAMP )
						{
							// User timestamp
#ifdef	SDB_USER_TIME_CONVERT_TO_INTERVAL
							doConvertUserTimestamp													= true;
#endif
							dbId																	= *( unsigned short* )
																									   ( pComperandData + ROW_DATA_OFFSET_DATABASE_NUMBER );
							tableId																	= *( unsigned short* )
																									   ( pComperandData + ROW_DATA_OFFSET_TABLE_NUMBER );
						}
						else
						{
							dbId																	=
							tableId																	= 0;
						}

						// Get the cached value as a time value
						getTimeCachedResult( pMysqlThd, pConstItem, dataType, &myTime );

						// Convert the time value to the field's data type
						if ( !( convertTimeToType( pMysqlThd, pConstItem, dataType, &myTime,
												   *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE,
												   *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA,
												   doConvertUserTimestamp, dbId, tableId ) ) )
						{
							// Time conversion error
							return false;
						}

						*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )	= ( unsigned short )( 8 );			// Size of a temporal value
						( *pItemOffset )																   += USER_DATA_OFFSET_USER_DATA + 8;
						break;
					}

					if ( isInt )
					{
						long long	intValue	= ( ( Item_int* ) pConstItem )->val_int();

						switch ( dataType )
						{
						case MYSQL_TYPE_TIME:
							*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )					= ( unsigned char  )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_TIME );
							*( long long* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA )		= intValue;
							break;
						case MYSQL_TYPE_YEAR:
							*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )					= ( unsigned char  )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_YEAR );
							*( unsigned
							   long long* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA )		= convertYearToStoredFormat( intValue,0 );
							break;
						default:
							return false;
						}

						*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )	= ( unsigned short )( 8 );			// Size of an int value
						( *pItemOffset )																   += USER_DATA_OFFSET_USER_DATA + 8;
						break;
					}

					switch ( dataType )
					{
					case MYSQL_TYPE_STRING:
						if ( isBinary )
						{
							*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )					= ( unsigned char  )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_BINARY );
						}
						else
						{
							*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )					= ( unsigned char  )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_CHAR );
						}
						break;
					default:
						// Validate the string length for variable length types
						unsigned int uiLength	= *( unsigned short* )( pComperandData + ROW_DATA_OFFSET_COLUMN_SIZE );

						if ( ( uiLength		   != stringSize ) &&																				// not an IPV6 function
							 ( ( uiLength	   != 16 ) || ( stringSize != 10 ) ) )																// not an IPV4 function
						{
							return false;
						}
					}
				}
				else
				{
					*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )							= ( unsigned char  )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_CHAR );
				}

				if ( stringSize					> 65535 )
				{
					//	65535 max string length since size is stored in 2 bytes
					return false;
				}

				*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )			= ( unsigned short )( stringSize );	// string length

				// Extend the condition string if necessary
				resultExtension					= checkConditionStringSize( pCondString, pItemOffset, USER_DATA_OFFSET_USER_DATA + stringSize );

				if ( resultExtension			< 0 )
				{
					// Extension failed
					return false;
				}
				if ( resultExtension )
				{
					// Condition string was extended
					if ( pComperandDataOffset )
					{
						pComperandData			= *pCondString + *pComperandDataOffset;
					}
				}

				memcpy( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA, pString, stringSize );			//copy user data to string
				( *pItemOffset )			   += USER_DATA_OFFSET_USER_DATA + stringSize;							//increment the length of condition string accordingly
				break;
			}
#endif

			case MYSQL_TYPE_DECIMAL:
			case MYSQL_TYPE_NEWDECIMAL:
			{
				if ( pComperandItem )
				{
					Item_field*	pComperandField;
					int			typeComperand;

					pComperandField				= ( pComperandItem->with_field ? ( ( Item_field* ) pComperandItem ) : NULL );

					if ( pComperandField )
					{
						typeComperand			= pComperandField->field->type();
					}
					else
					{
						typeComperand			= pComperandItem->type();
					}

					switch ( typeComperand )
					{
					case MYSQL_TYPE_DECIMAL:
					case MYSQL_TYPE_NEWDECIMAL:
						// Convert the constant to a decimal of the same precision and scale as the comperand, and deposit the converted value into the condition string
						if ( !( convertDecimalToPrecisionAndScale( pComperandItem, pComperandData, pConstItem, *pCondString + *pItemOffset, NULL ) ) )
						{
							return false;
						}

						*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )						=  ( unsigned char  )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_DECIMAL );
						( *pItemOffset )																   += ( USER_DATA_OFFSET_USER_DATA +
																												*( unsigned short* )
																												 ( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE ) );
						break;

					case MYSQL_TYPE_TINY:
					case MYSQL_TYPE_SHORT:
					case MYSQL_TYPE_INT24:
					case MYSQL_TYPE_LONG:
					case MYSQL_TYPE_LONGLONG:
						*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )						= ( unsigned char  )( SDB_PUSHDOWN_LITERAL_DATA_TYPE_SIGNED_INTEGER );
						*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )	= ( unsigned short )( 8 );	// max size in bytes of int
						*( long long*      )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA )	= ( ( Item_cache_int* ) pConstItem )->val_int();
						( *pItemOffset )																   += USER_DATA_OFFSET_USER_DATA + 8;
						break;
					default:
						return false;
					}
				}
				break;
			}

			case MYSQL_TYPE_FLOAT:
			case MYSQL_TYPE_DOUBLE:
				// We do not currently support floating point data types, as they are imprecise
				return false;

			case MYSQL_TYPE_TIMESTAMP:
			case MYSQL_TYPE_TIME:
			case MYSQL_TYPE_DATETIME:
			case MYSQL_TYPE_DATE:
			case MYSQL_TYPE_NEWDATE:
			case MYSQL_TYPE_YEAR:
				if ( pComperandItem )
				{
					int				dataType	= ( ( ( Item_field* ) pComperandItem )->field )->type();
					THD*			pMysqlThd	= ha_thd();

					MYSQL_TIME		myTime;
					unsigned short	dbId;
					unsigned short	tableId;

					// Get the cached value as a time value
					getTimeCachedResult( pMysqlThd, pConstItem, dataType, &myTime );

					if ( *( *pCondString + *pComperandDataOffset + ROW_DATA_OFFSET_ROW_TYPE )	   == SDB_PUSHDOWN_COLUMN_DATA_TYPE_USER_TIMESTAMP )
					{
						// User timestamp
#ifdef	SDB_USER_TIME_CONVERT_TO_INTERVAL
						doConvertUserTimestamp														= true;
#endif
						dbId																		= *( unsigned short* )
																									   ( pComperandData + ROW_DATA_OFFSET_DATABASE_NUMBER );
						tableId																		= *( unsigned short* )
																									   ( pComperandData + ROW_DATA_OFFSET_TABLE_NUMBER );
					}
					else
					{
						dbId																		=
						tableId																		= 0;
					}

					// Convert the time value to the field's data type
					if ( !( convertTimeToType( pMysqlThd, pConstItem, dataType, &myTime,
											   *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE,
											   *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA,
											   doConvertUserTimestamp, dbId, tableId ) ) )
					{
						// Time conversion error
						return false;
					}

					*( unsigned short* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE )= ( unsigned short )( 8 );
					( *pItemOffset )															   += USER_DATA_OFFSET_USER_DATA + 8;
					break;
				}
				// Fall through ...

			default:
			{
				int			typeComperand		= determineFieldType( pComperandItem, NULL );

				switch ( typeComperand )
				{
				case MYSQL_TYPE_DECIMAL:
				case MYSQL_TYPE_NEWDECIMAL:
				{
					// Convert the constant to a decimal of the same precision and scale as the comperand, and deposit the converted value into the condition string
					if ( !( convertDecimalToPrecisionAndScale( pComperandItem, pComperandData, pConstItem, *pCondString + *pItemOffset, NULL ) ) )
					{
						return false;
					}

					( *pItemOffset )															   += ( USER_DATA_OFFSET_USER_DATA +
																										*( unsigned short* )
																										 ( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_SIZE ) );
					break;
				}

				case MYSQL_TYPE_FLOAT:
				case MYSQL_TYPE_DOUBLE:
					// We do not currently support floating point data types, as they are imprecise
					return false;

				default:				// treat as an int
					convertIntegerToType( pComperandItem, pComperandData, pConstItem, *pCondString + *pItemOffset );
					( *pItemOffset )		   += USER_DATA_OFFSET_USER_DATA + 8;
				}

				break;
			}
			}

			break;
		}

		default:
		{
			*( *pCondString + *pItemOffset + USER_DATA_OFFSET_DATA_TYPE )							= ( unsigned char )( SDB_PUSHDOWN_NONE );
			*( ( long long* )( *pCondString + *pItemOffset + USER_DATA_OFFSET_USER_DATA ) )			= ( long long ) pConstItem->type();
			( *pItemOffset )																	   += USER_DATA_OFFSET_USER_DATA + 8;
			return false;
		}
	}

	return true;
}


/*
 enum Type {FIELD_ITEM= 0, FUNC_ITEM, SUM_FUNC_ITEM, STRING_ITEM,
	     INT_ITEM, REAL_ITEM, NULL_ITEM, VARBIN_ITEM,
	     COPY_STR_ITEM, FIELD_AVG_ITEM, DEFAULT_VALUE_ITEM,
	     PROC_ITEM,COND_ITEM, REF_ITEM, FIELD_STD_ITEM,
	     FIELD_VARIANCE_ITEM, INSERT_VALUE_ITEM,
             SUBSELECT_ITEM, ROW_ITEM, CACHE_ITEM, TYPE_HOLDER,
             PARAM_ITEM, TRIGGER_FIELD_ITEM, DECIMAL_ITEM,
             XPATH_NODESET, XPATH_NODESET_CMP,
             VIEW_FIXER_ITEM, EXPR_CACHE_ITEM};

			  enum Sumfunctype
  { COUNT_FUNC, COUNT_DISTINCT_FUNC, SUM_FUNC, SUM_DISTINCT_FUNC, AVG_FUNC,
    AVG_DISTINCT_FUNC, MIN_FUNC, MAX_FUNC, STD_FUNC,
    VARIANCE_FUNC, SUM_BIT_FUNC, UDF_SUM_FUNC, GROUP_CONCAT_FUNC
  };

			 */


	
int ha_scaledb::getSDBSize(enum_field_types fieldType, Field* field) 
{


	switch (fieldType) {
		case MYSQL_TYPE_SHORT:
			{
			return SDB_SIZE_OF_SHORT;

			}
		case MYSQL_TYPE_INT24:
			{
			return SDB_SIZE_OF_MEDIUMINT;

			}
		case MYSQL_TYPE_LONG:
			{
			return  SDB_SIZE_OF_INTEGER;

			}

		case MYSQL_TYPE_LONGLONG:
			{
			return ENGINE_TYPE_SIZE_OF_LONG;

			}
		case MYSQL_TYPE_TINY:
			{
			return SDB_SIZE_OF_TINYINT;

			}

		case MYSQL_TYPE_FLOAT: // FLOAT is treated as a 4-byte number
			{
			return SDB_SIZE_OF_FLOAT;

			}
		case MYSQL_TYPE_DOUBLE: // DOUBLE is treated as a 8-byte number
			{
			return  ENGINE_TYPE_SIZE_OF_DOUBLE;
		
	
			}

		case MYSQL_TYPE_DATE: // DATE is treated as a non-negative 3-byte integer
			{
			return SDB_SIZE_OF_DATE;

			}

		case MYSQL_TYPE_TIME: // TIME is treated as a non-negative 3-byte integer
			{
			return  SDB_SIZE_OF_TIME;

			}
		case MYSQL_TYPE_DATETIME: // DATETIME is treated as a non-negative 8-byte integer
			{
			return  SDB_SIZE_OF_DATETIME;
			}

		case MYSQL_TYPE_TIMESTAMP: // TIMESTAMP is treated as a non-negative 4-byte integer
			{
				return SDB_SIZE_OF_TIMESTAMP;
			}
	
		case MYSQL_TYPE_DECIMAL:
		case MYSQL_TYPE_NEWDECIMAL:
			{
				return 8;
			}
		case MYSQL_TYPE_STRING: // can be CHAR or BINARY data type
		{
			return  field->pack_length(); // exact size used in RAM

		}
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		{
			return  field->pack_length(); // exact size used in RA
		}
		

		default:

			return -1; //unsupported so fail.
			break;
		}

}
char ha_scaledb::getCASType(enum_field_types mysql_type, int flags)
{
		switch (mysql_type)
					{
					case MYSQL_TYPE_TINY:
					case MYSQL_TYPE_SHORT:
					case MYSQL_TYPE_INT24:
					case MYSQL_TYPE_LONG:
					case MYSQL_TYPE_LONGLONG:
						{
						if ( flags	& UNSIGNED_FLAG )
						{
							return  SDB_PUSHDOWN_COLUMN_DATA_TYPE_UNSIGNED_INTEGER;	// unsigned int (row)
						}
						else
						{
							return SDB_PUSHDOWN_COLUMN_DATA_TYPE_SIGNED_INTEGER;	//   signed int (row)
						}
						
						}
					case MYSQL_TYPE_STRING:
						return SDB_PUSHDOWN_COLUMN_DATA_TYPE_CHAR;					// char string (row)
						

					case MYSQL_TYPE_DOUBLE:
					case MYSQL_TYPE_FLOAT:
						return SDB_PUSHDOWN_COLUMN_DATA_TYPE_FLOAT;					// float (row)
					

					case MYSQL_TYPE_DECIMAL:
					case MYSQL_TYPE_NEWDECIMAL:
						return  SDB_PUSHDOWN_COLUMN_DATA_TYPE_DECIMAL;				// binary decimal (row)
					

					case MYSQL_TYPE_DATE:
					case MYSQL_TYPE_NEWDATE:
						return  SDB_PUSHDOWN_COLUMN_DATA_TYPE_DATE;					// date (row)
						

					case MYSQL_TYPE_TIME:
						return SDB_PUSHDOWN_COLUMN_DATA_TYPE_TIME;					// time (row)
					

					case MYSQL_TYPE_DATETIME:
						return  SDB_PUSHDOWN_COLUMN_DATA_TYPE_DATETIME;				// datetime (row)
					

					case MYSQL_TYPE_YEAR:
						return SDB_PUSHDOWN_COLUMN_DATA_TYPE_YEAR;					// year (row)


					case MYSQL_TYPE_TIMESTAMP:
						return  SDB_PUSHDOWN_COLUMN_DATA_TYPE_TIMESTAMP;			// timestamp (row)


					case MYSQL_TYPE_VARCHAR:
					case MYSQL_TYPE_VAR_STRING:
						{
							return SDB_PUSHDOWN_COLUMN_DATA_TYPE_VARCHAR;			// varchar (row)
						}
					default:
						{
						return  SDB_PUSHDOWN_NONE;									// unknown (row)
						break;
						}
					}
}

int ha_scaledb::numberInOrderBy()
{
	int n=0;
	SELECT_LEX*  lex=  (((THD*) ha_thd())->lex)->select_lex.parent_lex->current_select;
	if(lex->order_list.elements>0)
	{
		n=lex->order_list.elements;
	
	}
	else if(lex->group_list.elements>0)
	{
		n=lex->group_list.elements;
	}
	return n;
}


bool ha_scaledb::isInSelectList(SelectAnalyticsHeader* sah, char*  col_name, unsigned short dbid, unsigned short tabid, function_type ft)
{

	bool found=false;
	unsigned short columnNumber = SDBGetColumnNumberByName(dbid, tabid, col_name );
	if(columnNumber==0 ) {return false;}
    	
	int	field_offset=SDBGetColumnOffsetByNumber(dbid, tabid, columnNumber);

	char* pos=  ((char*)sah)+sizeof(SelectAnalyticsHeader);
	for (unsigned short i = 0; i < sah->numberOutputFields; ++i)
	{

		SelectAnalyticsBody1* sab1= (SelectAnalyticsBody1*)(pos);
		pos=pos+ sizeof(SelectAnalyticsBody1);
		for (unsigned short j = 0; j <  sab1->numberOfFieldsInFunction; ++j)
		{	
			SelectAnalyticsBody2* sab2= (SelectAnalyticsBody2*)(pos);

			
			int field=sab2->fieldOffset;	
			function_type ftype= (function_type)sab2->function;
			if(field==field_offset && ftype==ft) {found=true;}

			pos=pos+ sizeof(SelectAnalyticsBody2);

		}
	  
	}

	return found;


}

bool ha_scaledb::compareFunctionTypes(Item* item, function_type ft)
{
	Item::Type t= item->type();

	switch(t)
	{
	case Item::FIELD_ITEM:
		{
			if (ft==FT_NONE) {return true;}
			break;
		}
	case Item::SUM_FUNC_ITEM:
		{	
			Item_sum *sum = ((Item_sum *)item);
			int stype=sum->sum_func();

			if (stype == Item_sum::SUM_FUNC && ft==FT_SUM) {return true;}			
			else if (stype == Item_sum::COUNT_FUNC && (ft==FT_COUNT || ft==FT_COUNT_FIELD)) {return true;}
			else if (stype == Item_sum::COUNT_DISTINCT_FUNC  && ft==FT_COUNT_DISTINCT) {return true;}
			else if (stype == Item_sum::MIN_FUNC && ft==FT_MIN) {return true;}
			else if (stype == Item_sum::MAX_FUNC && ft==FT_MAX) {return true;}		
			else if (stype == Item_sum::AVG_FUNC && ft==FT_AVG) {return true;}		
				break;
		}
	case Item::FUNC_ITEM:
		{

			Item_func *func = ((Item_func *)item);

			Item_func_hour* dfunc=((Item_func_hour*)func);

			if(checkFunc((char*)dfunc->func_name(), "timestamp")||checkFunc((char*)dfunc->func_name(), "cast_as_datetime"))      
			{
				if(ft==FT_TIMESTAMP) {return true;}


			} else	if(checkFunc((char*)dfunc->func_name(), "date")||checkFunc((char*)dfunc->func_name(), "cast_as_date"))      
			{
				if(ft==FT_DATE) {return true;}


			}if(checkFunc((char*)dfunc->func_name(), "time")||checkFunc((char*)dfunc->func_name(), "cast_as_time"))      
			{
				if(ft==FT_TIME) {return true;}


			}else  if(checkFunc((char*)dfunc->func_name(), "year"))   
			{
				if(ft==FT_YEAR) {return true;}


			}else  if(checkFunc((char*)dfunc->func_name(), "month"))   
			{
				if(ft==FT_MONTH){return true;}


			}else  if(checkFunc((char*)dfunc->func_name(), "dayofmonth"))   
			{
				if(ft==FT_DAY){return true;}


			}else  if(checkFunc((char*)dfunc->func_name(), "hour"))   
			{
				if(ft=FT_HOUR){return true;}


			}else  if(checkFunc((char*)dfunc->func_name(), "minute"))   
			{
				if(ft==FT_MINUTE){return true;}


			}
			else  if(checkFunc((char*)dfunc->func_name(), "second"))   
			{
				if(ft==FT_SECOND){return true;}
			}
			break;
		}


	}

	return false;
}

int ha_scaledb::getOrderByPosition(const char* col_name, const char* col_alias, function_type ft, bool order_by_field, bool& ascending)
{	
	if(col_name==NULL) {return 0;}
	int pos=0;
	SELECT_LEX*  lex=   (((THD*) ha_thd())->lex)->select_lex.parent_lex->current_select;
	ORDER *order;
	const char* field_name=NULL;
	const char* alias_name=NULL;
	bool asc=true;
	if(lex->order_list.elements>0)
	{
	for (order = (ORDER *) lex->order_list.first; order;  order = order->next)
    {
        pos++;
		asc=order->asc;
		Item* item=*order->item;

		switch (item->type())
		{

			case Item::FIELD_ITEM:
			{
				
				Field *field =((Item_field *)item)->field;
				field_name=field->field_name;
				break;
			}
			case Item::FUNC_ITEM:	
			case Item::SUM_FUNC_ITEM:
			{			
				if(ft==FT_NONE) {break;}  //if the group field is NOT a function, then can being ordering over it.
				else
				{

                     Item::Type ft=item->next->type();
					if(ft==Item::FIELD_ITEM)
					{
						Field *field =((Item_field *)item->next)->field;

						if(field==NULL)
						{
							//ordering over a function which we dont support, eg CURDATE()
							//to support we need to add to the projection list
							throw "unsupport order by function";
						}
						else
						{
							
							field_name=field->field_name;
							alias_name= item->name;
						}
					}
					else
					{
						field_name="?";   //this is a count(*), anonymous types are treatedlike this
						alias_name= item->name;
						//no alias and not a field so do nothing.
					}
					break;
				}
			}
			default:
				{
					//all fields and aggregates would have already been caught, so can't really be here
					return 0;
				}
           
		}


		// if the column is in the order by so return the position (either 1,2,3 ...)
		// if there is an alias, check the alias.

		
			if(alias_name==NULL || col_alias == NULL)
			{
				if(field_name!= NULL &&  (strcmp( field_name, col_name ) == 0) && compareFunctionTypes(item,ft)  )
				{
					ascending=asc;
					return pos;
				}
			}
			else
			{
				if(strcmp( alias_name, col_alias ) == 0 && compareFunctionTypes(item,ft)) 
				{
					ascending=asc;
					return pos;
				}
			}
		
    }

	} else if(lex->group_list.elements>0)
	{

		for (ORDER *cur_group= lex->group_list.first ; cur_group ; cur_group= cur_group->next)
		{



			pos++;
			asc=cur_group->asc;
			Item* item=*cur_group->item;

			switch (item->type())
			{

			case Item::FIELD_ITEM:
				{

					Field *field =((Item_field *)item)->field;
					field_name=field->field_name;
					break;
				}
			case Item::FUNC_ITEM:	
			case Item::SUM_FUNC_ITEM:
				{			
					if(ft==FT_NONE) {break;}  //if the group field is NOT a function, then can being ordering over it.
					else
					{
						Item::Type ft=item->next->type();
						if(ft==Item::FIELD_ITEM)
						{
							Field *field =((Item_field *)item->next)->field;

							field_name=field->field_name;
							alias_name= item->name;
						}
						else
						{
							field_name="?";   //this is a count(*), anonymous types are treatedlike this
							alias_name= item->name;
							//no alias and not a field so do nothing.
						}
						break;
					}
				}
			default:
				{
					//all fields and aggregates would have already been caught, so can't really be here
					return 0;
				}

			}


			// if the column is in the order by so return the position (either 1,2,3 ...)
			// if there is an alias, check the alias.


			if(alias_name==NULL || col_alias == NULL)
			{
				if(field_name!= NULL &&  (strcmp( field_name, col_name ) == 0) && compareFunctionTypes(item,ft)  )
				{
					ascending=asc;
					return pos;
				}
			}
			else
			{
				if(strcmp( alias_name, col_alias ) == 0 && compareFunctionTypes(item,ft)) 
				{
					ascending=asc;
					return pos;
				}
			}
		}
	}
	return 0;
}

int ha_scaledb::addGroupBy(char* buf, unsigned short dbid, unsigned short tabid, enum_field_types type, int flag, int pos,    const char* col_name, const char* alias_name, enum function_type function)
{
		char castype=	getCASType(type,flag) ;
	
		GroupByAnalyticsBody* gab= (GroupByAnalyticsBody*)(buf+pos);

		unsigned short columnNumber = SDBGetColumnNumberByName(dbid, tabid, col_name );
		if(columnNumber==0 ) {return 0;}
	
		gab->gBfieldOffset = SDBGetColumnOffsetByNumber(dbid, tabid, columnNumber);
		gab->columnNumber = columnNumber;
		if(function==FT_DATE ||function==FT_YEAR || function== FT_MONTH ||function== FT_DAY || function==FT_HOUR || function==FT_MINUTE || function==FT_SECOND || function == FT_TIME ) 
		{
			gab->gBfieldLength = sizeof(int);
		}
		else  if ( function==FT_TIMESTAMP)
		{

			gab->gBfieldLength = 8;
		}
		else
		{
			gab->gBfieldLength = SDBGetMaxColumnLength(dbid, tabid, columnNumber); //the length of data - SDBGetMaxColumnLength works for char and varchar
		}
		gab->gBfieldType = castype;
		gab->gBFunction = function;
		gab->gBOutputFunctionLength = gab->gBfieldLength;
		bool ascending=true;
		gab->gBorderPosition = getOrderByPosition(col_name,alias_name,function,true,ascending); 
		gab->gBorderDirection = ascending ? 1: 0;
		pos=pos+sizeof(GroupByAnalyticsBody);
		return pos;

}


bool ha_scaledb::addSelectField(char* buf, int& pos, unsigned short dbid, unsigned short tabid, enum_field_types type, short function, const char* col_name, bool& contains_analytics, short precision, short scale, int flag, short result_precision, short result_scale, char* alias_name, bool order_by_field)
{
	char castype=	getCASType(type,flag) ;
	if(castype==SDB_PUSHDOWN_NONE && type!=MYSQL_TYPE_NULL)
	{
		return false;
	}

	if(function==FT_COUNT || function == FT_AVG || function == FT_COUNT_DISTINCT || function==FT_COUNT_FIELD)
	{
	     analytics_uses_count=true;
	}
	
	SelectAnalyticsBody2* sab2= (SelectAnalyticsBody2*)(buf+pos);
	if (( !col_name ) || ( strcmp( "?", col_name ) == 0 ) || ( !dbid ) || ( !tabid ) )
	{
		//for count(*) don't have column info
		sab2->fieldOffset=0;	
		sab2->columnNumber=0;
	
		sab2->fieldLength=0;		//the length of data

		sab2->fieldType = castype;				//the column type
		sab2->precision=precision;
		sab2->scale=scale;
		sab2->result_precision=result_precision;
		sab2->result_scale=result_scale;
		bool ascending=true;
		sab2->gBorderPosition=getOrderByPosition(col_name,alias_name,(function_type)function,order_by_field,ascending);	
		sab2->gBorderDirection= ascending ? 1: 0;
	}
	else
	{
		unsigned short columnNumber = SDBGetColumnNumberByName(dbid, tabid, col_name );

		if(columnNumber==0 ) 
		{
			return false;
		}

		sab2->fieldOffset = SDBGetColumnOffsetByNumber(dbid, tabid, columnNumber);;	//number of fields in operation
		sab2->columnNumber = columnNumber;
		if(function == FT_DATE || function==FT_YEAR || function== FT_MONTH ||function== FT_DAY || function==FT_HOUR || function==FT_MINUTE || function==FT_SECOND ||function== FT_TIME )		
		{
			sab2->fieldLength = sizeof(int);
		}
		else if (function==FT_TIMESTAMP)
		{
				sab2->fieldLength = 8;
		}
		else if(function== FT_INET6_TOA)
		{
			sab2->fieldLength =39;
		}
		else
		{
			sab2->fieldLength = SDBGetMaxColumnLength(dbid, tabid, columnNumber);		//the length of data - SDBGetMaxColumnLength works for char and varchar
		}

		sab2->fieldType = castype;				//the column type
		sab2->precision = precision;
		sab2->scale = scale;
		sab2->result_precision = result_precision;
		sab2->result_scale = result_scale;
		bool ascending=true;
		sab2->gBorderPosition = getOrderByPosition(col_name,alias_name,(function_type)function,order_by_field,ascending);
		sab2->gBorderDirection= ascending ? 1: 0;

		if ( columnNumber	   == SDBGetStreamingKeyFieldID( dbid, tabid ) )
		{
			sab2->flags			= SDB_FLAG_STREAMING_KEY;
		}
		else if ( columnNumber == SDBGetRangeKeyFieldID( dbid, tabid ) )
		{
			sab2->flags			= SDB_FLAG_RANGE_KEY;
		}
		else if ( columnNumber == SDBGetUserTimeKeyFieldID( dbid, tabid ) )
		{
			sab2->flags			= SDB_FLAG_USER_TIME_KEY;
		}
		else
		{
			sab2->flags			= 0;
		}
	}
	sab2->function=function;		//this is operation to perform on the field

	if(function!=FT_NONE) {contains_analytics=true;}
	pos=pos+sizeof(SelectAnalyticsBody2);
	return true;
}
char* trimwhitespace(char *str_base) {
    char* buffer = str_base;
    while((buffer = strchr(str_base, ' '))) {
        strcpy(buffer, buffer+1);
    }

    return str_base;
}
bool ha_scaledb::checkFunc(char* name, char* my_function)
{
	char tmp[1000];
	strcpy(tmp, name);

	 char* func_name= trimwhitespace(strtok(tmp, "("));
	 if(strcasecmp(my_function,func_name)==0) {return true;}
     else
	 {
		 return false;
	 }
}
bool ha_scaledb::checkNestedFunc(char* name, char* my_func1, char* my_func2)
{
	 char* func_name= trimwhitespace(strtok(name, "("));
	 if(strcasecmp(my_func1,func_name)==0)
	 {
		 char* func_name= trimwhitespace(strtok(NULL, "("));
		 if(func_name!=NULL && strcasecmp(my_func2,func_name)==0) {return true;}
		 else {return false;}
	 }
     else
	 {		
		 return false;
	 }
}





//-----------------------------------------------------------------------------------------
//	Extract the range key from pushdown condition
//-----------------------------------------------------------------------------------------
bool ha_scaledb::getRangeKeys( unsigned char * string, unsigned int length, key_range* key_start, key_range* key_end )
{
	unsigned int	place			= 0;
	unsigned short	numberOfChildren;
	unsigned char	nodeType;
	unsigned short	databaseNumber;
	unsigned short	tableNumber;
	unsigned short	columnNumber;
	unsigned short	columnOffset;
	unsigned short	columnSize;
	unsigned short	dataSize;
	long long		key_data;
	bool			hasStartKey		= false;
	bool			hasEndKey		= false;
	bool			is_range_index	= false;
	bool			contains_range_index	= false;

	clearIndexKeyRange( key_start );
	clearIndexKeyRange( key_end );


	while ( place					< length )
	{
		numberOfChildren			= *( short* )( string + place );
		place					   += sizeof( numberOfChildren );
		nodeType					= *( string + place++ );


		switch ( nodeType )
		{

		case SDB_PUSHDOWN_OPERATOR_EQ:
			{
				if(is_range_index)
				{
					key_start->key	= indexKeyRangeStartData_;
					memcpy((char*)key_start->key,&key_data, sizeof(key_data));
					key_start->flag=HA_READ_KEY_EXACT;
					key_start->length=sizeof(key_data);
					key_start->keypart_map=1;
					hasStartKey		= true;
				}
				place++;
				break;
			}
		case SDB_PUSHDOWN_OPERATOR_GE:
			{
				if(is_range_index)
				{
					key_start->key	= indexKeyRangeStartData_;
					memcpy((char*)key_start->key,&key_data, sizeof(key_data));
					key_start->flag=HA_READ_KEY_OR_NEXT;
					key_start->length=sizeof(key_data);
					key_start->keypart_map=1;
					hasStartKey		= true;
				}
				place++;
				break;
			}
		case SDB_PUSHDOWN_OPERATOR_GT:
			{
				if(is_range_index)
				{
					key_start->key	= indexKeyRangeStartData_;
					memcpy((char*)key_start->key,&key_data, sizeof(key_data));
					key_start->flag=HA_READ_AFTER_KEY;
					key_start->length=sizeof(key_data);
					key_start->keypart_map=1;
					hasStartKey		= true;
				}
				place++;
				break;
			}
		case SDB_PUSHDOWN_OPERATOR_LT:
			{
				if(is_range_index)
				{
					key_end->key	= indexKeyRangeEndData_;
					memcpy((char*)key_end->key,&key_data, sizeof(key_data));
					key_end->flag=HA_READ_BEFORE_KEY;
					key_end->length=sizeof(key_data);
					key_end->keypart_map=1;
					hasEndKey		= true;
				}
				place++;
				break;
			}
		case SDB_PUSHDOWN_OPERATOR_LE:
			{
				if(is_range_index)
				{
					key_end->key	= indexKeyRangeEndData_;
					memcpy((char*)key_end->key,&key_data, sizeof(key_data));
					key_end->flag=HA_READ_KEY_OR_PREV;
					key_end->length=sizeof(key_data);
					key_end->keypart_map=1;
					hasEndKey		= true;
				}
				place++;
				break;
			}

		case SDB_PUSHDOWN_OPERATOR_BETWEEN:
		case SDB_PUSHDOWN_OPERATOR_IN:
			{
				place++;
				break;
			}

			case SDB_PUSHDOWN_COLUMN_DATA_TYPE_SIGNED_INTEGER:			// signed integer
			case SDB_PUSHDOWN_COLUMN_DATA_TYPE_UNSIGNED_INTEGER:		// unsigned integer
			case SDB_PUSHDOWN_COLUMN_DATA_TYPE_BIT:						// bit
			case SDB_PUSHDOWN_COLUMN_DATA_TYPE_DECIMAL:					// binary decimal
			case SDB_PUSHDOWN_COLUMN_DATA_TYPE_FLOAT:					// float
			case SDB_PUSHDOWN_COLUMN_DATA_TYPE_BINARY:					// binary
			case SDB_PUSHDOWN_COLUMN_DATA_TYPE_CHAR:					// char
			case SDB_PUSHDOWN_COLUMN_DATA_TYPE_VARCHAR:					// varchar
			case SDB_PUSHDOWN_COLUMN_DATA_TYPE_DATE:					// date
			case SDB_PUSHDOWN_COLUMN_DATA_TYPE_TIME:					// time
			case SDB_PUSHDOWN_COLUMN_DATA_TYPE_DATETIME:				// datetime
			case SDB_PUSHDOWN_COLUMN_DATA_TYPE_YEAR:					// year
			case SDB_PUSHDOWN_COLUMN_DATA_TYPE_TIMESTAMP:				// timestamp
			{
				databaseNumber = *(unsigned short *)(string + place);
				place += 2;


				tableNumber = *(unsigned short *)(string + place);
				place += 2;


				columnNumber = *( unsigned short* )( string + place );
				place += 2;
				if(SDBGetRangeKeyFieldID(sdbDbId(),  sdbTableNumber())==columnNumber)
				{
					is_range_index=true;
					contains_range_index=true;
				}
				else
				{
					is_range_index=false;
				}

				columnOffset = *(unsigned short *)(string + place);
				place += 2;


				columnSize = *(unsigned short *)(string + place);
				place += 2;
	

				break;
			}

			case SDB_PUSHDOWN_LITERAL_DATA_TYPE_SIGNED_INTEGER:
			case SDB_PUSHDOWN_LITERAL_DATA_TYPE_TIMESTAMP:
			case SDB_PUSHDOWN_LITERAL_DATA_TYPE_DECIMAL:
			{
				dataSize = *(string + place++);


				long long data = *(long long *)(string + place);
				key_data=data;
				place += dataSize;
	
				break;
			}

			case SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER:
			case SDB_PUSHDOWN_LITERAL_DATA_TYPE_DATETIME:
			case SDB_PUSHDOWN_LITERAL_DATA_TYPE_BIT:
			{
				dataSize = *(string + place++);


				unsigned long long data = *( unsigned long long* )( string + place );
				place += dataSize;

				break;
			}

			case SDB_PUSHDOWN_LITERAL_DATA_TYPE_FLOAT:
			{
				dataSize = *(string + place++);


				double data = *(double *)(string + place);
				//DataPrintOut::printString((const char *)(string+place), dataSize);		//need printDouble
				place += dataSize;
				break;
			}

			case SDB_PUSHDOWN_LITERAL_DATA_TYPE_CHAR:
			case SDB_PUSHDOWN_LITERAL_DATA_TYPE_BINARY:
			{
				dataSize = *(string + place++);
	
				place += dataSize;
				break;
			}

			default:

				return contains_range_index;  
		}
	}

	if ( hasEndKey				   && ( !hasStartKey ) )
	{
		// Set up an empty start key for the beginning of the range
		copyIndexKeyRangeStart( NULL, 0, HA_READ_AFTER_KEY, 1 );
	}

	return contains_range_index;
}


//--------------------------------------------------------------------------------------------------
//Set up variables for condition pushdown and analytics pushdown strings to be sent to CAS
//--------------------------------------------------------------------------------------------------
void ha_scaledb::saveConditionToString(const COND *cond,TABLE_LIST* table_list)
{
	THD* thd = ha_thd();

	unsigned short DBID=0;
	unsigned short TABID=0;

	conditionStringLength_		= 0;
	analyticsStringLength_		=
	analyticsSelectLength_      = 0;

	rangeBounds.clear();
	char* s_disable_pushdown=SDBUtilFindComment(thd->query(), "disable_condition_pushdown") ; //if disable_condition_pushdown is in comment then don't do pushdown

	if (s_disable_pushdown==NULL)
	{
		if ( !( conditionTreeToString( cond, &conditionString_, &conditionStringLength_, &DBID, &TABID, table_list ) ) )
		{
			conditionStringLength_	= 0;
			rangeBounds.clear();
		}
		else
		{
			if ( rangeBounds.startSet && rangeBounds.opType && !( rangeBounds.endSet ) )
			{
				// Range key condition at the end of the WHERE
				rangeBounds.valid		=
				rangeBounds.endSet		= true;
				rangeBounds.endRange	= conditionStringLength_;
			}

#ifdef	CONDITION_PUSH_DEBUG
			printMYSQLConditionBuffer( conditionString_, conditionStringLength_ );
#endif	//CONDITION_PUSH_DEBUG
		}
	}
}



int ha_scaledb::getPrecision(Item *item)
{
  uint8 dec= item->decimals;
  uint8 intg= item->decimal_precision() - dec;
  uint32 len= item->max_char_length();

 // DBUG_ASSERT (item->result_type() == DECIMAL_RESULT);

  /*
    Trying to put too many digits overall in a DECIMAL(prec,dec)
    will always throw a warning. We must limit dec to
    DECIMAL_MAX_SCALE however to prevent an assert() later.
  */

	
	if (dec > 0)
	{
		dec= MY_MIN(dec, DECIMAL_MAX_SCALE);
		return (intg + dec);
	}
	else
	{
		 return my_decimal_length_to_precision(len, dec, item->unsigned_flag);
	}

}

bool ha_scaledb::isValidAnalyticType(enum_field_types type)
{
	switch(type)
	{
	case MYSQL_TYPE_DECIMAL:
	case MYSQL_TYPE_TINY:
	case MYSQL_TYPE_SHORT:
	case MYSQL_TYPE_LONG:
	case MYSQL_TYPE_FLOAT:
	case MYSQL_TYPE_DOUBLE:		
	case MYSQL_TYPE_LONGLONG:
	case MYSQL_TYPE_INT24: 
	case MYSQL_TYPE_NEWDECIMAL:		
		{
			return true;
		}
	default:
		{
			SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- unsupported type in analytic function.");
			return false;
		}
	}

}

bool ha_scaledb::generateAnalyticsString(List<Item> *list)
{


	char* s_force_analytics=NULL;

	try
	{
		THD* thd = ha_thd();


		analyticsStringLength_		=
			analyticsSelectLength_      = 0;

		analytics_uses_count=false;
	
		s_force_analytics=SDBUtilFindComment(thd->query(), "force_sdb_analytics") ;

		bool is_streaming_table=SDBIsStreamingTable(sdbDbId_, sdbTableNumber_);

		if ( is_streaming_table )
		{

			literal_buffer_offset=0;

			//only proceed if condition pushdown was successful
			int  cardinality=SDBUtilFindCommentIntValue(thd->query(), "cardinality") ;
			int  thread_count=SDBUtilFindCommentIntValue(thd->query(), "thread_count") ;

			char	buf[ 2000 ];

			int pos=0;
		

			int select_limit=-1;
			SELECT_LEX*  lex=   (((THD*) ha_thd())->lex)->select_lex.parent_lex->current_select;
			Item* limit= lex->select_limit;
			if(limit!=NULL)
			{
				if(limit->type()==Item::INT_ITEM)
				{
					select_limit=(long)limit->val_int();
				}
			}
			Item* is_null_orderby=NULL;
			int info=0;
			if(lex->order_list.elements>0)
			{
				info |= GH_ORDER_BY;   
				if(lex->order_list.first->asc) {info |= ANALYTIC_FLAG_ASCENDING;}
				ORDER *order ;				
				for (order = (ORDER *) lex->order_list.first; order;  order = order->next)
				{
					Item* item=*order->item;
					if(item->field_type()==MYSQL_TYPE_NULL)
					{
						is_null_orderby=item;
					}
				}
			}else if(lex->group_list.elements>0)
			{
				info |= GH_ORDER_BY;   
				if(lex->group_list.first->asc) {info |= ANALYTIC_FLAG_ASCENDING;}
			}
			if(analytics_uses_count)		   {info |= ANALYTIC_FLAG_USES_COUNT;}

			GroupByAnalyticsHeader* gbh = (GroupByAnalyticsHeader*)buf;
			gbh->cardinality = cardinality;
			gbh->thread_count= ( unsigned short ) thread_count;
			gbh->limit = select_limit;
			gbh->info_flag = info;
			gbh->offsetToAuxiliary = SDBGetOffsetAuxiliary(sdbDbId_, sdbTableNumber_);	// this is the offset to the var char fields

			pos=pos+sizeof(GroupByAnalyticsHeader);

			const char* col_name=NULL;
			const char* alias_name=NULL;
			int g=0;

			bool contains_agg_function=false;
	if((((THD*) ha_thd())->lex)->select_lex.parent_lex->current_select->options  & SELECT_DISTINCT)
	{
		//if this is a distinct query then lets add the field name to the group by
		// for example
		//
		// select distinct Store, Office from Payment56 group by House, Condo;
		// will get mapped to 
		//
		// select Store from Payment56 group by Store, Office, House, Condo;
		//
		Item *item;
		List_iterator_fast<Item> fi((((THD*) ha_thd())->lex)->select_lex.parent_lex->current_select->item_list);
		while ((item = fi++))
		{
	
			int function_length=0;
			enum_field_types type;
			function_type function=FT_NONE;
			int flag=0;
			bool skip=false;
			switch (item->type())
			{
		
				case Item::INT_ITEM:
				case Item::REAL_ITEM:
				case Item::STRING_ITEM:
				case Item::DECIMAL_ITEM:
				case Item::NULL_ITEM:
					{
						continue; //ignore literals,we don't need to groupby on them.
					}
			case Item::FIELD_ITEM:
				{
					Field *field = ((Item_field *)item)->field;
					col_name=field->field_name;	
					type= field->type();	
					flag=field->flags;
					break;
				}

			case Item::SUM_FUNC_ITEM:
				{	
					Item_sum *sum = ((Item_sum *)item);
					int stype=sum->sum_func();

					if (stype == Item_sum::MIN_FUNC
						|| stype==Item_sum::MAX_FUNC 	
						|| stype == Item_sum::AVG_FUNC)
					{
						contains_agg_function=true;
						continue;
					}	
					else
					{
						//invalid or not supported
						return 0;
					}
				

					break;
				}
			default:
				{
					//only support distinct on field values
					return 0;
				}
			}

			g++;
			pos=addGroupBy( buf,  sdbDbId_, sdbTableNumber_,  type,  flag,  pos,    col_name,   alias_name,  function);
			if(pos==0) {return 0;}

		}
	}

	if(contains_agg_function)
	{
		//then DISTINCT is redundant so remove any groupby that was added by distinct.
		g=0;
		pos=sizeof(GroupByAnalyticsHeader);
	}



			for (ORDER *cur_group= (((THD*) ha_thd())->lex)->select_lex.parent_lex->current_select->group_list.first ; cur_group ; cur_group= cur_group->next)
			{

				g++;


				int function_length=0;
				enum_field_types type;
				Item* item=*cur_group->item;
				function_type function=FT_NONE;
				int flag=0;

				switch (item->type())
				{

				case Item::FIELD_ITEM:
					{
						Field *field = ((Item_field *)item)->field;
						col_name=field->field_name;	
						type= field->type();	
						flag=field->flags;
						break;
					}
				case Item::FUNC_ITEM:
					{


						Item_func *func = ((Item_func *)item);

						Item::Type _type= item->next->type();
						if(_type!=Item::FIELD_ITEM)
						{
							//we only support simple functions, eg DATE(x) not DATE(MAX(X))
							return false;
						}


						Item_func_hour* dfunc=((Item_func_hour*)func);

						if(checkFunc((char*)dfunc->func_name(), "timestamp")||checkFunc((char*)dfunc->func_name(), "cast_as_datetime"))      
						{
							function=FT_TIMESTAMP;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;


						}else if(checkFunc((char*)dfunc->func_name(), "date")||checkFunc((char*)dfunc->func_name(), "cast_as_date"))      
						{
							function=FT_DATE;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;


						}else  if(checkFunc((char*)dfunc->func_name(), "year"))   
						{
							function=FT_YEAR;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;

						}else  if(checkFunc((char*)dfunc->func_name(), "month"))   
						{
							function=FT_MONTH;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;

						}else  if(checkFunc((char*)dfunc->func_name(), "dayofmonth"))   
						{
							function=FT_DAY;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;

						}else  if(checkFunc((char*)dfunc->func_name(), "time")||checkFunc((char*)dfunc->func_name(), "cast_as_time"))   
						{
							function=FT_TIME;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;
						}
						else  if(checkFunc((char*)dfunc->func_name(), "hour"))   
						{
							function=FT_HOUR;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;

						}else  if(checkFunc((char*)dfunc->func_name(), "minute"))   
						{
							function=FT_MINUTE;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;

						}
						else  if(checkFunc((char*)dfunc->func_name(), "second"))   
						{
							function=FT_SECOND;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;

						}
						else
						{
							return false;
						}

						break;
					}
				default:
					{
						return false;
					}
				}


				pos=addGroupBy( buf,  sdbDbId_, sdbTableNumber_,  type,  flag,  pos,    col_name,   alias_name,  function);
				if(pos==0) {return false;}
			}


			if(g==0)  
			{
				//there are NO group by so return analytics with col=0;
				gbh->numberColumns=0;
				gbh->numberInOrderby=numberInOrderBy();

			}
			else
			{

				gbh->numberColumns=g;
				gbh->numberInOrderby=numberInOrderBy();

			}




			int pos0=pos;

			List_iterator_fast<Item> it(*list);
			Item                     *item;

			int field_count= list->elements;
			int len=0;
			bool contains_analytics=false;


			SelectAnalyticsHeader* sah= (SelectAnalyticsHeader*)(buf+pos);
			pos += sizeof(SelectAnalyticsHeader);
			int n=0;
			while ((item= it++))
			{
//sometimes mysql expects the const items to get returned, and sometimes it does not, the following check
//is used to determine that. see bug 2778

				if(item->const_item() )
				{
					if( (item->basic_const_item()  && item!=is_null_orderby ) 
						|| (item->type()==Item::FUNC_ITEM && ((Item_func *)item)->functype()== Item_func::GUSERVAR_FUNC ) 
						|| item->type()==Item::SUM_FUNC_ITEM)
					{
						continue;
					}
				}					
				char* name= item->name;
			
				n++;
				SelectAnalyticsBody1* sab1= (SelectAnalyticsBody1*)(buf+pos);
				pos=pos+sizeof(SelectAnalyticsBody1);
				const char* col_name=NULL;
				char* alias_name=NULL;
				enum_field_types type= MYSQL_TYPE_NULL;
				function_type function;
				short precision=0;
				short result_precision=0;
				short result_scale=0;
				short scale=0;
				short no_fields=0;	//reset the number of fields
				int flag=0;
				alias_name= item->name;

				switch (item->type())
				{
				case Item::SUBSELECT_ITEM:
					{
						Item_subselect *sum = ((Item_subselect *)item);
						type= type=sum->field_type();
						col_name=NULL;
						function=FT_NONE;
						break;
					}
				case Item::INT_ITEM:
				case Item::REAL_ITEM:
				case Item::STRING_ITEM:
				case Item::DECIMAL_ITEM:
				case Item::NULL_ITEM:
					{
						function=FT_NONE;
						break;
					}
				case Item::COND_ITEM:
					{
						return false;

					}
				case Item::FIELD_ITEM:
					{
					
						//save each field
						Field *field1 = ((Item_field *)item)->field;
						col_name=field1->field_name;	
						type= field1->type();
						function=FT_NONE;
						flag=field1->flags;
						if(type==MYSQL_TYPE_NEWDECIMAL)
						{			
							Field_new_decimal* fnd=(Field_new_decimal*)field1;
							precision=fnd->precision;
							scale=fnd->decimals();
						}

					
						break;
					}

				case  Item::SUM_FUNC_ITEM:
					{			
						Item_sum *sum = ((Item_sum *)item);
						if (sum->sum_func() == Item_sum::SUM_FUNC)
						{				
							function=FT_SUM;
							Item::Type ft=item->next->type();
							if(ft==Item::FIELD_ITEM)
							{
								Field *field =((Item_field *)item->next)->field;
								col_name=field->field_name;
								type= field->type();

								if(!isValidAnalyticType(type))
								{
									return false;
								}


								flag=field->flags;
								if(type==MYSQL_TYPE_NEWDECIMAL)
								{			
									Field_new_decimal* fnd=(Field_new_decimal*)field;
									precision=fnd->precision;
									result_precision =item->decimal_precision();
									scale=fnd->decimals();
									result_scale = item->decimals;
								}
								else
								{
									result_precision= getPrecision(item);
								}
							}
							else
							{
								return false; //unsupported so bail.
							}
						}
						else if (sum->sum_func() == Item_sum::COUNT_FUNC)
						{


							function=FT_COUNT;
							Item::Type ft=item->next->type();
							if(ft==Item::FIELD_ITEM)
							{
				//enable this to enable the field count code.
								function=FT_COUNT_FIELD;
								Field *field =((Item_field *)item->next)->field;

								col_name= field->field_name;
								type= field->type();								
								flag=field->flags;		
								if(strcmp("?",col_name)==0)
								{			
									function=FT_COUNT;
									alias_name= item->name;
								}
							}
							else
							{
							function=FT_COUNT;
								//this is a count(*)
								col_name="?";
								type=MYSQL_TYPE_LONG;
								alias_name= item->name;
							}

						}

#ifdef  PROCESS_COUNT_DISTINCT
						else if (sum->sum_func() == Item_sum::COUNT_DISTINCT_FUNC)
						{
							function=FT_COUNT_DISTINCT;
							Field *field =((Item_field *)item->next)->field;
							Item::Type ft=item->next->type();
							if(ft==Item::FIELD_ITEM)
							{
								col_name=field->field_name;
								type= field->type();	
								flag=field->flags;

								if(type==MYSQL_TYPE_NEWDECIMAL)
								{			
									Field_new_decimal* fnd=(Field_new_decimal*)field;
									precision=fnd->precision;
									result_precision =item->decimal_precision();
									result_scale=item->decimals;
									scale=fnd->decimals();
								}
								else
								{
								//	result_precision= getPrecision(item);
								}
							}
							else
							{
								char* name= item->name;  //count distinct can have multile arguemnts
	
								return false;
							}						

						}				
#endif //  PROCESS_COUNT_DISTINCT
						else if (sum->sum_func() == Item_sum::MIN_FUNC)
						{
							Item_sum_min *max = ((Item_sum_min *)item);
							Item_result_field *rf= (Item_result_field *)sum;
							Item::Type ft=item->next->type();
							char* name= item->name;

							if(ft==Item::FUNC_ITEM)
							{
	
								return false;
							}
							else if(ft==Item::FIELD_ITEM)
							{
								Field *field =((Item_field *)item->next)->field;
								col_name=field->field_name;
								type= field->type();							
								flag=field->flags;
								function=FT_MIN;
								if(type==MYSQL_TYPE_NEWDECIMAL)
								{			
									Field_new_decimal* fnd=(Field_new_decimal*)field;
									precision=fnd->precision;
									result_precision =item->decimal_precision();
									result_scale=item->decimals;
									scale=fnd->decimals();
								}
								else
								{
							//		result_precision= getPrecision(item);
								}
							}
							else
							{
								return false; //bail
							}

						}
						else if (sum->sum_func() == Item_sum::MAX_FUNC)
						{
							Item_sum_max *max = ((Item_sum_max *)item);
							Item_result_field *rf= (Item_result_field *)sum;
							Item::Type ft=item->next->type();
							char* name= item->name;

							if(ft==Item::FUNC_ITEM)
							{

								return false;
/*
								Item_func *func = ((Item_func *)item);


								if(checkFunc((char*)func->func_name(), "bit_length"))     
								{
									function=FT_BIT_LENGTH;
									Field *field =((Item_field *)item->next)->field;
									col_name=field->field_name;
									type= field->type();
									flag=field->flags;
									break;
								}
								else
								{

								return false;
								}
*/
							}
							else if(ft==Item::FIELD_ITEM)
							{
								Field *field =((Item_field *)item->next)->field;
								col_name=field->field_name;
								type= field->type();							
								function=FT_MAX;
								flag=field->flags;
								if(type==MYSQL_TYPE_NEWDECIMAL)
								{			
									Field_new_decimal* fnd=(Field_new_decimal*)field;
									precision=fnd->precision;
									result_precision =item->decimal_precision();
									result_scale=item->decimals;
									scale=fnd->decimals();
								}
								else
								{
								//	result_precision= getPrecision(item);
								}

							}
							else
							{
								return false; //bail
							}

						}
						else if(sum->sum_func() == Item_sum::UDF_SUM_FUNC)
						{
						
								return false;
							
						}
						else if (sum->sum_func() == Item_sum::AVG_FUNC)
						{

							Item_result_field *rf= (Item_result_field *)sum;
							Item::Type ft=item->next->type();
							char* name= item->name;

							if(ft==Item::FUNC_ITEM)
							{
									
								return false;
							}
							else if(ft==Item::FIELD_ITEM)
							{
								Field *field =((Item_field *)item->next)->field;
								col_name=field->field_name;
								type= field->type();	
								if(!isValidAnalyticType(type))
								{
									return false;
								}
								function=FT_AVG;
								flag=field->flags;
								if(type==MYSQL_TYPE_NEWDECIMAL)
								{			
									Field_new_decimal* fnd=(Field_new_decimal*)field;
									precision=fnd->precision;
									result_precision =item->decimal_precision();
									result_scale=item->decimals;
									scale=fnd->decimals();
								}
								else
								{
									result_precision= getPrecision(item);
								}

							}
							else
							{
								return false; //bail
							}

						}
						else
						{
							return false;  //no condition string will get generated
						}

						break;
					}
				case Item::FUNC_ITEM:
					{

						Item_func *func = ((Item_func *)item);

						if(item->next!=NULL)
						{
							Item::Type _type= item->next->type();
							if(_type!=Item::FIELD_ITEM)
							{
								//we only support simple functions, eg DATE(x) not DATE(MAX(X))
								return false;
							}
						}
						Item_func_hour* dfunc=((Item_func_hour*)func);
						
						if(checkFunc((char*)dfunc->func_name(), "timestamp")||checkFunc((char*)dfunc->func_name(), "cast_as_datetime"))      
						{

							function=FT_TIMESTAMP;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;

						}else if(checkFunc((char*)dfunc->func_name(), "date")||checkFunc((char*)dfunc->func_name(), "cast_as_date"))      
						{
							function=FT_DATE;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;


						}else  if(checkFunc((char*)dfunc->func_name(), "year"))   
						{
							function=FT_YEAR;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;

						}else  if(checkFunc((char*)dfunc->func_name(), "month"))   
						{
							function=FT_MONTH;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;

						}else  if(checkFunc((char*)dfunc->func_name(), "dayofmonth"))   
						{
							function=FT_DAY;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;

						}else  if(checkFunc((char*)dfunc->func_name(), "time")||checkFunc((char*)dfunc->func_name(), "cast_as_time"))   
						{
							function=FT_TIME;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;
						}
						else  if(checkFunc((char*)dfunc->func_name(), "hour"))   
						{
							function=FT_HOUR;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;

						}else  if(checkFunc((char*)dfunc->func_name(), "minute"))   
						{
							function=FT_MINUTE;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;

						}
						else  if(checkFunc((char*)dfunc->func_name(), "second"))   
						{
							function=FT_SECOND;
							Field *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;

						}
						else  if(checkFunc((char*)dfunc->func_name(), "inet_ntoa"))   
						{


							function=FT_INET4_TOA;
							Field  *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;

						}
						else  if(checkFunc((char*)dfunc->func_name(), "inet6_ntoa"))   
						{


							function=FT_INET6_TOA;
							Field  *field =((Item_field *)item->next)->field;
							col_name=field->field_name;
							type= field->type();
							flag=field->flags;

						}
						else
						{
							return false;
						}


						break;
					}


					default:
						{

							return false; //not supported
						}




				}

				sab1->numberOfFieldsInFunction=1;
				sab1->analyticFunction=function;
				bool ret=addSelectField(buf,  pos, sdbDbId_, sdbTableNumber_, type, function, col_name ,contains_analytics,precision,scale,flag,result_precision,result_scale, alias_name,false);
				if(ret==false)
				{
					return false; //not supported
				}
			}


			if(analytics_uses_count)		   {info |= ANALYTIC_FLAG_USES_COUNT;}
			gbh->info_flag = info;

			
			sah->numberOutputFields=n;
			analyticsStringLength_=pos;
			analyticsSelectLength_=pos-pos0;
			//turn analytics off
			char* s_disable_analytics=SDBUtilFindComment(thd->query(), "disable_sdb_analytics") ;

			if  (s_disable_analytics==NULL && pos	> 0 )
			{
				//need to check condition string is big enough?
				memcpy(	analyticsString_, buf, pos );

				if(s_force_analytics!=NULL)
				{
					//analytics failed so return an error.
					forceAnalytics_=true;
				}

			}
			else
			{
				if(s_force_analytics!=NULL)
				{
					//analytics failed so return an error.
					forceAnalytics_=true;
				}
			}

		}
		else
		{

			if(s_force_analytics!=NULL)
			{
				//analytics failed so return an error.
				forceAnalytics_=true;
			}
		}
		return true;
	}
	catch(const char* )
	{
		//somethign bad happened so failing analytics
		analyticsStringLength_		=0;
		analyticsSelectLength_      = 0;
		if(s_force_analytics!=NULL)
		{
			//analytics failed so return an error.
			forceAnalytics_=true;
		}
		return false;
	}

}

/**
when table alias are used we need to lookup the real
table not the alias. needed for queries like joins.


*/

unsigned short ha_scaledb::getTableIDfromNameOrAlias(TABLE_LIST* table_list, unsigned short userId, unsigned short dbId,const char* table_name)
{
#define _DONT_SUPPORT_JOINS 
#ifdef _DONT_SUPPORT_JOINS
	int count=table_list->join_list->elements;
	if(count>1) {return 0;}
#endif
	unsigned short	tableId	=0;
	TABLE_LIST* t= table_list;
	do
	{
		if(stricmp(t->alias,table_name)==0 || stricmp(t->table_name,table_name)==0)
		{
			//found a match so lookup
			tableId			= SDBGetTableNumberByName( sdbUserId_, dbId, t->table_name );
			break;
		}
	}while(t= table_list->next_global);

	return tableId;
}


/**
  Push a condition and/or analytics to the scaledb storage engines for enhancing
  table scans. SDB Index can use suffixes of the key parts - not only prefixes.
  The conditions will be stored on a stack for possibly storing several conditions.
  The stack can be popped by calling cond_pop, handler::extra(HA_EXTRA_RESET) (handler::reset())
  will clear the stack.
*/

const COND* ha_scaledb::cond_push( const COND* pCond )
{
	bool		doPush;

	DBUG_ENTER( "cond_push" );

	if ( pushCondition_ )
	{
		if ( pCond )
		{
			switch ( sqlCommand_ )
			{
				case SQLCOM_CREATE_TABLE:
				case SQLCOM_SELECT:
				case SQLCOM_HA_READ:
				case SQLCOM_INSERT_SELECT:
				case SQLCOM_SET_OPTION:
					doPush				= true;
					break;
				default:
					doPush				= false;
					break;
			}

			if ( doPush )
			{

				TABLE_LIST* table_list=(((THD*) ha_thd())->lex)->select_lex.parent_lex->current_select->table_list.first; //this contains list of all tables in query, including alias
				//importanst for queries that have alias tables such as joins

				


				

				// Save condition as a string to a class variable to be pushed to CAS
				saveConditionToString( pCond,table_list );
			}
			else
			{
				conditionStringLength_	= 0;
				analyticsStringLength_	=
				analyticsSelectLength_	= 0;
				forceAnalytics_			= false;

				rangeBounds.clear();
			}
		}
		else
		{
			rangeBounds.clear();
		}

		// Avoid pushing the condition more than once for the same statement
		pushCondition_					= false;
	}
	else if ( !pCond )
	{
		rangeBounds.clear();
	}

	DBUG_RETURN( pCond );
}


/**
  Pop the top condition from the condition stack of the handler instance.
*/
void  ha_scaledb::cond_pop() 
{ 
	SDBConditionStackPop(conditions_);
    DBUG_ENTER("cond_pop");
}
#endif //SDB_PUSH_DOWN


// returns the next value in ascending order from the index
// the method should not consider what key was used in the original index_read
// but just return the next item in the index
int ha_scaledb::index_next(unsigned char* buf) {
	DBUG_ENTER("ha_scaledb::index_next");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_next(...) ");
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(", alias: ");
#ifdef _MARIA_DB
		SDBDebugPrintString(table->alias.c_ptr());
#else
		SDBDebugPrintString( table->alias);
#endif
		outputHandleAndThd();
		SDBDebugPrintString(", Query Manager ID #");
		SDBDebugPrintInt(sdbQueryMgrId_);
		SDBDebugPrintString(" counter = ");
		SDBDebugPrintInt(++readDebugCounter_);
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	ha_statistic_increment(&SSV::ha_read_next_count);
	int errorNum = 0;
	if ( !starLookupTraversal_) {	
		SDBQueryCursorSetFlags(sdbQueryMgrId_, sdbDesignatorId_, false, SDB_KEY_SEARCH_DIRECTION_GE, false, true,readJustKey_);
	}
	
	errorNum = fetchRow(buf);

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ && errorNum) {
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
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_next_same(...) ");
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(", alias: ");
#ifdef _MARIA_DB
		SDBDebugPrintString( table->alias.c_ptr());
#else
		SDBDebugPrintString( table->alias);
#endif
		outputHandleAndThd();
		SDBDebugPrintString(", Query Manager ID #");
		SDBDebugPrintInt(sdbQueryMgrId_);
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	int errorNum = 0;
	if ( !starLookupTraversal_ && active_index < MAX_KEY) {
		SDBQueryCursorSetFlags(sdbQueryMgrId_, sdbDesignatorId_, false, SDB_KEY_SEARCH_DIRECTION_GE,true, true,readJustKey_);
	}

	errorNum = fetchRow(buf);

	DBUG_RETURN(errorNum);
}

//This method returns the first key value in index
int ha_scaledb::index_first(uchar* buf) {
	DBUG_ENTER("ha_scaledb::index_first");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_first(...) ");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	ha_statistic_increment(&SSV::ha_read_first_count);

	THD* thd = ha_thd();
	sqlCommand_ = thd_sql_command(thd);
	int errorNum = index_read(buf, NULL, 0, HA_READ_AFTER_KEY);

	DBUG_RETURN(errorNum);
}

//This method returns the last key value in index
int ha_scaledb::index_last(uchar * buf) {
	DBUG_ENTER("ha_scaledb::index_last");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_last(...) ");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	ha_statistic_increment(&SSV::ha_read_last_count);

	int errorNum = index_read(buf, NULL, 0, HA_READ_BEFORE_KEY);

	DBUG_RETURN(errorNum);
}

//This method returns the prev key value in index
int ha_scaledb::index_prev(uchar * buf) {
	DBUG_ENTER("ha_scaledb::index_prev");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_prev(...) ");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	ha_statistic_increment(&SSV::ha_read_prev_count);
	int errorNum = 0;
	// Bug 1132: still need to call SDBQueryCursorSetFlags as distinct parameter may have diffrent value than the one set in index_read.
	if ( !starLookupTraversal_) {
		SDBQueryCursorSetFlags(sdbQueryMgrId_, sdbDesignatorId_, false, SDB_KEY_SEARCH_DIRECTION_LE, false, true,readJustKey_);
	}
	
	errorNum = fetchRow(buf);

	DBUG_RETURN(errorNum);
}

// This method prepares for a statement that need to access all records of a table.
// The statement can be SELECT, DELETE, UPDATE, etc.
int ha_scaledb::rnd_init(bool scan) {
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::rnd_init(...) ");
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(", alias: ");
#ifdef _MARIA_DB
		SDBDebugPrintString( table->alias.c_ptr());
#else
		SDBDebugPrintString(table->alias);
#endif
		outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	DBUG_ENTER("ha_scaledb::rnd_init");
	THD* thd = ha_thd();

	if ( isQueryEvaluation() )
	{
		DBUG_RETURN( 0 );
	}

	sqlCommand_ = thd_sql_command(thd);
	// For select statement, we use the sequential scan since full table scan is faster in this case.
	// For non-select statement, if the table has index, then we prefer to use index 
	// (most likely the primary key) to fetch each record of a table.
	resetSdbQueryMgrId();
	active_index = MAX_KEY; // we use sequential scan by default

	beginningOfScan_ = true;
	if (virtualTableFlag_) {
		prepareFirstKeyQueryManager(); // have to use multi-table index for virtual view		
	}
	else if (scan) {

		// For SELECT, INSERT ... SELECT and ALTER TABLE statements, we always use full table sequential scan (even there exists an index).
		// For all other statements, we use the first available index if an index exists.
		switch ( sqlCommand_ )
		{
		case SQLCOM_SET_OPTION:
		case SQLCOM_INSERT_SELECT:
		case SQLCOM_SELECT:
			{
				if (forceAnalytics_==true)
				{
					if(!SDBErrorExists(sdbUserId_))
					{
						//if we don't already have an error then add this error, otherwise we return the  current error (which is more relevant) 
						SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- force_sdb_analytics was set but not used." );
					}
					DBUG_RETURN(HA_ERR_GENERIC);
				}
		
				break;
			}
		case SQLCOM_HA_READ:
		case SQLCOM_ALTER_TABLE:
		case SQLCOM_CREATE_INDEX:
		case SQLCOM_DROP_INDEX:
			break;
		default:
			if ( SDBIsTableWithIndexes( sdbDbId_, sdbTableNumber_ ) )
			{
				prepareFirstKeyQueryManager();
			}
		}	
	}

	DBUG_RETURN(0);
}

// This method ends a full table scan
int ha_scaledb::rnd_end() {
	DBUG_ENTER("ha_scaledb::rnd_end");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::rnd_end(...) ");
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(", alias: ");
#ifdef _MARIA_DB
		SDBDebugPrintString( table->alias.c_ptr());
#else
		SDBDebugPrintString(table->alias);
#endif
		outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	if ( isQueryEvaluation() )
	{
		DBUG_RETURN( 0 );
	}

	char* pTableName		= SDBGetTableNameByNumber( sdbUserId_, sdbDbId_, sdbTableNumber_ );

#ifdef SDB_DEBUG
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart      ();
		SDBDebugPrintString( "\nEnd of sequential scan on table [" );
		SDBDebugPrintString( pTableName );
		SDBDebugPrintString( "]\n" );
		SDBDebugEnd        ();
	}
#endif

	SDBEndSequentialScan( sdbUserId_, sdbQueryMgrId_, sdbDbId_, sdbPartitionId_, pTableName, ( ( THD* ) ha_thd() )->query_id );
////
	conditionStringLength_	= 0;
	analyticsStringLength_	= 0;
	forceAnalytics_=false;
///
	int errorNum = 0;
	DBUG_RETURN(errorNum);
}

// This method returns the next record
int ha_scaledb::rnd_next(uchar* buf) {
	DBUG_ENTER("ha_scaledb::rnd_next");
	
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::rnd_next(...), index: ");
		SDBDebugPrintInt( active_index);
		SDBDebugPrintString(", Query Manager ID #");
		SDBDebugPrintInt(sdbQueryMgrId_);
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(", alias: ");
#ifdef _MARIA_DB
		SDBDebugPrintString( table->alias.c_ptr());
#else
		SDBDebugPrintString( table->alias);
#endif
		outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	int errorNum = 0;
	int retValue = 0;

	ha_statistic_increment(&SSV::ha_read_rnd_next_count);

	if ( isQueryEvaluation() )
	{
		errorNum	= evaluateTableScan();

		DBUG_RETURN( errorNum );
	}

	if (beginningOfScan_) {
		if ( !sdbQueryMgrId_ ) 
		{
			SDBTerminateEngine(0, "ha_scaledb::rnd_next - no query manger was taken", __FILE__, __LINE__ );
		}

		// build template at the begin of scan 
		buildRowTemplate(table, buf);

		if (active_index == MAX_KEY)
		{
			// prepare for sequential scan
			char* pTableName			= SDBGetTableNameByNumber( sdbUserId_, sdbDbId_, sdbTableNumber_ );

			retValue					= ( int ) SDBPrepareSequentialScan( sdbUserId_, sdbQueryMgrId_, sdbDbId_, sdbPartitionId_, pTableName, ( ( THD* ) ha_thd() )->query_id,
																			releaseLocksAfterRead_, rowTemplate_,
																			conditionString_, conditionStringLength_, analyticsString_, analyticsStringLength_ );

			if (temp_table){
				// build template representing the result set - if the result set is defined by a tmp table
				// i.e. with the group by handler.
				buildRowTemplate(temp_table, buf);
			}

			if ( conditionStringLength_ )
			{
				conditionStringLength_	= 0;
			}

			if ( analyticsStringLength_ )
			{
				analyticsStringLength_	= 0;
			}
			forceAnalytics_=false;

			// We fetch the result record and save it into buf
			if (retValue == 0) {
				errorNum = fetchRow(buf);
			} else {
				errorNum = convertToMysqlErrorCode(retValue);
				table->status = STATUS_NOT_FOUND;
			}
			//pSdbMysqlTxn_->setScanType(sdbQueryMgrId_, true);
			sdbSequentialScan_ = true;

		} else { // use index
			errorNum = index_first(buf);
		}

		beginningOfScan_ = false;
	} 
	else { // scaning the 2nd record and forwards
			errorNum = fetchRow(buf);	
	}

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::rnd_next(...) returns : ");
		SDBDebugPrintInt(errorNum);
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	DBUG_RETURN(errorNum);
}

// This method is called after each call to rnd_next() if the data needs to be ordered.
void ha_scaledb::position(const uchar* record) {
	DBUG_ENTER("ha_scaledb::position");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::position(...) on table ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(" ");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	unsigned long long rowPos;
	if (table->key_info && sdbSequentialScan_ == false && isStreamingTable_!=ST_TRUE) {
		// get the index cursor row position
		// This is the case of a table with index and the lookup was using the index.
		rowPos = (unsigned long long) SDBQueryCursorGetIndexCursorRowPosition(sdbQueryMgrId_);
	} else {
		//non index, return current row position
		// sequential scan, or no index or a streaming table..
		rowPos = SDBQueryCursorGetSeqRowPosition(sdbQueryMgrId_);
	}

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("position row id: ");
		SDBDebugPrint8ByteUnsignedLong(rowPos);
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	my_store_ptr(ref, sizeof(rowPos), rowPos);
	ref_length = sizeof(rowPos);

	DBUG_VOID_RETURN;
}

// This method is used for finding previously marked with position().
int ha_scaledb::rnd_pos(uchar * buf, uchar *pos) {
	DBUG_ENTER("ha_scaledb::rnd_pos");

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::rnd_pos(...) ");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	ha_statistic_increment(&SSV::ha_read_rnd_count);

	int retValue = 0;

	//prepare once
	if (beginningOfScan_)
	{
		// build template at the begin of scan 
		buildRowTemplate(table, buf);

		unsigned int old_active_index = active_index;
		resetSdbQueryMgrId();
		prepareFirstKeyQueryManager();

		active_index				= old_active_index;
		char to[512];
		uint n=tablename_to_filename( table->s->table_name.str, to, sizeof(to));
		retValue					= ( int ) SDBPrepareSequentialScan( sdbUserId_, sdbQueryMgrId_, sdbDbId_, sdbPartitionId_, to,
																		( ( THD* ) ha_thd() )->query_id, releaseLocksAfterRead_, rowTemplate_,
																		conditionString_, conditionStringLength_, analyticsString_, analyticsStringLength_ );

		beginningOfScan_			= false;
		sdbSequentialScan_			= true;

		if ( conditionStringLength_ )
		{
			conditionStringLength_	= 0;
		}

		if ( analyticsStringLength_ )
		{
			analyticsStringLength_	= 0;
		}
		forceAnalytics_=false;

	}

	

	if (retValue == 0) {
		int64 rowPos = my_get_ptr(pos, ref_length);

	

#ifdef SDB_DEBUG_LIGHT
		if (mysqlInterfaceDebugLevel_) {
			SDBDebugStart(); // synchronize threads printout
			SDBDebugPrintHeader("rnd_pos row id to be fetched: ");
			SDBDebugPrint8ByteUnsignedLong(rowPos);
			SDBDebugEnd(); // synchronize threads printout
		}
#endif
		retValue = fetchRowByPosition(buf, rowPos);
	}
	retValue = convertToMysqlErrorCode(retValue);

	DBUG_RETURN(retValue);
}

// This method sets up public variables for query optimizer to use 
int ha_scaledb::info(uint flag) {
	DBUG_ENTER("ha_scaledb::info");
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::info(...) ");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	if(table==NULL)
	{
		DBUG_RETURN(0);
	}

	THD* thd = ha_thd();
	
	// Temporary Fix- Maria DB ahould call my_ok() bfore running an SQL command 
	if ( thd->is_error() ) {
		my_ok(thd);
	}

	placeSdbMysqlTxnInfo(thd);
	sdbUserId_ = pSdbMysqlTxn_->getScaleDbUserId();

	if (!sdbDbId_) {
		DBUG_RETURN(0);
	}


//	printTableId("ha_scaledb::info(open):#1");
	// there are case this is the first contact point to the table - make sure it is open 
	if (sdbTableNumber_ == 0)
	{
		SessionSharedMetaLock ot(sdbDbId_);
		if(ot.lock()==false)
		{
			DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
		}
		char to[512];
		uint n=tablename_to_filename( table->s->table_name.str, to, sizeof(to));
		sdbTableNumber_				= SDBOpenTable(sdbUserId_, sdbDbId_, to, sdbPartitionId_, false); 
		//		printTableId("ha_scaledb::info(open):#2");
		if (!sdbTableNumber_)
			DBUG_RETURN(convertToMysqlErrorCode(TABLE_NAME_UNDEFINED));
	}
       assert(sdbTableNumber_!=0);
	if (!ha_scaledb::lockDML(sdbUserId_, sdbDbId_, sdbTableNumber_, 0)) {
		DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
	}
	if (flag & HA_STATUS_ERRKEY) {
		errkey = get_last_index_error_key();		
	}

	bool			isFactTable		= SDBIsStreamingTable(sdbDbId_, sdbTableNumber_) && !SDBIsDimensionTable(sdbDbId_, sdbTableNumber_);
	unsigned short	rangeDesignator	= ( unsigned short ) SDBGetRangeKey( sdbDbId_, sdbTableNumber_ );
	unsigned short	sdbIndexTableId	= SDBGetIndexTableNumberForTable( sdbDbId_, sdbTableNumber_ );
	bool			isVariableStats	= false;
	bool			isConstStats	= false;
	bool			isTimeStats		= false;
	bool			isAutoStats		= false;

	if ( !sdbIndexTableId )
	{
		sdbIndexTableId				= sdbTableNumber_;
	}

	if ( ( flag & HA_STATUS_VARIABLE ) || ( flag & HA_STATUS_NO_LOCK ) )
	{
		// update the 'variable' part of the info:
		stats.records				= ( ha_rows )	SDBGetTableStats( sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_, SDB_STATS_INFO_FILE_RECORDS );
		// this is a fixed to be compatibale with Innodb:
		//  The MySQL optimizer seems to assume in a left join that n_rows
		//  is an accurate estimate. If it is zero then, of course, it is not,
		//  Since SHOW TABLE STATUS seems to call this function with the
		//  HA_STATUS_TIME flag set, while the left join optimizer does not
		//  set that flag, we add one to a zero value if the flag is not
		//  set. That way SHOW TABLE STATUS will show the best estimate,
		//  while the optimizer never sees the table empty. 
		if (stats.records == 0 && !(flag & HA_STATUS_TIME)) {
			stats.records           = 1;
		}
		stats.deleted				= ( ha_rows )	SDBGetTableStats( sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_, SDB_STATS_INFO_FILE_DELETED );
		stats.delete_length			=				SDBGetTableStats( sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_, SDB_STATS_INFO_FILE_DELETE_LENGTH );
		stats.data_file_length		=				SDBGetTableStats( sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_, SDB_STATS_INFO_DATA_FILE_LENGTH );
		stats.index_file_length		=				SDBGetTableStats( sdbUserId_, sdbDbId_, sdbIndexTableId, sdbPartitionId_, SDB_STATS_INFO_INDEX_FILE_LENGTH );
		stats.mean_rec_length		= ( ulong )		SDBGetTableStats( sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_, SDB_STATS_INFO_MEAN_REC_LENGTH );
		stats.check_time			= 0;
		stats.update_time			= 0;
		isVariableStats				= true;
	}
	
	if ( flag & HA_STATUS_CONST )
	{
		stats.max_data_file_length	=				SDBGetTableStats( sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_, SDB_STATS_INFO_DATA_FILE_MAX_LENGTH );
		stats.max_index_file_length	=				SDBGetTableStats( sdbUserId_, sdbDbId_, sdbIndexTableId, sdbPartitionId_, SDB_STATS_INFO_INDEX_FILE_MAX_LENGTH );
        stats.block_size			=				METAINFO_BLOCK_SIZE;
       
        ha_rows		rec_per_key;
        ha_rows		records;
		
		// records-per-key statistics depend on the number of table records
		if ( isVariableStats )
		{
			// Caller requested the variable statistics as well
			records					= stats.records;
		}
		else
		{
			// Caller did not request the variable statistics, so get the number of table records now
			records					= ( ha_rows )	SDBGetTableStats( sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_, SDB_STATS_INFO_FILE_RECORDS );
		}

        for ( uint i = 0; i < table->s->keys; i++ )
		{
			KEY*			pKey			= table->key_info + i;
			char to[512];
			uint n=tablename_to_filename(table->s->table_name.str, to, sizeof(to));					
			char*			pszDesignator	= SDBUtilFindDesignatorName( to, pKey->name, i, true, sdbDesignatorName_, SDB_MAX_NAME_LENGTH);
			unsigned short	idDesignator	= SDBGetIndexTableNumberByName( sdbDbId_, pszDesignator );

			if ( !idDesignator )
			{
				SDBTerminateEngine( -1, "Table contains fewer indexes in ScaleDB than are defined in MySQL", __FILE__, __LINE__ );
			}

			for ( uint j = 0; j < table->key_info[ i ].user_defined_key_parts; j++ )

			{
			
				// Get number of distinct key values for this key part
				if ( isFactTable ) {
					// on fact table we don't have avarge estimate - we just pass a fix number and use the record in range 
					if (GET_DESIGNATOR_NUMBER(idDesignator) == rangeDesignator) {
						rec_per_key				= 1;
					}
					else {
						rec_per_key				= 2;
					}
				}
				else {
					ha_rows		nDistinctKeys	= ( ha_rows ) SDBGetTableStats( sdbUserId_, sdbDbId_, idDesignator, sdbPartitionId_, SDB_STATS_INFO_FILE_DISTINCT_KEYS, j );

					if ( !nDistinctKeys )
					{
						// The current transaction inserted rows - we treat each row as a uniuqe key - to get an upper bound on the current number of uniuqe keys 
						rec_per_key				= records;
					}
					else
					{
						rec_per_key				= records / nDistinctKeys;
					}

					/*	Since MySQL seems to favor table scans
					too much over index searches, we pretend
					index selectivity is 2 times better than
					our estimate: */

					rec_per_key					= rec_per_key / 2;
				}

				if ( !rec_per_key )
				{
					rec_per_key				= 1;
				}
				else if ( rec_per_key	   >= ~( ulong ) 0 )
				{
					rec_per_key				= ~( ulong ) 0;
				}

				if ( j					   && ( ( ulong ) rec_per_key > pKey->rec_per_key[ j - 1 ] ) )
				{
					SDBTerminateEngine( 10, "ha_scaledb::info - More records per key for the current key part than for the previous key part", __FILE__,__LINE__ );
				}

				pKey->rec_per_key[ j ]		= ( ulong ) rec_per_key;
			}
        }

		isConstStats						= true;
    }
        
	

	if (flag & HA_STATUS_TIME) {
		//TODO: store this to fileinfo; and update only when it is not set
		char path[FN_REFLEN];
		my_snprintf(path, sizeof(path), "%s%s", share->table_name, reg_ext);
		unpack_filename(path, path);
		struct stat statinfo;
		if (!stat(path, &statinfo))
			stats.create_time = (ulong) statinfo.st_ctime;

		isTimeStats							= true;
	}

	if ( flag & HA_STATUS_AUTO )
	{
		if ( table->found_next_number_field && pSdbMysqlTxn_ )
		{
			stats.auto_increment_value = SDBGetAutoIncrValue(sdbDbId_, sdbTableNumber_);
		}
		else
		{
			stats.auto_increment_value = 0;
		}

		isAutoStats							= true;
	}

	DBUG_RETURN(0);
}

// store_lock method can modify the lock level.
// Before adding the lock into the table lock handler (or before external_lock call), mysqld calls 
// this method with the requested locks.  Hence we should not save any ha_scaledb member variables.
// When releasing locks, this method is not called.  MySQL calls external_lock to release locks.  
THR_LOCK_DATA **ha_scaledb::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) {
#ifdef SDB_DEBUG_LIGHT

	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::store_lock(thd=");
		SDBDebugPrint8ByteUnsignedLong((uint64) thd);
		SDBDebugPrintString(")");
		if (mysqlInterfaceDebugLevel_ > 1) {
			SDBDebugPrintString(", handler=");
			SDBDebugPrint8ByteUnsignedLong((uint64) this);
			SDBDebugPrintString(", Query:");
			SDBDebugPrintString(thd->query());
		}
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	// We save transaction isolation level here for a given user thread (save into User object).
	// Note that transaction isolation level is set for a user, not for a table handler.
	placeSdbMysqlTxnInfo(thd);
	if (lock_type != TL_IGNORE) {
		enum_tx_isolation level = (enum_tx_isolation) thd_tx_isolation(thd);
		switch (level) {
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

		if(lock_type==TL_WRITE_DELAYED)
		{
			is_delayed_insert=true;
		}
		
		if (lock_type == TL_READ && sqlCommand == SQLCOM_LOCK_TABLES) {
			// In processing LOCK TABLES ... READ LOCAL, we treat READ LOCAL equivalent to READ. 
			// This shows same behavior as myisam and innodb

			lock_type = TL_READ_NO_INSERT; // equivalent to table level read lock
		}

		// We allow concurrent writes if a thread is not running LOCK TABLE, DISCARD/IMPORT TABLESPACE or TRUNCATE TABLE.
		// Note that ALTER TABLE uses a TL_WRITE_ALLOW_READ	< TL_WRITE_CONCURRENT_INSERT.

		// We allow concurrent writes if MySQL is at the start of a stored procedure call (SQLCOM_CALL) or in a
		// stored function call (MySQL has set thd_sql_command(thd) to true).

		if ((lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE) && !(thdInLockTables
		        && sqlCommand == SQLCOM_LOCK_TABLES)
		//&& !thd_tablespace_op(thd)	// We do not support tablespace yet
		        && sqlCommand != SQLCOM_TRUNCATE && sqlCommand != SQLCOM_OPTIMIZE && sqlCommand
		        != SQLCOM_CREATE_TABLE) {

			lock_type = TL_WRITE_ALLOW_WRITE;
		}

		// In processing INSERT INTO t1 SELECT ... FROM t2 ..., MySQL would use the lock TL_READ_NO_INSERT on t2.
		// This imposes a table level read lock on t2.  This is consisten with our design.  No need to change lock type.

		// We allow concurrent writes if MySQL is at the start of a stored procedure call (SQLCOM_CALL).
		// Also MySQL sets thd_in_lock_tables() true. We need to change to normal read for this case.

		if ((lock_type == TL_READ_NO_INSERT) && (sqlCommand != SQLCOM_INSERT_SELECT)
		        && (thdInLockTables == false)) {

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
	char* pEngine = SDBUtilStrstrCaseInsensitive(sqlStatement, (char*) "engine");
	char* pScaledb = SDBUtilStrstrCaseInsensitive(sqlStatement, (char*) "scaledb");
	if ((pEngine) && (pScaledb) && (pEngine > sqlStatement) && (pScaledb > pEngine))
		retValue = true;

	return retValue;
}


#ifdef _HIDDEN_DIMENSION_TABLE // UTIL FUNC IMPLEMENATION 

char * ha_scaledb::getDimensionTablePKName(char* table_name, char* col_name, char* dimension_pk_name) {
	strcat(getDimensionTableName(table_name, col_name,dimension_pk_name),"__primary_0");
	return dimension_pk_name;
}


char * ha_scaledb::getDimensionTableName(char* table_name, char* col_name, char* dimension_table_name)
{
	strcpy(dimension_table_name,DIMENSION_TABLE_PREFIX);
	strcat(dimension_table_name,table_name);
	strcat(dimension_table_name,"_");
	strcat(dimension_table_name,col_name);
	strcat(dimension_table_name,"_1");
	return dimension_table_name;
}




int ha_scaledb::parseTableOptions( THD *thd, TABLE * table, HA_CREATE_INFO *create_info, bool& streamTable, bool& dimensionTable, unsigned long long&  dimensionSize,
								   char** pSystemRangeKey, char** pUserRangeKey, int* pUserRangeStart, unsigned short* pUserRangeInterval, bool create_from_like )
{
	bool coded_range_start=false;
	isStreamingTable_=ST_UNKNOWN;
	engine_option_value* opt=create_info->option_list;

	while(opt)
	{
		if ( SDBUtilCompareStrings( opt->name.str, "TABLE_TYPE", true ) && ( opt->parsed == true ) )
		{

			if ( SDBUtilCompareStrings( opt->value.str, "STREAMING", true ) )
			{
				isStreamingTable_=ST_TRUE;
				streamTable=true;
			}
			if ( SDBUtilCompareStrings( opt->value.str, "OLTP", true ) )
			{
				isStreamingTable_=ST_FALSE;
				streamTable=false;
			}

		}
		
		opt=opt->next;
	}
	
	if(isStreamingTable_==ST_UNKNOWN)
	{
		//only allow OLTP or STREAMING tables
		SDBSetErrorMessage( sdbUserId_, CREATE_TABLE_FAILED_IN_CLUSTER, "- ScaleDB only supports Streaming Tables (TABLE_TYPE=STREAMING)." );
		return convertToMysqlErrorCode( HA_ERR_GENERIC );
	}

	bool range_interval_set=false;
	int n=table->s->keys;
	bool contains_streaming_key=false;
	bool is_primary=false;


	for(int i=0;i<n; i++)
	{
		KEY _key= table->s->key_info[i];
		is_primary=false;

		if(SDBUtilCompareStrings((char*)_key.name,(char*)"PRIMARY",true))
		{
			contains_streaming_key=true;
			is_primary=true;
		}

		opt=_key.option_list;


		while(opt)
		{

			////////

			if ( SDBUtilCompareStrings( opt->name.str, "RANGE_KEY", true ) && ( opt->parsed == true ) )
			{

				if ( is_primary )
				{
					//invalid RANGE_KEY option, return error
					SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- PRIMARY KEYS cannot be RANGE_KEYS." );
					return convertToMysqlErrorCode( HA_ERR_GENERIC );
				}

				if ( SDBUtilCompareStrings( opt->value.str, "SYSTEM", true ) )
				{
					if ( pSystemRangeKey != NULL )
					{
						if ( *pSystemRangeKey != NULL )
						{
							// Too many system range keys
							SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- A streaming table requires exactly one SYSTEM RANGE_KEY." );
							return convertToMysqlErrorCode( HA_ERR_GENERIC );
						}

						if ( streamTable==false )
						{

							SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "-  SYSTEM RANGE_KEY can only be created on streaming tables." );
							return convertToMysqlErrorCode( HA_ERR_GENERIC );
						}

						//lookup the column name
						const char* col_name= _key.key_part->field->field_name;
						*pSystemRangeKey	= ( char* ) col_name;
					}
				}
				else if ( SDBUtilCompareStrings( opt->value.str, "USER", true ) )
				{
					if ( pUserRangeKey != NULL )
					{
						if ( *pUserRangeKey != NULL )
						{
							// Too many user range keys
							SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- A streaming table may have at most one USER RANGE_KEY." );
							return convertToMysqlErrorCode( HA_ERR_GENERIC );
						}

						if ( streamTable==false )
						{

							SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "-  USER RANGE_KEY can only be created on streaming tables." );
							return convertToMysqlErrorCode( HA_ERR_GENERIC );
						}
						//lookup the column name
						const char* col_name= _key.key_part->field->field_name;
						*pUserRangeKey		= ( char* ) col_name;
					}
				}
				else
				{
					//invalid RANGE_KEY option, return error
					SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- Invalid RANGE_KEY type (only USER and SYSTEM RANGE_KEYS supported)." );
					return convertToMysqlErrorCode( HA_ERR_GENERIC );
				}

			}
		
			
			if ( SDBUtilCompareStrings( opt->name.str, "RANGE_START", true ) && ( opt->parsed == true ) )
			{
				if ( *pUserRangeKey == NULL )
				{
					SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- RANGE_START can only be set on a User RANGE_KEY." );
					return convertToMysqlErrorCode(HA_ERR_GENERIC);
				}

				if ( pUserRangeStart			   != NULL )
				{
					char* start_time=opt->value.str;

					// Verify that the value is a time string in the correct format
					struct tm			t			= { 0 };
					int					year		= 0;
					int					rc			= sscanf( start_time, "%04d-%02d-%02d", &year, &t.tm_mon, &t.tm_mday );

					if ( rc						   == 3 )
					{
						// Time string in timestamp format
						if ( *pUserRangeInterval   == SI_CODED )
						{
							SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- For RANGE_INTERVAL=CODED, RANGE_START must be a positive integer value." );
							return convertToMysqlErrorCode( HA_ERR_GENERIC );
						}

						const char*		pString		= start_time;
						unsigned int	stringSize	= ( unsigned int ) strlen( start_time );
						THD*			pMysqlThd	= ha_thd();
						MYSQL_TIME		myTime;
						long			timestamp;
						unsigned int	uiError;

						// Convert the time string to a time value
						if ( convertStringToTime( pMysqlThd, pString, stringSize, MYSQL_TYPE_TIMESTAMP, &myTime, false ) )
						{
							// Convert the time value to a timestamp value
							timestamp				= TIME_to_timestamp( pMysqlThd, &myTime, &uiError );

							if ( ( uiError		   == ER_WARN_DATA_OUT_OF_RANGE ) || ( timestamp <= 0 ) )
							{
								// Converted timestamp value is less than or equal to 0
								char buf[ 1000 ];
								char pszTimestamp[ 20 ];

								thd_gmt_sec_to_TIME( pMysqlThd, &myTime, 1 );

								my_TIME_to_str( &myTime, pszTimestamp, 0 );

								sprintf( buf, "- RANGE_START '%s' is less than the minimum for your time zone '%s'. ",
										 pString, pszTimestamp );

								SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, buf );

								return convertToMysqlErrorCode( HA_ERR_GENERIC );
							}
							else if ( uiError )
							{
								// Invalid format for conversion to a timestamp value
								char buf[ 1000 ];
								sprintf( buf, "- Failed to convert RANGE_START '%s' to a timestamp. ", pString );
								SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, buf );

								return convertToMysqlErrorCode( HA_ERR_GENERIC );
							}

							*pUserRangeStart		= ( int ) timestamp;
						}
						else
						{
							// Time value conversion error
							char buf[ 1000 ];
							sprintf( buf, "- Failed to convert RANGE_START '%s' to a numeric time value. ", pString );
							SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, buf );

							return convertToMysqlErrorCode( HA_ERR_GENERIC );
						}
					}
					else
					{
						//might be for a coded type
						int					coded_time	= 0;
						int					rc			= sscanf( start_time, "%d", &coded_time );

						if ( rc						   == 1 )
						{
							if ( coded_time			   <= 0 )
							{
								SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- For RANGE_INTERVAL=CODED, RANGE_START must be a positive integer value." );
								return convertToMysqlErrorCode( HA_ERR_GENERIC );
							}

							coded_range_start		= true;
							*pUserRangeStart		= coded_time;
						}
						else
						{
							// Invalid time string format
							SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- Invalid RANGE_START format. Should be yyyy-mm-dd for time based RANGE_INTERVALS, or an integer for CODED." );
							return convertToMysqlErrorCode( HA_ERR_GENERIC );
						}
					}



				}
			}
			
			if( isStreamingTable_==ST_TRUE &&contains_streaming_key==false) 
			{
				SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- STREAMING TABLE must contain a PRIMARY KEY" );
				return convertToMysqlErrorCode(HA_ERR_GENERIC);
			}

			if ( SDBUtilCompareStrings( opt->name.str, "RANGE_INTERVAL", true ) && ( opt->parsed == true ) )
			{
				range_interval_set=true;

				if ( *pUserRangeKey == NULL )
				{
					SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- RANGE_INTERVAL can only be set on a User RANGE_KEY." );
					return convertToMysqlErrorCode(HA_ERR_GENERIC);
				}

				if ( pUserRangeInterval				   != NULL )
				{
					char* interval=opt->value.str;

					if(strcasecmp(interval,"CODED")==0)
					{
						*pUserRangeInterval				= SI_CODED;
						if ( !coded_range_start )
						{
							*pUserRangeStart			= 1;	// for coded interval the default start value is 1
						}
					}
					else if(strcasecmp(interval,"SECOND")==0)
					{
						*pUserRangeInterval				= SI_SECOND;
					}
					else if(strcasecmp(interval,"MINUTE")==0)
					{
						*pUserRangeInterval				= SI_MINUTE;
					}else if(strcasecmp(interval,"HOUR")==0)
					{
						*pUserRangeInterval				= SI_HOUR;
					}else if (strcasecmp(interval,"DAY")==0)
					{
						*pUserRangeInterval				= SI_DAY;
					}else if (strcasecmp(interval,"WEEK")==0)
					{
						*pUserRangeInterval				= SI_WEEK;
					}else 
					{
						SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- Unsupported Range Interval." );
						return convertToMysqlErrorCode(HA_ERR_GENERIC);
					}


					//support following, SECOND,MINUTE, HOUR, DAY, WEEK, MONTH, YEAR 


				}
			}



			opt=opt->next;
		}


		if ( pUserRangeKey != NULL )
		{
			if ( *pUserRangeKey != NULL && coded_range_start == true && *pUserRangeInterval != SI_CODED )
			{
				// Invalid time string format
				SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- Invalid RANGE_START format for Time based RANGE_INTERVALs. Should be yyyy-mm-dd" );

				return convertToMysqlErrorCode( HA_ERR_GENERIC );
			}
		}


	}


	return 0;
}

int ha_scaledb::create_dimension_table(TABLE *fact_table_arg,char * col_name, unsigned char col_type, unsigned short col_size, unsigned long long  hash_size,  unsigned short ddlFlag, unsigned short tableCharSet, SdbDynamicArray * fkInfoArray ) 
{
	int retValue;
	char    _pDimensionTableName[255];
	char    _pDimensionPKName[255];
	char    _pFactFKName[255];
	char *  _pDimensionColName = col_name;
	char to[512];
	uint n=tablename_to_filename( fact_table_arg->s->table_name.str, to, sizeof(to));
	char *  _pFactTableName = to;
	char *  _pkeyFields[2] = {NULL ,NULL};
	unsigned short keySizes[2] = {0    ,0   };
	char    _pKeysStr[255];
	int keyNum;

	DBUG_ENTER("ha_scaledb::create_dimension_table");
	getDimensionTableName(_pFactTableName, col_name, _pDimensionTableName);
	getDimensionTablePKName(_pFactTableName,col_name, _pDimensionPKName);
	// 1. create_dimension_table in meta info 
	unsigned short dimensionTableNumber = SDBCreateTable(sdbUserId_, sdbDbId_, _pDimensionTableName,
		true, _pDimensionTableName, _pDimensionTableName, false,
		false, true, true,hash_size,tableCharSet,ddlFlag);
	 
	if (dimensionTableNumber == 0) { // createTable fails, need to rollback
		SDBRollBack(sdbUserId_, NULL, 0, true);
		DBUG_RETURN( HA_ERR_GENERIC);
	}
	dimension_list.push_back(dimensionTableNumber);
	// 2. Add single field to the dimension_table - this field is the primary key (foreign key in the fact table )
	int _sdbFieldId = SDBCreateField(sdbUserId_, sdbDbId_, dimensionTableNumber,
		_pDimensionColName, col_type, col_size, 0, NULL,
		false, 0, 2, NULL, true, false, true,false,0,0);


	// 3. Add primary key index on the field 
	_pkeyFields[0] = col_name;
	  keySizes[0]  = col_size;
	retValue = SDBCreateIndex(sdbUserId_, sdbDbId_, dimensionTableNumber,
		(char*)_pDimensionPKName, _pkeyFields, keySizes, true, false, NULL, 0, 0,
		INDEX_TYPE_IMPLICIT);

///


	// 6. Define FK for the fact table  
	strcpy(_pFactFKName,col_name);
	retValue = SDBDefineForeignKey(sdbUserId_, sdbDbId_, _pFactTableName, _pDimensionTableName, _pFactFKName, _pkeyFields, _pkeyFields);
	if (retValue)
	{	
		DBUG_RETURN(convertToMysqlErrorCode(CREATE_TABLE_FAILED_IN_CLUSTER));
	}

	// 7. Add FK Info to list - to create Index 
	MysqlForeignKey* pKeyI = new MysqlForeignKey();
	pKeyI->setForeignKeyName(_pFactFKName);
	keyNum = pKeyI->setKeyNumber(fact_table_arg->key_info, (int) fact_table_arg->s->keys, _pKeysStr);
	if (keyNum == -1) {
		DBUG_RETURN(METAINFO_WRONG_FOREIGN_FIELD_NAME);
	}
	pKeyI->setParentColumnNames(_pDimensionColName);
	pKeyI->setParentTableName(_pDimensionTableName);
	strcpy(_pKeysStr,_pDimensionColName);strcat(_pKeysStr,")");
	

	SDBArrayPutPtr(fkInfoArray, keyNum + 1, pKeyI);
	DBUG_RETURN( 0);
}

int ha_scaledb::getSDBType(Field* pField, enum_field_types fieldType,  unsigned char& sdbFieldType, unsigned short& sdbMaxDataLength, unsigned short& sdbFieldSize, unsigned short&  dataLength) 
{
	const char* charsetName;

	switch (fieldType) {
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

		case MYSQL_TYPE_FLOAT: // FLOAT is treated as a 4-byte number
			if (pField->flags & UNSIGNED_FLAG)
				sdbFieldType = ENGINE_TYPE_U_NUMBER;
			else
				sdbFieldType = ENGINE_TYPE_S_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_FLOAT;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_DOUBLE: // DOUBLE is treated as a 8-byte number
			if (pField->flags & UNSIGNED_FLAG)
				sdbFieldType = ENGINE_TYPE_U_NUMBER;
			else
				sdbFieldType = ENGINE_TYPE_S_NUMBER;
			sdbFieldSize = ENGINE_TYPE_SIZE_OF_DOUBLE;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_BIT: // BIT is treated as a byte array.  Its length is (M+7)/8
			sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			sdbFieldSize = pField->pack_length();
			//			sdbFieldSize = ((Field_bit*) pField)->pack_length();	// same effect as last statement (due to polymorphism)
			sdbMaxDataLength = 0;
			break;

			// In MySQL 5.1.42, MySQL treats SET as string during create table, insert, and select.
		case MYSQL_TYPE_SET: // SET is treated as a non-negative integer.  Its length is up to 8 bytes
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = ((Field_set*) pField)->pack_length();
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_DATE: // DATE is treated as a non-negative 3-byte integer
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_DATE;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_TIME: // TIME is treated as a non-negative 3-byte integer
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_TIME;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_DATETIME: // DATETIME is treated as a non-negative 8-byte integer
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_DATETIME;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_TIMESTAMP: // TIMESTAMP is treated as a non-negative 4-byte integer
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			if(dataLength>4)
			{
				sdbFieldSize = SDB_SIZE_OF_DATETIME;
			}
			else
			{
				sdbFieldSize = SDB_SIZE_OF_TIMESTAMP;
			}
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_YEAR:
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_TINYINT;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_NEWDECIMAL: // treat decimal as a fixed-length byte array
			sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			sdbFieldSize = ((Field_new_decimal*) pField)->bin_size;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_ENUM:
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = ((Field_enum*) pField)->pack_length(); // TBD: need validation
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_STRING: // can be CHAR or BINARY data type
			// if the character set name contains the string "bin" we infer that this is a binary string
			charsetName = pField->charset()->name;
			charsetName = strstr(charsetName, "bin");
			if (pField->binary()) {
				sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			} else if (charsetName) {
				sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			} else {
				sdbFieldType = ENGINE_TYPE_STRING_CHAR;
			}
			// do NOT use pField->field_length as it may be too big
			sdbFieldSize = pField->pack_length(); // exact size used in RAM
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_TINY_BLOB: // tiny	blob applies to TEXT as well
			if (pField->binary())
				sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			else {
				sdbFieldType = ENGINE_TYPE_STRING_VAR_CHAR;
	//			reduceSizeOfString = true;
			}
	//		keyFieldLength = sdbFieldSize = get_field_key_participation_length(thd, table_arg, pField);// if this is a key field - get the key length
	//		if (keyFieldLength){
	//			// for a key field - use the length of the key
	//			sdbFieldSize = keyFieldLength;

	//		}else{
	//			sdbFieldSize = 0;		// this is a variable field
	//		}
			sdbMaxDataLength = pField->field_length;
			break;

		case MYSQL_TYPE_BLOB: // These 3 data types apply to TEXT as well
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
			if (pField->binary())
				sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			else
				sdbFieldType = ENGINE_TYPE_STRING_VAR_CHAR;

			// We parse through the index keys, based on the participation of this field in the
			// keys we decide how much of the field we want to store as part of the record and 
			// how much in the overflow file. If field does not participate in any index then
			// we do not store anything as part of the record.

			// For a blob - the data is kept in the OVF file (sdbFieldSize is 0).
			//	If the blob data is a key, we keep the key portion in the table data.


		//	sdbFieldSize = get_field_key_participation_length(thd, table_arg, pField);
			sdbMaxDataLength = pField->field_length;
			break;

		case MYSQL_TYPE_GEOMETRY:
			if ( pField->get_geometry_type() == pField->GEOM_POINT )
			{
				// Point values are stored in the row as they are fixed length and relatively short.
				// When it is an indexed column, the entire value is indexed.
				sdbFieldType		= ENGINE_TYPE_BYTE_ARRAY;
				sdbFieldSize		= SRID_SIZE + WKB_HEADER_SIZE + POINT_DATA_SIZE;
				sdbMaxDataLength	= 0;		// max data length has value only with variable length.
				break;
			}
			// Fall through: Other geometric data types are not supported

		default:
			sdbFieldType = 0;
#ifdef SDB_DEBUG_LIGHT
			SDBDebugStart(); // synchronize threads printout
			SDBDebugPrintString("\0This data type is not supported yet.\0");
			SDBDebugEnd(); // synchronize threads printout
#endif
			return HA_ERR_UNSUPPORTED;
			break;
		}
		return 0;
}

int ha_scaledb::create_multi_dimension_table(TABLE *fact_table_arg, char* index_name, KEY_PART_INFO* hash_key, int key_parts, unsigned long long  hash_size,  unsigned short ddlFlag, unsigned short tableCharSet, SdbDynamicArray * fkInfoArray )
{
	int retValue;
	char    _pDimensionTableName[255];
	char    _pDimensionPKName[255];
	char    _pFactFKName[255];
	char   _pDimensionColNames[1000];
	_pDimensionColNames[0]=0;
	char to[512];
	uint n=tablename_to_filename( fact_table_arg->s->table_name.str, to, sizeof(to));
	char *  _pFactTableName = to;
	char    _pKeysStr[255];
	int keyNum;

	DBUG_ENTER("ha_scaledb::create_multi_dimension_table");

	char ** _pkeyFields = new char*[key_parts+1];
	unsigned short* keySizes = new unsigned short[key_parts+1] ;
	getDimensionTableName(_pFactTableName, index_name, _pDimensionTableName);
	getDimensionTablePKName(_pFactTableName,index_name, _pDimensionPKName);
	// 1. create_dimension_table in meta info 
	short dimensionTableNumber = SDBCreateTable(sdbUserId_, sdbDbId_, _pDimensionTableName,
		true, _pDimensionTableName, _pDimensionTableName, false,
		false, true, true,hash_size,tableCharSet,ddlFlag);
	 
	if (dimensionTableNumber == 0) { // createTable fails, need to rollback

		DBUG_RETURN( HA_ERR_GENERIC);
	}


	for(int i=0;i<key_parts;i++)
	{
		const char* col_name=hash_key[i].field->field_name;


		Field* pField =fact_table_arg->field[ hash_key[0].field->field_index];
		enum_field_types fieldType =pField->type();

		unsigned char sdbFieldType;
		unsigned short sdbMaxDataLength;
		unsigned short dataLength;
		unsigned short sdbFieldSize;
		
		int rc=getSDBType( pField, fieldType,  sdbFieldType, sdbMaxDataLength, sdbFieldSize, dataLength) ;
	    if(rc!=0)
		{
			DBUG_RETURN(convertToMysqlErrorCode(CREATE_TABLE_FAILED_IN_CLUSTER));
		}
		int col_type=sdbFieldType; //hash_key[i].type;
		int col_size= hash_key[i].length; //sdbMaxDataLength; //hash_key[i].length;

 	    _pkeyFields[i]=(char*)col_name;
		keySizes[i]=col_size;
		strcat(_pDimensionColNames,col_name);
		if(i<key_parts-1) {strcat(_pDimensionColNames,", ");} //dont add for last column
	// 2. Add ALL fields to the dimension_table - this field is the primary key (foreign key in the fact table )
		int _sdbFieldId = SDBCreateField(sdbUserId_, sdbDbId_, dimensionTableNumber,
		(char*)col_name, col_type, col_size, 0, NULL,
		false, 0, 2, NULL, true, false, true,false,0,0);

	}
	//termiate the arrays
	_pkeyFields[key_parts]=NULL;
	keySizes[key_parts]=0;

	// 3. Add primary key index for all fields 

	retValue = SDBCreateIndex(sdbUserId_, sdbDbId_, dimensionTableNumber,
		(char*)_pDimensionPKName, _pkeyFields, keySizes, true, false, NULL, 0, 0,
		INDEX_TYPE_IMPLICIT);



	// 5. Bug 1008:  we need to open and close after create.
	SDBOpenTable(sdbUserId_, sdbDbId_, _pDimensionTableName, 0, false);
	int errorNum = SDBCloseTable(sdbUserId_, sdbDbId_, _pDimensionTableName, 0, false, true, false, true);
	if (errorNum) {
		SDBRollBack(sdbUserId_, NULL, 0, false); // rollback new table record in transaction
		DBUG_RETURN(convertToMysqlErrorCode(CREATE_TABLE_FAILED_IN_CLUSTER));
	}

	// 6. Define FK for the fact table  
	strcpy(_pFactFKName,index_name);
	retValue = SDBDefineForeignKey(sdbUserId_, sdbDbId_, _pFactTableName, _pDimensionTableName, _pFactFKName, _pkeyFields, _pkeyFields);
	if (retValue)
	{	
		DBUG_RETURN(convertToMysqlErrorCode(CREATE_TABLE_FAILED_IN_CLUSTER));
	}

	// 7. Add FK Info to list - to create Index 
	MysqlForeignKey* pKeyI = new MysqlForeignKey();
	pKeyI->setForeignKeyName(_pFactFKName);
	keyNum = pKeyI->setKeyNumber(fact_table_arg->key_info, (int) fact_table_arg->s->keys, _pKeysStr);
	if (keyNum == -1) {
		DBUG_RETURN(METAINFO_WRONG_FOREIGN_FIELD_NAME);
	}

	char *  _pDimensionColName = index_name;


	pKeyI->setParentColumnNames(_pDimensionColNames);
	pKeyI->setParentTableName(_pDimensionTableName);
	strcpy(_pKeysStr,_pDimensionColName);strcat(_pKeysStr,")");
	

	SDBArrayPutPtr(fkInfoArray, keyNum + 1, pKeyI);

	

	DBUG_RETURN( 0);
}

#endif //_HIDDEN_DIMENSION_TABLE -  UTIL FUNC IMPLEMENATION 


/*
  Safe characters:
   '\0'  NULL
   A..Z  capital letters,
   a..z  small letters
   0..9  digits
   _     underscore
*/
static const char Filename_safe_char[128]=
{
  1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, /* ................ */
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, /* ................ */
  0,0,0,1,0,0,0,0,0,0,0,0,0,1,1,0, /*  !"#$%&'()*+,-./ */
  1,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0, /* 0123456789:;<=>? */
  1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, /* @ABCDEFGHIJKLMNO */
  1,1,1,1,1,1,1,1,1,1,1,0,0,0,0,1, /* PQRSTUVWXYZ[\]^_ */
  0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, /* `abcdefghijklmno */
  1,1,1,1,1,1,1,1,1,1,1,0,0,0,0,0, /* pqrstuvwxyz{|}~. */
};

bool ha_scaledb::isSafeFilename(char* s)
{
	bool safe_tablename=true;
	
	for(int i=0;i<strlen(s);i++)
	{
		if(s[i]>127 || Filename_safe_char[s[i]]==0)
		{
			safe_tablename=false;
			break;
		}
	}
	return safe_tablename;
}


void ha_scaledb::removeLocalInfo()
{

	SDBRemoveLocalTableInfo(sdbUserId_, sdbDbId_, sdbTableNumber_,false);
//there can be multiple dimension tables, need to remove them
	std::list<unsigned short>::iterator it;
	for (it=dimension_list.begin(); it!=dimension_list.end(); ++it)
	{
		SDBCloseTable(sdbUserId_, sdbDbId_, *it, sdbPartitionId_);
	}
	dimension_list.clear();	

	for (it=parent_list.begin(); it!=parent_list.end(); ++it)
	{
		int ret=SDBCloseTableAndChildrenTables(sdbUserId_, sdbDbId_,*it,false, false,false);

	}
	parent_list.clear();

}

/*
 Create a table. You do not want to leave the table open after a call to
 this (the database will call ::open if it needs to).
 Parameter name contains database name and table name that are file system compliant names.
 The user-defined table name is saved in table_arg->s->table_name.str
 */
int ha_scaledb::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info) {
	DBUG_ENTER("ha_scaledb::create");

#ifdef SDB_DEBUG
	debugHaSdb("create", name, NULL, table_arg);
#endif

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::create(name= ");
		SDBDebugPrintString((char*) name);
		SDBDebugPrintString(" )");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugFlush();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	unsigned int errorNum = 0;
	THD* thd = ha_thd();
	unsigned int sqlCommand = thd_sql_command(thd);
	placeSdbMysqlTxnInfo(thd);
	unsigned short ddlFlag = 0;


	SDBResetErrorMessage(sdbUserId_); //this is needed because oldwarnings/error will get returned if we dont reset

	// we don't support temp tables -- REJECT
	if (create_info->options & HA_LEX_CREATE_TMP_TABLE) {
		DBUG_RETURN( HA_ERR_UNSUPPORTED);
	}

	if (!table_arg) {
		SDBDebugPrintHeader("table_arg is NULL");
		SDBDebugFlush();
		DBUG_RETURN( HA_ERR_UNKNOWN_CHARSET);
	}

	if (!table_arg->s) {
		SDBDebugPrintString("table_arg->s is NULL");
		SDBDebugFlush();
		DBUG_RETURN( HA_ERR_UNKNOWN_CHARSET);
	}
	
	if (!table_arg->s->db.str) {
		SDBDebugPrintString("table_arg->s->db.str is NULL");
		SDBDebugFlush();
		DBUG_RETURN( HA_ERR_UNKNOWN_CHARSET);
	}

	if (!table_arg->s->table_name.str) {
		SDBDebugPrintString("table_arg->s->table_name.str is NULL");
		SDBDebugFlush();
		DBUG_RETURN( HA_ERR_UNKNOWN_CHARSET);
	}

	unsigned short tableCharSet = this->tableCharSet(create_info);
	if(tableCharSet == SDB_CHARSET_UNDEFINED) {
		DBUG_RETURN( HA_ERR_UNKNOWN_CHARSET);
	}

	char dbFsName[METAINFO_MAX_IDENTIFIER_SIZE]  = { 0 }; // database name that is compliant with file system
	char tblFsName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 }; // table name that is compliant with file system
	char pathName[METAINFO_MAX_IDENTIFIER_SIZE]  = { 0 };
	char partitionName[METAINFO_MAX_IDENTIFIER_SIZE]  = { 0 };

	// First we put db and table information into metadata memory
	fetchIdentifierName(name, dbFsName, tblFsName, pathName);

	char* pDbName = table_arg->s->db.str; // points to user-defined database name

	char* pTableName =tblFsName;


	if (sqlCommand == SQLCOM_ALTER_TABLE || sqlCommand==SQLCOM_CREATE_INDEX || sqlCommand==SQLCOM_DROP_INDEX)
	{
		char to[512];
		uint n=tablename_to_filename( tblFsName, to, sizeof(to));
		pTableName=to;
	}


	unsigned short numberOfPartitions = 0;
	bool bIsAlterTableStmt = false;
	
	

	if(!isSafeFilename(pTableName))
	{
//table name is not compatabile with file system , eg create table 't\4' , so going to fail it.
		SDBRollBack(sdbUserId_, NULL, 0, false);
		SDBSetErrorMessage( sdbUserId_, CREATE_TABLE_FAILED_IN_CLUSTER, "- table name is invalid." );
		DBUG_RETURN(convertToMysqlErrorCode(HA_ERR_GENERIC));
	}


	if (table_arg->part_info) {
#ifdef _MARIA_DB
		int num_sub_parts=table_arg->part_info->num_subparts;
		int num_parts=table_arg->part_info->num_parts;
#else
		int num_sub_parts=table_arg->part_info->no_subparts;
		int num_parts=table_arg->part_info->no_parts;
#endif
		if (num_sub_parts) {
			SDBSetErrorMessage( sdbUserId_, SCALEDB_GENERIC_ERROR, " Create table failed - ScaleDB does not support sub-partitions." );
			SDBRollBack(sdbUserId_, NULL, 0, true);
			DBUG_RETURN(convertToMysqlErrorCode(SCALEDB_GENERIC_ERROR));
		}

		getAndRemovePartitionName(tblFsName, partitionName);
		//only support 256 partitions now
		if (num_parts > SCALEDB_MAX_PARTITIONS) {
			SDBSetErrorMessage( sdbUserId_, SCALEDB_GENERIC_ERROR, " Create table failed - Maximum number of partitions exceeded.");
			SDBRollBack(sdbUserId_, NULL, 0, true);
			DBUG_RETURN(convertToMysqlErrorCode(SCALEDB_GENERIC_ERROR));
		}

	}

	virtualTableFlag_ = SDBTableIsVirtual(tblFsName);
	if (virtualTableFlag_) {
		DBUG_RETURN(0);
	}

	

	// Because MySQL calls this method outside of a normal user transaction,
	// hence we use a different user id to open database and table in order to avoid session lock issues.
	// need to do like ::open 
	// unsigned int userIdforOpen = SDBGetNewUserId();
	// for now we commit for free session lock only - TBD
	SDBCommit(sdbUserId_, true);


#ifdef SDB_DEBUG_LIGHT
   SDBLogSqlStmt(sdbUserId_, thd->query(), thd->query_id); // inform engine to log user query for DDL
#endif
	
	// Only the single node solution or the primary node of a cluster will proceed after this point.
	errorNum = ha_scaledb::openUserDatabase(pDbName, dbFsName, sdbDbId_,NULL,pSdbMysqlTxn_); 
	if (errorNum) {
		DBUG_RETURN(convertToMysqlErrorCode(errorNum));
	}

	SessionExclusiveMetaLock ot(sdbDbId_,sdbUserId_);
	if(ot.lock()==false)
	{
		DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
	}	
		
	char* pCreateTableStmt = (char*) ALLOCATE_MEMORY(thd->query_length() + 1, SOURCE_HA_SCALEDB, __LINE__);
	// convert LF to space, remove extra space, convert to lower case letters if needed.
	// MySQL variable lower_case_table_names defines if a table name is case sensitive.
	if (lower_case_table_names) {	// On Windows, table names are in lower case
		convertSeparatorLowerCase(pCreateTableStmt, thd->query(), true, thd->query_length());
	} else {	// On Linux, table names are case sensitive
		convertSeparatorLowerCase(pCreateTableStmt, thd->query(), false, thd->query_length());
	}
	bool streamTable=false;
	bool dimensionTable=false;
	unsigned long long  dimensionSize = 0;
	
	char*			pSystemRangeKey		= NULL;
	char*			pUserRangeKey		= NULL;

	int				userTimeStreamStart	= 0;
	unsigned short	userTimeUnit		= SI_NO_TYPE;
	bool create_from_like=false;

	if (sqlCommand == SQLCOM_CREATE_TABLE ) {

		char* pLike = SDBUtilSqlSubStrIgnoreComments(pCreateTableStmt, (char*) "like", true);
		if(pLike)
		{
			create_from_like=true;
		}
		char* pSelect = SDBUtilSqlSubStrIgnoreComments(pCreateTableStmt, (char*) "select ", true);
		if (!pSelect) {// DDL doesn't contains key word 'select'
			SDBCommit(sdbUserId_, false);	// bug hunting 1/23
			SDBStartTransaction(sdbUserId_);
			pSdbMysqlTxn_->setScaleDbTxnId(SDBGetTransactionIdForUser(sdbUserId_));
			pSdbMysqlTxn_->setActiveTrn(true);
		}

		if(!pSelect)
		{


			errorNum	= parseTableOptions( thd, table_arg, create_info, streamTable, dimensionTable, dimensionSize,
											 &pSystemRangeKey, &pUserRangeKey, &userTimeStreamStart, &userTimeUnit, create_from_like );


			if (errorNum) {
				SDBRollBack(sdbUserId_, NULL, 0, true); // rollback new table record in transaction
				FREE_MEMORY(pCreateTableStmt);
				DBUG_RETURN(errorNum);
			}
		}
		if ( streamTable )
		{
			// In a streaming table, when an index by the user time is used - the initial user time. The default value is the current time.
			if ( !userTimeStreamStart )
			{
				// Default is current timestamp, which is the table's time of creation
				THD*			pMysqlThd	= ha_thd();

				userTimeStreamStart			= getCurrentTimestamp( pMysqlThd );

				if ( !userTimeStreamStart )
				{
					// Timestamp conversion error
					userTimeStreamStart		= TIMESTAMP_1_1_2000;
				}
			}

			// In a streaming table, when an index by the user time is used - the user time intervals used by the index - seconds, minutes, hours, days, weeks etc.
			if ( userTimeUnit			   == SI_NO_TYPE )
			{
				// Default is day interval
				userTimeUnit				= SI_DAY;
			}
		}
		else
		{
			// Not a streaming table: use default values
			userTimeStreamStart				= TIMESTAMP_1_1_2000;
			userTimeUnit					= SI_DAY;
		}


#ifdef SDB_DEBUG_LIGHT
		if (mysqlInterfaceDebugLevel_) {
			SDBDebugStart(); // synchronize threads printout
			SDBDebugPrintHeader("In ha_scaledb::create, SDBLockMetaInfo retCode=");
			SDBDebugPrintInt(SUCCESS);
			if (mysqlInterfaceDebugLevel_ > 1)
				outputHandleAndThd();
			SDBDebugEnd(); // synchronize threads printout
		}
#endif

	} else {


		parseTableOptions( thd, table_arg, create_info, streamTable, dimensionTable, dimensionSize,
						   &pSystemRangeKey, &pUserRangeKey, &userTimeStreamStart, &userTimeUnit, create_from_like );


#ifdef ALLOW_STREAMING_ALTER
#else
		if(streamTable)
		{
				//alter not supported on streaming tables
				SDBRollBack(sdbUserId_, NULL, 0, true);
				FREE_MEMORY(pCreateTableStmt);
				SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- Alter Not supported on Streaming Tables." );
				DBUG_RETURN(convertToMysqlErrorCode(HA_ERR_GENERIC));
			}
#endif
		if (pSdbMysqlTxn_->getAlterTableName() == NULL) {
			// If the to-be-altered table is NOT a ScaleDB table,
			// for cluster, we return an error (Bug 934);
			SDBRollBack(sdbUserId_, NULL, 0, true);
			FREE_MEMORY(pCreateTableStmt);
			DBUG_RETURN( HA_ERR_UNSUPPORTED); // disallow this

		}
		pSdbMysqlTxn_->setOrOpDdlFlag((unsigned short) SDBFLAG_ALTER_TABLE_CREATE);
	

		// For ALTER TABLE, CREATE/DROP INDEX statements, primary node issues FLUSH TABLE statement so that
		// all other nodes can close the table to be altered.

		if ( SDBUtilStrstrCaseInsensitive(thd->query(), SCALEDB_ADD_PARTITION ) ||
				SDBUtilStrstrCaseInsensitive(thd->query(), SCALEDB_COALESCE_PARTITION)) {

			sdbTableNumber_ = SDBGetTableNumberByName(sdbUserId_, sdbDbId_, tblFsName);
			if (!sdbTableNumber_) {
				sdbTableNumber_ = SDBOpenTable(sdbUserId_, sdbDbId_, pTableName, sdbPartitionId_, true);
			}

				
			//add a new partition to existing table
			sdbPartitionId_ = SDBAddPartitions(sdbUserId_, sdbDbId_, sdbTableNumber_, partitionName, 0, false);
			SDBCommit(sdbUserId_, false);
			//open will add data,index file for the temp partition
			SDBOpenTable(sdbUserId_, sdbDbId_, pTableName, sdbPartitionId_, false); 
			SDBCommit(sdbUserId_, false);	// this commit is needed to release the table locks at level 2 on the tables - as in updateFrmData - the tables will be locked at level 3
			// update the lock 
			updateFrmData( tblFsName, sdbUserId_, sdbDbId_ );
			// commit FRM and ralease DDL lock
			SDBCommit(sdbUserId_, true);	
			//cleanup and return
			FREE_MEMORY(pCreateTableStmt);
			DBUG_RETURN(0);
		}

		bIsAlterTableStmt = true;

	}


	//on the first partition we created the table, now add files for respective partition
	//open the meta table to get the tablenumber, since on previous close during delete we removed the metainfo
	if (table_arg->part_info) {
		if ((sdbTableNumber_ = SDBOpenTable(sdbUserId_, sdbDbId_, pTableName, sdbPartitionId_, true))) {

			
			//add the partition into the partition meta table
			sdbPartitionId_ = SDBAddPartitions(sdbUserId_, sdbDbId_, sdbTableNumber_, partitionName, sdbPartitionId_, false);

			// In case alter table, we are here to add partition for the temp table.so no need to open
			// and close/commit the table,since it will be done by alter table
			if (bIsAlterTableStmt) {
				// unlock DDL
			        SDBCommit(sdbUserId_, true);
				FREE_MEMORY(pCreateTableStmt);
				DBUG_RETURN( 0);
			}

			//commit for create table only + unlock DDL
	SDBCommit(sdbUserId_, false);

			//open and close to create the files for each partition
			SDBOpenTable(sdbUserId_, sdbDbId_, pTableName, sdbPartitionId_, false); //protected by previous metalock

			//close the table
			SDBCloseTable(sdbUserId_, sdbDbId_, pTableName, sdbPartitionId_, false, true, false, true);
			
	SDBCommit(sdbUserId_, true);
			//cleanup and return
			FREE_MEMORY(pCreateTableStmt);
			DBUG_RETURN( 0);
		}
	}


	// find out if we will have overflows
	bool hasOverflow = has_overflow_fields(thd, table_arg);

	DDLInProgress ddl_in_progress(sdbUserId_);

	//temp put pTableName in tblFsName...
	sdbTableNumber_ = SDBCreateTable(sdbUserId_, sdbDbId_, pTableName,
			create_info->auto_increment_value, tblFsName, pTableName, virtualTableFlag_,
			hasOverflow, streamTable, dimensionTable,dimensionSize, tableCharSet, ddlFlag, userTimeStreamStart, userTimeUnit);


	if (sdbTableNumber_ == 0) { // createTable fails, need to rollback
		ddl_in_progress.complete();
		SDBRollBack(sdbUserId_, NULL, 0, true);
		removeLocalInfo();
		FREE_MEMORY(pCreateTableStmt);
		DBUG_RETURN( HA_ERR_GENERIC);
	}


	if (hasOverflow)
		SDBSetOverflowFlag(sdbDbId_, sdbTableNumber_, true); // this table will now have over flows.

	// Second we add column information to metadata memory
	int number_streaming_keys=0;
	int number_streaming_attributes=0;
	int numOfKeys = (int) table_arg->s->keys;
	// store info about foreign keys so the foreign key index can be created properly when designator is created
	SdbDynamicArray*	fkInfoArray	= SDBArrayInit( numOfKeys + 1, numOfKeys + 1, sizeof( void* ), false );	// +1 because we use the locations 1,2,3 rather than 0,1,2
	dimension_list.clear();
	errorNum = add_columns_to_table( thd, bIsAlterTableStmt, table_arg, ddlFlag, tableCharSet,
									 pSystemRangeKey, pUserRangeKey, number_streaming_keys, number_streaming_attributes, dimensionTable, fkInfoArray, create_from_like, userTimeUnit );
	if (errorNum) {
		ddl_in_progress.complete();
		SDBRollBack(sdbUserId_, NULL, 0, true); // rollback new table record in transaction
		removeLocalInfo();
		FREE_MEMORY(pCreateTableStmt);
		DBUG_RETURN(errorNum);
	}

	
	if (numOfKeys > 0) { // index/designator exists
		// we need to create the foreign key metadata before we work on index because we need
		// foreign key information to build multi-table index.


		errorNum = create_fks(thd, table_arg, pTableName, fkInfoArray, pCreateTableStmt, bIsAlterTableStmt);
		if (errorNum) {
			ddl_in_progress.complete();
			SDBRollBack(sdbUserId_, NULL, 0, true); // rollback new table record in transaction
			removeLocalInfo();
			FREE_MEMORY(pCreateTableStmt);
//the error message will get set in create_fks
			DBUG_RETURN(convertToMysqlErrorCode(HA_ERR_GENERIC));
		}

		errorNum = add_indexes_to_table( thd, table_arg, pTableName, ddlFlag, fkInfoArray, pCreateTableStmt );
		if (errorNum) {
			//if index create fails then the transaction will get rolled back, so dont check ddl_in_progress.complete() here
			ddl_in_progress.setComplete();
			SDBRollBack(sdbUserId_, NULL, 0, true); // rollback new table record in transaction
			removeLocalInfo();
			FREE_MEMORY(pCreateTableStmt);
			DBUG_RETURN(errorNum);
		}
	}

	for (unsigned short i = SDBArrayGetNextElementPosition(fkInfoArray, 0); i; i= SDBArrayGetNextElementPosition(fkInfoArray, i)) {
		MysqlForeignKey *ptr = (MysqlForeignKey *) SDBArrayGetPtr(fkInfoArray, i);
		delete ptr;
	}
	SDBArrayFree(fkInfoArray);


#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 1)
		SDBPrintStructure(sdbDbId_); // print metadata structure

	//if (ha_scaledb::mysqlInterfaceDebugLevel_ > 3) { // for debugging memory leak
	//	SDBDebugStart(); // synchronize threads printout
	//	SDBPrintMemoryInfo();
	//	SDBDebugEnd(); // synchronize threads printout
	//}
	if (mysqlInterfaceDebugLevel_ > 4) {
		// print user lock status on the primary node
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader(
				"In ha_scaledb::create, print user locks imposed by primary node before sending DDL ");
		SDBDebugEnd(); // synchronize threads printout
		SDBShowUserLockStatus(sdbUserId_);
	}
#endif

	//add partitions, if any..
	if (table_arg->part_info) {
		//add the partition into the partition meta table
		sdbPartitionId_ = SDBAddPartitions(sdbUserId_, sdbDbId_, sdbTableNumber_, partitionName, sdbPartitionId_, true);
	}

	if ( streamTable == true && pSystemRangeKey == NULL )
	{
		ddl_in_progress.complete();
		SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- Streaming table must contain a Range KEY." );
		SDBRollBack(sdbUserId_, NULL, 0, true); // rollback new table record in transaction
		removeLocalInfo();
		FREE_MEMORY(pCreateTableStmt);
		DBUG_RETURN(convertToMysqlErrorCode(HA_ERR_GENERIC));
	}


	if(streamTable==false && number_streaming_attributes>0)
	{
		ddl_in_progress.complete();
		SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- It is illegal to specify Streaming attributes for non-Streaming tables." );
		SDBRollBack(sdbUserId_, NULL, 0, true); // rollback new table record in transaction
		removeLocalInfo();
		FREE_MEMORY(pCreateTableStmt);
		DBUG_RETURN(convertToMysqlErrorCode(HA_ERR_GENERIC));
	}

	// pass the DDL statement to engine so that it can be propagated to other nodes.
	// We need to make sure it is a regular CREATE TABLE command because this method is called
	// when a user has ALTER TABLE or CREATE/DROP INDEX commands.
	// We also need to make sure that the scaledb hint is not found in the user query.
	
	if ((sqlCommand == SQLCOM_CREATE_TABLE) )                                     
	{


		//with the new discovery process, the engine is responsible for creating the FRM file.
		//the following code creates the create stmt that is required to generate the FRM.
		//this is necessary because the current SQL might not be a valid create table, could be a 
		//create from select or a create if dont exist which will fail the init_from_sql_statement_string

//donot allow creation of partition table with engine specified

		
		if(thd->lex->create_info.used_fields & HA_CREATE_USED_ENGINE && thd->work_part_info)
		{
			//we donot support ceation ofpartition tables when engine specified, so return an error.
			ddl_in_progress.complete();
			SDBRollBack(sdbUserId_, NULL, 0, true); // rollback new table record in transaction
			removeLocalInfo();
			FREE_MEMORY(pCreateTableStmt);
			SDBSetErrorMessage( sdbUserId_, CREATE_TABLE_FAILED_IN_CLUSTER, "- Partition tables only work with default storage engine (remove the engine specification)." );
			DBUG_RETURN(convertToMysqlErrorCode(HA_ERR_GENERIC));

		}
		int errorNum=SCALEDB_GENERIC_ERROR;	
		if(thd->lex->alter_info.create_list.elements>0)
		{
			//probably a create from select so init for m sql not supported, so generate sql and try again.
			errorNum=init_from_sql_statement_string(table_arg,thd, true,thd->query(), thd->query_length());		
		}
		if(errorNum!=SUCCESS)
		{
			//there is a problem with the create table, probably a create select into, try generating the SQL from the
			//table meta data.
			thd->clear_error();
			TABLE_LIST table_list;
			table_arg->s->tmp_table=NO_TMP_TABLE;
			table_list.init_one_table(STRING_WITH_LEN(pDbName), tblFsName, strlen(tblFsName), NULL, TL_WRITE);
			table_list.table=table_arg;
			String _buffer; 

			int rc=show_create_table(thd, &table_list, &_buffer,
                     NULL, WITHOUT_DB_NAME);	

		  	errorNum=table_arg->s->init_from_sql_statement_string(thd, true,_buffer.c_ptr(), _buffer.length());
		}

		if (errorNum) {
			ddl_in_progress.complete();
			SDBRollBack(sdbUserId_, NULL, 0, true); // rollback new table record in transaction
			removeLocalInfo();
			FREE_MEMORY(pCreateTableStmt);
			DBUG_RETURN(convertToMysqlErrorCode(CREATE_TABLE_FAILED_IN_CLUSTER));
		}		

		// commit the changes such that a different node would be able to read the updates.

		// Now we store  the CREATE TABLE frm 
			saveFrmData( tblFsName, sdbUserId_, sdbDbId_, sdbTableNumber_ );
	}



	ddl_in_progress.complete();
	// CREATE TABLE: Primary node needs to release lockMetaInfo here after all nodes finish processing.
	// ALTER TABLE: Primary node needs to release lockMetaInfo in the last step: delete_table .

	removeLocalInfo(); //all tables should be closed after the create.

	SDBCommit(sdbUserId_, true);

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 4) {
		// print user lock status on the primary node
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader(
			"In ha_scaledb::create, print user locks imposed by primary node after commit ");
		SDBDebugEnd(); // synchronize threads printout
		SDBShowUserLockStatus(sdbUserId_);
	}
#endif

	FREE_MEMORY(pCreateTableStmt);
	errorNum = convertToMysqlErrorCode(SUCCESS);
	
	//	SDBPrintStructure(sdbDbId_);

	DBUG_RETURN(errorNum);

}

//the following code has been copied directly from mariadb table.cpp, with only 1 change the
//sql_unusable_for_discovery check has been removed because it will cause
// failures in the following SQL
// create from where does not exist
// partition tables



int  ha_scaledb::init_from_sql_statement_string(TABLE *table_arg, THD *thd, bool write,
                                        const char *sql, size_t sql_length)
{
  ulonglong saved_mode= thd->variables.sql_mode;
  CHARSET_INFO *old_cs= thd->variables.character_set_client;
  Parser_state parser_state;
  bool error;
  char *sql_copy;
  handler *file;
  LEX *old_lex;
  Query_arena *arena, backup;
  LEX tmp_lex;
  KEY *unused1;
  uint unused2;
  handlerton *hton= plugin_hton(table_arg->s->db_plugin);
  LEX_CUSTRING frm= {0,0};

  DBUG_ENTER("TABLE_SHARE::init_from_sql_statement_string");

  /*
    Ouch. Parser may *change* the string it's working on.
    Currently (2013-02-26) it is used to permanently disable
    conditional comments.
    Anyway, let's copy the caller's string...
  */
  if (!(sql_copy= thd->strmake(sql, sql_length)))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  if ( parser_state.init( thd, sql_copy, ( unsigned int ) sql_length ) )
  {
	  DBUG_RETURN( HA_ERR_OUT_OF_MEM );
  }
  

   thd->variables.sql_mode= MODE_NO_ENGINE_SUBSTITUTION | MODE_NO_DIR_IN_CREATE | MODE_ANSI_QUOTES | thd->variables.sql_mode;

  thd->variables.character_set_client= system_charset_info;
  tmp_disable_binlog(thd);
  old_lex= thd->lex;
  thd->lex= &tmp_lex;

  arena= thd->stmt_arena;
  if (arena->is_conventional())
    arena= 0;
  else
    thd->set_n_backup_active_arena(arena, &backup);

  lex_start(thd);

  if ((error= parse_sql(thd, & parser_state, NULL) ))   //i have removed teh "suitable for discovery check" from the code to permit partitions, create from if don't exist etc
    goto ret;

  thd->lex->create_info.db_type= hton;

  if (table_arg->s->tabledef_version.str)
    thd->lex->create_info.tabledef_version= table_arg->s->tabledef_version;

  promote_first_timestamp_column(&thd->lex->alter_info.create_list);
  file= mysql_create_frm_image(thd, table_arg->s->db.str, table_arg->s->table_name.str,
                               &thd->lex->create_info, &thd->lex->alter_info,
                               C_ORDINARY_CREATE, &unused1, &unused2, &frm);
  error|= file == 0;
  delete file;

  if (frm.str)
  {
    table_arg->s->option_list= 0;             // cleanup existing options ...
    table_arg->s->option_struct= 0;           // ... if it's an assisted discovery
    error= table_arg->s->init_from_binary_frm_image(thd, write, frm.str, frm.length);
  }

ret:
  my_free(const_cast<uchar*>(frm.str));
  lex_end(thd->lex);
  thd->lex= old_lex;
  if (arena)
    thd->restore_active_arena(arena, &backup);
  reenable_binlog(thd);
  thd->variables.sql_mode= saved_mode;
  thd->variables.character_set_client= old_cs;
  if (thd->is_error() || error)
  {
    thd->clear_error();
    my_error(ER_SQL_DISCOVER_ERROR, MYF(0),
             plugin_name(table_arg->s->db_plugin)->str, table_arg->s->db.str, table_arg->s->table_name.str,
             sql_copy);
    DBUG_RETURN(HA_ERR_GENERIC);
  }
  DBUG_RETURN(0);
}


//create table will call this function to save frm file
int saveFrmData(const char* name, unsigned short userId, unsigned short dbId, unsigned short tableId) {

	DBUG_ENTER("savrFrmData");
	// Save frm data for this table
	uchar *data= NULL, *frmData= NULL;
	size_t length;

	uchar *data_par= NULL;
	size_t length_par;
	//read the frm info
#ifdef SDB_WINDOWS
	char frmFilePathName[80];
	strcpy(frmFilePathName, ".\\" );
	strcat(frmFilePathName, SDBGetDatabaseNameByNumber(dbId));
	strcat(frmFilePathName, "\\" );
	strcat(frmFilePathName, name);
#else
	char frmFilePathName[80];
	strcpy(frmFilePathName, "./" );
	strcat(frmFilePathName, SDBGetDatabaseNameByNumber(dbId));
	strcat(frmFilePathName, "/" );
	strcat(frmFilePathName, name);
#endif	

	if (readfrm(frmFilePathName, (const uchar**)&data, &length)) {

#ifdef SDB_DEBUG
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader(
				    "In ha_scaledb -- saveFrmData; can't read frm file: " );
		SDBDebugPrintString( frmFilePathName );
		SDBDebugEnd(); // synchronize threads printout
#endif
			DBUG_RETURN(1);
	}




	if (readpar(frmFilePathName, &data_par, &length_par)==0) {
	}



	//store into metadata
	SDBInsertFrmData(userId, dbId, tableId, (char *)data, (unsigned short)length, (char *)data_par, (unsigned short)length_par);


	if(data_par!=NULL)
	{
#ifdef _MARIA_DB
             	my_free(data_par);
#else
                my_free(data_par, MYF(0));
#endif
	}



	assert(data);
#ifdef _MARIA_DB
             	my_free(data);
#else
                my_free(data, MYF(0));
#endif
	
	DBUG_RETURN(0);
}

int updateFrmData(const char* name, unsigned int userId, unsigned short dbId) {
	int retCode;
	uchar *frmData= NULL;
	size_t frmLength;
	uchar *parData= NULL;
	size_t parLength;
	unsigned short tableId = SDBGetTableNumberByFileSystemName(userId, dbId, name);

#ifdef DISABLE_DISCOVER
		return(1);
#endif

#ifdef SDB_WINDOWS
	char frmFilePathName[80];
	strcpy(frmFilePathName, ".\\" );
	strcat(frmFilePathName, SDBGetDatabaseNameByNumber(dbId));
	strcat(frmFilePathName, "\\" );
	strcat(frmFilePathName, name);
#else
	char frmFilePathName[80];
	strcpy(frmFilePathName, "./" );
	strcat(frmFilePathName, SDBGetDatabaseNameByNumber(dbId));
	strcat(frmFilePathName, "/" );
	strcat(frmFilePathName, name);
#endif

	if (readfrm(frmFilePathName, (const uchar**)&frmData, &frmLength)) {

		return 1;
	}

	if (readpar(frmFilePathName, &parData, &parLength)==0) {
	}
	retCode = SDBDeleteFrmData( userId, dbId, tableId );
	retCode = SDBInsertFrmData( userId, dbId, tableId, ( char* ) frmData, ( unsigned short ) frmLength, ( char* ) parData,  ( unsigned short )  parLength );
	if(parData!=NULL)
	{
#ifdef _MARIA_DB
             	my_free(parData);
#else
                my_free(parData, MYF(0));
#endif
	}

#ifdef _MARIA_DB
             	my_free(frmData);
#else
                my_free(frmData, MYF(0));
#endif

	return 0;
}


int updateFrmData(const char* from_name, const char* name, unsigned int userId, unsigned short dbId) {
	int retCode;
	uchar *frmData= NULL;
	size_t frmLength;
	unsigned short tableId = SDBGetTableNumberByFileSystemName(userId, dbId, name);

#ifdef DISABLE_DISCOVER
		return(1);
#endif

#ifdef SDB_WINDOWS
	char frmFilePathName[80];
	strcpy(frmFilePathName, ".\\" );
	strcat(frmFilePathName, SDBGetDatabaseNameByNumber(dbId));
	strcat(frmFilePathName, "\\" );
	strcat(frmFilePathName, from_name);
#else
	char frmFilePathName[80];
	strcpy(frmFilePathName, "./" );
	strcat(frmFilePathName, SDBGetDatabaseNameByNumber(dbId));
	strcat(frmFilePathName, "/" );
	strcat(frmFilePathName, from_name);
#endif

	if (readfrm(frmFilePathName, (const uchar**)&frmData, &frmLength)) {

		return 1;
	}

	retCode = SDBDeleteFrmData( userId, dbId, tableId );
	retCode = SDBInsertFrmData( userId, dbId, tableId, ( char* ) frmData, ( unsigned short ) frmLength, NULL, 0 );

#ifdef _MARIA_DB
             	my_free(frmData);
#else
                my_free(frmData, MYF(0));
#endif
	
	return 0;
}

void deleteFrmData(const char* name, unsigned int userId, unsigned short dbId) {

	unsigned short tableId = SDBGetTableNumberByFileSystemName(userId, dbId, name);

#ifdef DISABLE_DISCOVER
	return;
#endif
	
	SDBDeleteFrmData(userId, dbId, tableId);
}


// add columns to a table, part of create table
int ha_scaledb::has_overflow_fields(THD* thd, TABLE *table_arg) {
	Field* pField;
	enum_field_types fieldType;
	unsigned short sdbFieldSize;
	unsigned int sdbMaxDataLength;

	for (unsigned short i = 0; i < (int) table_arg->s->fields; ++i) {
		pField = table_arg->field[i];
		fieldType = pField->type();
		switch (fieldType) {
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_TINY_BLOB: // tiny	blob applies to to TEXT as well
			sdbFieldSize = 0;
			sdbMaxDataLength = pField->field_length;

			if (sdbMaxDataLength > sdbFieldSize)
				return true; // this table will have overflows
			break;

		case MYSQL_TYPE_BLOB: // These 3 data types apply to to TEXT as well
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
			// We parse through the index keys, based on the participation of this field in the
			// keys we decide how much of the field we want to store as part of the record and 
			// how much in the overflow file. If field does not participate in any index then
			// we do not store anything as part of the record.
			sdbFieldSize = get_field_key_participation_length(thd, table_arg, pField);
			
			sdbFieldSize = 0;		// this is a var field

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
int ha_scaledb::add_columns_to_table( THD* thd, bool isAlter, TABLE *table_arg, unsigned short ddlFlag, unsigned short tableCharSet,
									  char* pSystemRangeKey, char* pUserRangeKey, int& number_streaming_keys, int& number_streaming_attributes,
									  bool dimensionTable, SdbDynamicArray* fkInfoArray, bool create_from_like, unsigned short userTimeUnit )
{
	bool reduceSizeOfString = false;
	Field* pField;
	enum_field_types fieldType;
	unsigned char sdbFieldType;
	unsigned short sdbFieldSize;
	unsigned short keyFieldLength;
	unsigned int sdbMaxDataLength;
	unsigned short sdbFieldId;
	unsigned short sdbError;
	unsigned short dataLength;
	
	bool isAutoIncrField;
	Field* pAutoIncrField = NULL;
	const char* charsetName = NULL;
	const char* streamingKey=NULL;
	set<string> hashkeys;
	if (table_arg->s->found_next_number_field) // does it have an auto_increment field?
		pAutoIncrField = table_arg->s->found_next_number_field[0]; // points to auto_increment field

	for (unsigned short i = 0; i < (int) table_arg->s->fields; ++i) {
		reduceSizeOfString = false;
		isAutoIncrField = false;
		sdbFieldId =0;
		sdbFieldType =0;
	




		if (pAutoIncrField) { // check to see if this table has an auto_increment column
			if (pAutoIncrField->field_index == i)
				isAutoIncrField = true;
		}

		pField = table_arg->field[i];
		dataLength=pField->data_length();
		fieldType = pField->type();
		switch (fieldType) {
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

		case MYSQL_TYPE_FLOAT: // FLOAT is treated as a 4-byte number
			if (pField->flags & UNSIGNED_FLAG)
				sdbFieldType = ENGINE_TYPE_U_NUMBER;
			else
				sdbFieldType = ENGINE_TYPE_S_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_FLOAT;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_DOUBLE: // DOUBLE is treated as a 8-byte number
			if (pField->flags & UNSIGNED_FLAG)
				sdbFieldType = ENGINE_TYPE_U_NUMBER;
			else
				sdbFieldType = ENGINE_TYPE_S_NUMBER;
			sdbFieldSize = ENGINE_TYPE_SIZE_OF_DOUBLE;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_BIT: // BIT is treated as a byte array.  Its length is (M+7)/8
			sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			sdbFieldSize = pField->pack_length();
			//			sdbFieldSize = ((Field_bit*) pField)->pack_length();	// same effect as last statement (due to polymorphism)
			sdbMaxDataLength = 0;
			break;

			// In MySQL 5.1.42, MySQL treats SET as string during create table, insert, and select.
		case MYSQL_TYPE_SET: // SET is treated as a non-negative integer.  Its length is up to 8 bytes
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = ((Field_set*) pField)->pack_length();
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_DATE: // DATE is treated as a non-negative 3-byte integer		
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_DATE;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_TIME: // TIME is treated as a non-negative 3-byte integer
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
				if(dataLength>4)
			{
				sdbFieldSize = SDB_SIZE_OF_DATETIME;
			}
			else
			{
				sdbFieldSize = SDB_SIZE_OF_TIMESTAMP;
			}
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_DATETIME: // DATETIME is treated as a non-negative 8-byte integer
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_DATETIME;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_TIMESTAMP: // TIMESTAMP is treated as a non-negative 4-byte integer
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			if(dataLength>4)
			{
				sdbFieldSize = SDB_SIZE_OF_DATETIME;
			}
			else
			{
				sdbFieldSize = SDB_SIZE_OF_TIMESTAMP;
			}
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_YEAR:
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = SDB_SIZE_OF_TINYINT;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_NEWDECIMAL: // treat decimal as a fixed-length byte array
			sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			sdbFieldSize = ((Field_new_decimal*) pField)->bin_size;
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_ENUM:
			sdbFieldType = ENGINE_TYPE_U_NUMBER;
			sdbFieldSize = ((Field_enum*) pField)->pack_length(); // TBD: need validation
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_STRING: // can be CHAR or BINARY data type
			// if the character set name contains the string "bin" we infer that this is a binary string
			charsetName = pField->charset()->name;
			charsetName = strstr(charsetName, "bin");
			if (pField->binary()) {
				sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			} else if (charsetName) {
				sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			} else {
				sdbFieldType = ENGINE_TYPE_STRING_CHAR;
			}
			// do NOT use pField->field_length as it may be too big
			sdbFieldSize = pField->pack_length(); // exact size used in RAM
			sdbMaxDataLength = 0;
			break;

		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_TINY_BLOB: // tiny	blob applies to TEXT as well
			if (pField->binary())
				sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			//no supported for streaming tables
			else {
				sdbFieldType = ENGINE_TYPE_STRING_VAR_CHAR;
				reduceSizeOfString = true;
			}
			keyFieldLength = sdbFieldSize = get_field_key_participation_length(thd, table_arg, pField);// if this is a key field - get the key length
			if (keyFieldLength){
				// for a key field - use the length of the key
				sdbFieldSize = keyFieldLength;

			}else{
				sdbFieldSize = 0;		// this is a variable field
			}
			sdbMaxDataLength = pField->field_length;
			break;

		case MYSQL_TYPE_BLOB: // These 3 data types apply to TEXT as well
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
			if (pField->binary())
				sdbFieldType = ENGINE_TYPE_BYTE_ARRAY;
			else
				sdbFieldType = ENGINE_TYPE_STRING_VAR_CHAR;

			// We parse through the index keys, based on the participation of this field in the
			// keys we decide how much of the field we want to store as part of the record and 
			// how much in the overflow file. If field does not participate in any index then
			// we do not store anything as part of the record.

			// For a blob - the data is kept in the OVF file (sdbFieldSize is 0).
			//	If the blob data is a key, we keep the key portion in the table data.


			sdbFieldSize = get_field_key_participation_length(thd, table_arg, pField);
			sdbMaxDataLength = pField->field_length;
			break;

		case MYSQL_TYPE_GEOMETRY:
			if ( pField->get_geometry_type() == pField->GEOM_POINT )
			{
				// Point values are stored in the row as they are fixed length and relatively short.
				// When it is an indexed column, the entire value is indexed.
				sdbFieldType		= ENGINE_TYPE_BYTE_ARRAY;
				sdbFieldSize		= SRID_SIZE + WKB_HEADER_SIZE + POINT_DATA_SIZE;
				sdbMaxDataLength	= 0;		// max data length has value only with variable length.
				break;
			}
			// Fall through: Other geometric data types are not supported

		default:
			sdbFieldType = 0;
#ifdef SDB_DEBUG_LIGHT
			SDBDebugStart(); // synchronize threads printout
			SDBDebugPrintString("\0This data type is not supported yet.\0");
			SDBDebugEnd(); // synchronize threads printout
#endif
			return HA_ERR_UNSUPPORTED;
			break;
		}

			if( fieldType!=MYSQL_TYPE_STRING &&
				fieldType!=MYSQL_TYPE_BLOB &&
				fieldType!=MYSQL_TYPE_MEDIUM_BLOB &&
				fieldType!=MYSQL_TYPE_LONG_BLOB &&
				fieldType!=MYSQL_TYPE_GEOMETRY &&
				fieldType!=MYSQL_TYPE_VARCHAR &&
				fieldType!=MYSQL_TYPE_VAR_STRING &&
				fieldType!=MYSQL_TYPE_VAR_STRING && 
				fieldType!=MYSQL_TYPE_TINY_BLOB && dataLength>sdbFieldSize)
			{
					char buf[1000];
					sprintf(buf,"- Unsupported type for Column %s",pField->field_name);
					SDBSetErrorMessage( sdbUserId_, SCALEDB_GENERIC_ERROR, buf );
					return convertToMysqlErrorCode(HA_ERR_GENERIC);
			}


		if (sdbFieldType ) { // there is a field 
			// API bug fix - the UTF8 size is calculated in the storage engine , remove the reduandent calculation of UTF8 in MySQL
			// CURRENT SDB API only VARCHAR is reduandent 
			const char* charsetName = pField->charset()->name;

			if ( (strcmp(charsetName, "utf8_general_ci") == 0) && !(pField->flags & (ENUM_FLAG | SET_FLAG)) && reduceSizeOfString ) 
			{

#ifdef SDB_DEBUG_LIGHT
				if ( sdbMaxDataLength && sdbMaxDataLength % 3 ) {
					SDBTerminateEngine(1, "MAX Column size of UTF8 is not a multiple of 3", __FILE__,__LINE__);
				}
#endif
				sdbMaxDataLength = sdbMaxDataLength /3;

#ifdef SDB_DEBUG_LIGHT
				if ( sdbFieldSize && sdbFieldSize % 3 ) {
					SDBTerminateEngine(1, "Column size of UTF8 is not a multiple of 3", __FILE__,__LINE__);
				}
#endif
				sdbFieldSize = sdbFieldSize /3;

			}

			if ( pSystemRangeKey != NULL && strcmp( pSystemRangeKey, ( char* ) pField->field_name ) == 0 )
			{
//this is a fix for bug 2854
//but leaving disabled because it will make lots of old streaming create tables to fail
//eg  ctime TIMESTAMP DEFAULT '0000-00-00 00:00:00',
		/*		if(!pField->has_insert_default_function())
				{
						SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- Range KEY DEFAULT must be CURRENT_TIMESTAMP." );
						return convertToMysqlErrorCode(HA_ERR_GENERIC);
				}
*/
				if(fieldType == MYSQL_TYPE_TIMESTAMP) {} //all OK
				else
				{
						SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- Range KEY must be a Timestamp column." );
						return convertToMysqlErrorCode(HA_ERR_GENERIC);
				}

				if(pField->null_bit!=0)  //0 means NOT NULL
				{
						SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- Range KEY must be a NOT NULL column." );
						return convertToMysqlErrorCode(HA_ERR_GENERIC);
				}

			}

	/*		if( isStreamingTable_==ST_TRUE && fieldType==MYSQL_TYPE_VARCHAR && sdbFieldType == ENGINE_TYPE_BYTE_ARRAY)
			{
					SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- Streaming table does Not support varbinary coluns." );
					return convertToMysqlErrorCode(HA_ERR_GENERIC);
			}
		*/	
			if ( pUserRangeKey != NULL && strcmp( pUserRangeKey, ( char* ) pField->field_name ) == 0 )
			{
				if(userTimeUnit==SI_CODED)	
				{
					//supports 4 byte int columns
					if(fieldType == MYSQL_TYPE_LONG) {} //all OK
					else
					{
						SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- USER RANGE_KEY of CODED type must be an Integer column." );
						return convertToMysqlErrorCode(HA_ERR_GENERIC);
					}

				}
				else
				{
					if(fieldType == MYSQL_TYPE_TIMESTAMP ) {} //all OK
					else
					{
				
						char* ut_type=NULL;
						switch(userTimeUnit)
						{
						case SI_SECOND: {ut_type="SECOND"; break;}	
						case SI_MINUTE: {ut_type="MINUTE"; break;}	
						case SI_HOUR:	{ut_type="HOUR"; break;}	
						case SI_DAY:	{ut_type="DAY"; break;}	
						case SI_WEEK:	{ut_type="WEEK"; break;}	
						case SI_MONTH:	{ut_type="MONTH"; break;}	
						case SI_YEAR:	{ut_type="YEAR"; break;}	
						default: {ut_type="UNKNOWN"; break;}	
						}

						char buf[1000];
						sprintf(buf,"- USER RANGE_KEY of %s type must be a Timestamp columns",ut_type);


						SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, buf );
						return convertToMysqlErrorCode(HA_ERR_GENERIC);
					}
				}
				if(pField->null_bit!=0)  //0 means NOT NULL
				{
						SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- USER RANGE_KEY must be a NOT NULL column." );
						return convertToMysqlErrorCode(HA_ERR_GENERIC);
				}

			}
			bool isIndexed=false;
			unsigned short fieldFlag=SDB_FIELD_DEFAULT;
			if(!create_from_like) //if create_from_like=true, then dont need to check index strutre because it must be ok (because copied from valid table)
			{
				
				List_iterator<Key> key_iterator(thd->lex->alter_info.key_list);
				Key* key=NULL;

				set<string> keys;
				set<string>::iterator it;
			
				while ((key=key_iterator++))
				{
					Key_part_spec*  ks=(Key_part_spec*)key->columns.first_node()->info;

					if (isStreamingTable_==ST_TRUE)
					{
						string s(ks->field_name.str);
						it = keys.find(s);
						if ( it !=keys.end() )
						{
							//duplicate index
							char buf[1000];
							sprintf(buf,"- Streaming Table includes duplicate index %s",ks->field_name.str);
							SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, buf );
							return convertToMysqlErrorCode(HA_ERR_GENERIC);
						}
						if ( key->columns.elements>1)
						{
							SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "Streaming Table does not support composite keys" );
							return convertToMysqlErrorCode(HA_ERR_GENERIC);
						}
						keys.insert(s);
					}

					if(strcmp(ks->field_name.str,(char*) pField->field_name)==0)
					{
						isIndexed=true;
						if(key->type == Key::FOREIGN_KEY) {fieldFlag=fieldFlag|SDB_FIELD_FK;}	
						if(key->type == Key::PRIMARY || key->type == Key::UNIQUE) {fieldFlag=fieldFlag|SDB_FIELD_PK_OR_UNIQUE;}
					}

				}
			}			
			bool isHashKey=false;
			bool isMappedField=false;
			bool isInsertCascade=false;
			bool isStreamingKey=false;
			bool isHashSize=false;
			int hashSize=0;



			//iterate through the index until you find the field


			int n=table_arg->s->keys;
			for(int i=0;i<n; i++)
			{
				KEY _key= table_arg->s->key_info[i];


			

				const char* col_name= _key.key_part->field->field_name;
				if(SDBUtilCompareStrings((char*)col_name,(char*) pField->field_name,true))
				{

					if(SDBUtilCompareStrings((char*)_key.name,(char*)"PRIMARY",true))
					{
						//this is a streaming key
						if( isStreamingTable_==ST_TRUE) 
						{
							isStreamingKey=true;
							if(isAutoIncrField!=true)
							{
								SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- STREAMING_KEY must be an AUTO_INCREMENT Field" );
								return convertToMysqlErrorCode(HA_ERR_GENERIC);
							}
							if(	sdbFieldType != ENGINE_TYPE_U_NUMBER)
							{
								SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- STREAMING_KEY must be an unsigned bigint Field" );
								return convertToMysqlErrorCode(HA_ERR_GENERIC);
							}
						
						}
					}
					engine_option_value* opt=_key.option_list;
					while(opt)
					{


					

						if(SDBUtilStrstrCaseInsensitive(opt->name.str,"STREAMING_KEY") && opt->parsed==true &&SDBUtilStrstrCaseInsensitive(opt->value.str,"YES") && isStreamingKey==false)
						{
							SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- STREAMING_KEY must be a Primary Key" );
							return convertToMysqlErrorCode(HA_ERR_GENERIC);
						}	


						if(isStreamingTable_==ST_TRUE && SDBUtilCompareStrings(opt->name.str,"HASH_KEY",true) && opt->parsed==true &&SDBUtilCompareStrings(opt->value.str,"YES",true))
						{
							if(hashSize=0) {hashSize=1000;} //set default hash size, can get overwritten
							number_streaming_attributes++;
							isHashKey=true;                    // hashkey implies the following:
							isInsertCascade=true;              // 1.add parent to hidden table 
							fieldFlag=fieldFlag|SDB_FIELD_FK;  // 2.foregin key to the hidden table  
							isMappedField =true;               // 3.mapped column  
							string s(pField->field_name);
							hashkeys.insert(s);
						}	
						if(isStreamingTable_==ST_TRUE && SDBUtilCompareStrings(opt->name.str,"HASH_SIZE",true) && opt->parsed==true )
						{
							isHashSize=true;
							hashSize=SDBUtilStringToInt(opt->value.str);



							if (hashSize<MIN_STREAMING_TABLE_HASH_SIZE)
							{
								char buf[1000];
								sprintf(buf,"- HASHSIZE %d below minimum HASHSIZE %d",hashSize,MIN_STREAMING_TABLE_HASH_SIZE);
								SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, buf );
								return convertToMysqlErrorCode(HA_ERR_GENERIC);

							}

							if (hashSize>MAX_STREAMING_TABLE_HASH_SIZE)
							{
								char buf[1000];
								sprintf(buf,"- HASHSIZE %d above maximum HASHSIZE %d",hashSize,MAX_STREAMING_TABLE_HASH_SIZE);
								SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, buf );
								return convertToMysqlErrorCode(HA_ERR_GENERIC);

							}



							//	}

						}
						opt=opt->next;
					}

					
					if(isHashKey==false && hashSize>0)
					{
						SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- HASH_SIZE can only be specified on a HASH_KEY" );
						return convertToMysqlErrorCode(HA_ERR_GENERIC);
					}

					if(isHashKey)
					{
						if(isStreamingKey==true)
						{
							SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- HASH_KEY cannot be set on a Primary Key" );
							return convertToMysqlErrorCode(HA_ERR_GENERIC);
						}
						if ( pSystemRangeKey != NULL && strcmp( pSystemRangeKey, ( char* ) pField->field_name ) == 0 )
						{
							SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- HASH_KEY cannot be set on a RANGE_KEY" );
							return convertToMysqlErrorCode(HA_ERR_GENERIC);
						}
						if ( pUserRangeKey != NULL && strcmp( pUserRangeKey, ( char* ) pField->field_name ) == 0 )
						{
							SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- HASH_KEY cannot be set on a RANGE_KEY" );
							return convertToMysqlErrorCode(HA_ERR_GENERIC);
						}

					}
					
					if(isStreamingKey)
					{
						if ( pSystemRangeKey != NULL && strcmp( pSystemRangeKey,( char* ) pField->field_name ) == 0 )
						{
							SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- RANGE_KEY cannot be set on the STREAMING_KEY" );
							return convertToMysqlErrorCode(HA_ERR_GENERIC);
						}
						if ( pUserRangeKey != NULL && strcmp( pUserRangeKey, ( char* ) pField->field_name ) == 0 )
						{
							SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- RANGE_KEY cannot be set on the STREAMING_KEY" );
							return convertToMysqlErrorCode(HA_ERR_GENERIC);
						}

					}

				}
			}


			sdbFieldId = SDBCreateField( sdbUserId_, sdbDbId_, sdbTableNumber_,
				(char*) pField->field_name, sdbFieldType, sdbFieldSize, sdbMaxDataLength, NULL,
				isAutoIncrField, ddlFlag, fieldFlag, pSystemRangeKey ? stricmp( pField->field_name, pSystemRangeKey ) == 0 : false, isMappedField, isInsertCascade,
				isStreamingKey, pUserRangeKey ? stricmp( pField->field_name, pUserRangeKey ) == 0 : false, 0, 0 );

#ifdef _HIDDEN_DIMENSION_TABLE // CREATE HIDDEN DIMENSION TABLE + INDEXES 
			if ( !isAlter && isHashKey )
			{
				if(!create_from_like && !isIndexed)
				{
					//streaming table require an index
					
					SDBSetErrorMessage( sdbUserId_, INVALID_STREAMING_OPERATION, "- Streaming Table requires the HASH key be Indexed." );
					return convertToMysqlErrorCode(HA_ERR_GENERIC);
				}
				int rret=create_dimension_table(table_arg, (char*) pField->field_name, sdbFieldType, sdbFieldSize, hashSize, ddlFlag, tableCharSet, fkInfoArray);
				if(rret!=SUCCESS)
				{
					return convertToMysqlErrorCode(CREATE_TABLE_FAILED_IN_CLUSTER);
				}
			}
#endif

		}


	


		if(!sdbFieldType ||  !sdbFieldId ) { // an error occurs
#ifdef SDB_DEBUG_LIGHT
			SDBDebugStart(); // synchronize threads printout
			SDBDebugPrintString("\0fails to create user column ");
			SDBDebugPrintString((char*) pField->field_name);
			SDBDebugPrintString("\0");
			SDBDebugEnd(); // synchronize threads printout
#endif
			sdbError = SDBGetLastUserErrorNumber(sdbUserId_);

			return convertToMysqlErrorCode(sdbError);
		}
	}



	return SUCCESS;
}

// create the foreign keys for a table
int ha_scaledb::create_fks(THD* thd, TABLE *table_arg, char* tblName, SdbDynamicArray* fkInfoArray,
        char* pCreateTableStmt, bool bIsAlterTableStmt) {

	unsigned int errorNum = 0;
	int retValue = 0;
	int foreignKeyNum;
	int numOfKeys = (int) table_arg->s->keys;

	if (thd->query_length() > 0) {

		// foreign key constraint syntax:
		// [CONSTRAINT [symbol]] FOREIGN KEY [index_name] (index_col_name, ...) 
		//		REFERENCES tbl_name (index_col_name,...)
		// MySQL creates a non-unique secondary index in the child table for a foreign key constraint

		char* pCurrConstraintClause = SDBUtilSqlSubStrIgnoreComments(pCreateTableStmt, (char*) "constraint", true);
		char* pCurrForeignKeyClause = SDBUtilSqlSubStrIgnoreComments(pCreateTableStmt, (char*) "foreign key", true); 

		// BUG 1208- do not use "foreign key " as it is possible that there is no lagging space after "foreign key" .

		char* pConstraintName = NULL;

		// If the ALTER TABLE (including CREATE/DROP INDEX) statement has no FOREIGN KEY clause,
		// then we are modifying other constraints.  We need to carry the existing foreign key constraints
		// into the newly created table.

		if (isStreamingTable_==ST_TRUE) {} //dont copy over streaming dimenstion key, need special treatment
		else
		{

			if (bIsAlterTableStmt && (!pCurrForeignKeyClause)) {
				retValue = SDBCopyForeignKey(sdbUserId_, sdbDbId_, sdbTableNumber_,
					pSdbMysqlTxn_->getAlterTableName());
				return convertToMysqlErrorCode(retValue); // we should exit early for this case.
			}

			if (bIsAlterTableStmt){

				if (pSdbMysqlTxn_->getAlterTableName()){

					SDBCopyForeignKey(sdbUserId_, sdbDbId_, sdbTableNumber_, pSdbMysqlTxn_->getAlterTableName());
				}

				foreignKeyNum = SDBGetNumberOfForeignTables(sdbUserId_, sdbDbId_,pSdbMysqlTxn_->getAlterTableName());
			}else{
				foreignKeyNum = 0;
			}
		}
		while (pCurrForeignKeyClause != NULL) { // foreign key clause exists

			pConstraintName = NULL;
			if (pCurrConstraintClause && pCurrForeignKeyClause) {
				if (pCurrForeignKeyClause - pCurrConstraintClause > 11)
					pConstraintName = pCurrConstraintClause + 11; // there are 11 characters in "constraint "
			}

			MysqlForeignKey* pKeyI = new MysqlForeignKey();
			char* pOffset = pCurrForeignKeyClause + 11; // there are 11 characters in "foreign key"
			if (pConstraintName) // use constraint symbol as foreign key constraint name
				pKeyI->setForeignKeyName(pConstraintName);
			else { // use index_name as foreign key constraint name
				char* pUserTableName = tblName;
				if (bIsAlterTableStmt)
					pUserTableName = pSdbMysqlTxn_->getAlterTableName();

				++foreignKeyNum;
				unsigned short keyNameLen = pKeyI->setForeignKeyName(pOffset, pUserTableName,
				        foreignKeyNum);
				pOffset = pOffset + keyNameLen; // advance after index_name
			}

			bool bAddForeignKey = true; // the default is to add foreign key
			if (SDBUtilSqlSubStrIgnoreComments(pCreateTableStmt, (char*) "drop foreign key", true))
				bAddForeignKey = false;

			if (bAddForeignKey) {
				// define/add a foreign key constraint
				char* pColumnNames = strstr(pOffset, "("); // points to column name
				if (!pColumnNames) {
					retValue = METAINFO_WRONG_FOREIGN_FIELD_NAME;
					SDBSetErrorMessage( sdbUserId_, CREATE_TABLE_FAILED_IN_CLUSTER, " Foreign key - Wrong Foreign Field Name" );
					break; // early exit
				}

				pColumnNames++;

				// set key number, index column names, number of keyfields
				int keyNum = pKeyI->setKeyNumber(table_arg->key_info, numOfKeys, pColumnNames);
				if (keyNum == -1) {
					retValue = METAINFO_WRONG_FOREIGN_FIELD_NAME;
					SDBSetErrorMessage( sdbUserId_, CREATE_TABLE_FAILED_IN_CLUSTER, " Foreign key - Wrong Foreign Field Name" );
					break; // early exit
				}

				char* pTableName = SDBUtilSqlSubStrIgnoreComments(pOffset, (char*) "references", true) + 11; // points to parent table name
				pKeyI->setParentTableName(pTableName);
				pOffset = pTableName + pKeyI->getParentTableNameLength();
				pColumnNames = strstr(pOffset, "(") + 1; // points to column name
				pKeyI->setParentColumnNames(pColumnNames);

				// test case 109.sql, Make sure the parent table is open.  
				char* pParentTableName = pKeyI->getParentTableName();
				unsigned short parentTableId = SDBGetTableNumberByName(sdbUserId_, sdbDbId_,
				        pParentTableName);
				if (parentTableId == 0)
				{

//we are here if the table was NOT already open, so we open it and we sav the table ID so that we will close it. 
//but need to hold the shared table lock while it is open

					parentTableId = SDBOpenTable(sdbUserId_, sdbDbId_, pParentTableName, 0, false,false);				
					if(parentTableId!=0)
					{

						parent_list.push_back(parentTableId); //need to close when create done	
						if (!ha_scaledb::lockDML(sdbUserId_, sdbDbId_, parentTableId, 0)) {
							parentTableId = 0; //failed to get shared lock so will fail create.
						}


					}
		
				}
				if (parentTableId == 0) {
					retValue = METAINFO_WRONG_FOREIGN_TABLE_NAME;
					SDBSetErrorMessage( sdbUserId_, CREATE_TABLE_FAILED_IN_CLUSTER, " Foreign key - Wrong Foreign Table Name" );
					break; // early exit
				}

				// save the info about this foreign key for use when creating the associated designator
				if (keyNum >= 0) {
					SDBArrayPutPtr(fkInfoArray, keyNum + 1, pKeyI);
				}

				// For ALTER TABLE statement, Need to return an error code if the constraint already exists.
				if (bIsAlterTableStmt && pSdbMysqlTxn_->getAlterTableName()) {
					if (SDBCheckConstraintIfExits(sdbUserId_, sdbDbId_, tblName, pKeyI->getForeignKeyName())) {
						retValue = METAINFO_DUPLICATE_FOREIGN_KEY_CONSTRAINT;
						SDBSetErrorMessage( sdbUserId_, CREATE_TABLE_FAILED_IN_CLUSTER, " Foreign key - Duplicate Foreign Key Constraint" );
						break;
					}
				}

				retValue = SDBDefineForeignKey(sdbUserId_, sdbDbId_, tblName, pParentTableName, pKeyI->getForeignKeyName(), pKeyI->getIndexColumnNames(), pKeyI->getParentColumnNames());
				if (retValue > 0)
				{

					switch(retValue)
					{
					case METAINFO_WRONG_TABLE_NAME:
						{
							SDBSetErrorMessage( sdbUserId_, CREATE_TABLE_FAILED_IN_CLUSTER, " Foreign key - Wrong table name or Missing Parent.");
							break;
						}
					case METAINFO_WRONG_FOREIGN_FIELD_NAME:
						{
							SDBSetErrorMessage( sdbUserId_, CREATE_TABLE_FAILED_IN_CLUSTER, " Foreign key - Wrong field name in table.");
							break;
						}

					case METAINFO_MISSING_FOREIGN_TABLE:
						{
							SDBSetErrorMessage( sdbUserId_, CREATE_TABLE_FAILED_IN_CLUSTER, " Foreign key - Wrong foreign table name for table.");
							break;
						}

					case METAINFO_WRONG_FIELD_TYPE:
						{
							SDBSetErrorMessage( sdbUserId_, CREATE_TABLE_FAILED_IN_CLUSTER, " Foreign key - Wrong field type in table.");
							break;
						}

					case METAINFO_WRONG_FIELD_SIZE:
						{
							SDBSetErrorMessage( sdbUserId_, CREATE_TABLE_FAILED_IN_CLUSTER, " Foreign key - Wrong field length in table:");
							break;
						}

					case  METAINFO_NON_UNIQUE_PARENT_KEY_NOT_SUPPORTED:
						{

							SDBSetErrorMessage( sdbUserId_, CREATE_TABLE_FAILED_IN_CLUSTER, " Foreign key - Non indexed table is not supported (parent and child keys must be identical)." );
							break;
						}
					}
					break; // early exit
                   }
				} else {

				// ALTER TABLE tableName DROP FOREIGN KEY fk_symbol
				if (bIsAlterTableStmt && pSdbMysqlTxn_->getAlterTableName()) {

					if (SDBCheckConstraintIfExits(sdbUserId_, sdbDbId_, pSdbMysqlTxn_->getAlterTableName(), pKeyI->getForeignKeyName())) {
						// First we copy all the existing foreign key constraits into the newly created table
						//  this apears to be redundent (see copy at the start of the function) and causes aborts...
						//retValue = SDBCopyForeignKey(sdbUserId_, sdbDbId_, sdbTableNumber_,
						//        pSdbMysqlTxn_->getAlterTableName());
						if (retValue)
							break;
						// now we need to delete the foreign key constraint from the newly created table
						retValue = SDBDeleteForeignKeyConstraint(sdbUserId_, sdbDbId_,
						        sdbTableNumber_, pKeyI->getForeignKeyName());
						if (retValue)
							break;
					} else {
						retValue = METAINFO_MISSING_FOREIGN_KEY_CONSTRAINT;
						SDBSetErrorMessage( sdbUserId_, CREATE_TABLE_FAILED_IN_CLUSTER, " Foreign key - Missing Foreign Key Constraint" );
						break;
					}

				} else {
					// it is an error if this is not an ALTER TABLE DROP FOREIGN KEY statement
					retValue = METAINFO_MISSING_FOREIGN_KEY_CONSTRAINT;
					SDBSetErrorMessage( sdbUserId_, CREATE_TABLE_FAILED_IN_CLUSTER, " Foreign key - Missing Foreign Key Constraint" );
					break;
				}

			} //	if ( bAddForeignKey )

			pCurrConstraintClause = SDBUtilSqlSubStrIgnoreComments(pOffset, (char*) "constraint", true);
			pCurrForeignKeyClause = SDBUtilSqlSubStrIgnoreComments(pOffset, (char*) "foreign key", true); 
			// BUG 1208- do not use "foreign key " as it is possible that there is no lagging space after "foreign key".
		} // while ( pCurrForeignKeyClause != NULL )

	} // if ( thd->query_length() > 0 )

	errorNum = convertToMysqlErrorCode(retValue);
	return errorNum;
}

// Find if a given field is participating in indexes (is a keypart), and if yes for how much size?
int ha_scaledb::get_field_key_participation_length(THD* thd, TABLE *table_arg, Field * pField) {

	KEY_PART_INFO* pKeyPart;
	int numOfKeys = (int) table_arg->s->keys;
	int maxParticipationLength = 0;

	for (int i = 0; i < numOfKeys; ++i) {
		KEY* pKey = table_arg->key_info + i;

		int numOfKeyFields = (int) pKey->user_defined_key_parts;

		for (int i2 = 0; i2 < numOfKeyFields; ++i2) {
			pKeyPart = pKey->key_part + i2;
			if (SDBUtilCompareStrings(pKeyPart->field->field_name, pField->field_name, true))
				if (maxParticipationLength < pKeyPart->length)
					maxParticipationLength = pKeyPart->length;
		}
	}
	return maxParticipationLength;
}

// add indexes to a table, part of create table
int ha_scaledb::add_indexes_to_table( THD* thd, TABLE *table_arg, char* tblName, unsigned short ddlFlag, SdbDynamicArray* fkInfoArray, char* pCreateTableStmt )
{
	unsigned int errorNum = 0;
	int retCode = 0;
	unsigned int primaryKeyNum = table_arg->s->primary_key;
	KEY_PART_INFO* pKeyPart;
	int numOfKeys = (int) table_arg->s->keys;
	char* pTableFsName = tblName; 

	// set up the default index type at table level
	unsigned char sdbTableLevelIndexType = INDEX_TYPE_IMPLICIT;
	if (table_arg->s->comment.length) {
		if ( SDBUtilSqlSubStrIgnoreComments(table_arg->s->comment.str, SCALEDB_HINT_INDEX_BTREE, false) )
			sdbTableLevelIndexType = INDEX_TYPE_BTREE;
		else if ( SDBUtilSqlSubStrIgnoreComments(table_arg->s->comment.str, SCALEDB_HINT_INDEX_TRIE, false) )
			sdbTableLevelIndexType = INDEX_TYPE_TRIE;
	}

	unsigned char sdbIndexType = INDEX_TYPE_IMPLICIT;
	for (int i = 0; i < numOfKeys; ++i) {
		KEY* pKey = table_arg->key_info + i;

		// set the index type
		switch (pKey->algorithm) {
		case HA_KEY_ALG_UNDEF:
			sdbIndexType = sdbTableLevelIndexType;
			break;

		case HA_KEY_ALG_BTREE:
//			sdbIndexType = INDEX_TYPE_BTREE;
			sdbIndexType = INDEX_TYPE_TRIE;		// Btree support is replaced with trie
			break;

		case HA_KEY_ALG_RTREE:
			sdbIndexType = sdbTableLevelIndexType;
			break;

		case HA_KEY_ALG_HASH:
			sdbIndexType = INDEX_TYPE_HASH;
			break;

		default:
			sdbIndexType = sdbTableLevelIndexType;
			break;
		}

		char* designatorName = SDBUtilFindDesignatorName(pTableFsName, pKey->name, i, true, sdbDesignatorName_, SDB_MAX_NAME_LENGTH);

		if ((primaryKeyNum == 0) && (i == 0)) { // primary key specified

			int numOfKeyFields = (int) pKey->user_defined_key_parts;

			char** keyFields = (char **) ALLOCATE_MEMORY((numOfKeyFields + 1) * sizeof(char *), SOURCE_HA_SCALEDB, __LINE__);
			unsigned short* keySizes = (unsigned short*) ALLOCATE_MEMORY((numOfKeyFields + 1) * sizeof(unsigned short), SOURCE_HA_SCALEDB, __LINE__);

			for (int i2 = 0; i2 < numOfKeyFields; ++i2) {
				pKeyPart = pKey->key_part + i2;
				keyFields[i2] = (char*) pKeyPart->field->field_name;
				keySizes[i2] = (unsigned short) pKeyPart->length;
			}
			keyFields[numOfKeyFields] = NULL; // must end with NULL to signal the end of pointer list

			MysqlForeignKey* fKeyInfo = (MysqlForeignKey*) SDBArrayGetPtr(fkInfoArray, i + 1);

			char** parentKeyColumns = fKeyInfo ? fKeyInfo->getParentColumnNames() : NULL;
			char* parent = SDBGetParentIndexByForeignFields(sdbDbId_, tblName, keyFields,
			        parentKeyColumns);

			// a foreign key and we can't find the parent designator
			if (fKeyInfo && !parent) {
				retCode = METAINFO_WRONG_PARENT_DESIGNATOR_NAME;
				break;
			}

			unsigned short retValue = SDBCreateIndex(sdbUserId_, sdbDbId_, sdbTableNumber_,
			        designatorName, keyFields, keySizes, true, false, parent, ddlFlag, 0,
			        sdbIndexType);
			// Need to free keyFields, but not the field names used by MySQL.
			FREE_MEMORY(keyFields);
			FREE_MEMORY(keySizes);
			if (retValue == 0) { // an error occurs
				SDBRemoveLocalTableInfo(sdbUserId_, sdbDbId_, sdbTableNumber_,false); // cleans up table name and field name
				retCode = METAINFO_FAIL_TO_CREATE_INDEX;
				break;
			}

		} else { // add secondary index (including the foreign key)

			int numOfKeyFields = (int) pKey->user_defined_key_parts;

			bool isUniqueIndex = (pKey->flags & HA_NOSAME) ? true : false;
			char** keyFields = (char **) ALLOCATE_MEMORY((numOfKeyFields + 1) * sizeof(char *), SOURCE_HA_SCALEDB, __LINE__);
			unsigned short* keySizes = (unsigned short*) ALLOCATE_MEMORY((numOfKeyFields + 1) * sizeof(unsigned short), SOURCE_HA_SCALEDB, __LINE__);
			for (int i2 = 0; i2 < numOfKeyFields; ++i2) {
				pKeyPart = pKey->key_part + i2;
				keyFields[i2] = (char*) pKeyPart->field->field_name;
				keySizes[i2] = (unsigned short) pKeyPart->length;
			}
			keyFields[numOfKeyFields] = NULL; // must end with NULL to signal the end of pointer list

			MysqlForeignKey* fKeyInfo = (MysqlForeignKey*) SDBArrayGetPtr(fkInfoArray, i + 1);

			char** parentKeyColumns = fKeyInfo ? fKeyInfo->getParentColumnNames() : NULL;
			char* parent = SDBGetParentIndexByForeignFields(sdbDbId_, tblName, keyFields,
			        parentKeyColumns);

			// a foreign key and we can't find the parent designator
			if (fKeyInfo && !parent) {
				retCode = METAINFO_WRONG_PARENT_DESIGNATOR_NAME;
				break;
			}

			unsigned short retValue = SDBCreateIndex(sdbUserId_, sdbDbId_, sdbTableNumber_,
			        designatorName, keyFields, keySizes, false, !isUniqueIndex, parent, ddlFlag, i,
			        sdbIndexType);
			// Need to free keyFields, but not the field names used by MySQL.
			FREE_MEMORY(keyFields);
			FREE_MEMORY(keySizes);
			if (retValue == 0) { // an error occurs as index number should be > 0

				retCode = METAINFO_FAIL_TO_CREATE_INDEX;
				break;
			}


			//lets create the streaming / hash index

			bool isHashKey=false;
			int hashSize=1000; //set to 1000 by default.
			engine_option_value* opt=pKey->option_list;
			while(opt)
			{
				if(SDBUtilCompareStrings(opt->name.str,"HASHKEY",true) && opt->parsed==true &&SDBUtilCompareStrings(opt->value.str,"YES",true))
				{					
					isHashKey=true;                    // hashkey implies the following:
				}	
				if(SDBUtilCompareStrings(opt->name.str,"HASHSIZE",true) && opt->parsed==true )
				{	
					hashSize=SDBUtilStringToInt(opt->value.str);
				}			
				opt=opt->next;
			}
			
			if ( isHashKey )
			{

				int rret=create_multi_dimension_table(table_arg, pKey->name,  pKey->key_part, pKey->user_defined_key_parts,   hashSize, ddlFlag, 0, fkInfoArray);

				if(rret!=SUCCESS)
				{
					return convertToMysqlErrorCode(CREATE_TABLE_FAILED_IN_CLUSTER);
				}
			}


		}
	} // for (int i=0; i < numOfKeys; ++i )

	errorNum = convertToMysqlErrorCode(retCode);
	return errorNum;
}

// delete a user table. 
// This method is outside the pair of external_lock calls.  Hence we need to set sdbTableNumber_.
// Note that the name is file system compliant (or file system safe for table file names).
// We cannot use table->s->table_name.str because it is not defined.
int ha_scaledb::delete_table(const char* name) {
	DBUG_ENTER("ha_scaledb::delete_table");
#ifdef SDB_DEBUG
	debugHaSdb("delete_table", name, NULL, NULL);
#endif
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::delete_table(name=");
		SDBDebugPrintString((char *) name);
		SDBDebugPrintString(") ");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif


	char dbFsName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };
	char tblFsName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };
	char pathName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };

	char* pTableName = NULL;
	char* pDropTableStmt = NULL;
	int errorNum = 0;
	unsigned short retCode = 0;
	bool bIsAlterTableStmt = false;
	bool isTableAlreadyOpen  = true;
	THD* thd = ha_thd();
	placeSdbMysqlTxnInfo(thd);
	unsigned short ddlFlag = 0;
	char* partitionName = NULL;
#ifdef SDB_DEBUG_LIGHT
	SDBLogSqlStmt(sdbUserId_, thd->query(), thd->query_id); // inform engine to log user query for DDL
#endif

	// Do nothing on virtual view table since it is just a mapping, not a real table
	if (virtualTableFlag_) {
		DBUG_RETURN(errorNum);
	}

	// First we fetch db and table names
	fetchIdentifierName(name, dbFsName, tblFsName, pathName);


	char* pTblFsName = &tblFsName[0]; // points to the beginning of tblFsName
	unsigned int sqlCommand = thd_sql_command(thd);
	if (ha_scaledb::isAlterCommand(sqlCommand) ) {
		bIsAlterTableStmt = true;
	}

	// Open user database if it is not open yet.
	retCode = ha_scaledb::openUserDatabase(dbFsName, dbFsName, sdbDbId_,NULL,pSdbMysqlTxn_); 
	if (retCode) {
		DBUG_RETURN(convertToMysqlErrorCode(retCode));
	}


	
	SDBCommit(sdbUserId_, true);		

	SessionExclusiveMetaLock ot(sdbDbId_,sdbUserId_);
	if(ot.lock()==false)
	{

		DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
	}	
	if(sdbUserId_!= 0)
	{
		int tid = SDBGetTableNumberByFileSystemName(sdbUserId_, sdbDbId_, tblFsName);
		if(tid!=0)
		{
			SDBRemoveLocalTableInfo(sdbUserId_, sdbDbId_, tid,false);			
		}
	}

	//get the partitionName and Id, if exists
	bool ispartition=false;
	if (SDBUtilStrstrCaseInsensitive(tblFsName, "#P#")) {
		partitionName = SDBUtilDuplicateString(tblFsName);
		getAndRemovePartitionName(pTblFsName, partitionName);
		sdbPartitionId_ = SDBGetPartitionId(sdbUserId_, sdbDbId_, partitionName, pTblFsName);
		ispartition=true;
	}

	sdbTableNumber_ = SDBGetTableNumberByFileSystemName(sdbUserId_, sdbDbId_, pTblFsName);


	if ((sdbTableNumber_ == 0)) {
		isTableAlreadyOpen	= false;
		//need to open the table
		sdbTableNumber_ = SDBOpenTable(sdbUserId_, sdbDbId_, pTblFsName, sdbPartitionId_, false); // bug137
	}

	
	if ((sdbTableNumber_ == 0)) {
		//table really does NOTexist (must have already got dropped so bail)
			DBUG_RETURN(convertToMysqlErrorCode(TABLE_NAME_UNDEFINED));		
		}

    assert(sdbTableNumber_!=0);
	if (!lockDDL(sdbUserId_, sdbDbId_, sdbTableNumber_, 0)){
		if(!isTableAlreadyOpen)
		{
			//open just for delete so now close
			int ret=SDBCloseTableAndChildrenTables(sdbUserId_, sdbDbId_,sdbTableNumber_,false, false,false);
			int ret2=SDBCloseParentTables(sdbUserId_, sdbDbId_,pTblFsName);
		}
		SDBSetErrorMessage( sdbUserId_, TABLE_IN_USE_ON_ANOTHER_NODE, " table cannot be dropped because it is in use or is open on another MySql node." );
		SDBRollBack(sdbUserId_, NULL, 0, true);
		DBUG_RETURN(convertToMysqlErrorCode(TABLE_IN_USE_ON_ANOTHER_NODE));
	}


	// If a table name has special characters such as $ or + enclosed in back-ticks,
	// then MySQL encodes the user table name by replacing the special characters with ASCII code.
	// In our metadata, we save the original user-defined table name and the file-system-compliant name.
	pTableName = SDBGetTableNameByNumber(sdbUserId_, sdbDbId_, sdbTableNumber_);
	retCode = SDBCanTableBeDropped(sdbUserId_, sdbDbId_, pTableName);
	if (bIsAlterTableStmt && (retCode == METAINFO_ATTEMPT_DROP_REFERENCED_TABLE)) {
		ddlFlag |= SDBFLAG_DONOT_CHECK_TABLE_REFERENCE;
		retCode = SUCCESS; // Bug 1151: allow it to proceed if it is ALTER TABLE statement
	}

	

	// The primary node now replicates DDL to other nodes.
	//replicate the drop table to other nodes, need to do this after commit since
	//we take lock metainfo for drop table

	// pass the DDL statement to engine so that it can be propagated to other nodes
	// The user statement may be DROP TABLE, ALTER TABLE or CREATE/DROP INDEX,
	if (retCode == SUCCESS) {
		// remove the table name from lock table vector if it exists
	

		// single node solution: need to drop table and its in-memory metadata.
		// cluster solution: primary node needs to drop table and its in-memory metadata.  
		// cluster solution: secondary node: MySQL already closed the to-be-dropped table and deleted MySQL metadata for the table.
		unsigned short _tableId = SDBGetTableNumberByFileSystemName(sdbUserId_, sdbDbId_, tblFsName);
		retCode = SDBDeleteTable(sdbUserId_, sdbDbId_, pTableName, partitionName, sdbPartitionId_, ddlFlag);
		
		if ( retCode == CAS_FILE_IN_USE )
		{
			

			if ( !isTableAlreadyOpen )
			{
				// Close the table which was opened above
				char*			userFromTblName	= SDBGetTableNameByNumber( sdbUserId_, sdbDbId_, sdbTableNumber_ );
				unsigned short	retClose		= SDBCloseTable( sdbUserId_, sdbDbId_, userFromTblName, sdbPartitionId_, false, false, false, true );
			}
			SDBSetErrorMessage( sdbUserId_, TABLE_IN_USE_ON_ANOTHER_NODE, " table cannot be dropped because it is in use or is open on another MySql node." );

			SDBRollBack(sdbUserId_, NULL, 0, true);
			FREE_MEMORY(partitionName);
			DBUG_RETURN(convertToMysqlErrorCode(retCode));

		}

		if (retCode == SUCCESS && ispartition==false)  //dropping the partition file should not cause removal of frm file.
		{                                              //to remove frm you need to drop the physical file
			// With multiple mysql nodes, the delete_table might fail. Need to defer the 
			// removal of frm data until the delete succeeds.
			SDBDeleteFrmData(sdbUserId_, sdbDbId_, _tableId);
		}

	} 

	
	pSdbMysqlTxn_->setDdlFlag(0); // Bug1039: reset the flag as a subsequent statement may check this flag
	pSdbMysqlTxn_->setActiveTrn(false); // mark the end of long implicit transaction

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 1) {
		if (retCode == SUCCESS)
			SDBPrintStructure(sdbDbId_); // print metadata structure
	}
#endif

#ifdef SDB_DEBUG
	if (mysqlInterfaceDebugLevel_ > 4) {
		// print user lock status on the primary node
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("In ha_scaledb::delete_table, print user locks imposed by primary node ");
		SDBDebugEnd(); // synchronize threads printout
		SDBShowUserLockStatus(sdbUserId_);
	}
#endif
	SDBCommit(sdbUserId_, true);	
	
	FREE_MEMORY(partitionName);
	errorNum = convertToMysqlErrorCode(retCode);
	DBUG_RETURN(errorNum);
}

unsigned short rename_partition_within_table(char* fromPartitionId, unsigned short toPartitionId) {
	
	return 0;
	
	
}

// rename a user table.
// This method is outside the pair of external_lock calls.  Hence we need to set sdbTableNumber_.
// we need to rename all the file name as file name depends on table name
// Note that the name is file system compliant (or file system safe for table file names).
// We cannot use table->s->table_name.str because it is not defined.
int ha_scaledb::rename_table(const char* fromTable, const char* toTable) {
	DBUG_ENTER("ha_scaledb::rename_table");
#ifdef SDB_DEBUG
	debugHaSdb("rename_table", fromTable, toTable, NULL);
#endif

#ifdef SDB_DEBUG_LIGHT
	bool debugRename = false;
	if (debugRename || mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::rename_table(fromTable=");
		SDBDebugPrintString((char *) fromTable);
		SDBDebugPrintString(", toTable=");
		SDBDebugPrintString((char *) toTable);
		SDBDebugPrintString(") ");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	char pathName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };
	char dbName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };
	char fromDbName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };
	char fromTblFsName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };
	char toTblFsName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };
	char fromPartitionName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };
	char toPartitionName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };

	char* userFromTblName = NULL;
	unsigned short ddlFlag = 0;

	// First we fetch db and table names
	fetchIdentifierName(fromTable, fromDbName, fromTblFsName, pathName);
	fetchIdentifierName(toTable, dbName, toTblFsName, pathName);
	if (!SDBUtilCompareStrings(dbName, fromDbName, true)) {
		DBUG_RETURN( HA_ERR_UNSUPPORTED);
	}

	char* pFromTblFsName = &fromTblFsName[0]; // points to the beginning of tblFsName
	char* pToTblFsName = &toTblFsName[0];
	bool bIsAlterTableStmt = false;
	bool bExistsTempTable = false;
	bool bpartitionTable = false;

	unsigned int errorNum = 0;
	unsigned short retCode = 0;

	THD* thd = ha_thd();
	placeSdbMysqlTxnInfo(thd);
#ifdef SDB_DEBUG_LIGHT
	SDBLogSqlStmt(sdbUserId_, thd->query(), thd->query_id); // inform engine to log user query for DDL
#endif

	unsigned int sqlCommand = thd_sql_command(thd);
	if (ha_scaledb::isAlterCommand(sqlCommand) ) {
		bIsAlterTableStmt = true;

		// For statement "ALTER TABLE t1 RENAME [TO] t2", need to set sqlCommand to SQLCOM_RENAME_TABLE.
		// If this is a true ALTER TABLE statement, then the flag SDBFLAG_ALTER_TABLE_CREATE must be turned on.
		// Also there is a temp table in the arguments.  If both condition fails, then it is actually a RENAME TABLE statement.

		if ( strstr(toTblFsName, MYSQL_TEMP_TABLE_PREFIX) || strstr(fromTblFsName, MYSQL_TEMP_TABLE_PREFIX) || strstr(fromTable, "#TMP#") ) {
			bExistsTempTable = true;
		}

		if ((!bExistsTempTable) && (!(pSdbMysqlTxn_->getDdlFlag() & SDBFLAG_ALTER_TABLE_CREATE))) {
			bIsAlterTableStmt = false;
			sqlCommand = SQLCOM_RENAME_TABLE;
		}
	}

	// Open user database if it is not open yet.
	retCode = ha_scaledb::openUserDatabase(fromDbName, fromDbName, sdbDbId_,NULL,pSdbMysqlTxn_); 
	if (retCode)
		DBUG_RETURN(convertToMysqlErrorCode(retCode));


	//check for partition name and copy it.
	if (SDBUtilStrstrCaseInsensitive(pFromTblFsName, "#P#")) {
		getAndRemovePartitionName(pFromTblFsName, fromPartitionName);
		sdbPartitionId_ = SDBGetPartitionId(sdbUserId_, sdbDbId_, fromPartitionName, pFromTblFsName);
		bpartitionTable = true;
	}

	//check for partition name and copy it.
	if (SDBUtilStrstrCaseInsensitive(pToTblFsName, "#P#")) {
		getAndRemovePartitionName(pToTblFsName, toPartitionName);
		bpartitionTable = true;

	}

	SDBCommit(sdbUserId_, false);	// release previous lock as we will take exclusive lock on the meta data tables
	SDBRollBack(sdbUserId_, NULL, 0, true); // release session locks 
	

	SessionExclusiveMetaLock ot(sdbDbId_,sdbUserId_);
	if (ot.lock()==false) {
		DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
	}

		// open the table for every partition.
	sdbTableNumber_ = SDBOpenTable(sdbUserId_, sdbDbId_, pFromTblFsName, sdbPartitionId_, false);
	assert(sdbTableNumber_!=0);
	if (!lockDDL(sdbUserId_, sdbDbId_, sdbTableNumber_, 0)){
		SDBRollBack(sdbUserId_, NULL, 0, true);
		DBUG_RETURN(convertToMysqlErrorCode(LOCK_TABLE_FAILED));
	}

	// If a table name has special characters such as $ or + enclosed in back-ticks,
	// then MySQL encodes the user table name by replacing the special characters with ASCII code.
	// In our metadata, we save the original user-defined table name.
	userFromTblName = SDBGetTableNameByNumber(sdbUserId_, sdbDbId_, sdbTableNumber_);

	char* userToTblName = toTblFsName; 
	//if no partition indicate no update to partition table

	if (strstr(fromTable, "#TMP#")) {

		retCode = SDBRenamePartition(sdbUserId_, sdbDbId_, pFromTblFsName, fromPartitionName, toPartitionName, sdbPartitionId_);

	} else {

		retCode = SDBRenameTable(sdbUserId_, sdbDbId_, sdbPartitionId_, userFromTblName, pFromTblFsName, userToTblName,
			toTblFsName, userToTblName, true, ddlFlag, bIsAlterTableStmt);
	}

	if (retCode != SUCCESS){
		SDBRollBack(sdbUserId_, NULL, 0, true);
		unsetSdbQueryMgrId();
#ifdef SDB_DEBUG
		SDBCheckAllQueryManagersAreFree(sdbUserId_);
#endif
		errorNum = convertToMysqlErrorCode(retCode);
		DBUG_RETURN(errorNum);
	}

	// RENAME TABLE: primary node needs to release lock on MetaInfo
	// ALTER TABLE: primary node releases lockMetaInfo in ::delete_table
	if ((sqlCommand == SQLCOM_RENAME_TABLE)) {
		updateFrmData(toTblFsName, sdbUserId_, sdbDbId_); 
	}
	// Build the name of the .frm file that we want to use to
	//   update our copy, since we altered the table..
	else
	{
		unsigned int retValx = updateFrmData(fromTblFsName, toTblFsName, sdbUserId_, sdbDbId_ );
	}


#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 1) {
		SDBPrintStructure(sdbDbId_); // print metadata structure
	}

	if (mysqlInterfaceDebugLevel_ > 4) {
		// print user lock status on the primary node
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader(
		        "In ha_scaledb::rename_table, print user locks imposed by primary node ");
		SDBDebugEnd(); // synchronize threads printout
		SDBShowUserLockStatus(sdbUserId_);
	}
#endif

	

	//need to commit before closing parent table. 
	SDBCommit(sdbUserId_, false);
	SDBCloseParentTables(sdbUserId_, sdbDbId_,userToTblName);
	SDBRollBack(sdbUserId_, NULL, 0, true); // release session locks 

	errorNum = convertToMysqlErrorCode(retCode);
	DBUG_RETURN(errorNum);
}



// analyze the table
int ha_scaledb::analyze(THD* thd, HA_CHECK_OPT* check_opt) {
	DBUG_ENTER("ha_scaledb::analyze");
#ifdef SDB_DEBUG
	debugHaSdb("analyze", NULL, NULL, NULL);
#endif

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::analyze(...) ");
		SDBDebugEnd(); // synchronize threads printout
	}
#endif
	int errorNum = info(HA_STATUS_TIME | HA_STATUS_CONST | HA_STATUS_VARIABLE);
	DBUG_RETURN(errorNum);
}

// initialize the index
int ha_scaledb::index_init(uint keynr, bool sorted) {
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_init(...)");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif
	// the qmId in case of write can do index_read , index_init, index_read without any unlock of tables - therefore we nead to clear the data here 
	resetSdbQueryMgrId();
	active_index = keynr;
	prepareIndexQueryManager(active_index);
	return 0;
}

// end the index
int ha_scaledb::index_end(void) {
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_end()");
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(", alias: ");
#ifdef _MARIA_DB
		SDBDebugPrintString(table->alias.c_ptr());
#else
		SDBDebugPrintString( table->alias);
#endif
		outputHandleAndThd();
		SDBDebugPrintString(", Query Manager ID #");
		SDBDebugPrintInt(sdbQueryMgrId_);
		SDBDebugPrintString(" counter = ");
		SDBDebugPrintInt(readDebugCounter_);
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	if ( isQueryEvaluation() )
	{
		return 0;
	}

	char* pTableName		= SDBGetTableNameByNumber( sdbUserId_, sdbDbId_, sdbTableNumber_ );

#ifdef SDB_DEBUG
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart      ();
		SDBDebugPrintString( "\nEnd of indexed query on table [" );
		SDBDebugPrintString( pTableName );
		SDBDebugPrintString( "]\n" );
		SDBDebugEnd        ();
	}
#endif

	SDBEndIndexedQuery( sdbUserId_, sdbQueryMgrId_, sdbDbId_, sdbPartitionId_, pTableName, ( ( THD* ) ha_thd() )->query_id );

	active_index			= MAX_KEY;
	return 0;
}

//
int ha_scaledb::index_read_last(uchar * buf, const uchar * key, uint key_len) {
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::index_read_last(...) ");
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	return index_read(buf, key, key_len, HA_READ_PREFIX_LAST);
}

// return last key which actually resulted in an error (duplicate error)
unsigned short ha_scaledb::get_last_index_error_key() {
	print_header_thread_info("MySQL Interface: executing ha_scaledb::get_last_index_error_key() ");

	unsigned short last_designator = SDBGetLastIndexError(sdbUserId_);
	// if last_designator is not set yet, it is set in the first select statement.
	// We can get it from sdbDesignatorId_.
	if ((last_designator == 0) && sdbDesignatorId_)
		last_designator = sdbDesignatorId_;

	unsigned short last_errkey = active_index;
	if (last_designator)	// If ScaleDB designator id is defined, then map it to mysql index id
		last_errkey = SDBGetLastIndexPositionInTable(sdbDbId_, last_designator);

	if (last_errkey == MAX_KEY) {
		int key = active_index != MAX_KEY ? active_index : 0;
		last_errkey = table->key_info ? ((KEY *) (table->key_info + key))->key_part->fieldnr - 1
		        : 0;
	}

	Field* pAutoIncrField = table->found_next_number_field;
	if (pAutoIncrField == table->field[last_errkey]) {
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

		char to[512];
		uint n=tablename_to_filename( table->s->table_name.str, to, sizeof(to));
		unsigned short tableId = SDBGetTableNumberByName(sdbUserId_, sdbDbId_, to);
		char* dupeValue = SDBGetFileDataField(sdbUserId_, tableId, last_errkey + 1);
		if (dupeValue)		// set key value if it is defined.
			memcpy(table->field[last_errkey]->ptr, dupeValue, length);
	}
	return last_errkey;
}

//extra information passed by the handler
int ha_scaledb::extra(enum ha_extra_function operation) {
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::extra(...) ");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	switch (operation) {
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
		break;
	case HA_EXTRA_NO_KEYREAD:
		readJustKey_ = false;
		break;
	case HA_EXTRA_KEYREAD:
		readJustKey_ = true;
		break;
	}
	return (0);
}


// ---------------------------------------------------------------------
//	Get the storage engine error message - call ScaleDB to get a message
// ---------------------------------------------------------------------
bool ha_scaledb::get_error_message(int error, String *buf) {

	// this function is expensive (lots of string ops) but it is only for error message condition

	unsigned short messageLength;
	char detailed_error[1024];

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::get_error_message(...) ");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif
	if(lastSDBErrorLength==0)
	{
		messageLength = SDBGetErrorMessage(sdbUserId_, detailed_error, 1024);
	}
	else
	{
		//there is a mesage on the handle, so lets use it.
		messageLength=getLastSDBError(detailed_error,1024);
	}
	buf->copy(detailed_error, messageLength, &my_charset_latin1);


	return false;
}

// returns total records in the current table
// this is needed for select count(*) without full table scan
ha_rows ha_scaledb::records() {

	DBUG_ENTER("ha_scaledb::records");
#ifdef SDB_DEBUG
	debugHaSdb("records", NULL, NULL, NULL);
#endif

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::records() ");
		SDBDebugPrintString(", table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(", alias: ");
#ifdef _MARIA_DB
		SDBDebugPrintString(table->alias.c_ptr());
#else
		SDBDebugPrintString( table->alias);
#endif
		outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	if (pSdbMysqlTxn_->getScaledbDbId() == 0) {
		// not yet initialized .. This method may be called directly
		pSdbMysqlTxn_->setScaledbDbId(sdbDbId_);
	}

	int64 totalRows = SDBGetTableRowCount( sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_ );

	if (totalRows < 0) {
		// error condition
		totalRows = HA_POS_ERROR;
	}

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_ > 3) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("total count from table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(": ");
		SDBDebugPrint8ByteUnsignedLong((uint64) totalRows);
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	DBUG_RETURN((ha_rows) totalRows);
}
//  -----------------------------------------------------------------------------
//  Return actual upper bound of number of records in the table.
//	MySQL fails in setting buffers for sort if the number returned is wrong.
//	To disable - return -  HA_POS_ERROR
// ------------------------------------------------------------------------------
ha_rows ha_scaledb::estimate_rows_upper_bound() {

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::estimate_rows_upper_bound() ");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	if (sdbTableNumber_ > 0) {
		// stats.records should be accurate - other wise MySQL exits on Error because the buffer is to small 
		stats.records				= ( ha_rows )	SDBGetTableStats( sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_, SDB_STATS_INFO_FILE_RECORDS );
	}

	return stats.records ? stats.records : HA_POS_ERROR;
}

// Return time for a scan of the table
double ha_scaledb::scan_time() {

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::scan_time() ");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	unsigned long long seekLength = 1000;
	if (virtualTableFlag_) // return a large number for virtual table scan because it should use index
		return (double) seekLength;

	// TODO: this should be stored in file handle; auto updated during dictionary change.
	// Compute disk seek length for normal tables.
	//if (!pSdbMysqlTxn_->ddlFlag_) { //do this only on primary
	char to[512];
	uint n=tablename_to_filename( table->s->table_name.str, to, sizeof(to));

	if (!sdbTableNumber_) {
		sdbTableNumber_ = SDBGetTableNumberByName(sdbUserId_, sdbDbId_, to);
	}
	if (sdbTableNumber_) {
		seekLength = SDBGetTableStats(sdbUserId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_, SDB_STATS_INFO_SEEK_LENGTH);
	}

	//  //  if the seekLength is 0 it is an empty table - leave it to be zero in order to favor sequential scan 
	//	if (seekLength == 0)
	//		seekLength = 1;
	return (double) seekLength;
}

// Return read time of set of range of rows.
// TODO: The current version will make query processor choose index if an index exists. 
double ha_scaledb::read_time(uint inx, uint ranges, ha_rows rows) {

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::read_time(...) ");
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	double readTime ;
	

	if (rows <= 2) {
		readTime = ((double) rows);
	}
	else {
		/* Assume that the read time is proportional, but slightly bigger (*1.1),  to the scan time for all
		rows  + at most two seeks per range. */
		double time_for_scan_index = scan_time()*1.1;

		if (stats.records < rows) {

			readTime =  time_for_scan_index;
		}
		else {
			readTime = (ranges + (((double) rows / (double) stats.records) * time_for_scan_index));

		}
	}

	////STREAMING_OPTIMIZATION
	//if(sdbDbId_!=0 && sdbTableNumber_ !=0 && SDBIsStreamingTable(sdbDbId_, sdbTableNumber_))
	//{
	//	//if a table scan is required, lets make sure that the range key
	//	// will get used
	//	int range_index=SDBGetRangeKey(sdbDbId_, sdbTableNumber_) ;
	//	int index_id=SDBGetIndexExternalId(sdbDbId_, range_index);

	//	if(inx ==index_id)
	//	{
	//		// if range key return minimum 
	//		readTime 1;
	//	}
	//	else
	//	{
	//		// on dimension and pk return bigger number
	//		return 2;
	//	}
	//}

	return readTime;
}


// ------------------------------------------------------------------------------
// records_in_range: estimate records in range [min_key,max_key] on a specific key where:
// min_key.flag can have one of the following values:
//      HA_READ_KEY_EXACT - Include the key in the range
//      HA_READ_AFTER_KEY - Don't include key in range
//  
//  max_key.flag can have one of the following values:
//      HA_READ_BEFORE_KEY - Don't include key in range  
//     HA_READ_AFTER_KEY - Include all 'end_key' values in the range
//  
// ------------------------------------------------------------------------------
ha_rows ha_scaledb::records_in_range(uint inx, key_range* min_key, key_range* max_key) {
	DBUG_ENTER("ha_scaledb::records_in_range");
#ifdef SDB_DEBUG
	debugHaSdb("records_in_range", NULL, NULL, NULL);
#endif
#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		if (mysqlInterfaceDebugLevel_ > 1)
			outputHandleAndThd();
		SDBDebugPrintHeader("MySQL Interface: executing ha_scaledb::records_in_range: ");
		SDBDebugEnd(); // synchronize threads printout
	}
#endif
	ha_rows rangeRows=1;

	unsigned char *minKey;
	unsigned char minKeyLength;
	unsigned char *maxKey;
	unsigned char maxKeyLength;
	bool isFullKey;
	bool allNulls;
	bool inverseRange = false;

	SdbKeySearchDirection minkeyDirection,maxkeyDirection;

	if (min_key){
		minKey = (unsigned char *)min_key->key;
		minKeyLength = min_key->length;
		minkeyDirection = (min_key->flag == HA_READ_KEY_EXACT) ?  SDB_KEY_SEARCH_DIRECTION_GE : SDB_KEY_SEARCH_DIRECTION_GT;
		buildKeyTemplate(this->keyTemplate_[0],minKey,minKeyLength,inx,isFullKey,allNulls);
	}else{
		minKey = NULL;
		minKeyLength = 0;
		minkeyDirection = SDB_KEY_SEARCH_DIRECTION_GT;
		isFullKey = false;
	}
	if (max_key){
		maxKey = (unsigned char *)max_key->key;
		maxKeyLength = max_key->length;
		maxkeyDirection = (max_key->flag == HA_READ_AFTER_KEY) ?  SDB_KEY_SEARCH_DIRECTION_LE : SDB_KEY_SEARCH_DIRECTION_LT;
		buildKeyTemplate(this->keyTemplate_[1],maxKey,maxKeyLength,inx,isFullKey,allNulls);
	}
	else if (minkeyDirection == SDB_KEY_SEARCH_DIRECTION_GT && isFullKey && allNulls) // on "is not NULL" 
	{ 
		inverseRange = true; // search for the inverse range and sustruct - more accurate  
		minkeyDirection = SDB_KEY_SEARCH_DIRECTION_GE; 
		maxKey = minKey;
		buildKeyTemplate(this->keyTemplate_[1],minKey,minKeyLength,inx,isFullKey,allNulls);
		maxkeyDirection = SDB_KEY_SEARCH_DIRECTION_LE;
	}
	else
	{
		maxKey = NULL;
		maxKeyLength = 0;
		maxkeyDirection =  SDB_KEY_SEARCH_DIRECTION_LT; 
	}

	resetSdbQueryMgrId();

	rangeRows = SDBRowsInRange(sdbUserId_,sdbQueryMgrId_, sdbDbId_, sdbTableNumber_, sdbPartitionId_, inx + 1, minKey, this->keyTemplate_[0], minkeyDirection, maxKey, this->keyTemplate_[1],maxkeyDirection);

	resetSdbQueryMgrId();

	if ( inverseRange )
	{
		rangeRows = (stats.records - rangeRows);
	}
	else
	{
		//favor index to scan for ranges by always returing less then a full scan  
		rangeRows =  (ha_rows)((double)rangeRows * 0.9);
	}
	// the estimated number of rows has to be >= 1.  Otherwise, MySQL thinks it is an empty set.
	if (rangeRows < 1) {
		rangeRows = 1;
	} 

#ifdef SDB_DEBUG_LIGHT
	if (mysqlInterfaceDebugLevel_) {
		SDBDebugStart(); // synchronize threads printout
		SDBDebugPrintHeader(" - total range count estimate from table: ");
		SDBDebugPrintString(table->s->table_name.str);
		SDBDebugPrintString(": ");
		SDBDebugPrintInt((int)rangeRows);
		SDBDebugEnd(); // synchronize threads printout
	}
#endif

	DBUG_RETURN(rangeRows);
}


// /////////////////////////////////////////////////////////////////////////
// Plugin information
//
////////////////////////////////////////////////////////////////////////////

struct st_mysql_storage_engine scaledb_storage_engine = { MYSQL_HANDLERTON_INTERFACE_VERSION };

static MYSQL_SYSVAR_STR(config_file, scaledb_config_file,
		PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
		"ScaledDB Config File Path",
		NULL, NULL, NULL);

static MYSQL_SYSVAR_STR(data_directory, scaledb_data_directory,
		PLUGIN_VAR_NOCMDOPT | PLUGIN_VAR_READONLY,
		"The default data directory used by ScaledDB storage engine",
		NULL, NULL, NULL);

static MYSQL_SYSVAR_STR(log_directory, scaledb_log_directory,
		PLUGIN_VAR_NOCMDOPT | PLUGIN_VAR_READONLY,
		"The default log directory used by ScaledDB storage engine",
		NULL, NULL, NULL);

static MYSQL_SYSVAR_BOOL(log_dir_append_host, scaledb_log_dir_append_host,
		PLUGIN_VAR_NOCMDOPT | PLUGIN_VAR_READONLY,
		"Make hostname be appeneded to the log directory",
		NULL, NULL, FALSE);

static MYSQL_SYSVAR_UINT(buffer_size_index, scaledb_buffer_size_index,
		PLUGIN_VAR_NOCMDOPT | PLUGIN_VAR_READONLY,
		"This parameter specifies the number of Mega-Bytes (MB) memory space reserved for the index cache by ScaleDB engine",
		NULL, NULL, 10, 1, 1000000, 8192);

static MYSQL_SYSVAR_UINT(buffer_size_data, scaledb_buffer_size_data,
		PLUGIN_VAR_NOCMDOPT | PLUGIN_VAR_READONLY,
		"This parameter specifies the number of Mega-Bytes (MB) memory space reserved for the data cache by ScaleDB engine",
		NULL, NULL, 30, 1, 1000000, 8192);

static MYSQL_SYSVAR_UINT(max_file_handles, scaledb_max_file_handles,
		PLUGIN_VAR_NOCMDOPT | PLUGIN_VAR_READONLY,
		"This parameter specifies the maximum number of file handles for each physical file.",
		NULL, NULL, 2, 2, 100, 0);


static MYSQL_SYSVAR_BOOL(force_sdb_analytics, scaledb_system_variable_force_sdb_analytics,
		PLUGIN_VAR_NOCMDOPT,
		"Enable or disable force_sdb_analytics for all analytic queries on streaming tables",
		NULL, NULL, FALSE);

static MYSQL_SYSVAR_BOOL(aio_flag, scaledb_aio_flag,
		PLUGIN_VAR_NOCMDOPT | PLUGIN_VAR_READONLY,
		"Make hostname be appeneded to the log directory",
		NULL, NULL, TRUE);

static MYSQL_SYSVAR_UINT(max_column_length_in_base_file, scaledb_max_column_length_in_base_file,
		PLUGIN_VAR_NOCMDOPT | PLUGIN_VAR_READONLY,
		"This parameter specifies the maximum number of bytes saved in the base file for a variable-length (VARCHAR or VARBINARY) column.",
		NULL, NULL, 255, 127, 2047, 0);

static MYSQL_SYSVAR_UINT(dead_lock_milliseconds, scaledb_dead_lock_milliseconds,
		PLUGIN_VAR_NOCMDOPT | PLUGIN_VAR_READONLY,
		"This parameter sets the time for ScaleDB engine to detect a dead lock.",
		NULL, NULL, 3000, 0, 180000, 0);

static MYSQL_SYSVAR_BOOL(cluster, scaledb_cluster,
		PLUGIN_VAR_NOCMDOPT | PLUGIN_VAR_READONLY,
		"This flag shows whether ScaleDB cluster software is enabled.",
		NULL, NULL, FALSE);


static MYSQL_SYSVAR_UINT(statistics_level, statisticsLevel_,
  PLUGIN_VAR_NOCMDOPT,
  "Set the statistics level",
  NULL, NULL, 2L, 1L, 1000L, 0);


static MYSQL_SYSVAR_STR(debug_string, scaledb_debug_string,
		PLUGIN_VAR_NOCMDOPT | PLUGIN_VAR_READONLY ,
		"The debug string used to print additional debug messages by ScaledDB engineers",
		NULL, NULL, NULL);

static struct st_mysql_sys_var* scaledb_system_variables[] = { MYSQL_SYSVAR(config_file),
        MYSQL_SYSVAR(data_directory), MYSQL_SYSVAR(log_directory),
        MYSQL_SYSVAR(log_dir_append_host), MYSQL_SYSVAR(buffer_size_index),
        MYSQL_SYSVAR(buffer_size_data), MYSQL_SYSVAR(max_file_handles), MYSQL_SYSVAR(aio_flag),
        MYSQL_SYSVAR(max_column_length_in_base_file), MYSQL_SYSVAR(dead_lock_milliseconds),
        MYSQL_SYSVAR(cluster), 
  	    MYSQL_SYSVAR(force_sdb_analytics),
  	    MYSQL_SYSVAR(statistics_level),
        MYSQL_SYSVAR(debug_string), NULL // the last element of this array must be NULL
        };




////////////////////define Scaledb STAT_METHODS methods here///////////////////
void setLongLongBuffer(char* buf, long long n, struct st_mysql_show_var *var)
{
  var->type= SHOW_LONGLONG;
  *((long long*)buf)=n;
  var->value= buf; 
}
 long long stat_getMemoryUsage(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  long long n=SDBstat_getMemoryUsage();
  setLongLongBuffer( buf,  n, var);

  return 0;
}
  long long stat_connectedVolumnsCount(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  long long n=SDBstat_connectedVolumnsCount();
  setLongLongBuffer( buf,  n, var);

  return 0;
}
long long stat_IOThreadsCount(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  long long n=SDBstat_IOThreadsCount();
  setLongLongBuffer( buf,  n, var);
  return 0;
}
long long stat_SLMThreadsCount(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  long long n=SDBstat_SLMThreadsCount();
  var->type= SHOW_LONGLONG;
  *((long long*)buf)=n;
  var->value= buf; 

  return 0;
}
long long stat_IndexThreadsCount(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
  long long n=SDBstat_IndexThreadsCount();
  setLongLongBuffer( buf,  n, var);
  return 0;
}
long long stat_getCacheAccessCountIndex(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
  long long n=SDBGetStatistics(0);
  setLongLongBuffer( buf,  n, var);
  return 0;
}
long long stat_getCacheAccessCountData(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
  long long n=SDBGetStatistics(0);
  setLongLongBuffer( buf,  n, var); 
  return 0;
}
long long stat_getCacheAccessCountBlob(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
  long long n=SDBGetStatistics(0);
  setLongLongBuffer( buf,  n, var);
  return 0;
}
long long stat_getCacheHitCountIndex(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
  long long n=SDBGetStatistics(0);
  setLongLongBuffer( buf,  n, var);
  return 0;
}
long long stat_getCacheHitCountData(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
  long long n=SDBGetStatistics(0);
  setLongLongBuffer( buf,  n, var);
  return 0;
}

long long stat_getCacheHitCountBlob(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
  long long n=SDBGetStatistics(0);
  setLongLongBuffer( buf,  n, var);
  return 0;
}

long long stat_getBlockReadCountIndex(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
  long long n=SDBGetStatistics(0);
  setLongLongBuffer( buf,  n, var);
  return 0;
}
long long stat_getBlockReadCountData(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
  long long n=SDBGetStatistics(0);
  setLongLongBuffer( buf,  n, var);
  return 0;
}
int stat_getBlockReadCountBlob(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
  long long n=SDBGetStatistics(0);
  setLongLongBuffer( buf,  n, var);
  return 0;
}
	
long long stat_getBlockWriteCountIndex(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
  long long n=SDBGetStatistics(0);
  setLongLongBuffer( buf,  n, var); 
  return 0;
}
long long stat_getBlockWriteCountData(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
  long long n=SDBGetStatistics(0);
  setLongLongBuffer( buf,  n, var);
  return 0;
}
long long stat_getBlockWriteCountBlob(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
  long long n=SDBGetStatistics(0);
  setLongLongBuffer( buf,  n, var);
  return 0;

}
 int stat_getBlockName(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  int n=SDBstat_getBlockName(buf,SHOW_VAR_FUNC_BUFF_SIZE);
  var->type= SHOW_CHAR;
  var->value= buf; 

  return 0;
}

 

long long stat_IndexCacheSize(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_IndexCacheSize();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_IndexCacheUsed(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_IndexCacheUsed();
	setLongLongBuffer( buf,  n, var);
    return n;
}
long long stat_DataCacheSize(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_DataCacheSize();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_DataCacheUsed(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_DataCacheUsed();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_BlobCacheSize(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_IndexCacheSize();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_BlobCacheUsed(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_BlobCacheUsed();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_DBIndexCacheHits(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_DBIndexCacheHits();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_DBIndexBlockReads(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_DBIndexBlockReads();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_IndexBlockSyncWrites(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_IndexBlockSyncWrites();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_IndexBlockASyncWrites(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_IndexBlockASyncWrites();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_IndexCacheHitsStorageNode(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_IndexCacheHitsStorageNode();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_IndexBlockReadsStorageNode(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_IndexBlockReadsStorageNode();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_IndexBlockSyncWritesStorageNode(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_IndexBlockSyncWritesStorageNode();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_IndexBlockASyncWritesStorageNode(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_IndexBlockASyncWritesStorageNode();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_DBDataCacheHits(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_DBDataCacheHits();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_DBDataBlockReads(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_DBDataBlockReads();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_DataBlockSyncWrites(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_DataBlockSyncWrites();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_DataBlockASyncWrites(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_DataBlockASyncWrites();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_DataCacheHitsStorageNode(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_DataCacheHitsStorageNode();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_DataBlockReadsStorageNode(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_DataBlockReadsStorageNode();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_DataBlockSyncWritesStorageNode(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_DataBlockSyncWritesStorageNode();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_DataBlockASyncWritesStorageNode(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_DataBlockASyncWritesStorageNode();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_DBBlobCacheHits(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_DBBlobCacheHits();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_DBBlobBlockReads(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_DBBlobBlockReads();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_BlobBlockSyncWrites(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_BlobBlockSyncWrites();
	setLongLongBuffer( buf,  n, var);
		return n;
}
long long stat_BlobBlockASyncWrites(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_BlobBlockASyncWrites();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_BlobCacheHitsStorageNode(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_BlobCacheHitsStorageNode();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_BlobBlockReadsStorageNode(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_BlobBlockReadsStorageNode();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_BlobBlockSyncWritesStorageNode(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_BlobBlockSyncWritesStorageNode();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_BlockSendsSequentialScan(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_BlockSendsSequentialScan();
	setLongLongBuffer( buf,  n, var);
	return n;
}

long long stat_RowsReturnedSequentialScan(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_RowsReturnedSequentialScan();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_QueriesByIndex(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_QueriesByIndex();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_QueryUniqueKeyLookup(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_QueryUniqueKeyLookup();
	setLongLongBuffer( buf,  n, var);
	return n;
}

long long stat_QueryNonUniqueKeyLookup(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_QueryNonUniqueKeyLookup();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_RangeLookup(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_RangeLookup();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_QuerIndexRowsReturned(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_QuerIndexRowsReturned();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_RowsReturnedDynamicHash(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_RowsReturnedDynamicHash();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_NumberInserts(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_NumberInserts();
	setLongLongBuffer( buf,  n, var);
	return n;
}


long long stat_NumberUpdates(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_NumberUpdates();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_NumberDeletes(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_NumberDeletes();
	setLongLongBuffer( buf,  n, var);
	return n;
}
long long stat_NumberRollbacks(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_NumberRollbacks();
	setLongLongBuffer( buf,  n, var);
	return n;
}

long long stat_NumberCommits(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_NumberCommits();
	setLongLongBuffer( buf,  n, var);
	return n;
}






long long stat_BlobBlockASyncWritesStorageNode(MYSQL_THD thd, struct st_mysql_show_var *var,char *buf)
{
	long long n=SDBstat_BlobBlockASyncWritesStorageNode();
	setLongLongBuffer( buf,  n, var);
	return n;
}





static struct st_mysql_show_var scaledb_stats[]=
{

 	 {"Number of Connected volumes",  (char *)stat_connectedVolumnsCount, SHOW_SIMPLE_FUNC},
 	 {"Number of IO threads",  (char *)stat_IOThreadsCount, SHOW_SIMPLE_FUNC},
 	 {"Number of SLM thread",  (char *)stat_SLMThreadsCount, SHOW_SIMPLE_FUNC},
	 {"Number of Index threads",  (char *)stat_IndexThreadsCount, SHOW_SIMPLE_FUNC},
 	 {"Index Cache Access Count",  (char *)stat_getCacheAccessCountIndex, SHOW_SIMPLE_FUNC},
	 {"Blob Cache Access Count",  (char *)stat_getCacheAccessCountData, SHOW_SIMPLE_FUNC},
 	 {"Data Cache Access Count",  (char *)stat_getCacheAccessCountBlob, SHOW_SIMPLE_FUNC},
 	 {"Index Cache Hit Count",  (char *)stat_getCacheHitCountIndex, SHOW_SIMPLE_FUNC},
 	 {"Blob Cache Hit Count",  (char *)stat_getCacheHitCountData, SHOW_SIMPLE_FUNC},
 	 {"Data Cache Hit Count",  (char *)stat_getCacheHitCountBlob, SHOW_SIMPLE_FUNC},
 	 {"Index Block Read Count",  (char *)stat_getBlockReadCountIndex, SHOW_SIMPLE_FUNC},
 	 {"Blob Block Read Count",  (char *)stat_getBlockReadCountData, SHOW_SIMPLE_FUNC},
 	 {"Data Block Read Count",  (char *)stat_getBlockReadCountBlob, SHOW_SIMPLE_FUNC},
 	 {"Index Block Write Count",  (char *)stat_getBlockWriteCountIndex, SHOW_SIMPLE_FUNC},
 	 {"Blob Blobk Write Count",  (char *)stat_getBlockWriteCountData, SHOW_SIMPLE_FUNC},
 	 {"Data Block Write Count",  (char *)stat_getBlockWriteCountBlob, SHOW_SIMPLE_FUNC},

	 {"Index Cache Size",  (char *)stat_IndexCacheSize, SHOW_SIMPLE_FUNC},
 	 {"Index Cache Used",  (char *)stat_IndexCacheUsed, SHOW_SIMPLE_FUNC},
	 {"Data Cache Size",  (char *)stat_DataCacheSize, SHOW_SIMPLE_FUNC},
	 {"Data Cache Used",  (char *)stat_DataCacheUsed, SHOW_SIMPLE_FUNC},
 	 {"Data Blob Cache Size",  (char *)stat_BlobCacheSize, SHOW_SIMPLE_FUNC},
	 {"Data Blob Cache Used",  (char *)stat_BlobCacheUsed, SHOW_SIMPLE_FUNC},

	 {"Index Cache Hits",  (char *)stat_DBIndexCacheHits, SHOW_SIMPLE_FUNC},
	 {"Index Block Reads",  (char *)stat_DBIndexBlockReads, SHOW_SIMPLE_FUNC},
	 {"Index Block Sync Writes",  (char *)stat_IndexBlockSyncWrites, SHOW_SIMPLE_FUNC},
 	 {"Index Block ASync Writes",  (char *)stat_IndexBlockASyncWrites, SHOW_SIMPLE_FUNC},
	 {"Index Cache Hits Storage Node",  (char *)stat_IndexCacheHitsStorageNode, SHOW_SIMPLE_FUNC},
	 {"Index Block Reads Storage Node",  (char *)stat_IndexBlockReadsStorageNode, SHOW_SIMPLE_FUNC},
 	 {"Index Blok Sync Writes Storage Node",  (char *)stat_IndexBlockSyncWritesStorageNode, SHOW_SIMPLE_FUNC},
	 {"Index Block Async Writes Storage Node",  (char *)stat_IndexBlockASyncWritesStorageNode, SHOW_SIMPLE_FUNC},

	 {"Data Cache Hits",  (char *)stat_DBDataCacheHits, SHOW_SIMPLE_FUNC},
 	 {"Data Block Reads",  (char *)stat_DBDataBlockReads, SHOW_SIMPLE_FUNC},
	 {"Data Block Sync Writes",  (char *)stat_DataBlockSyncWrites, SHOW_SIMPLE_FUNC},
	 {"Data Block ASync Writes",  (char *)stat_DataBlockASyncWrites, SHOW_SIMPLE_FUNC},
 	 {"Data Cache Hits Storage Node",  (char *)stat_DataCacheHitsStorageNode, SHOW_SIMPLE_FUNC},
	 {"Data Block Reads Storage Node",  (char *)stat_DataBlockReadsStorageNode, SHOW_SIMPLE_FUNC},
	 {"Data Blok Sync Writes Storage Node",  (char *)stat_DataBlockSyncWritesStorageNode, SHOW_SIMPLE_FUNC},
 	 {"Data Block Async Writes Storage Node",  (char *)stat_DataBlockASyncWritesStorageNode, SHOW_SIMPLE_FUNC},

	 {"Blob Cache Hits",  (char *)stat_DBBlobCacheHits, SHOW_SIMPLE_FUNC},
	 {"Blob Block Reads",  (char *)stat_DBBlobBlockReads, SHOW_SIMPLE_FUNC}, 
 	 {"Blob Block Sync Writes",  (char *)stat_BlobBlockSyncWrites, SHOW_SIMPLE_FUNC},
	 {"Blob Block ASync Writes",  (char *)stat_BlobBlockASyncWrites, SHOW_SIMPLE_FUNC},
	 {"Blob  Cache Hits Storage Node",  (char *)stat_BlobCacheHitsStorageNode, SHOW_SIMPLE_FUNC},
	 {"Blob Block Reads Storage Node",  (char *)stat_BlobBlockReadsStorageNode, SHOW_SIMPLE_FUNC},
 	 {"Blob Blok Sync Writes Storage Node",  (char *)stat_BlobBlockSyncWritesStorageNode, SHOW_SIMPLE_FUNC},
	 {"Blob Block Async Writes Storage Node",  (char *)stat_BlobBlockASyncWritesStorageNode, SHOW_SIMPLE_FUNC},


	 {"Block Sends Sequential Scan",  (char *)stat_BlockSendsSequentialScan, SHOW_SIMPLE_FUNC},
	 {"Rows Returned Sequential Scan",  (char *)stat_RowsReturnedSequentialScan, SHOW_SIMPLE_FUNC}, 
 	 {"Queries By Index",  (char *)stat_QueriesByIndex, SHOW_SIMPLE_FUNC},
	 {"Query Unique Key Lookup",  (char *)stat_QueryUniqueKeyLookup, SHOW_SIMPLE_FUNC},
	 {"Range Lookup",  (char *)stat_RangeLookup, SHOW_SIMPLE_FUNC},
	 {"Query Index Rows Returned",  (char *)stat_QuerIndexRowsReturned, SHOW_SIMPLE_FUNC},
 	 {"Rows Returned Dynamic Hash",  (char *)stat_RowsReturnedDynamicHash, SHOW_SIMPLE_FUNC},
	 {"Number of Inserts",  (char *)stat_NumberInserts, SHOW_SIMPLE_FUNC},
	 {"Number of Updates",  (char *)stat_NumberUpdates, SHOW_SIMPLE_FUNC},
 	 {"Number of Deletes",  (char *)stat_NumberDeletes, SHOW_SIMPLE_FUNC},
	 {"Number of Rollbacks",  (char *)stat_NumberRollbacks, SHOW_SIMPLE_FUNC},
	 {"Number of Commits",  (char *)stat_NumberCommits, SHOW_SIMPLE_FUNC},
	 {0,0,SHOW_UNDEF}
};
struct st_mysql_show_var* GetStatistics()
{
	return scaledb_stats;
}
////////////////////////////////////////////////////////////////////////////////////////

 static int show_scaledb_vars(THD *thd, SHOW_VAR *var, char *buff)
{

     var->type= SHOW_ARRAY;
     SHOW_VAR* gs= GetStatistics();
     var->value= (char *) gs;
  
     return 0;
}
  
 static SHOW_VAR scaledb_status_variables_export[]= {
     {"ScaleDB",                   (char*) &show_scaledb_vars, SHOW_FUNC},
     {NullS, NullS, SHOW_LONG}
};



#ifndef _MARIA_DB
mysql_declare_plugin(scaledb)
{        MYSQL_STORAGE_ENGINE_PLUGIN,
        &scaledb_storage_engine,
        "ScaleDB",
        "ScaleDB, Inc.",
        "Transactional storage engine for ScaleDB Cluster ",
        PLUGIN_LICENSE_PROPRIETARY,
        scaledb_init_func, /* Plugin Init		*/
        scaledb_done_func, /* Plugin Deinit	*/
        0x0100 , /* version 1.0		*/
        //scaledb_status_variables_export,	/* status variables, will change to this one later	*/
        scaledb_status_variables_export, /* status variables	*/
        scaledb_system_variables, /* system variables	*/
        NULL /* config options	*/
    }
    mysql_declare_plugin_end;
#else
maria_declare_plugin(scaledb)
{        MYSQL_STORAGE_ENGINE_PLUGIN,
        &scaledb_storage_engine,
        "ScaleDB",
        "ScaleDB, Inc.",
        "Transactional storage engine for ScaleDB Cluster ",
        PLUGIN_LICENSE_PROPRIETARY,
        scaledb_init_func, /* Plugin Init		*/
        scaledb_done_func, /* Plugin Deinit	*/
        0x0100 , /* version 1.0		*/
        //scaledb_status_variables_export,	/* status variables, will change to this one later	*/
        scaledb_status_variables_export, /* status variables	*/
        scaledb_system_variables, /* system variables	*/
        "1.0",
		MariaDB_PLUGIN_MATURITY_STABLE /* config options	*/
    }
    maria_declare_plugin_end;

#endif //_MARIA_DB
#endif // SDB_MYSQL

