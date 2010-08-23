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

//  File Name: SdbCommon.h
//
//  Description: ScaleDB Storage API
//
//  Version: 1.0
//
//  Copyright: ScaleDB, Inc. 2007
//
//  History: 09/23/2009  Venu
//

#ifndef _SDB_COMMON_H
#define _SDB_COMMON_H

// types of users
#define SDB_USER_TYPE_SYSTEM 1
#define SDB_USER_TYPE_NODE   2
#define SDB_USER_TYPE_DEFAULT 3
#define SDB_USER_TYPE_SYSTEM_BLOCK_NEEDED 4

#ifndef SDB_MASTER_DBID
#define SDB_MASTER_DBID 1
#endif

typedef enum SdbTableInfo {
	SDB_STATS_INFO_FILE_RECORDS=1,		// the number of rows in a table
	SDB_STATS_INFO_FILE_DELETED=2,		// the number of rows deleted from a table
	SDB_STATS_INFO_FILE_LENGTH=3,		// the size of the file in bytes
	SDB_STATS_INDEX_FILE_LENGTH=4,		// the size of the index file
	SDB_STATS_INFO_REC_LENGTH=5,		// record length
	SDB_STATS_INFO_IO_TO_SCAN=6,		// IOs for full table scan
	SDB_STATS_INFO_SEEK_LENGTH=7		// Seek lengh
} SDB_TABLE_STAT_INFO;

typedef enum SdbKeySearchDirection {
    SDB_KEY_SEARCH_DIRECTION_EQ = 1, 
    SDB_KEY_SEARCH_DIRECTION_GT = 2,
    SDB_KEY_SEARCH_DIRECTION_GE = 3,

    SDB_KEY_SEARCH_DIRECTION_LT = 4,
    SDB_KEY_SEARCH_DIRECTION_LE = 5
} SDB_KEY_SEARCH_DIRECTION;

#define METAINFO_SLM_MAX_NODES 64
#define INITIAL_QUERY_MANAGER_ID_IN_VECTOR 20
#define METAINFO_MAX_KEY_FIELDS 10
#define METAINFO_BLOCK_SIZE  8192

// About 15 bytes for an IP address string, about 5 bytes for a port number string, plus 4 length bytes.
// Hence one node ip+port takes about 24 bytes.
#define MAX_NODE_DATA_BUFFER_LENGTH 2000

#define METAINFO_TOKEN_SEPARATORS " ,.;()[]+-*/`"

#define METAINFO_MAX_FIELDS  256
#define METAINFO_MAX_DESIGNATORS  512
#define METAINFO_MAX_IDENTIFIER_SIZE  80
#define METAINFO_MAX_KEY_FIELDS 10
#define SDB_MAX_ROWID_VALUE 0xffffffff

#ifndef IDENTIFIER_INTERFACE
#define IDENTIFIER_INTERFACE		  16000000
#endif

#define SDB_SIZE_OF_INTEGER    4
#define SDB_SIZE_OF_MEDIUMINT  3
#define SDB_SIZE_OF_SHORT      2
#define SDB_SIZE_OF_TINYINT    1
#define SDB_SIZE_OF_FLOAT      4
#define SDB_SIZE_OF_DATE       3
#define SDB_SIZE_OF_TIME       3
#define SDB_SIZE_OF_DATETIME   8
#define SDB_SIZE_OF_TIMESTAMP  4

#define ENGINE_TYPE_SIZE_OF_LONG 8
#define ENGINE_TYPE_SIZE_OF_DOUBLE 8

#define ENGINE_TYPE_STRING		1	// On comparisons, ENGINE_TYPE_STRING is space extended
#define ENGINE_TYPE_BYTE_ARRAY	2
#define ENGINE_TYPE_U_NUMBER	3
#define ENGINE_TYPE_S_NUMBER	4

#define SDBFLAG_DDL_META_TABLES (1 << 0) /* execute DDL on a meta table */
#define SDBFLAG_DDL_SECOND_NODE (1 << 1) /* execute on non-primary node in cluster */
#define SDBFLAG_CMD_OPEN_FILE	(1 << 2) /* open table files on a non-primary node */
#define SDBFLAG_CMD_CLOSE_FILE	(1 << 3) /* close table files on a non-primary node */
#define SDBFLAG_ALTER_TABLE_KEYS (1 << 4) /* set up this flag if the statement is ALTER TABLE DISABLE/ENABLE KEYS */
#define SDBFLAG_ALTER_TABLE_CREATE (1 << 5) /* set up this flag if MySQL has called create() method for ALTER TABLE */
#define SDBFLAG_DONOT_CHECK_TABLE_REFERENCE (1 << 6)	// It is okay not to check foreign key constraint

#define SDB_VIRTUAL_VIEW "sdb_view_"
#define SDB_VIRTUAL_VIEW_PREFIX_BYTES  9


// The first table id in a user database.  
// Tables with id 1 to 7 are all our meta tables (or data dictionary tables.
#define SDB_FIRST_USER_TABLE_ID 16

// Command type
#define SDB_COMMAND_INSERT		1
#define SDB_COMMAND_LOAD		2
#define SDB_COMMAND_ALTER_TABLE 3

// Transaction Isolation Level
#define SDB_ISOLATION_READ_UNCOMMITTED		0
#define SDB_ISOLATION_READ_COMMITTED		1
#define SDB_ISOLATION_REPEATABLE_READ		2
#define SDB_ISOLATION_SERIALIZABLE			3

// Reference locks
#define REFERENCE_LOCK_EXCLUSIVE 3
#define DEFAULT_REFERENCE_LOCK_LEVEL 2
#define	REFERENCE_READ_ONLY 1

// Detail locks
#define DETAIL_LOCK_UPDATE 3
#define DETAIL_LOCK_SHARED 2

// Resource locks (such as files)
#define RESOURCE_LOCK_EXCLUSIVE 3
#define RESOURCE_LOCK_SHARED	2

#define DEFAULT_LOCK_LEVEL 2

// SDB ERROR CODES
#define SUCCESS 0
#define DATA_EXISTS 1
#define EMPTY_BLOCK 2
#define CONTINUE_TRAVERSAL 3
#define STOP_TRAVERSAL 4
#define WRONG_DATA_KEY 5
#define DESIGNATOR_NOT_DEFINED 6
#define ATTEMPT_TO_DELETE_KEY_WITH_SUBORDINATES 7
#define FILES_NOT_OPEN 8
#define WRONG_PARENT_KEY 9
#define RESTART 10
#define WRONG_FOREIGN_KEY 11
#define KEY_DOES_NOT_EXIST 12
#define INDEX_FIELDS_NOT_EQUAL 13
#define DEAD_LOCK	14
#define REDO_TRAVERSAL	15
#define MISSING_ROW_ID 16
#define DELETE_BLOCK 17
#define RESTART_NEW_FILE 18
#define QUERY_KEY_DOES_NOT_EXIST  19
#define COMPRESSION_FAILED 20
#define CONTINUE_TO_NEXT_ROW 21
#define QUERY_END 22
#define ROW_RETRY_FETCH 23
#define NOT_IN_SEQUENCE 24
#define WRONG_ROW_ID 25
#define DELETED_ROW 26
#define INDEX_BLOCK_CHANGE 27
#define DATABASE_NAME_UNDEFINED 28
#define TABLE_NAME_UNDEFINED 29
#define CREATE_DATABASE_FAILED_IN_CLUSTER 30
#define CREATE_TABLE_FAILED_IN_CLUSTER 31
#define ALTER_TABLE_FAILED_IN_CLUSTER 32
#define LOCK_META_INFO_FAILED 33
#define LOCK_TABLE_FAILED 34
#define INDEX_BLOCKS_NOT_LAYERED_CORRECTLY 35
#define INSERT_AUTO_INCR_WITH_VALUE 36
#define DATA_TRANSFER_NOT_COMPLETED 37
#define DATA_EXISTS_FATAL_ERROR 40

#define ENGINE_INDEX_ERROR 101

// error codes issued by MetaData component
#define METAINFO_UNDEFINED_DATA_TABLE  1001
#define METAINFO_PRIMARY_KEY_EXISTS_FOR_TABLE  1002
#define METAINFO_UNDEFINED_PARENT_DESIGNATOR  1003
#define METAINFO_NUMBER_OF_DESIGNATORS_IS_AT_LIMIT  1004
#define METAINFO_DUPLICATE_DESIGNATOR_NAME  1005
#define METAINFO_MISTAKE_IN_KEY_FIELD_NAME  1006
#define METAINFO_WRONG_DESIGNATOR_NAME  1007
#define METAINFO_MISSING_PARAMS_FO_QUERY_DEF  1008
#define METAINFO_DUPLICATE_TABLE_NAME  1009
#define METAINFO_WRONG_TABLE_NAME  1010
#define METAINFO_WRONG_FIELD_SIZE  1011
#define METAINFO_DUPLICATE_FIELD_NAME  1012
#define METAINFO_WRONG_FIELD_NAME  1013
#define METAINFO_WRONG_FIELD_VALUE  1014
#define METAINFO_WRONG_FIELD_TYPE  1015
#define METAINFO_WRONG_PARENT_DESIGNATOR_NAME  1016
#define METAINFO_WRONG_FOREIGN_FIELD_NAME  1017
#define METAINFO_NO_PK_IN_FOREIGN_TABLE  1018
#define METAINFO_WRONG_KEY_FIELD_NUMBER  1019
#define METAINFO_WRONG_FOREIGN_TABLE_NAME  1020
#define METAINFO_WRONG_DBMS_ID 1021
#define METAINFO_NOT_DEFINED 1022
#define METAINFO_DUPLICATE_FOREIGN_TABLE_DEFINITION 1023
#define METAINFO_VAR_LENGTH_FIELD_LARGER_THAN_MAX 1024
#define METAINFO_UNKOWN_TABLE 1025
#define METAINFO_ATTEMPT_DROP_REFERENCED_TABLE 1026
#define METAINFO_WRONG_FIELD_TYPE_FOR_AUTO_INCREMENT 1027
#define METAINFO_MAX_ROW_SIZE_ERROR 1028
#define METAINFO_NON_UNIQUE_PARENT_KEY_NOT_SUPPORTED 1029
#define METAINFO_MAX_TABLE_COUNT_REACHED 1030
#define METAINFO_MAX_INDEX_COUNT_REACHED 1031
#define METAINFO_FAIL_TO_CREATE_INDEX  1032
#define METAINFO_DUPLICATE_FOREIGN_KEY_CONSTRAINT 1033
#define METAINFO_MISSING_FOREIGN_KEY_CONSTRAINT 1034
#define METAINFO_DELETE_ROW_BY_ROW 1035

#endif //_SDB_COMMON_H

