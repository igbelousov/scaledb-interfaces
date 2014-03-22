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

#define SDB_PUSH_DOWN

// types of users
#define SDB_USER_TYPE_SYSTEM 1
#define SDB_USER_TYPE_NODE   2
#define SDB_USER_TYPE_DEFAULT 3
#define SDB_USER_TYPE_SYSTEM_BLOCK_NEEDED 4

#ifndef SDB_MASTER_DBID
#define SDB_MASTER_DBID 1
#endif

typedef enum SdbTableInfo {
	SDB_STATS_INFO_FILE_RECORDS				= 1,		// the number of rows in a table
	SDB_STATS_INFO_FILE_DISTINCT_KEYS		= 2,		// the number of distinct keys in an index
	SDB_STATS_INFO_FILE_DELETED				= 3,		// the number of rows deleted from a table
	SDB_STATS_INFO_FILE_DELETE_LENGTH		= 4,		// the freespace that comprises the rows deleted from a table
	SDB_STATS_INFO_DATA_FILE_LENGTH			= 5,		// the size of the data file in bytes
	SDB_STATS_INFO_DATA_FILE_MAX_LENGTH		= 6,		// the maximum size of the data file in bytes
	SDB_STATS_INFO_INDEX_FILE_LENGTH		= 7,		// the size of the index file in bytes
	SDB_STATS_INFO_INDEX_FILE_MAX_LENGTH	= 8,		// the maximum size of the data file in bytes
	SDB_STATS_INFO_REC_LENGTH				= 9,		// record length
	SDB_STATS_INFO_MEAN_REC_LENGTH			= 10,		// mean record length
	SDB_STATS_INFO_AUTOINCREMENT			= 11,		// last autoincrement value
	SDB_STATS_INFO_IO_TO_SCAN				= 12,		// IOs for full table scan
	SDB_STATS_INFO_SEEK_LENGTH				= 13		// Seek length
} SDB_TABLE_STAT_INFO;

typedef enum SdbKeySearchDirection {
    SDB_KEY_SEARCH_DIRECTION_EQ = 1, 
    SDB_KEY_SEARCH_DIRECTION_GT = 2,
    SDB_KEY_SEARCH_DIRECTION_GE = 3,
    SDB_KEY_SEARCH_DIRECTION_LT = 4,
    SDB_KEY_SEARCH_DIRECTION_LE = 5,
	SDB_KEY_SEARCH_DIRECTION_NONE = -1
} SDB_KEY_SEARCH_DIRECTION;

#define METAINFO_SLM_MAX_NODES 64
#define INITIAL_QUERY_MANAGER_ID_IN_VECTOR 20
#define METAINFO_MAX_KEY_FIELDS 10
#define METAINFO_BLOCK_SIZE  8192
#define SMALL_VAR_FIELD_SIZE	1024

// About 15 bytes for an IP address string, about 5 bytes for a port number string, plus 4 length bytes.
// Hence one node ip+port takes about 24 bytes.
#define MAX_NODE_DATA_BUFFER_LENGTH 2000

#define METAINFO_TOKEN_SEPARATORS " ,.;()[]+-*/`"

#define METAINFO_MAX_FIELDS  256
#define METAINFO_MAX_DESIGNATORS  512
#define METAINFO_MAX_IDENTIFIER_SIZE  80
#define METAINFO_MAX_KEY_FIELDS 10
#define SDB_MAX_ROWID_VALUE 0xffffffff
#define SDB_MAX_NAME_LENGTH 256

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


#define ENGINE_TYPE_STRING_CHAR	1	// On comparisons, ENGINE_TYPE_STRING_CHAR is space extended
#define ENGINE_TYPE_STRING_VAR_CHAR 2 // On comparisons ENGINE_TYPE_STRING_VAR_CHAR is null extended
#define ENGINE_TYPE_BYTE_ARRAY	3
#define ENGINE_TYPE_U_NUMBER	4
#define ENGINE_TYPE_S_NUMBER	5

#define SDBFLAG_DDL_META_TABLES (1 << 0) /* execute DDL on a meta table */

#define SDBFLAG_CMD_OPEN_FILE	(1 << 2) /* open table files on a non-primary node */
#define SDBFLAG_CMD_CLOSE_FILE	(1 << 3) /* close table files on a non-primary node */
#define SDBFLAG_ALTER_TABLE_KEYS (1 << 4) /* set up this flag if the statement is ALTER TABLE DISABLE/ENABLE KEYS */
#define SDBFLAG_ALTER_TABLE_CREATE (1 << 5) /* set up this flag if MySQL has called create() method for ALTER TABLE */
#define SDBFLAG_DONOT_CHECK_TABLE_REFERENCE (1 << 6)	// It is okay not to check foreign key constraint

#define SDB_VIRTUAL_VIEW "sdb_view_"
#define SDB_VIRTUAL_VIEW_PREFIX_BYTES  9

#define SDB_FIELD_DEFAULT 0
#define SDB_FIELD_PK_OR_UNIQUE (1 << 1)
#define SDB_FIELD_FK (1 << 2)
#define SDB_FIELD_MAPPED (1 << 3)
// The first table id in a user database.  
// Tables with id 1 to 7 are all our meta tables (or data dictionary tables.
#define SDB_FIRST_USER_TABLE_ID 16

// ScaleDB Command type used in ScaleDB engine
#define SDB_COMMAND_INSERT		1
#define SDB_COMMAND_SELECT		2
#define SDB_COMMAND_DELETE		3
#define SDB_COMMAND_UPDATE		4
#define SDB_COMMAND_LOAD		5
#define SDB_COMMAND_CREATE_TABLE 6
#define SDB_COMMAND_ALTER_TABLE  7
#define SDB_COMMAND_MULTI_DELETE 20
#define SDB_COMMAND_MULTI_UPDATE 21

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

// index type
#define INDEX_TYPE_IMPLICIT		0
#define INDEX_TYPE_TRIE			1
#define INDEX_TYPE_BTREE		2
#define INDEX_TYPE_HASH			3


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
#define DATA_TRANSFER_NOT_COMPLETED 37
#define DATA_EXISTS_FATAL_ERROR 40
#define SDB_ROLLBACK_COMPONENT_FAILURE	51	// Storage engine rolled back all transactions because of a component failure (such as SLM)
#define SDB_NO_DISK_SPACE			52
#define SDB_FILE_DELETION_FAILED	53
#define SDB_ROW_NOT_IN_DYNAMIC_HASH 54
#define UPDATE_REJECTED 55
#define WRONG_AUTO_INCR_VALUE 56
#define BLOCK_DOES_NOT_EXIST 57
#define INVALID_STREAMING_OPERATION 58
#define ROW_REQUESTED_OUTSIDE_FILE_RANGE 59

#define CAS_FILE_IN_USE		 100

#define ENGINE_INDEX_ERROR 201


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
#define METAINFO_MISSING_FOREIGN_TABLE  1036
#define METAINFO_ROW_SIZE_TOO_LARGE	1037

// Pushdown operators and data types
#define SDB_PUSHDOWN_OPERATOR_AND						'&'												// & for AND
#define SDB_PUSHDOWN_OPERATOR_OR						'|'												// | for OR
#define SDB_PUSHDOWN_OPERATOR_XOR						'X'												// X for XOR
#define SDB_PUSHDOWN_OPERATOR_EQ						'='												// = for EQ
#define SDB_PUSHDOWN_OPERATOR_LE						'{'												// { for LE
#define SDB_PUSHDOWN_OPERATOR_GE						'}'												// } for GE
#define SDB_PUSHDOWN_OPERATOR_LT						'<'												// < for LT
#define SDB_PUSHDOWN_OPERATOR_GT						'>'												// > for GT
#define SDB_PUSHDOWN_OPERATOR_NE						'!'												// ! for NE
#define SDB_PUSHDOWN_COLUMN_DATA_TYPE_UNSIGNED_INTEGER	'U'												// U for unsigned int	(row)
#define SDB_PUSHDOWN_COLUMN_DATA_TYPE_SIGNED_INTEGER	'I'												// I for   signed int	(row)
#define SDB_PUSHDOWN_COLUMN_DATA_TYPE_FLOAT				'F'												// F for float			(row)
#define SDB_PUSHDOWN_COLUMN_DATA_TYPE_DECIMAL			'B'												// B for binary decimal	(row)
#define SDB_PUSHDOWN_COLUMN_DATA_TYPE_DATE				'D'												// D for date			(row)
#define SDB_PUSHDOWN_COLUMN_DATA_TYPE_TIME				'T'												// T for time			(row)
#define SDB_PUSHDOWN_COLUMN_DATA_TYPE_DATETIME			'A'												// A for datetime		(row)
#define SDB_PUSHDOWN_COLUMN_DATA_TYPE_YEAR				'Y'												// Y for year			(row)
#define SDB_PUSHDOWN_COLUMN_DATA_TYPE_TIMESTAMP			'S'												// S for timestamp		(row)
#define SDB_PUSHDOWN_COLUMN_DATA_TYPE_CHAR				'C'												// C for char			(row)
#define SDB_PUSHDOWN_COLUMN_DATA_TYPE_VARCHAR			'V'												// V for varchar		(row)
#define SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER	'u'												// u for unsigned int	(user)
#define SDB_PUSHDOWN_LITERAL_DATA_TYPE_SIGNED_INTEGER	'i'												// i for signed   int	(user)
#define SDB_PUSHDOWN_LITERAL_DATA_TYPE_FLOAT			'f'												// f for float			(user)
#define SDB_PUSHDOWN_LITERAL_DATA_TYPE_DECIMAL			'b'												// b for binary decimal	(user)
#define SDB_PUSHDOWN_LITERAL_DATA_TYPE_DATE				SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER	// u for unsigned int	(user)
#define SDB_PUSHDOWN_LITERAL_DATA_TYPE_TIME				SDB_PUSHDOWN_LITERAL_DATA_TYPE_SIGNED_INTEGER	// i for signed   int	(user)
#define SDB_PUSHDOWN_LITERAL_DATA_TYPE_DATETIME			'a'												// a for datetime		(user)
#define SDB_PUSHDOWN_LITERAL_DATA_TYPE_YEAR				SDB_PUSHDOWN_LITERAL_DATA_TYPE_UNSIGNED_INTEGER	// u for unsigned int	(user)
#define SDB_PUSHDOWN_LITERAL_DATA_TYPE_TIMESTAMP		's'												// s for timestamp		(user)
#define SDB_PUSHDOWN_LITERAL_DATA_TYPE_CHAR				'c'												// c for char			(user)
#define SDB_PUSHDOWN_LITERAL_DATA_TYPE_VARCHAR			'v'												// v for varchar		(user)
#define SDB_PUSHDOWN_UNKNOWN							'?'												// ? for unknown

// Pushdown condition string offsets
#define LOGIC_OP_OFFSET_CHILDCOUNT 0
#define LOGIC_OP_OFFSET_OPERATION (LOGIC_OP_OFFSET_CHILDCOUNT + 1)
#define LOGIC_OP_NODE_LENGTH (LOGIC_OP_OFFSET_OPERATION + 1)

#define COMP_OP_OFFSET_CHILDCOUNT 0
#define COMP_OP_OFFSET_OPERATION (COMP_OP_OFFSET_CHILDCOUNT + 1)
#define COMP_OP_NODE_LENGTH (COMP_OP_OFFSET_OPERATION + 1)

#define ROW_DATA_OFFSET_CHILDCOUNT 0
#define ROW_DATA_OFFSET_ROW_TYPE (ROW_DATA_OFFSET_CHILDCOUNT + 1)
#define ROW_DATA_OFFSET_DATABASE_NUMBER (ROW_DATA_OFFSET_ROW_TYPE + 1)
#define ROW_DATA_OFFSET_TABLE_NUMBER (ROW_DATA_OFFSET_DATABASE_NUMBER + 2)
#define ROW_DATA_OFFSET_COLUMN_OFFSET (ROW_DATA_OFFSET_TABLE_NUMBER + 2)
#define ROW_DATA_OFFSET_COLUMN_SIZE (ROW_DATA_OFFSET_COLUMN_OFFSET + 2)
#define ROW_DATA_NODE_LENGTH (ROW_DATA_OFFSET_COLUMN_SIZE + 2)

#define USER_DATA_OFFSET_CHILDCOUNT 0
#define USER_DATA_OFFSET_DATA_TYPE (USER_DATA_OFFSET_CHILDCOUNT + 1)
#define USER_DATA_OFFSET_DATA_SIZE (USER_DATA_OFFSET_DATA_TYPE + 1)
#define USER_DATA_OFFSET_USER_DATA (USER_DATA_OFFSET_DATA_SIZE + 1)

#endif //_SDB_COMMON_H