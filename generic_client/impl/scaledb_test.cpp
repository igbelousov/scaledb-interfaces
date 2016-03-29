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

The following example creates a ScaleDB Engine instance, initializes a data table and demonstrates read and write 
operations to the table.

The demo below is using the generic, low level calls to a ScaleDB instance.

Note: ScaleDB is initialized with a ScaleDB configuration file. 

*/

#include "incl/SdbStorageAPI.h"
#include <stdio.h>
#include <string.h>

#define METALOCK 0
#define TABLE_NAME	"tt3"
#define DB_NAME		"test"
int main(int argc, char** argv){

	

	// test that the ini file is passed
	if (argc < 2){
		printf("\nusage: scaledb_test configfile\n");
		return -1;
	}

	// initialize engine using the ini file params
	printf("\nScaleDB: initializing the storage engine\n");
	if (SDBGlobalInit(argv[1],9306)) {
		printf("\nScaleDB: Failed to initialize the storage engine");
		printf("Check the configuration file and its location: %s", argv[1]);
		return -1;
	}

	printf("\nScaledb Engine initialized successfully\n");
	fflush(stdout);

	// Get a user ID. The user is registered in the engine and can send DDL and DML requests
	unsigned int userId = SDBGetNewUserId();

	printf("\nSDB new user. User id is %u\n", userId);
	fflush(stdout);


	SDBOpenMasterDbAndRecoverLog(userId);

	// initialize a database (dbms name == test)

	unsigned short dbId =SDBGetDatabaseNumberByName(userId, (char*)DB_NAME,false);
	printf("\nScaledb DBMS Test initialized successfully\n");
	fflush(stdout);



	printf("\nCreating ScaleDB User Table %s\n",TABLE_NAME);
	// Create a table in database test.
	bool b1=	SDBSessionLock(userId, dbId,METALOCK,0,0,3);  //get exclusive lock
	unsigned short tableId = SDBCreateTable(userId, dbId, (char*)TABLE_NAME,0, (char *)TABLE_NAME, (char *)TABLE_NAME, false, false, false, false,0,1,0);
	if(tableId!=0)
	{
	unsigned short fieldId;

	// create field - Movie id (4 bytes unsigned number)

		fieldId = SDBCreateField(userId, dbId, tableId, (char*)"Movie id", ENGINE_TYPE_U_NUMBER, 4, 0, NULL, false, 0,0,false,false,false,false,false,0,0);

	}
	else
	{
		//table already exists
	}
	//open the table  before you use it (write or read)
	tableId=SDBOpenTable(userId, dbId, (char *)TABLE_NAME, 0, false);



	printf("\nTable %s is ready for read and write\n",TABLE_NAME);
	fflush(stdout);



	// Insert some data into the table

	unsigned int id;
	unsigned int numberOfInserts = 10;

	id = 1;
	int retVal=0;
	//start a transaction (auto-commit is turned off)
	SDBStartTransaction(userId,false);

	while( id <= numberOfInserts) {

		// Bind the variable id to the first column of the table
		retVal = SDBPrepareNumberField(userId, dbId, tableId, 1, (void *)&id);

		//insert to rowSDBInsertRow(unsigned short userId, bool isdelayed, unsigned short dbId, unsigned short tableId, 

		retVal = SDBInsertRow(userId, false, dbId, tableId, 0, 0, 0, 0);

		// increment the counter
		id++;
	}

	//commit all inserted data
	SDBCommit(userId,false);


	printf("\nInserterted  %d records\n", numberOfInserts);
	fflush(stdout);

	//Now select the Table data.

	// get a query manager. The query manager maintains the state of the query
	unsigned short queryMgrId = SDBGetQueryManagerId(userId);

	//initialize a default ROW template, won't get used
	SDBRowTemplate rowTemplate;
	rowTemplate.fieldArray_		= SDBArrayInit( 10, 5, sizeof( SDBFieldTemplate ),   true ); 

	//prepare a sequential scan
	retVal= SDBPrepareSequentialScan(userId, queryMgrId,dbId, 0,TABLE_NAME,0,true,rowTemplate,NULL,0,NULL,0);


	void *field;
	printf("\nNow select back the data\n");
	printf("\nID    ");
	printf("\n----  ");

	//Iterate through all results.
	while ((SDBQueryCursorNextSequential(userId, queryMgrId,NULL,NULL, SDB_COMMAND_SELECT)== 0)){
		// retrieve the rows

		// get the field value from the first column
		field = SDBQueryCursorGetFieldByTableId(queryMgrId, tableId, 1);
		printf("\n%04u", *(unsigned int *)field);

	}

	//free the Query manager
	SDBFreeQueryManager(userId,queryMgrId) ;

	//delete the table
	printf("\n");
	printf("\nDeleting the User Table \n");
	bool b2=	SDBSessionLock(userId, dbId,METALOCK,0,0,3);  //get exclusive lock
	bool b3=	SDBSessionLock(userId, dbId,tableId,0,0,3);  //get exclusive lock
	SDBDeleteTable(userId, dbId, (char *)TABLE_NAME, NULL, 0, 0);


	printf("\nTest program completed... starting shutdown of Scaledb Engine\n");
	SDBCommit(userId,true);

	return 0;
}
