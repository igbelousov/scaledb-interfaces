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

#include "../../../interface/scaledb/incl/SdbStorageAPI.h"
#include <stdio.h>
#include <string.h>

 
int main(int argc, char** argv){

	// SETUP

	// test that the ini file is passed
	if (argc < 2){
		printf("\nusage: scaledb_test configfile\n");
		return -1;
	}

	// initialize engine using the ini file params
	if (SDBGlobalInit(argv[1])) {
        printf("\nScaleDB: Failed to initialize the storage engine");
        printf("Check the configuration file and its location: %s", argv[1]);
        return -1;
	}

	printf("\nScaledb Engine initialized successfully");
	fflush(stdout);

	// Get a user ID. The user is registered in the engine and can send DDL and DML requests
	unsigned int userId = SDBGetNewUserId();

	printf("\nSDB new user. User id is %u", userId);
	fflush(stdout);

	// initialize a database (dbms name == test)
	unsigned short dbId = SDBOpenDatabase(userId, (char*)"test");

	printf("\nScaledb DBMS Test initialized successfully");
	fflush(stdout);

	// DDL

	// Create a table in database test.
	unsigned short tableId = SDBCreateTable(userId, dbId, (char*)"Catalog");

	unsigned short fieldId;
	
	// create field - product id (4 bytes unsigned number)
	fieldId = SDBCreateField(userId, dbId, tableId, (char*)"product id", ENGINE_TYPE_U_NUMBER, 4, 0, NULL, false, 0);

	// create field product name (note: ENGINE_TYPE_STRING is space extended)

	fieldId = SDBCreateField(userId, dbId, tableId, (char*)"product name", ENGINE_TYPE_STRING, 20, 0, NULL, false, 0);

	// create field price (8 bytes unsigned number)
	fieldId = SDBCreateField(userId, dbId, tableId, (char*)"price", ENGINE_TYPE_S_NUMBER, 8, 0, NULL, false, 0);

	
	// Create index over product id
	char *keyFields[2];				// maintains an array of names of fields that make the key
	keyFields[0] = (char*)"product id";	// first field in the key
	keyFields[1] = 0;
	

	// note: productIdIndex is a unique index
	unsigned short indexId = SDBCreateIndex(userId, dbId, tableId, (char*)"productIdIndex", keyFields, NULL, true, false, NULL, 0, 0);
                       
	
	printf("\nTable Catalog created successfully");
	fflush(stdout);



	// open files of the new table to allow read + write
	SDBOpenAllDBFiles(userId, dbId );

	printf("\nTable Catalog is ready for read and write");
	fflush(stdout);


	// DML

	// INSERT


	unsigned int id;
	char productName[21];
	productName[20] =0;
	unsigned long long price;

	unsigned short retVal;

	id = 1;
	memcpy(productName, "TV                  ", 20);
	price = 100;

	// prepare id
	retVal = SDBPrepareNumberField(userId, dbId, tableId, 1, (void *)&id);

	// prepare product name
	retVal = SDBPrepareStrField(userId, dbId, tableId, 2, productName, 20);

	// prepare price
	retVal = SDBPrepareNumberField(userId, dbId, tableId, 3, (void *)&price);

	// Do the insert of 1 - TV - 100
	retVal = SDBInsertRow(userId, dbId, tableId, NULL, 0, 0);

	printf("\nRet value for insert is %u", retVal);
	fflush(stdout);

	//-->  Do the insert of 1 - TV - 100 AGAIN. 
	// note: retVal should be 1 (duplicate key)
	retVal = SDBInsertRow(userId, dbId, tableId, NULL, 0, 0);

	printf("\nRet value for insert is %u", retVal);
	fflush(stdout);

	id = 5;
	memcpy(productName, "BOOK                ", 20);
	price = 12;

	// prepare id
	retVal = SDBPrepareNumberField(userId, dbId, tableId, 1, (void *)&id);

	// prepare product name
	retVal = SDBPrepareStrField(userId, dbId, tableId, 2, productName, 20);

	// prepare price
	retVal = SDBPrepareNumberField(userId, dbId, tableId, 3, (void *)&price);

	// Do the insert of 1 - TV - 100
	retVal = SDBInsertRow(userId, dbId, tableId, NULL, 0, 0);

	printf("\nRet value for insert is %u", retVal);
	fflush(stdout);


	// QUERY

	// get a query manager. The query manager maintains the state of the query
	unsigned short queryMgrId = SDBGetQueryManagerId(userId);

	// delete old definitions maintained in the query manager
	SDBResetQuery(queryMgrId);

	// define a query to retrieve the data set 
	retVal = SDBDefineQuery(queryMgrId, dbId, indexId, (char*)"product id", NULL);
							  
	// set the cursor by the query definitions
	retVal = SDBPrepareQuery(queryMgrId, 0, 0, true) ;

	void *field;

	printf("\nID    Product Name          Price");
	printf("\n----  --------------------  ------------");

	while (!SDBNext(queryMgrId)){
		// retrieve the rows

		// get the product id
		field = SDBQueryCursorGetFieldByTableId(queryMgrId, tableId, 1);
		printf("\n%04u", *(unsigned int *)field);

		// get the product name
		field = SDBQueryCursorGetFieldByTableId(queryMgrId, tableId, 2);
		memcpy(productName, field, 20);
		printf("  %s", (char *)productName);

		// get the product id
		field = SDBQueryCursorGetFieldByTableId(queryMgrId, tableId, 3);
		printf("  %08u", *(unsigned int *)field);
	}

	SDBCommit(userId);

	printf("\nTest program completed... starting shutdown of Scaledb Engine");

	SDBGlobalEnd();

	return 0;
}
