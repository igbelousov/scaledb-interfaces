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

char* createBlobData(int dataSize);


 
int main(int argc, char** argv){

	// SETUP
	int dataSize = 1073741824;

	// test that the ini file is passed
	if (argc < 2){
		printf("\nusage: scaledb_test configfile\n");
		return -1;
	}

	// initialize engine using the ini file params
	printf("\nScaleDB: initializing the storage engine\n");
	if (SDBGlobalInit(argv[1])) {
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
	unsigned short dbId = SDBOpenDatabaseByName(userId, (char*)"test",0,0,0);

	printf("\nScaledb DBMS Test initialized successfully\n");
	fflush(stdout);

	// DDL

	printf("\nCreating ScaleDB User Table Catalog\n");
	// Create a table in database test.
	SDBLockMetaInfo(userId, dbId);
	unsigned short tableId = SDBCreateTable(userId, dbId, (char*)"Catalog",1, (char *)"Catalog", (char *)"Catalog", false, false, 0, 0);

	unsigned short fieldId;
	
	// create field - Movie id (4 bytes unsigned number)
	fieldId = SDBCreateField(userId, dbId, tableId, (char*)"Movie id", ENGINE_TYPE_U_NUMBER, 4, 0, NULL, false, 0);


	// create field Movie name (note: ENGINE_TYPE_STRING is space extended)
	fieldId = SDBCreateField(userId, dbId, tableId, (char*)"Movie Name", ENGINE_TYPE_STRING, 20, 0, NULL, false, 0);

	// create field Movie
	fieldId = SDBCreateField(userId, dbId, tableId, (char*)"Movie", ENGINE_TYPE_STRING, 0, dataSize, NULL, false, 0);
	
	// Create index over product id
	char *keyFields[2];				// maintains an array of names of fields that make the key
	keyFields[0] = (char*)"Movie id";	// first field in the key
	keyFields[1] = 0;

	// note: productIdIndex is a unique index
	unsigned short indexId = SDBCreateIndex(userId, dbId, tableId, (char*)"MovieIdIndex", keyFields, NULL,
											true, false, NULL, 0, 0, false);
	SDBOpenTable(userId, dbId, (char *)"Catalog", 0, false);
	SDBCommit(userId);
	
	printf("\nTable Catalog created successfully\n");
	fflush(stdout);


	printf("\nTable Catalog is ready for read and write\n");
	fflush(stdout);

	// DML

	// INSERT

	unsigned int id;
	char MovieName[21];
	MovieName[20] =0;
	int numberOfInserts = 100000;
	int commitCounter = 0;

	char *movie;
	unsigned short retVal;

	id = 1;
	movie = createBlobData(dataSize);


	while( id <= numberOfInserts) {

		// prepare id
		retVal = SDBPrepareNumberField(userId, dbId, tableId, 1, (void *)&id);

		// prepare movie Name
		sprintf(MovieName, "Home Alone%d", id);
		retVal = SDBPrepareStrField(userId, dbId, tableId, 2, MovieName, 20, 0);

		// prepare Movie data
		retVal = SDBPrepareVarField(userId, dbId, tableId, 3, movie, dataSize, 0, false);

		// Do the insert of the Movie
		retVal = SDBInsertRow(userId, dbId, tableId, 0, 0, 0, 0);

		// increment the counter
		id++;
		if (commitCounter++ == 1000) {
			SDBCommit(userId);
			commitCounter = 0;
		}

	}

	//commit the remaining data, this is just, in case if you change the
	//numberOfInsert from 100K to some other value.
	if (commitCounter != 0) {
		SDBCommit(userId);
	}

	printf("\nInserterted  %d records\n", numberOfInserts);
	fflush(stdout);

	// QUERY

	// get a query manager. The query manager maintains the state of the query
	unsigned short queryMgrId = SDBGetQueryManagerId(userId);

	// delete old definitions maintained in the query manager
	SDBResetQuery(queryMgrId);

	// define a query to retrieve the data set 
	retVal = SDBDefineQuery(queryMgrId, dbId, indexId, (char*)"Movie id", NULL);
							  
	// set the cursor by the query definitions
	retVal = SDBPrepareQuery(userId, queryMgrId, 0, 0, true,retVal) ;

	void *field;

	printf("\nID    Movie Name            Movie");
	printf("\n----  --------------------  ------------");

	while ((SDBQueryCursorNext(userId, queryMgrId, SDB_COMMAND_SELECT)== 0)){
		// retrieve the rows

		// get the product id
		field = SDBQueryCursorGetFieldByTableId(queryMgrId, tableId, 1);
		printf("\n%04u", *(unsigned int *)field);

		// get the product name
		field = SDBQueryCursorGetFieldByTableId(queryMgrId, tableId, 2);
		memcpy(MovieName, field, 20);
		printf("  %s", (char *)MovieName);

		// get the movie
		unsigned int bytes;
		field = SDBQueryCursorGetFieldByTableVarData(queryMgrId, tableId, 3, &bytes);
		memcpy(movie, field, bytes);
		for (int i =0; i < 10; i++) {
			printf("  %c", movie[i]);
		}
	}

	//delete the table
	printf("\n");
	printf("\nDeleting the User Table Catalog....\n");

	SDBDeleteTable(userId, dbId, (char *)"Catalog", NULL, 0, 0);


	printf("\nTest program completed... starting shutdown of Scaledb Engine\n");

	SDBGlobalEnd();
	free(movie);

	return 0;
}

char* createBlobData(int dataSize) {
	char* movie = (char *)malloc(dataSize * sizeof(char));
	memcpy(movie,"0123456789", 10);

	for(int i = 11;i < 1048576 * 3; i++) {
		movie[i] = i;
	}
	return movie;

}
