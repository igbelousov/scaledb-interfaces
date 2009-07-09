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


#include "../../../interface/scaledb/incl/SdbStorageAPI.h"
#include <stdio.h>


int main(int argc, char** argv){

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


	// Create a table in database test.
	unsigned short tableId = SDBCreateTable(userId, dbId, (char*)"foo(3)");

	unsigned short fieldId = SDBCreateField(userId, dbId, tableId, (char*)"id", ENGINE_TYPE_S_NUMBER, 0, 0, NULL, false, 0);
	
	SDBOpenAllDBFiles(userId, dbId );

	int x, retVal;
	x = 49;
	retVal = SDBPrepareNumberField(userId, dbId, tableId, 1, &x);
	retVal = SDBInsertRow(userId, dbId, tableId, NULL, 0, 0);

	x = 50;
	retVal = SDBPrepareNumberField(userId, dbId, tableId, 1, &x);
	retVal = SDBInsertRow(userId, dbId, tableId, NULL, 0, 0);
	
	x = 51;
	retVal = SDBPrepareNumberField(userId, dbId, tableId, 1, &x);
	retVal = SDBInsertRow(userId, dbId, tableId, NULL, 0, 0);

	SDBCommit(userId);

	SDBGlobalEnd();

	return 0;
}
