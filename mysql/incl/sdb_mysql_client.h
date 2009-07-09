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
//////////////////////////////////////////////////////////////////////
//
//  File Name: sdb_mysql_client.h
//
//  Description: This file contains a thin wrapper around some basic
//  api function calls for the mysql C client api
//
//  Copyright: ScaleDB, Inc. 2008
//
//////////////////////////////////////////////////////////////////////

#ifndef _SDB_MYSQL_CLIENT_H
#define _SDB_MYSQL_CLIENT_H

#ifdef SDB_WINDOWS
#include <Ws2tcpip.h>
#endif

#include "mysql.h"


// it appears that this is required for making mysql client thread safe
// see http://lists.mysql.com/internals/36147
#define MYSQL_SERVER 1

class SdbMysqlClient {

public:

	SdbMysqlClient(char* host, char* user, char* password, char* socket, unsigned int port, unsigned char debugLevel);
	~SdbMysqlClient();
	
	int executeQuery(char* query, unsigned long length);

private:

	bool connect();
	int sendQuery(char* query, unsigned long length);

	MYSQL *mysql_;
	char *host_;
	char *user_;
	char *password_;
	char* socket_;
	unsigned int port_;

	bool connected_;

	unsigned char debugLevel_;

};

#endif 
