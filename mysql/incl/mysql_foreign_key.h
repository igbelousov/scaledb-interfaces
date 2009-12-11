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
/* Copyright (C) 2007  ScaleDB Inc.

Note that ScaleDB's header files must come before MySQL header file.
This is because we have STL header files which must be declared before C header files.
 */
#ifdef SDB_MYSQL

#ifndef _MYSQL_FOREIGN_KEY_H
#define _MYSQL_FOREIGN_KEY_H

#include "string.h"           
#include "mysql_priv.h"           // this must come second
#include <mysql/plugin.h>         // this must come third
#include "../../scaledb/incl/SdbStorageAPI.h"

#define ID_INTERFACE_FOREIGN_KEY		30000

class MysqlForeignKey {

public:

    // constructor and destructor 
	MysqlForeignKey();   
	~MysqlForeignKey();

	void setForeignKeyName(char* pForeignKeyName_);
    unsigned short setKeyNumber(KEY* keyInfo, unsigned short numOfKeys, char* pColumnNames);
    unsigned short getForeignKeyNameLength() { return ((unsigned short) strlen( pForeignKeyName_ )); }
	char** getIndexColumnNames() { return indexColumnNames; }
    void setParentTableName( char* pTableName );
	char* getParentTableName() { return pParentTableName_; }
    unsigned short getParentTableNameLength() { return ((unsigned short) strlen( pParentTableName_ )); }
    void setParentColumnNames( char* pOffset );
	char** getParentColumnNames() { return parentColumnNames_; }
    //bool findParentDesignatorName(MetaInfo* pMetaInfo);

private:
    char* pForeignKeyName_;
    unsigned short keyNumber;
    unsigned short numOfKeyFields;
    char* indexColumnNames[METAINFO_MAX_KEY_FIELDS];
    char* pParentTableName_;
    char* parentColumnNames_[METAINFO_MAX_KEY_FIELDS];
//  char* pParentDesignatorName_;    // no longer use this field

};

#endif   // _MYSQL_FOREIGN_KEY_H

#endif //  SDB_MYSQL

