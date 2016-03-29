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
//  File Name: mysql_foreign_key.cpp
//
//  Description: This file contains a single foreign key information.
//     Foreign Key is defined in CREATE TABLE statement.
//
//  Version: 1.0
//
//  Copyright: ScaleDB, Inc. 2007
//
//  History: 10/12/2007  RCH   converted from Java.
//
//
//////////////////////////////////////////////////////////////////////
#ifdef SDB_MYSQL
#include "key.h"                                // key_copy
#include "../incl/mysql_foreign_key.h"
#ifdef __DEBUG_CLASS_CALLS
#include "../../../cengine/engine_util/incl/debug_class.h"
#endif
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000

#endif
bool isSeparator(char c) {
	char separatorArray[] = METAINFO_TOKEN_SEPARATORS;
	bool boolValue = false;

	for (unsigned short i = 0; i < (unsigned short) strlen(separatorArray); ++i) {
		if (separatorArray[i] == c)
			boolValue = true;
	}

	return boolValue;
}

MysqlForeignKey::MysqlForeignKey() {
#ifdef __DEBUG_CLASS_CALLS
	DebugClass::countClassConstructor("MysqlForeignKey");
#endif
	keyNumber = -1;
	numOfKeyFields = 0;
	pForeignKeyName_ = NULL;
	pParentTableName_ = NULL;

	for (unsigned short i = 0; i < METAINFO_MAX_KEY_FIELDS; ++i) {
		indexColumnNames[i] = NULL;
		parentColumnNames_[i] = NULL;
	}

}

MysqlForeignKey::~MysqlForeignKey() {
#ifdef __DEBUG_CLASS_CALLS
	DebugClass::countClassDestructor("MysqlForeignKey");
#endif
	if (pForeignKeyName_)
		FREE_MEMORY(pForeignKeyName_);
	if (pParentTableName_)
		FREE_MEMORY(pParentTableName_);

	for (unsigned short i = 0; i < METAINFO_MAX_KEY_FIELDS; ++i) {
		if (indexColumnNames[i])
			FREE_MEMORY(indexColumnNames[i]);
		if (parentColumnNames_[i])
			FREE_MEMORY(parentColumnNames_[i]);
	}
}

// get next token from character string cstr, and return the new char pointer position
// This utility function can get next MySQL token from character string cstr, and return the new char pointer position.
// Make it static so that it can be called without instantiating the object.
char* MysqlForeignKey::getNextToken(char* token, char* cstr) {
	unsigned short i = 0;
	unsigned short j = 0;

	while ((cstr[i] == ' ') && (cstr[i] != '\0'))
		++i; // remove the preceding blanks

	if (cstr[i] == '`') {
		++i; // skip the left backtick
		while (cstr[i] != '`') { // the token consists of all characters between two backticks
			token[j++] = cstr[i++];
		};

		++i; // skip the right backtick
	} else {
		while (!isSeparator(cstr[i]) && (cstr[i] != '\0')) {
			token[j] = cstr[i];
			++i;
			++j;
		}
	}

	return cstr + i;
}

// save foreign key name and return the length of foreign key index name
unsigned short MysqlForeignKey::setForeignKeyName(char* pForeignKeyName,
        char* pUserTableName/*=NULL*/, int fkNum/*=0*/) {
	unsigned short keyNameLen = 0;
	char keyName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };

	getNextToken(keyName, pForeignKeyName);
	keyNameLen = (unsigned short) strlen(keyName);
	if (keyNameLen == 0) { // index name is NOT specified
		pForeignKeyName_ = SDBUtilFindDesignatorName(pUserTableName, "sdbfk", fkNum, true, NULL, 0);
	} else
		// index name is specified
		pForeignKeyName_ = SDBUtilDuplicateString(keyName);

	return keyNameLen;
}

// Based on foreignKeyName, we need to search all the keys to find which key number.
// And then we save keyNumber, numOfKeyFields, and index column names.

unsigned short MysqlForeignKey::setKeyNumber(KEY* keyInfo, unsigned short numOfKeys, char* pOffset) {
	unsigned short i;
	KEY* pKey;

	for (i = 0; i < numOfKeys; ++i) {
		pKey = keyInfo + i;
		if (SDBUtilCompareStrings(pForeignKeyName_, pKey->name, true) || SDBUtilCompareStrings(pForeignKeyName_, pKey->key_part->field->field_name,true)) { //check both the index name and the alias.
			break;
		}
	}

	if (i < numOfKeys) {
		keyNumber = i;

		numOfKeyFields = (unsigned short) pKey->user_defined_key_parts;

		KEY_PART_INFO* pKeyPart;
		for (unsigned short j = 0; j < numOfKeyFields; ++j) {
			pKeyPart = pKey->key_part + j;
			char* pColName = SDBUtilDuplicateString((char*) pKeyPart->field->field_name);
			indexColumnNames[j] = pColName;
		}

	} else { // MySQL does not take foreiegn key as a regular key/index
		unsigned short j = 0;
		while (pOffset[0] != ')') {
			char colName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };

			getNextToken(colName, pOffset);
			char* pColName = SDBUtilDuplicateString(colName);
			indexColumnNames[j] = pColName;
			pOffset = pOffset + strlen(pColName);
			char* pComma = strstr(pOffset, ",");
			char* pRightParen = strstr(pOffset, ")");
			if (pComma && pRightParen)
				pOffset = (pComma < pRightParen) ? pComma : pRightParen;
			else
				pOffset = pRightParen;

			if (pOffset[0] == ',')
				++pOffset; // skip comma in order to fetch next token

			++j;
		}

		numOfKeyFields = j;

		// now try to find the key number
		// by searching all keys in mysql table struct for a match against this FK as defined by parsing SQL text
		for (i = 0; i < numOfKeys; ++i) {
			pKey = keyInfo + i;

			if (pKey->user_defined_key_parts >= numOfKeyFields) {

				KEY_PART_INFO* pKeyPart;
				unsigned short k;

				// test this key to see if key parts match
				bool foundMatch = false;
				for (k = 0; k < numOfKeyFields; ++k) {
					pKeyPart = pKey->key_part + k;
					foundMatch = true;
					if (!SDBUtilCompareStrings(pKeyPart->field->field_name, indexColumnNames[k],
					        true)) {
						foundMatch = false;
						break;
					}
				} // end loop key parts

				if (foundMatch) {
					// found a match
					this->keyNumber = i;
					break;
				}
			}
		}// end loop keys in the table
	}

	return keyNumber;
}

void MysqlForeignKey::setParentTableName(char* pTableName) {
	char tblName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };

	getNextToken(tblName, pTableName);
	pParentTableName_ = SDBUtilDuplicateString(tblName);
}

void MysqlForeignKey::setParentColumnNames(char* pOffset) {
	for (unsigned short i = 0; i < numOfKeyFields; ++i) {
		char colName[METAINFO_MAX_IDENTIFIER_SIZE] = { 0 };

		getNextToken(colName, pOffset);
		char* pIColName = SDBUtilDuplicateString(colName);
		parentColumnNames_[i] = pIColName;

		if (i < numOfKeyFields - 1)
			pOffset = strstr(pOffset, ",") + 1; // points to position after ','
	}

}

/* TBD: We will use the following method after we fix defineForeignKey()
 bool MysqlForeignKey::findParentDesignatorName(MetaInfo* pMetaInfo) {
 char* pFoundName = pMetaInfo->getDesignatorNameByTableAndColumnNames( pParentTableName_, pParentColumnNames_, numOfKeyFields);

 if ( pFoundName == NULL )
 return false;

 pParentDesignatorName_ = DataUtil::getStrInLower( pFoundName );
 return true;
 }
 */

#endif	//  SDB_MYSQL
