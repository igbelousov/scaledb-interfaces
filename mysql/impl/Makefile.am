#   Copyright (C) 2009 - ScaleDB Inc.
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; version 2 of the License.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA 

MYSQLDATAdir =          $(localstatedir)
MYSQLSHAREdir =         $(pkgdatadir)
MYSQLBASEdir=           $(prefix)
MYSQLLIBdir=            $(pkglibdir)
pkgplugindir=      		$(pkglibdir)/plugin
INCLUDES =              -I$(top_builddir)/include \
						-I$(top_srcdir)/include \
						-I$(top_srcdir)/regex \
						-I$(top_srcdir)/sql \
						-I$(srcdir)

LDADD =					
WRAPLIBS =
DEFS = -DMYSQL_SERVER -D_LIB -DSDB_LINUX -DSDB_LINUX_NEW -DSDB_MYSQL -DSDB_DEBUG @DEFS@

noinst_HEADERS= interface/mysql/incl/sdb_mysql_client.h \
			interface/mysql/incl/mysql_txn.h \
			interface/mysql/incl/mysql_foreign_key.h \
			interface/mysql/incl/ha_scaledb.h \
			interface/scaledb/incl/SdbCommon.h \
			interface/scaledb/incl/SdbStorageAPI.h 

libscaledb_a_SOURCES= interface/mysql/impl/mysql_foreign_key.cpp \
			interface/mysql/impl/mysql_txn.cpp \
			interface/mysql/impl/ha_scaledb.cpp \
			interface/mysql/impl/sdb_mysql_client.cpp

# Static load
EXTRA_LIBRARIES =	libscaledb.a
noinst_LIBRARIES =	@plugin_scaledb_static_target@
libscaledb_a_CXXFLAGS =	$(AM_CFLAGS)
libscaledb_a_CFLAGS =  $(AM_CFLAGS)

# Dynamic load
EXTRA_LTLIBRARIES = ha_scaledb.la
pkgplugin_LTLIBRARIES = @plugin_scaledb_shared_target@
ha_scaledb_la_LDFLAGS = -module -rpath $(pkgplugindir)
ha_scaledb_la_CXXFLAGS = $(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
ha_scaledb_la_CFLAGS =  $(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
ha_scaledb_la_SOURCES = $(libscaledb_a_SOURCES)

EXTRA_DIST = plug.in

%::SCCS/s.%
