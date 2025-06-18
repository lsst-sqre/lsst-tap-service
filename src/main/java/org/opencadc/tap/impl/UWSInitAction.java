/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
*  Government of Canada                 Gouvernement du Canada
*  National Research Council            Conseil national de recherches
*  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
*  All rights reserved                  Tous droits réservés
*
*  NRC disclaims any warranties,        Le CNRC dénie toute garantie
*  expressed, implied, or               énoncée, implicite ou légale,
*  statutory, of any kind with          de quelque nature que ce
*  respect to the software,             soit, concernant le logiciel,
*  including without limitation         y compris sans restriction
*  any warranty of merchantability      toute garantie de valeur
*  or fitness for a particular          marchande ou de pertinence
*  purpose. NRC shall not be            pour un usage particulier.
*  liable in any event for any          Le CNRC ne pourra en aucun cas
*  damages, whether direct or           être tenu responsable de tout
*  indirect, special or general,        dommage, direct ou indirect,
*  consequential or incidental,         particulier ou général,
*  arising from the use of the          accessoire ou fortuit, résultant
*  software.  Neither the name          de l'utilisation du logiciel. Ni
*  of the National Research             le nom du Conseil National de
*  Council of Canada nor the            Recherches du Canada ni les noms
*  names of its contributors may        de ses  participants ne peuvent
*  be used to endorse or promote        être utilisés pour approuver ou
*  products derived from this           promouvoir les produits dérivés
*  software without specific prior      de ce logiciel sans autorisation
*  written permission.                  préalable et particulière
*                                       par écrit.
*
*  This file is part of the             Ce fichier fait partie du projet
*  OpenCADC project.                    OpenCADC.
*
*  OpenCADC is free software:           OpenCADC est un logiciel libre ;
*  you can redistribute it and/or       vous pouvez le redistribuer ou le
*  modify it under the terms of         modifier suivant les termes de
*  the GNU Affero General Public        la "GNU Affero General Public
*  License as published by the          License" telle que publiée
*  Free Software Foundation,            par la Free Software Foundation
*  either version 3 of the              : soit la version 3 de cette
*  License, or (at your option)         licence, soit (à votre gré)
*  any later version.                   toute version ultérieure.
*
*  OpenCADC is distributed in the       OpenCADC est distribué
*  hope that it will be useful,         dans l’espoir qu’il vous
*  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
*  without even the implied             GARANTIE : sans même la garantie
*  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
*  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
*  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
*  General Public License for           Générale Publique GNU Affero
*  more details.                        pour plus de détails.
*
*  You should have received             Vous devriez avoir reçu une
*  a copy of the GNU Affero             copie de la Licence Générale
*  General Public License along         Publique GNU Affero avec
*  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
*  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
*                                       <http://www.gnu.org/licenses/>.
*
************************************************************************
*/

package org.opencadc.tap.impl;

import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.rest.InitAction;
import ca.nrc.cadc.uws.server.impl.InitDatabaseUWS;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author pdowler
 */
public class UWSInitAction extends InitAction {
    private static final Logger log = Logger.getLogger(UWSInitAction.class);

    public UWSInitAction() {
    }

    @Override
    public void doInit() {
        DataSource uws = null;
         try {
            uws = DBUtil.findJNDIDataSource("jdbc/uws");
            if (!schemaExists(uws, "uws")) {
                log.debug("uws schema does not exist, creating...");
                createSchema(uws, "uws");
                log.debug("uws schema created");
                // Continue with initialization only if the schema was just created
                InitDatabaseUWS uwsi = new InitDatabaseUWS(uws, null, "uws");
                uwsi.doInit();
                log.debug("init uws: OK");
            } else {
                log.debug("uws schema already exists");
                return; // Exit the method early if the schema already exists
            }
        } catch (Exception ex) {
            throw new RuntimeException("INIT FAIL", ex);
        } 
    }

    private boolean schemaExists(DataSource uws, String schemaName) throws SQLException {
        Connection conn = null;
        try {
            conn = uws.getConnection();
            DatabaseMetaData dbMetaData = conn.getMetaData();
            ResultSet rs = dbMetaData.getSchemas();
            while (rs.next()) {
                if (schemaName.equalsIgnoreCase(rs.getString("TABLE_SCHEM").trim())) {
                    return true;
                }
            }
        } catch (Exception ex) {
              throw new RuntimeException("Failed to check if schema exists", ex);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("Failed to close connection", e);
                }
            }
        }
        return false;
    }

    private void createSchema(DataSource uws, String schemaName) throws SQLException {
        Connection conn = null;
        try {
            conn = uws.getConnection();
            java.sql.Statement stmt = conn.createStatement();
            stmt.execute("CREATE SCHEMA " + schemaName);
        } catch (Exception ex) {
            throw new RuntimeException("Create Schema failed", ex);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("Failed to close connection", e);
                }
            }
        }
    }
}