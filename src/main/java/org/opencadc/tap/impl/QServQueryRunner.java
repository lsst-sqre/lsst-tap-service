/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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
*  the GNU Affero General Public        la “GNU Affero General Public
*  License as published by the          License” telle que publiée
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
*  $Revision: 4 $
*
************************************************************************
*/

package org.opencadc.tap.impl;
import org.apache.log4j.Logger;
import ca.nrc.cadc.tap.QueryRunner;

/**
 * Implementation of the JobRunner interface from the cadcUWS framework. This is the
 * main class that implements TAP semantics; it is usable with both the async and sync
 * servlet configurations from cadcUWS.
 * This class dynamically loads and uses implementation classes as described in the
 * package documentation. This allows one to control the behavior of several key components:
 * query processing, upload support, and writing the result-set to the output file format.
 * In addition, this class uses JDNI to find java.sql.DataSource instances for
 * executing database statements.
 * A datasource named jdbc/tapuser is required; this datasource
 * is used to query the TAP_SCHEMA and to run user-queries. The connection(s) provided by this
 * datasource must have read permission to the TAP_SCHEMA and all tables described within the
 * TAP_SCHEMA.
 * A datasource named jdbc/tapuploadadm is optional; this datasource is used to create tables
 * in the TAP_UPLOAD schema and to populate these tables with content from uploaded tables. If this
 * datasource is provided, it is passed to the UploadManager implementation. For uploads to actually work,
 * the connection(s) provided by the datasource must have create table permission in the current database and
 * TAP_UPLOAD schema.
 *
 * @author stvoutsin
 */
public class QServQueryRunner extends QueryRunner
{
    private static final Logger log = Logger.getLogger(QServQueryRunner.class);

    public QServQueryRunner() { 
        super(true);
    }

    @Override
    public void run() {
        log.info("QservQueryRunner starting execution");
        super.run();
        log.info("QServQueryRunner finished execution");
    }

}
