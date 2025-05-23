/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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
************************************************************************
*/

package org.opencadc.tap.impl;

import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.CapabilitiesReader;
import ca.nrc.cadc.rest.InitAction;
import ca.nrc.cadc.rest.Version;
import ca.nrc.cadc.util.StringUtil;

import java.io.StringReader;
import java.net.URL;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.log4j.Logger;

/**
 * InitAction implementation for VOSI-capabilities from template xml file.
 * 
 * @author pdowler
 */
public class CapInitAction extends InitAction {
    private static final Logger log = Logger.getLogger(CapInitAction.class);
    /**
     * Default constructor for CapInitAction.
     * 
     */
    public CapInitAction() { 
        super();
    }

    static Capabilities getTemplate(String componentID) {
        String jndiKey = componentID + ".cap-template";
        try {
            log.debug("retrieving capabilities template via JNDI: " + jndiKey);
            Context initContext = new InitialContext();
            String tmpl = (String) initContext.lookup(jndiKey);
            CapabilitiesReader cr = new CapabilitiesReader(false); // validated in doInit
            StringReader sr = new StringReader(tmpl);
            Capabilities caps = cr.read(sr);
            return caps;
        } catch (Exception ex) {
            throw new IllegalStateException("failed to find template via JNDI: init failed", ex);
        }
    }
    
    static boolean getAuthRequired(String componentID) {
        String jndiKey = componentID + ".authRequired";
        try {
            log.debug("retrieving authRequired via JNDI: " + jndiKey);
            Context initContext = new InitialContext();
            Boolean authRequired = (Boolean) initContext.lookup(jndiKey);
            if (authRequired == null) {
                return false;
            }
            return authRequired;
        } catch (Exception ex) {
            throw new IllegalStateException("failed to find authRequired via JNDI: init failed", ex);
        }
    }
    
    static String getVersion(String componentID) {
        String jndiKey = componentID + ".version";
        try {
            log.debug("retrieving version via JNDI: " + jndiKey);
            Context initContext = new InitialContext();
            String version = (String) initContext.lookup(jndiKey);
            return version;
        } catch (Exception ex) {
            throw new IllegalStateException("failed to find version via JNDI: init failed", ex);
        }
    }
    
    @Override
    public void doInit() {

        final Context initContext;
        try {
            initContext = new InitialContext();
        } catch (NamingException ex) {
            throw new IllegalStateException("failed to find JDNI InitialContext", ex);
        }

        String jndiKey = componentID + ".cap-template";
        String str = initParams.get("input");
        
        log.debug("doInit: static capabilities: " + str);
        try {
            URL resURL = super.getResource(str);
            String tmpl = StringUtil.readFromInputStream(resURL.openStream(), "UTF-8");
            
            // validate
            CapabilitiesReader cr = new CapabilitiesReader();
            cr.read(tmpl);
            try {
                log.debug("unbinding possible existing template");
                initContext.unbind(jndiKey);
            } catch (NamingException e) {
                log.debug("no previously bound template, continuting");
            }
            initContext.bind(jndiKey, tmpl);
            log.info("doInit: capabilities template=" + str + " stored via JNDI: " + jndiKey);
        } catch (Exception ex) {
            throw new IllegalArgumentException("CONFIG: failed to read capabilities template: " + str, ex);
        }
        
        try {
            String authRequired = initParams.get("authRequired");
            jndiKey = componentID + ".authRequired";
            try {
                log.debug("unbinding possible authRequired value");
                initContext.unbind(jndiKey);
            } catch (NamingException e) {
                log.debug("no previously bound value, continuting");
            }
            if ("true".equals(authRequired)) {
                initContext.bind(jndiKey, Boolean.TRUE);
                log.info("doInit: authRequired=true stored via JNDI: " + jndiKey);
            } else {
                initContext.bind(jndiKey, Boolean.FALSE);
                log.info("doInit: authRequired=false stored via JNDI: " + jndiKey);
            }
        } catch (Exception ex) {
            throw new IllegalArgumentException("CONFIG: failed to set authRequired flag", ex);
        }
        
        try {
            Version version = getLibraryVersion(CapInitAction.class);
            
            jndiKey = componentID + ".version";
            try {
                log.debug("unbinding possible version value");
                initContext.unbind(jndiKey);
            } catch (NamingException e) {
                log.debug("no previously bound value, continuting");
            }
            initContext.bind(jndiKey, version.getMajorMinor());
            log.info("doInit: version=" + version + " stored via JNDI: " + jndiKey);
        } catch (Exception ex) {
            throw new IllegalArgumentException("CONFIG: failed to set version flag", ex);
        }
    }
}