/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2020.                            (c) 2020.
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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.AccessURL;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.CapabilitiesWriter;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Interface;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.rest.SyncOutput;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Set;
import java.util.TreeSet;
import org.opencadc.tap.impl.CapInitAction;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class CapGetAction extends RestAction {
    private static final Logger log = Logger.getLogger(CapGetAction.class);
    private static final String baseURL = System.getProperty("base_url");
    private static final String pathPrefix = System.getProperty("path_prefix");

    /**
     * Enable transformation of the capabilities template (default: true). Subclasses
     * may disable this according to some policy. The current transform is to change
     * the host name in every accessURL in the capabilities to match the host name used
     * in the request. This works fine in most cases but would not work
     * if some accessURL(s) within an application are deployed on a different host.
     * For example, if the VOSI-availability endpoint is deployed on an separate host
     * so it can probe the service from the outside, then capabilities transform
     * would need to be disabled.
     */
    protected boolean doTransform = true;
    
    /**
     * Default constructor for CapGetAction.
     * 
     */
    public CapGetAction() {
        super();
    }

    @Override
    protected String getServerImpl() {
        return CapInitAction.getVersion(componentID);
    }
    
    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }

    @Override
    public void doAction() throws Exception {
        if (CapInitAction.getAuthRequired(componentID)) {
            AuthMethod am = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
            if (am == null || am.equals(AuthMethod.ANON)) {
                throw new NotAuthenticatedException("permission denied");
            }
        }
        
        Capabilities caps = CapInitAction.getTemplate(componentID);

        log.debug("transformAccessURL=" + doTransform);
        
        if (doTransform) {
            transform(caps);
        }
        
        doOutput(caps, syncOutput);
        logInfo.setSuccess(true);
    }
    
    // transform all accessURL so the hostname and context path match that used to invoke
    // the /capabilities endpoint
    private void transform(Capabilities caps) throws MalformedURLException {
        log.debug("context: " + syncInput.getContextPath());
        log.debug("component: " + syncInput.getComponentPath());
        
        String hostname = new URL(syncInput.getRequestURI()).getHost();
        
        // find context path in the template using capabilities endpoint
        Capability cap = caps.findCapability(Standards.VOSI_CAPABILITIES);
        URL capURL = cap.getInterfaces().get(0).getAccessURL().getURL();
        String capPath = capURL.getPath();
        String basePath = capURL.getPath().substring(0, capPath.indexOf("/capabilities")); // chop
        
        // capabilities in the request
        String actualPath = syncInput.getContextPath() + syncInput.getComponentPath();
        actualPath = actualPath.substring(0, actualPath.indexOf("/capabilities")); // chop
        
        log.debug("transform: basePath in template: " + basePath + " actualPath: " + actualPath);
        for (Capability c : caps.getCapabilities()) {
            for (Interface i : c.getInterfaces()) {
                AccessURL u = i.getAccessURL();
                URL url = u.getURL();
                String path = url.getPath();
                String npath = path.replace(basePath, pathPrefix);
                URL nurl = new URL(url.getProtocol(), hostname, npath);
                u.setURL(nurl);
                log.debug("transform: " + url + " -> " + nurl);
            }
        }
    }

    private void doOutput(Capabilities caps, SyncOutput out) throws IOException {
        out.setHeader(HttpTransfer.CONTENT_TYPE, "text/xml");
        out.setCode(200);
        CapabilitiesWriter w = new CapabilitiesWriter();
        w.write(caps, syncOutput.getOutputStream());
    }
    
    
    private void injectAuthProviders(Capabilities caps) throws IOException {
        Set<URI> sms = new TreeSet<>();
        for (Capability cap : caps.getCapabilities()) {
            for (Interface i : cap.getInterfaces()) {
                for (URI s : i.getSecurityMethods()) {
                    sms.add(s);
                }
            }
            
        }
        log.debug("found " + sms.size() + " unique SecurityMethod(s)");
        if (sms.isEmpty()) {
            return;
        }

        LocalAuthority loc = new LocalAuthority();
        RegistryClient reg = new RegistryClient();
        for (URI sm : sms) {
            URI resourceID = loc.getServiceURI(sm.toASCIIString());
            try {
                if (resourceID != null) {
                    Capabilities srv = reg.getCapabilities(resourceID);
                    if (srv != null) {
                        Capability auth = srv.findCapability(sm);
                        if (auth != null) {
                            caps.getCapabilities().add(auth);
                        } else {
                            log.debug("not found: " + sm + " in " + resourceID);
                        }
                    } else {
                        log.debug("not found: " + resourceID + " capabilities");
                    }
                } else {
                    log.debug("not found: " + sm);
                }
            } catch (ResourceNotFoundException ex) {
                log.warn("failed to find auth service: " + resourceID + "cause: " + ex);
            }
        }
    }
    
}
