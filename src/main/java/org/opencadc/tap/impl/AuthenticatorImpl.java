package org.opencadc.tap.impl;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;

import ca.nrc.cadc.auth.Authenticator;
import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.NumericPrincipal;
import ca.nrc.cadc.auth.BearerTokenPrincipal;

import org.apache.log4j.Logger;

import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.JWT;

/**
 * Implementes the Authenticator for processing LSST JWT tokens,
 * and using them to authenticate against the TAP service. Each
 * JWT token is self contained and self signed, having the user
 * and group information contained.
 *
 * @author cbanek
 */
public class AuthenticatorImpl implements Authenticator
{
    private static final Logger log = Logger.getLogger(AuthenticatorImpl.class);

    public AuthenticatorImpl()
    {
    }

    public Subject getSubject(Subject subject)
    {
        log.debug("getSubject subject starts as: " + subject);

        try {
            List<Principal> addedPrincipals = new ArrayList<Principal>();

            for (Principal principal : subject.getPrincipals()) {
                if (principal instanceof BearerTokenPrincipal) {
                    BearerTokenPrincipal tp = (BearerTokenPrincipal) principal;
                    DecodedJWT jwt = JWT.decode(tp.getName());

                    Map<String, Claim> claims = jwt.getClaims();
                    String uid = jwt.getClaim("uid").asString();
                    Integer uidNumber = Integer.valueOf(jwt.getClaim("uidNumber").asString());
                    String[] scopes = jwt.getClaim("scope").asString().split(" ");

                    log.debug("Found this in the token:");
                    log.debug("uid = " + uid);
                    log.debug("uidNumber = " + uidNumber);

                    for (String scope : scopes) {
                        log.debug("scope = " + scope);
                    }

                    X500Principal xp = new X500Principal("CN=" + uid);
                    addedPrincipals.add(xp);

                    HttpPrincipal hp = new HttpPrincipal(uid);
                    addedPrincipals.add(hp);

                    UUID uuid = new UUID(0L, (long) uidNumber);
                    NumericPrincipal np = new NumericPrincipal(uuid);
                    addedPrincipals.add(np);
                }
            }

            subject.getPrincipals().addAll(addedPrincipals);
            subject.getPublicCredentials().add(AuthMethod.TOKEN);
        } catch (JWTDecodeException exception) {
            log.debug("Exception decoding JWT: " + exception);
        }

        log.debug("getSubject's new subject is " + subject);
        return subject;
    }
}
