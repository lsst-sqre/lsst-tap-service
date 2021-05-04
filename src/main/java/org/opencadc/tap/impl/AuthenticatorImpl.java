package org.opencadc.tap.impl;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpRequest;
import java.net.URI;
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

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.log4j.Logger;

/**
 * Implementes the Authenticator for processing Gafaelfawr auth,
 * and using it to authenticate against the TAP service.
 *
 * The token in the authorization header is used to make a call
 * to Gafaelfawr to retrieve details such as the uid and uidNumber.
 *
 * @author cbanek
 */
public class AuthenticatorImpl implements Authenticator
{
    private static final Logger log = Logger.getLogger(AuthenticatorImpl.class);

    private static final String gafaelfawr_url = System.getProperty("gafaelfawr_url");

    private static final HttpClient client = HttpClient.newHttpClient();

    public AuthenticatorImpl()
    {
    }

    public Subject getSubject(Subject subject)
    {
        log.debug("getSubject subject starts as: " + subject);

        List<Principal> addedPrincipals = new ArrayList<Principal>();

        for (Principal principal : subject.getPrincipals()) {
            if (principal instanceof BearerTokenPrincipal) {
                BearerTokenPrincipal tp = (BearerTokenPrincipal) principal;

                HttpRequest request = HttpRequest.newBuilder(
                        URI.create(gafaelfawr_url))
                    .header("Accept", "application/json")
                    .header("Authorization", "bearer " + tp.getName())
                    .build();

                try {
                    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
                    String body = response.body();

                    Gson gson = new Gson();
                    JsonObject authData = gson.fromJson(body, JsonObject.class);

                    String username = authData.getAsJsonPrimitive("username").getAsString();
                    int uid = authData.getAsJsonPrimitive("uid").getAsInt();

                    X500Principal xp = new X500Principal("CN=" + username);
                    addedPrincipals.add(xp);

                    HttpPrincipal hp = new HttpPrincipal(username);
                    addedPrincipals.add(hp);

                    UUID uuid = new UUID(0L, (long) uid);
                    NumericPrincipal np = new NumericPrincipal(uuid);
                    addedPrincipals.add(np);
                } catch (InterruptedException e) {
                    log.warn("InterruptedException thrown while getting info from Gafaelfawr");
                    log.warn(e);
                } catch (IOException e) {
                    log.warn("IOException while getting info from Gafaelfawr");
                    log.warn(e);
                }
            }
        }

        subject.getPrincipals().addAll(addedPrincipals);
        subject.getPublicCredentials().add(AuthMethod.TOKEN);

        log.debug("getSubject's new subject is " + subject);
        return subject;
    }
}
