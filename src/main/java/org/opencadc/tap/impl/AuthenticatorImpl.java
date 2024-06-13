package org.opencadc.tap.impl;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpRequest;
import java.net.URI;
import java.security.AccessControlException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;

import ca.nrc.cadc.auth.Authenticator;
import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthorizationTokenPrincipal;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.NumericPrincipal;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.log4j.Logger;

/**
 * @deprecated This class is deprecated and will be removed in future releases.
 * The TAP Service now uses IdentityManager for authentication, available in the opencadc library
 * 
 * @author cbanek
 */
@Deprecated
public class AuthenticatorImpl implements Authenticator
{
    private static final Logger log = Logger.getLogger(AuthenticatorImpl.class);

    // Size of the token cache is read from the maxTokenCache property, with
    // a default of 1000 tokens cached.
    private static final int maxTokenCache = Integer.getInteger("maxTokenCache", 1000);

    private static final String gafaelfawr_url = System.getProperty("gafaelfawr_url");

    private static final HttpClient client = HttpClient.newHttpClient();

    private static final ConcurrentHashMap<String,TokenInfo> tokenCache = new ConcurrentHashMap<>();

    private final class TokenInfo
    {
        public final String username;
        public final int uid;

        public TokenInfo(String username, int uid)
        {
            this.username = username;
            this.uid = uid;
        }
    }

    public AuthenticatorImpl()
    {
    }

    public Subject validate(Subject subject) throws AccessControlException {
        log.debug("Subject to augment starts as: " + subject);

        // Check if the cache is too big, and if so, clear it out.
        if (tokenCache.size() > maxTokenCache) {
            tokenCache.clear();
        }

        List<Principal> addedPrincipals = new ArrayList<Principal>();
        AuthorizationTokenPrincipal tokenPrincipal = null;

        for (Principal principal : subject.getPrincipals()) {
            if (principal instanceof AuthorizationTokenPrincipal) {
                tokenPrincipal = (AuthorizationTokenPrincipal) principal;
                TokenInfo tokenInfo = null;

                for (int i = 1; i < 5 && tokenInfo == null; i++) {
                    try {
                        tokenInfo = getTokenInfo(tokenPrincipal.getHeaderValue());
                    } catch (IOException|InterruptedException e) {
                        log.warn("Exception thrown while getting info from Gafaelfawr");
                        log.warn(e);
                    }
                }

                if (tokenInfo != null) {
                    X500Principal xp = new X500Principal("CN=" + tokenInfo.username);
                    addedPrincipals.add(xp);

                    HttpPrincipal hp = new HttpPrincipal(tokenInfo.username);
                    addedPrincipals.add(hp);

                    UUID uuid = new UUID(0L, (long) tokenInfo.uid);
                    NumericPrincipal np = new NumericPrincipal(uuid);
                    addedPrincipals.add(np);
                }
                else {
                    log.error("Gave up retrying user-info requests to Gafaelfawr");
                }
            }
        }

        if (tokenPrincipal != null) {
            subject.getPrincipals().remove(tokenPrincipal);
        }

        subject.getPrincipals().addAll(addedPrincipals);
        subject.getPublicCredentials().add(AuthMethod.TOKEN);

        log.debug("Augmented subject is " + subject);
        return subject;
    }

    // Here we could check the token again, but gafaelfawr should be
    // doing that for us already by the time it gets to us.  So for
    // this layer, we just let this go through.
    public Subject augment(Subject subject) {
        return subject;
    }

    private TokenInfo getTokenInfo(String token) throws IOException, InterruptedException {
        // If the request has gotten this far, the token has already
        // been checked upstream, so we know it's valid, we just need
        // to determine the uid and the username.
        if (!tokenCache.containsKey(token)) {
            HttpRequest request = HttpRequest.newBuilder(URI.create(gafaelfawr_url))
                .header("Accept", "application/json")
                .header("Authorization", token)
                .build();

            HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
            String body = response.body();

            Gson gson = new Gson();
            JsonObject authData = gson.fromJson(body, JsonObject.class);
            String username = authData.getAsJsonPrimitive("username").getAsString();
            int uid = authData.getAsJsonPrimitive("uid").getAsInt();

            // Insert the info into the cache here since we retrieved it.
            tokenCache.put(token, new TokenInfo(username, uid));
        }

        return tokenCache.get(token);
    }
}
