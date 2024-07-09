package com.signomix.sentinel.domain;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.signomix.common.Token;
import com.signomix.common.User;
import com.signomix.common.db.AuthDao;
import com.signomix.common.db.AuthDaoIface;
import com.signomix.common.db.UserDao;
import com.signomix.common.db.UserDaoIface;

import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

/**
 * Klasa zawierająca logikę biznesową dotyczącą autoryzacji.
 * 
 * @author Grzegorz
 */
@ApplicationScoped
public class AuthLogic {
    private static final Logger LOG = Logger.getLogger(AuthLogic.class);

    /*
     * @ConfigProperty(name = "signomix.app.key", defaultValue = "not_configured")
     * String appKey;
     * 
     * @ConfigProperty(name = "signomix.auth.host", defaultValue = "not_configured")
     * String authHost;
     */
    // TODO: move to config
    private long sessionTokenLifetime = 30; // minutes
    private long permanentTokenLifetime = 10 * 365 * 24 * 60; // 10 years in minutes

    @Inject
    @DataSource("auth")
    AgroalDataSource authDataSource;

    @Inject
    @DataSource("user")
    AgroalDataSource userDataSource;
    
    @Inject
    @DataSource("oltp")
    AgroalDataSource tsDs;

    AuthDaoIface authDao;
    UserDaoIface userDao;

    @ConfigProperty(name = "signomix.database.type")
    String databaseType;

    @ConfigProperty(name = "questdb.client.config")
    String questDbConfig;

    void onStart(@Observes StartupEvent ev) {
        if("h2".equalsIgnoreCase(databaseType)){
            authDao = new AuthDao();
            authDao.setDatasource(authDataSource, questDbConfig);
            userDao = new UserDao();
            userDao.setDatasource(userDataSource);
        }else if("postgresql".equalsIgnoreCase(databaseType)){
            authDao = new com.signomix.common.tsdb.AuthDao();
            authDao.setDatasource(tsDs, questDbConfig);
            userDao = new com.signomix.common.tsdb.UserDao();
            userDao.setDatasource(tsDs);
        }

    }

    public String getUserId(String token) {
        LOG.info("getUserId: " + token);
        /*
         * try {
         * DatabaseMetaData metadata =
         * authDao.getDataSource().getConnection().getMetaData();
         * System.out.println("Connected to " + metadata.getDatabaseProductName() + " "
         * + metadata.getDatabaseProductVersion());
         * System.out.println(metadata.getDriverName() + " " +
         * metadata.getDriverVersion());
         * System.out.println(metadata.getURL());
         * System.out.println(metadata.getUserName());
         * } catch (Exception ex) {
         * LOG.error("DB connection problem.");
         * ex.printStackTrace();
         * }
         */
        return authDao.getUserId(token, sessionTokenLifetime, permanentTokenLifetime);
    }

    public User getUser(String uid) {
        User user = null;
        try {
            user = userDao.getUser(uid);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return user;
    }

    public Token getToken(String token) {
        Token tokenObj = null;
        try {
            tokenObj = authDao.getToken(token, sessionTokenLifetime, permanentTokenLifetime);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return tokenObj;
    }

    /*
     * public User getUser(String uid) {
     * if(null==uid){
     * return null;
     * }
     * UserServiceClient client;
     * User completedUser = null;
     * 
     * try {
     * client = RestClientBuilder.newBuilder()
     * .baseUri(new URI(authHost))
     * .followRedirects(true)
     * .build(UserServiceClient.class);
     * completedUser = client.getUser(uid, appKey);
     * } catch (URISyntaxException ex) {
     * LOG.error(ex.getMessage());
     * // TODO: notyfikacja użytkownika o błędzie
     * } catch (ProcessingException ex) {
     * LOG.error(ex.getMessage());
     * } catch (WebApplicationException ex) {
     * LOG.error(ex.getMessage());
     * } catch (Exception ex) {
     * LOG.error(ex.getMessage());
     * // TODO: notyfikacja użytkownika o błędzie
     * }
     * return completedUser;
     * }
     */

}
