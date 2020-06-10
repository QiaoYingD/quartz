package demo_quartz.demo.quartz;

import com.alibaba.druid.pool.DruidDataSource;

import lombok.Data;
import org.quartz.SchedulerException;
import org.quartz.utils.ConnectionProvider;

import java.sql.Connection;
import java.sql.SQLException;

@Data
public class DruidConnectionProvider implements ConnectionProvider {

    /**
     * JDBC driver
     */
    public String driver;

    /**
     * JDBC URL
     */
    public String URL;

    /**
     * Database user name
     */
    public String user;

    /**
     * Database password
     */
    public String password;

    /**
     * Maximum number of database connections
     */
    public int maxConnections;

    /**
     * The query that validates the database connection
     */
    public String validationQuery;

    /**
     * Whether the database sql query to validate connections should be executed every time
     * a connection is retrieved from the pool to ensure that it is still valid.  If false,
     * then validation will occur on check-in.  Default is false.
     */
    private boolean validateOnCheckout;

    /**
     * The number of seconds between tests of idle connections - only enabled
     * if the validation query property is set.  Default is 50 seconds.
     */
    private int idleConnectionValidationSeconds;

    /**
     * The maximum number of prepared statements that will be cached per connection in the pool.
     * Depending upon your JDBC Driver this may significantly help performance, or may slightly
     * hinder performance.
     * Default is 120, as Quartz uses over 100 unique statements. 0 disables the feature.
     */
    public String maxCachedStatementsPerConnection;

    /**
     * Discard connections after they have been idle this many seconds.  0 disables the feature. Default is 0.
     */
    private String discardIdleConnectionsSeconds;

    /**
     * Default maximum number of database connections in the pool.
     */
    public static final int DEFAULT_DB_MAX_CONNECTIONS = 10;

    /**
     * The maximum number of prepared statements that will be cached per connection in the pool.
     */
    public static final int DEFAULT_DB_MAX_CACHED_STATEMENTS_PER_CONNECTION = 120;

    /**
     * Druid connection pool
     */
    private DruidDataSource datasource;

    @Override
    public Connection getConnection() throws SQLException {
        return datasource.getConnection();
    }

    @Override
    public void shutdown() throws SQLException {
        datasource.close();
    }

    @Override
    public void initialize() throws SQLException {
        if (this.driver == null) {
            throw new SQLException("DBPool driver could not be created: DB driver class name cannot be null!");
        }
        if (this.URL == null) {
            throw new SQLException("DBPool could not be created: DB URL cannot be null");
        }
        if (this.maxConnections < 0) {
            throw new SQLException("DBPool maxConnectins could not be created: Max connections must be greater than zero!");
        }
        datasource = new DruidDataSource();

        try {
            datasource.setDriverClassName(this.driver);
        } catch (Exception e) {
            try {
                throw new SchedulerException("Problem setting driver class name on datasource: " + e.getMessage(), e);
            } catch (SchedulerException ex) {
                ex.printStackTrace();
            }
        }

        datasource.setUrl(this.URL);
        datasource.setUsername(this.user);
        datasource.setPassword(this.password);
        datasource.setMaxActive(this.maxConnections);
        datasource.setMinIdle(1);
        datasource.setMaxWait(0);
        datasource.setMaxPoolPreparedStatementPerConnectionSize(this.DEFAULT_DB_MAX_CONNECTIONS);
        if (this.validationQuery != null) {
            datasource.setValidationQuery(this.validationQuery);
            if(!validateOnCheckout){
                datasource.setTestOnReturn(true);
            }else{
                datasource.setTestOnBorrow(true);
            }
            datasource.setValidationQueryTimeout(this.idleConnectionValidationSeconds);
        }


    }
}
