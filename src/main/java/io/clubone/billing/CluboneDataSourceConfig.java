package io.clubone.billing;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class CluboneDataSourceConfig {

  @Bean
  @ConfigurationProperties("spring.datasource.clubone")
  public DataSourceProperties cluboneDataSourceProperties() {
    return new DataSourceProperties();
  }

  @Bean(name = "cluboneDataSource")
  public DataSource cluboneDataSource(@Qualifier("cluboneDataSourceProperties") DataSourceProperties props) {
    return props.initializeDataSourceBuilder().build();
  }

  @Bean(name = "cluboneJdbcTemplate")
  public JdbcTemplate cluboneJdbcTemplate(@Qualifier("cluboneDataSource") DataSource ds) {
    return new JdbcTemplate(ds);
  }
}
