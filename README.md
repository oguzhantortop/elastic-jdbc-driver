# elasticjdbc

* This JDBC Driver is a wrapper of Elastic SQL Rest API (https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-rest.html)
* Currently it only supports features of SQL Rest API 
	DDL, DML and keywords like DISTINCT is not supported. For detailed info please look at https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-rest.html
* This JDBC driver is heavily inspired from following projects:
	- https://github.com/fabrizio-pasti/esqlj-elastic-jdbc
	
	- https://github.com/opensearch-project/sql-jdbc
* You can also add this jdbc driver to DBBeaver by building from it is pom.xml, just copy elastic-jdbc-driver-jar-with-dependencies.jar from target directory. And add it through driver manager of DBBEAVER.
	- driver class name: io.github.oguzhantortop.elasticjdbc.ElasticJdbcDriver
	- url template: jdbc:elasticjdbc:http://{host}:{port}
	- example url : jdbc:elasticjdbc:http://localhost:9200
* You can reach me out by sending an email to oguzhantortop@gmail.com

	