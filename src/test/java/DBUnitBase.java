package org.miqi.dbunit;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;
import org.dbunit.database.ForwardOnlyResultSetTable;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.RowOutOfBoundsException;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.util.fileloader.FlatXmlDataFileLoader;
import org.dbunit.util.fileloader.FullXmlDataFileLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.transaction.annotation.Transactional;
import com.github.springtestdbunit.TransactionDbUnitTestExecutionListener;
import com.github.springtestdbunit.bean.DatabaseDataSourceConnectionFactoryBean;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dbUnit.xml" })
@Transactional
@TestExecutionListeners({ DependencyInjectionTestExecutionListener.class, DirtiesContextTestExecutionListener.class,
		TransactionDbUnitTestExecutionListener.class })
public abstract class DBUnitBase {
	private static final Logger logger = LoggerFactory.getLogger(DBUnitBase.class);
	@Autowired
	protected BasicDataSource dataSource;
	@Autowired
	protected DatabaseDataSourceConnectionFactoryBean dbUnitDatabaseConnection;
	private IDatabaseConnection conn;
	public static final String ROOT_URL = System.getProperty("user.dir") + "/src/test/resources/test/";

	public DataSource getDataSource() {
		return dataSource;
	}

	@Before
	public void before() throws Exception {
		conn = dbUnitDatabaseConnection.getObject();
		logger.info("Get connection, conn=" + conn);
	}

	@After
	public void after() throws Exception {
		if (conn != null) {
			conn.close();
			logger.info("Close connection, conn=" + conn);
			conn = null;
		}
	}

	final public IDatabaseConnection getDatabaseConnection() {
		return conn;
	}

	/**
	 * @param tableNames
	 * @return
	 * @throws DataSetException
	 * @throws SQLException
	 */
	final public IDataSet queryAllDataSet() throws DataSetException, SQLException {
		return conn.createDataSet();
	}

	/**
	 * create a file with "fileName" by the type of FlatXmlDataSet from all DB
	 * tables
	 * 
	 * @param fileName
	 * @throws DataSetException
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws SQLException
	 */
	final public void createAllFlatXmlDataSetFile(String fileName)
			throws DataSetException, FileNotFoundException, IOException, SQLException {
		FlatXmlDataSet.write(queryAllDataSet(), new FileOutputStream(ROOT_URL + fileName));
	}

	/**
	 * create a file with "fileName" by the type of XmlDataSet from all DB
	 * tables
	 * 
	 * @param fileName
	 * @throws DataSetException
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws SQLException
	 */
	final public void createAllXmlDataSetFile(String fileName)
			throws DataSetException, FileNotFoundException, IOException, SQLException {
		XmlDataSet.write(queryAllDataSet(), new FileOutputStream(ROOT_URL + fileName));
	}

	/**
	 * @param tableNames
	 * @return
	 * @throws DataSetException
	 * @throws SQLException
	 */
	final public IDataSet queryDataSet(String... tableNames) throws DataSetException, SQLException {
		return conn.createDataSet(tableNames);
	}

	/**
	 * create a file with "fileName" by the type of FlatXmlDataSet from
	 * different "tableNames"
	 * 
	 * @param fileName
	 * @param tableNames
	 * @throws DataSetException
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws SQLException
	 */
	final public void createFlatXmlDataSetFile(String fileName, String... tableNames)
			throws DataSetException, FileNotFoundException, IOException, SQLException {
		FlatXmlDataSet.write(queryDataSet(tableNames), new FileOutputStream(ROOT_URL + fileName));
	}

	/**
	 * create a file with "fileName" by the type of XmlDataSet from different
	 * "tableNames"
	 * 
	 * @param fileName
	 * @param tableNames
	 * @throws DataSetException
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws SQLException
	 */
	final public void createXmlDataSetFile(String fileName, String... tableNames)
			throws DataSetException, FileNotFoundException, IOException, SQLException {
		XmlDataSet.write(queryDataSet(tableNames), new FileOutputStream(ROOT_URL + fileName));
	}

	/**
	 * @param tableName
	 * @param sql
	 * @return
	 * @throws DataSetException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	final public QueryDataSet queryDataSet(String tableName, String sql)
			throws DataSetException, FileNotFoundException, IOException {
		QueryDataSet partialDataSet = new QueryDataSet(conn);
		partialDataSet.addTable(tableName, sql);
		return partialDataSet;
	}

	/**
	 * 
	 * @param tableName
	 * @param sql
	 * @return
	 * @throws DataSetException
	 * @throws SQLException
	 */
	final public ITable queryTable(String tableName, String sql) throws DataSetException, SQLException {
		ITable table = conn.createQueryTable(tableName, sql);
		return table;
	}

	final public List<Map<String, Object>> queryTableList(String tableName, String sql)
			throws DataSetException, SQLException {
		ITable table = conn.createQueryTable(tableName, sql);
		return extractTable(table);
	}

	final public List<Map<String, Object>> extractTable(ITable table) throws DataSetException {
		List<Map<String, Object>> ret = new ArrayList<Map<String, Object>>();
		ITableMetaData tableMetaData = table.getTableMetaData();
		String tableName = tableMetaData.getTableName();
		if (table instanceof ForwardOnlyResultSetTable) {
			try {
				Column[] columns = tableMetaData.getColumns();
				for (int i = 0;; i++) {
					Map<String, Object> map = new TreeMap<>();
					for (Column column : columns) {
						String columnName = column.getColumnName().toUpperCase();
						Object columnValue = table.getValue(i, column.getColumnName());
						map.put(columnName, columnValue);
					}
					ret.add(map);
				}
			} catch (RowOutOfBoundsException e) {
				logger.debug("records are exhausted.");
			}
		} else {
			int count_table = table.getRowCount();
			if (count_table > 0) {
				Column[] columns = tableMetaData.getColumns();
				for (int i = 0; i < count_table; i++) {
					Map<String, Object> map = new TreeMap<>();
					for (Column column : columns) {
						String columnName = column.getColumnName().toUpperCase();
						Object columnValue = table.getValue(i, column.getColumnName());
						map.put(columnName, columnValue);
					}
					ret.add(map);
				}
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug(printResultList(tableName, ret));
		}
		return ret;
	}

	final public String printResultList(String tableName, List<Map<String, Object>> ret) {
		StringBuilder sb = new StringBuilder("\n");
		sb.append("[").append(tableName).append(" count:").append("(").append(ret.size()).append(")").append("]")
				.append("\n");
		for (Map<String, Object> map : ret) {
			for (Entry<String, Object> entry : map.entrySet()) {
				String columnName = entry.getKey();
				Object columnValue = entry.getValue();
				sb.append(columnName).append("=").append(columnValue).append(", ");
			}
			sb.append("\n");
		}
		return sb.toString();
	}

	/**
	 * create a file with "fileName" by the type of FlatXmlDataSet from
	 * tableName&&sql
	 * 
	 * @param fileName
	 * @param tableName
	 * @param sql
	 * @throws DataSetException
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws SQLException
	 */
	final public void createFlatXmlDataSetFile(String fileName, String tableName, String sql)
			throws DataSetException, FileNotFoundException, IOException, SQLException {
		FlatXmlDataSet.write(queryDataSet(tableName, sql), new FileOutputStream(ROOT_URL + fileName));
	}

	/**
	 * create a file with "fileName" by the type of XmlDataSet from
	 * tableName&&sql
	 * 
	 * @param fileName
	 * @param tableName
	 * @param sql
	 * @throws DataSetException
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws SQLException
	 */
	final public void createXmlDataSetFile(String fileName, String tableName, String sql)
			throws DataSetException, FileNotFoundException, IOException, SQLException {
		XmlDataSet.write(queryDataSet(tableName, sql), new FileOutputStream(ROOT_URL + fileName));
	}

	/**
	 * @param tableName
	 * @param sql
	 * @return
	 * @throws DataSetException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	final public QueryDataSet queryDataSet(Map<String, String> tableSqlMap)
			throws DataSetException, FileNotFoundException, IOException {
		QueryDataSet partialDataSet = new QueryDataSet(conn);
		Set<Entry<String, String>> entrySet = tableSqlMap.entrySet();
		for (Entry<String, String> entry : entrySet) {
			partialDataSet.addTable(entry.getKey(), entry.getValue());
		}
		return partialDataSet;
	}

	/**
	 * create a file with "fileName" by the type of XmlDataSet from
	 * tableName&&sql
	 * 
	 * @param fileName
	 * @param tableSqlMap
	 * @throws DataSetException
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws SQLException
	 */
	final public void createFlatXmlDataSetFile(String fileName, Map<String, String> tableSqlMap)
			throws DataSetException, FileNotFoundException, IOException, SQLException {
		FlatXmlDataSet.write(queryDataSet(tableSqlMap), new FileOutputStream(ROOT_URL + fileName));
	}

	/**
	 * create a file with "fileName" by the type of XmlDataSet from
	 * tableSqlMap(key=tableName, value=sql)
	 * 
	 * @param fileName
	 * @param tableSqlMap
	 * @throws DataSetException
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws SQLException
	 */
	final public void createXmlDataSetFile(String fileName, Map<String, String> tableSqlMap)
			throws DataSetException, FileNotFoundException, IOException, SQLException {
		XmlDataSet.write(queryDataSet(tableSqlMap), new FileOutputStream(ROOT_URL + fileName));
	}

	/**
	 * fileName = "/the/package/prepData.xml"
	 * 
	 * @param fileName
	 * @return
	 */
	final public IDataSet loadFlatXmlDataSetFile(String fileName) {
		FlatXmlDataFileLoader loader = new FlatXmlDataFileLoader();
		return loader.load(fileName);
	}

	/**
	 * fileName = "/the/package/prepData.xml"
	 * 
	 * @param fileName
	 *            file location
	 * @return
	 */
	final public IDataSet loadXmlDataSetFile(String fileName) {
		FullXmlDataFileLoader loader = new FullXmlDataFileLoader();
		return loader.load(fileName);
	}
}

//