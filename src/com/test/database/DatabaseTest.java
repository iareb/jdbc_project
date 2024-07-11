package com.test.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.mysql.cj.jdbc.MysqlDataSource;

public class DatabaseTest {
	
	private Connection conn;
	private String dbName = "";

	public static void main(String[] args) {
		
		DatabaseTest test = new DatabaseTest();
		
		try {
			
			System.out.println(test.startConnection(null).isValid(100));
			test.createDatabase("test_db");
			test.closeConnection();
			System.out.println(test.startConnection("test_db").isValid(100));
			test.createTable("test_db", "utente");
			
			// Esecuzione delle query
			test.firstQuery();
			test.secondQuery();
			test.thirdQuery();
			test.fourthQuery("Esposito");
			test.fifthQuery();
			test.sixthQuery();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	
	private Connection startConnection(String dbName) throws SQLException {
		
		if (conn == null) {
			
			MysqlDataSource dataSource = new MysqlDataSource();

			dataSource.setServerName("127.0.0.1");
			dataSource.setPortNumber(3306);
			dataSource.setUser("root");
			dataSource.setPassword("admin");
			
			dataSource.setDatabaseName(dbName);
			this.dbName = dbName;
			conn = dataSource.getConnection();
		}
		
		return conn;
	}
	
	
	private void closeConnection() throws SQLException {
		
		if (conn != null) {
			conn.close();
			conn = null;
		}
	}
	
	
	// Questo metodo si occupa di creare il database, controllando che non sia già esistente.
	private void createDatabase(String dbName) throws SQLException {
		
		String sql = "CREATE DATABASE IF NOT EXISTS " + dbName;
		PreparedStatement ps = startConnection(null).prepareStatement(sql);
		ps.executeUpdate();
	}
	
	// Questo metodo si occupa soltanto di eseguire l'istruzione "USE test_db;"
	private void useDatabase() throws SQLException {
		String sql = "USE " + dbName + ";";
		PreparedStatement ps = startConnection(dbName).prepareStatement(sql);
		ps.executeUpdate();	
	}
	
	
	// Questo metodo crea la tabella utente.
	private void createTable(String dbName, String tableName) throws SQLException {
		
		useDatabase();
		String sql = "CREATE TABLE IF NOT EXISTS " + tableName + "(" +
               		 "id INT PRIMARY KEY," +
               		 "cognome VARCHAR(255)," +
               		 "nome VARCHAR(255)" +
               		 ")";
		
		PreparedStatement ps = startConnection(dbName).prepareStatement(sql);
		ps.executeUpdate();	
	}
	

	
	// Metodo che passa al database la prima query.
	private void firstQuery() throws SQLException {
		
		String sql = "SELECT u.cognome, l.titolo, p.inizio, p.fine \r\n"
					+ "FROM utente u JOIN libro l JOIN prestito p\r\n"
					+ "ON l.id = p.id_l AND u.id = p.id_u\r\n"
					+ "WHERE u.cognome = \"Vallieri\"\r\n"
					+ "ORDER BY p.inizio";
		
		useDatabase();
		PreparedStatement ps = startConnection(this.dbName).prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		
		System.out.println("----- PRIMA QUERY -----");
		while(rs.next()) {
			System.out.println("cognome: " + rs.getString(1));
			System.out.println("titolo: " + rs.getString(2));
			System.out.println("inizio: " + rs.getDate(3));
			System.out.println("fine: " + rs.getDate(4));
			System.out.println("-----------");
		}
		System.out.println();
	}
	
	
	// Metodo che passa al database la seconda query.
	private void secondQuery() throws SQLException {
		
		String sql = "SELECT u.nome, u.cognome, COUNT(*) as libri_letti\r\n"
					+ "FROM utente u JOIN prestito p \r\n"
					+ "ON u.id = p.id_u \r\n"
					+ "GROUP BY p.id_u\r\n"
					+ "HAVING count(*) > 1\r\n"
					+ "LIMIT 3";
		
		useDatabase();
		PreparedStatement ps = startConnection(this.dbName).prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		
		System.out.println("----- SECONDA QUERY -----");
		while(rs.next()) {
			System.out.println("nome: " + rs.getString(1));
			System.out.println("cognome: " + rs.getString(2));
			System.out.println("libri letti: " + rs.getInt(3));
			System.out.println("-----------");
		}
		System.out.println();
	}
	
	
	// Metodo che passa al database la terza query.
	private void thirdQuery() throws SQLException {
		
		String sql = "SELECT u.nome, u.cognome, l.titolo, p.inizio \r\n"
					+ "FROM utente u JOIN libro l JOIN prestito p\r\n"
					+ "ON u.id = p.id_u AND l.id = p.id_l\r\n"
					+ "WHERE p.fine IS NULL;";
		
		useDatabase();
		PreparedStatement ps = startConnection(this.dbName).prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		
		System.out.println("----- TERZA QUERY -----");
		while(rs.next()) {
			System.out.println("nome: " + rs.getString(1));
			System.out.println("cognome: " + rs.getString(2));
			System.out.println("libro: " + rs.getString(3));
			System.out.println("inizio: " + rs.getDate(4));
			System.out.println("-----------");
		}
		System.out.println();
		
	}
	
	
	// Metodo che passa al database la quarta query. Il cognome dell'utente viene passato in input come parametro.
	private void fourthQuery(String cognome) throws SQLException {
		
		String sql = "SELECT u.cognome, l.titolo, p.inizio, p.fine\r\n"
					+ "FROM utente u JOIN libro l JOIN prestito p\r\n"
					+ "ON u.id = p.id_u AND l.id = p.id_l\r\n"
					+ "WHERE u.cognome = " + "'"+cognome+"'";
		
		useDatabase();
		PreparedStatement ps = startConnection(this.dbName).prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		
		System.out.println("----- QUARTA QUERY -----");
		while(rs.next()) {
			System.out.println("cognome: " + rs.getString(1));
			System.out.println("titolo: " + rs.getString(2));
			System.out.println("inizio: " + rs.getDate(3));
			System.out.println("fine: " + rs.getDate(4));
			System.out.println("-----------");
		}
		System.out.println();
	}
	
	
	// Metodo che passa al database la quinta query.
	private void fifthQuery() throws SQLException {
		
		String sql = "SELECT l.*, COUNT(*) AS Numero_Prestiti\r\n"
					+ "FROM libro l JOIN prestito p\r\n"
					+ "ON l.id = p.id_l \r\n"
					+ "GROUP BY l.id\r\n"
					+ "HAVING COUNT(*) > 1;";
		
		useDatabase();
		PreparedStatement ps = startConnection(this.dbName).prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		
		System.out.println("----- QUINTA QUERY -----");
		while(rs.next()) {
			System.out.println("id: " + rs.getInt(1));
			System.out.println("titolo: " + rs.getString(2));
			System.out.println("autore: " + rs.getString(3));
			System.out.println("numero prestiti: " + rs.getInt(4));
			System.out.println("-----------");
		}
		System.out.println();
		
	}
	
	// Metodo che passa al database la sesta query.
	private void sixthQuery() throws SQLException {
		
		String sql = "SELECT p.id, p.inizio, p.fine\r\n"
					+ "FROM prestito p\r\n"
					+ "WHERE DATEDIFF(p.fine, p.inizio) > 15;\r\n";
		
		useDatabase();
		PreparedStatement ps = startConnection(this.dbName).prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		
		System.out.println("----- SESTA QUERY -----");
		while(rs.next()) {
			System.out.println("id: " + rs.getInt(1));
			System.out.println("inizio: " + rs.getDate(2));
			System.out.println("fine: " + rs.getDate(3));
			System.out.println("-----------");
		}
		System.out.println();
	}
}
