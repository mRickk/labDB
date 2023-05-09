package lab.db.tables;

 import java.sql.Connection;
 import java.sql.PreparedStatement;
 import java.sql.ResultSet;
 import java.sql.Statement;
 import java.sql.SQLException;
 import java.sql.SQLIntegrityConstraintViolationException;
 import java.util.*;

 import lab.utils.Utils;
 import lab.db.Table;
 import lab.model.Student;

 public final class StudentsTable implements Table<Student, Integer> {
     public static final String TABLE_NAME = "students";

     private final Connection connection; 

     public StudentsTable(final Connection connection) {
         this.connection = Objects.requireNonNull(connection);
     }

     @Override
     public String getTableName() {
         return TABLE_NAME;
     }

     @Override
     public boolean createTable() {
         // 1. Create the statement from the open connection inside a try-with-resources
         try (final Statement statement = this.connection.createStatement()) {
             // 2. Execute the statement with the given query
             statement.executeUpdate(
                 "CREATE TABLE " + TABLE_NAME + " (" +
                         "id INT NOT NULL PRIMARY KEY," +
                         "firstName CHAR(40)," + 
                         "lastName CHAR(40)," + 
                         "birthday DATE" + 
                     ")");
             return true;
         } catch (final SQLException e) {
             // 3. Handle possible SQLExceptions
             return false;
         }
     }

     @Override
     public Optional<Student> findByPrimaryKey(final Integer id) {
         final String query = "SELECT * FROM " + TABLE_NAME + " WHERE id = ?";
         try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
             statement.setInt(1, id);
             final ResultSet result = statement.executeQuery();
             return readStudentsFromResultSet(result).stream().findFirst();
         } catch (SQLException e) {
             throw new IllegalStateException(e);
         }
     }

     /**
      * Given a ResultSet read all the students in it and collects them in a List
      * @param resultSet a ResultSet from which the Student(s) will be extracted
      * @return a List of all the students in the ResultSet
      */
     private List<Student> readStudentsFromResultSet(final ResultSet resultSet) throws SQLException {
         // Create an empty list, then
         // Inside a loop you should:
         //      1. Call resultSet.next() to advance the pointer and check there are still rows to fetch
         //      2. Use the getter methods to get the value of the columns
         //      3. After retrieving all the data create a Student object
         //      4. Put the student in the List
         // Then return the list with all the found students
         final List<Student> students = new LinkedList<>();
         while (resultSet.next()) {
             final int id = resultSet.getInt("Id");
             final String firstName = resultSet.getString("firstName");
             final String lastName = resultSet.getString("lastName");
             final Date tempDate = resultSet.getDate("birthday");
             final Optional<Date> optDate = tempDate == null ? Optional.empty() : Optional.of(tempDate);
             students.add(new Student(id, firstName, lastName, optDate));
         }
         return students;
         // Helpful resources:
         // https://docs.oracle.com/javase/7/docs/api/java/sql/ResultSet.html
         // https://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html
     }

     @Override
     public List<Student> findAll() {
         final String query = "SELECT * FROM " + TABLE_NAME;
         try (final Statement statement = this.connection.createStatement()) {
             final ResultSet resultSet = statement.executeQuery(query);
             return this.readStudentsFromResultSet(resultSet);
         } catch (SQLException e) {
             throw new IllegalStateException(e);
         }
     }

     public List<Student> findByBirthday(final Date date) {
         final String query = "SELECT * FROM " + TABLE_NAME + " WHERE birthday = ?";
         try (final PreparedStatement preparedStatement = this.connection.prepareStatement(query)) {
             preparedStatement.setDate(1, Utils.dateToSqlDate(date));
             final ResultSet resultSet = preparedStatement.executeQuery();
             return this.readStudentsFromResultSet(resultSet);
         } catch (SQLException e) {
             throw new IllegalStateException(e);
         }
     }

     @Override
     public boolean dropTable() {
         try (final Statement statement = this.connection.createStatement()) {
             statement.execute("DROP TABLE " + TABLE_NAME);
             return true;
         } catch (SQLException e) {
             return false;
         }
     }

     @Override
     public boolean save(final Student student) {
         try (final PreparedStatement preparedStatement = this.connection.prepareStatement("INSERT INTO " + TABLE_NAME + " VALUES (?,?,?,?)")) {
             preparedStatement.setInt(1, student.getId());
             preparedStatement.setString(2, student.getFirstName());
             preparedStatement.setString(3, student.getLastName());
             preparedStatement.setDate(4, student.getBirthday().isEmpty() ? null : Utils.dateToSqlDate(student.getBirthday().get()));
             return preparedStatement.executeUpdate() != 0;
         } catch (SQLException e) {
             return false;
         }
     }

     @Override
     public boolean delete(final Integer id) {
         String delete = "DELETE FROM " + TABLE_NAME + " WHERE id = ?";
         try (final PreparedStatement preparedStatement = this.connection.prepareStatement(delete)) {
             preparedStatement.setInt(1, id);
             return preparedStatement.executeUpdate() != 0;
         } catch (SQLException e) {
             return false;
         }
     }

     @Override
     public boolean update(final Student student) {
         String update = "UPDATE " + TABLE_NAME + " SET id = ?, firstName = ?, lastName = ?, birthday = ? WHERE id = ?";
         try (PreparedStatement preparedStatement = this.connection.prepareStatement(update)) {
             preparedStatement.setInt(1, student.getId());
             preparedStatement.setString(2, student.getFirstName());
             preparedStatement.setString(3, student.getLastName());
             preparedStatement.setDate(4, student.getBirthday().isEmpty() ? null : Utils.dateToSqlDate(student.getBirthday().get()));
             preparedStatement.setInt(5, student.getId());
             return preparedStatement.executeUpdate() != 0;
         } catch (SQLException e) {
             throw new IllegalStateException(e);
         }
     }
 }