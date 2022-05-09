package lab.db.tables;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
    public boolean dropTable() {
        // 1. Create the statement from the open connection inside a try-with-resources
        try (final Statement statement = this.connection.createStatement()) {
            // 2. Execute the statement with the given query
            statement.executeUpdate("DROP TABLE " + TABLE_NAME);
            return true;
        } catch (final SQLException e) {
            // 3. Handle possible SQLExceptions
            return false;
        }
    }

    @Override
    public Optional<Student> findByPrimaryKey(final Integer id) {
        // 1. Define the query with the "?" placeholder(s)
        final String query = "SELECT * FROM " + TABLE_NAME + " WHERE id = ?";
        // 2. Prepare a statement inside a try-with-resources
        try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
            // 3. Fill in the "?" with actual data
            statement.setInt(1, id);
            // 4. Execute the query, this operations returns a ResultSet
            final ResultSet resultSet = statement.executeQuery();
            // 5. Do something with the result of the query execution; 
            //    here we extract the first (and only) student from the ResultSet
            return readStudentsFromResultSet(resultSet).stream().findFirst();
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Given a ResultSet read all the students in it and collects them in a List
     * @param resultSet a ResultSet from which the Student(s) will be extracted
     * @return a List of all the students in the ResultSet
     */
    private List<Student> readStudentsFromResultSet(final ResultSet resultSet) {
        final List<Student> students = new ArrayList<>();
        try {
            // ResultSet encapsulate a pointer to a table with the results: it starts with the pointer
            // before the first row. With next the pointer advances to the following row and returns 
            // true if it has not advanced past the last row
            while (resultSet.next()) {
                // To get the values of the columns of the row currently pointed we use the get methods 
                final int studentId = resultSet.getInt("id");
                final String firstName = resultSet.getString("firstName");
                final String lastName = resultSet.getString("lastName");
                final Optional<Date> birthday = Optional.ofNullable(Utils.sqlDateToDate(resultSet.getDate("birthday")));
                // After retrieving all the data we create a Student object
                final Student student = new Student(studentId, firstName, lastName, birthday);
                students.add(student);
            }
        } catch (final SQLException e) {}
        return students;
    }

    @Override
    public List<Student> findAll() {
        try (final Statement statement = this.connection.createStatement()) {
            final ResultSet resultSet = statement.executeQuery("SELECT * FROM " + TABLE_NAME);
            return readStudentsFromResultSet(resultSet);
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public List<Student> findByBirthday(final Date date) {
        final String query = "SELECT * FROM " + TABLE_NAME + " WHERE birthday = ?";
        try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
            statement.setDate(1, Utils.dateToSqlDate(date));
            final ResultSet resultSet = statement.executeQuery();
            return readStudentsFromResultSet(resultSet);
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }
    
    @Override
    public boolean save(final Student student) {
        final String query = "INSERT INTO " + TABLE_NAME + "(id, firstName, lastName, birthday) VALUES (?,?,?,?)";
        try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
            statement.setInt(1, student.getId());
            statement.setString(2, student.getFirstName());
            statement.setString(3, student.getLastName());
            statement.setDate(4, student.getBirthday().map(Utils::dateToSqlDate).orElse(null));
            statement.executeUpdate();
            return true;
        } catch (final SQLIntegrityConstraintViolationException e) {
            return false;
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }
    
    @Override
    public boolean delete(final Integer id) {
        final String query = "DELETE FROM " + TABLE_NAME + " WHERE id = ?";
        try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
            statement.setInt(1, id);
            return statement.executeUpdate() > 0;
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }
    
    @Override
    public boolean update(final Student student) {
        final String query =
            "UPDATE " + TABLE_NAME + " SET " +
                "firstName = ?," + 
                "lastName = ?," +
                "birthday = ? " + 
            "WHERE id = ?";
        try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
            statement.setString(1, student.getFirstName());
            statement.setString(2, student.getLastName());
            statement.setDate(3, student.getBirthday().map(Utils::dateToSqlDate).orElse(null));
            statement.setInt(4, student.getId());
            return statement.executeUpdate() > 0;
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
