package tests;

import databaseConnect.ConnectionJDBC;
import org.junit.jupiter.api.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Проверка подключения к БД sakila, создание таблиц и отправки запросов")
public class DatabaseTest {

    @Test
    @Order(1)
    @DisplayName("Проверка создания таблицы студентов в БД")
    public void testCreateTableStudents() {
        String query = "CREATE TABLE students ("
                + "ID int(3) NOT NULL,"
                + "FIRST_NAME VARCHAR(21) NOT NULL,"
                + "LAST_NAME VARCHAR(21) NOT NULL,"
                + "AGE int(3),"
                + "EMAIL VARCHAR(64) NOT NULL,"
                + "PRIMARY KEY (id))";
        ConnectionJDBC.createTable(query);
    }

    @Test
    @Order(2)
    @DisplayName("Проверка создания таблицы курса студентов в БД")
    public void testCreateCourseTable() {
        String query = "CREATE TABLE course ("
                + "ID int(2) NOT NULL,"
                + "COURSE VARCHAR(45) NOT NULL,"
                + "CITY VARCHAR(45) NOT NULL,"
                + "PRIMARY KEY (id))";
        ConnectionJDBC.createTable(query);
    }


    @Test
    @Order(3)
    @DisplayName("Отправка INSERT запроса для таблицы студентов")
    public void testInsertRequestStudents() {
        String query = "INSERT INTO students (ID, FIRST_NAME, LAST_NAME, AGE, EMAIL) " +
                "VALUES (1, 'Uladzimir', 'Tarasavets', 28, 'tarasovets21@gmail.com')";
        ConnectionJDBC.insertIntoTable(query);

        String selectQuery = "SELECT * FROM students";
        ResultSet rs = ConnectionJDBC.selectFromTable(selectQuery);
        assertAll("Should return inserted data",
                () -> assertEquals("1", rs.getString("ID")),
                () -> assertEquals("Uladzimir", rs.getString("FIRST_NAME")),
                () -> assertEquals("Tarasavets", rs.getString("LAST_NAME")),
                () -> assertEquals("28", rs.getString("AGE")),
                () -> assertEquals("tarasovets21@gmail.com", rs.getString("EMAIL")));
    }


    @Test
    @Order(4)
    @DisplayName("Отправка INSERT запроса для таблицы курса")
    public void testInsertCourseRequest() {
        String query = "INSERT INTO course (ID, COURSE, CITY) VALUES (1, 'AQA', 'MINSK')";
        String query2 = "INSERT INTO course (ID, COURSE, CITY) VALUES (2, 'JAVA SE', 'GRODNO')";
        ConnectionJDBC.insertIntoTable(query);
        ConnectionJDBC.insertIntoTable(query2);

        String selectQuery = "SELECT * FROM course";
        ResultSet rs = ConnectionJDBC.selectFromTable(selectQuery);
        assertAll("Should return inserted data",
                () -> assertEquals("1", rs.getString("ID")),
                () -> assertEquals("AQA", rs.getString("COURSE")),
                () -> assertEquals("MINSK", rs.getString("CITY")));
//                () -> assertEquals("2", rs.getString("ID")),
//                () -> assertEquals("JAVA SE", rs.getString("COURSE")),
//                () -> assertEquals("GRODNO", rs.getString("CITY")));

    }

    @Test
    @Order(5)
    public void testUpdateRequest() throws SQLException {
        String query = "UPDATE students SET AGE = '29' WHERE ID='1'";
        ConnectionJDBC.updateInTable(query);
        String selectQuery = "SELECT AGE FROM students WHERE ID=1";
        ResultSet rs = ConnectionJDBC.selectFromTable(selectQuery);
        String expectedAge = "29";
        String actualAge = rs.getString("AGE");
        assertEquals(expectedAge, actualAge, "Actual age is '" + actualAge + "'. Expected - '" + expectedAge + "'.");
    }

    @Test
    @Order(6)
    @DisplayName("Отправка простого SELECT запроса. Проверка фамилии")
    public void testSelectRequest_checkAddress() throws SQLException {
        String query = "SELECT * FROM students WHERE LAST_NAME='Tarasavets'";
        ResultSet rs = ConnectionJDBC.selectFromTable(query);
        String expectedLastName = "Tarasavets";
        String actualLastName = rs.getString("LAST_NAME");
        assertEquals(expectedLastName, actualLastName, "Actual LAST_NAME is '" + actualLastName + "'. Expected - '" + expectedLastName + "'.");
    }

    @Test
    @Order(7)
    @DisplayName("Отправка INNER JOIN запроса")
    public void testInnerJoin() throws SQLException {

    /*  SELECT поля FROM имя_таблицы
        LEFT JOIN имя_связанной_таблицы ON условие_связи
        WHERE условие_выборки
        Оператор SQL INNER JOIN формирует таблицу из записей двух или нескольких таблиц.
        Каждая строка из первой (левой) таблицы, сопоставляется с каждой строкой из второй (правой) таблицы,
        после чего происходит проверка условия. Если условие истинно, то строки попадают в результирующую таблицу.
        В результирующей таблице строки формируются конкатенацией строк первой и второй таблиц.
        */

        String query = "SELECT * " +
                "FROM students " +
                "INNER JOIN course ON course.id = students.id";
        ResultSet rs = ConnectionJDBC.selectFromTable(query);
        String expectedID = "1";
        String actualID = rs.getString("ID");
        assertEquals(expectedID, actualID, "Actual students is '" + actualID + "'. Expected - '" + expectedID + "'.");
    }

//    @Test
    @Disabled
    @Order(8)
    @DisplayName("Отправка FULL JOIN запроса")
    public void testFullJoin() throws SQLException {
        /*
        Оператор SQL FULL JOIN осуществляет формирование таблицы из записей двух или нескольких таблиц.
        В операторе SQL FULL JOIN не важен порядок следования таблиц, он никак не влияет на окончательный результат,
        так как оператор является симметричным.
        Оператор SQL FULL JOIN можно воспринимать как сочетание операторов SQL INNER JOIN + SQL LEFT JOIN + SQL RIGHT JOIN.
        Алгоритм его работы следующий:
        1) Сначала формируется таблица на основе внутреннего соединения (оператор SQL INNER JOIN).
        2) Затем, в таблицу добавляются значения не вошедшие в результат формирования из правой таблицы (оператор SQL LEFT JOIN).
            Для них, соответствующие записи из правой таблицы заполняются значениями NULL.
        3) Наконец, в таблицу добавляются значения не вошедшие в результат формирования из левой таблицы (оператор SQL RIGHT JOIN).
            Для них, соответствующие записи из левой таблицы заполняются значениями NULL.
        */
        String query = "SELECT * " +
                "FROM students " +
                "FULL JOIN course ON course.id = students.id";
        ResultSet rs = ConnectionJDBC.selectFromTable(query);
        String expectedID = "1";
        String actualID = rs.getString("ID");
        assertEquals(expectedID, actualID, "Actual students is '" + actualID + "'. Expected - '" + expectedID + "'.");
    }

    @Test
    @Order(9)
    @DisplayName("Отправка DELETE запроса")
    public void testDeleteRequest() {
        String query = "DELETE FROM students WHERE ID=1";
        ConnectionJDBC.deleteFromTable(query);
    }

    @Test
    @Order(10)
    @DisplayName("Проверка удаления таблицы студентов из БД")
    public void test_dropTableStudents() {
        ConnectionJDBC.dropTable("students");
    }

    @Test
    @Order(11)
    @DisplayName("Проверка удаления таблицы курсов из БД")
    public void test_dropCourseTable() {
        ConnectionJDBC.dropTable("course");
    }

}