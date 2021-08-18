package tests;

import databaseConnect.ConnectionJDBC;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import utils.Log;

import java.io.IOException;

public class SetupTest {

    @BeforeEach
    public void setUp(TestInfo testInfo) throws IOException {
        Log.info("------- Started test: " + testInfo.getDisplayName() + " -------");
        Assertions.assertNotNull(ConnectionJDBC.connectToDB());
    }

    @AfterEach
    public void tearDown(TestInfo testInfo) {
        ConnectionJDBC.closeConnection();
        Log.info("------- Finished test: " + testInfo.getDisplayName() + " -------");
    }

}
