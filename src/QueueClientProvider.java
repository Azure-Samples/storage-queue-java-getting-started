import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.queue.CloudQueueClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;

/**
 * Manages the storage queue client
 */
class QueueClientProvider {

    /**
     * Validates the connection string and returns the storage queue client.
     * The connection string must be in the Azure connection string format.
     *
     * @return The newly created CloudQueueClient object
     *
     */
    static CloudQueueClient getQueueClientReference() throws RuntimeException, IOException, URISyntaxException, InvalidKeyException {

        // Retrieve the connection string
        Properties prop = new Properties();
        try {
            InputStream propertyStream = QueueBasics.class.getClassLoader().getResourceAsStream("config.properties");
            if (propertyStream != null) {
                prop.load(propertyStream);
            }
            else {
                throw new RuntimeException();
            }
        } catch (RuntimeException|IOException e) {
            System.out.println("\nFailed to load config.properties file.");
            throw e;
        }

        CloudStorageAccount storageAccount;
        try {
            storageAccount = CloudStorageAccount.parse(prop.getProperty("StorageConnectionString"));
        }
        catch (IllegalArgumentException|URISyntaxException e) {
            System.out.println("\nConnection string specifies an invalid URI.");
            System.out.println("Please confirm the connection string is in the Azure connection string format.");
            throw e;
        }
        catch (InvalidKeyException e) {
            System.out.println("\nConnection string specifies an invalid key.");
            System.out.println("Please confirm the AccountName and AccountKey in the connection string are valid.");
            throw e;
        }

        return storageAccount.createCloudQueueClient();
    }

}
