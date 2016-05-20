//----------------------------------------------------------------------------------
// Microsoft Developer & Platform Evangelism
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
// EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES
// OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
//----------------------------------------------------------------------------------
// The example companies, organizations, products, domain names,
// e-mail addresses, logos, people, places, and events depicted
// herein are fictitious.  No association with any real company,
// organization, product, domain name, email address, logo, person,
// places, or events is intended or should be inferred.
//----------------------------------------------------------------------------------

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.EnumSet;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.microsoft.azure.storage.queue.MessageUpdateFields;

/*
 * Azure Queue Service Sample - Demonstrate how to perform common tasks using the Microsoft Azure Queue Service
 * including creating a Queue, common queue operations, processing batch messages in a queue.
 *
 * Documentation References:
 *  - What is a Storage Account - http://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/
 *  - How to use Azure Queue - https://azure.microsoft.com/en-us/documentation/articles/storage-java-how-to-use-queue-storage/
 *  - Queue Service Concepts - https://msdn.microsoft.com/library/azure/dd179353.aspx
 *  - Queue Service REST API - https://msdn.microsoft.com/library/azure/dd179363.aspx
 *  - Queue Service Java API - http://azure.github.io/azure-storage-java/
 *  - Delegating Access with Shared Access Signatures - http://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-shared-access-signature-part-1/
 *  - Storage Emulator - http://azure.microsoft.com/en-us/documentation/articles/storage-use-emulator/
 *
 * Instructions:
 *      This sample can be run using either the Azure Storage Emulator or your Azure Storage
 *      account by updating the config.properties file with your "AccountName" and "Key".
 *
 *      To run the sample using the Storage Emulator (default option - Only available on Microsoft Windows OS)
 *          1.  Start the Azure Storage Emulator by pressing the Start button or the Windows key and searching for it
 *              by typing "Azure Storage Emulator". Select it from the list of applications to start it.
 *          2.  Set breakpoints and run the project.
 *
 *      To run the sample using the Storage Service
 *          1.  Open the config.properties file and comment out the connection string for the emulator (UseDevelopmentStorage=True) and
 *              uncomment the connection string for the storage service (AccountName=[]...)
 *          2.  Create a Storage Account through the Azure Portal and provide your [AccountName] and [AccountKey] in the config.properties file.
 *              See https://azure.microsoft.com/en-us/documentation/articles/storage-create-storage-account/ for more information.
 *          3.  Set breakpoints and run the project.
 */
public class QueueBasics {

     /**
     * Azure Storage Queue Sample
     *
     * @param args No input arguments are expected from users.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        System.out.println("Azure Storage Queue sample - Starting.");

        Scanner scan = null;
        CloudQueueClient queueClient = null;
        CloudQueue queue1 = null;
        CloudQueue queue2 = null;

        try {
            // Create a scanner for user input
            scan = new Scanner(System.in);

            // Create a queue client for interacting with the queue service
            queueClient = getQueueClientReference();

            // Create new queues with randomized names
            System.out.println("\nCreate queues for the sample demonstration");
            queue1 = createQueue(queueClient, createRandomName("queuebasics-"));
            System.out.println(String.format("\tSuccessfully created the queue \"%s\".", queue1.getName()));
            queue2 = createQueue(queueClient, createRandomName("queuebasics-"));
            System.out.println(String.format("\tSuccessfully created the queue \"%s\".", queue2.getName()));

            // Insert a message into the queue
            System.out.println("\nInsert a couple of messages into the queue");
            queue1.addMessage(new CloudQueueMessage("Hello!"));
            queue1.addMessage(new CloudQueueMessage("Hello World!"));
            System.out.println("\tSucessfully enqueued the messages.");

            // Peek at the message in the front of a queue without removing it from the queue using PeekMessage
            System.out.println("\nPeek at the message at the front of the queue");
            CloudQueueMessage peekedMessage = queue1.peekMessage();
            if (peekedMessage != null) {
                System.out.println("\tThe peeked message is: " + peekedMessage.getMessageContentAsString());
            }

            // De-queue the next message
            // You de-queue a message in two steps. Call RetrieveMessage at which point the message becomes invisible to any other code reading messages
            // from this queue for a default period of 30 seconds. To finish removing the message from the queue, you call DeleteMessage.
            // This two-step process ensures that if your code fails to process a message due to hardware or software failure, another instance
            // of your code can get the same message and try again.
            System.out.println("\nDe-queue the next message");
            CloudQueueMessage message = queue1.retrieveMessage();
            if (message != null) {
                System.out.println("\tProcessing & deleting message with content: " + message.getMessageContentAsString());
                queue1.deleteMessage(message);
            }

            // Insert another test message into the queue
            System.out.println("\nInsert another message into the queue");
            queue1.addMessage(new CloudQueueMessage("Hello World Again!"));
            System.out.println("\tSucessfully enqueued the message.");

            // Change the contents of an already queued message
            System.out.println("\nChange the contents of a queued message");
            EnumSet<MessageUpdateFields> updateFields = EnumSet.of(MessageUpdateFields.CONTENT, MessageUpdateFields.VISIBILITY);
            CloudQueueMessage updatedMessage = queue1.retrieveMessage();
            updatedMessage.setMessageContent(updatedMessage.getMessageContentAsString() + " - updated");
            queue1.updateMessage(updatedMessage, 0 /* Visible immediately */, updateFields, null, null);
            System.out.println("\tSucessfully updated the message.");

            // Enqueue 20 messages by which to demonstrate batch retrieval
            System.out.println("\nEnqueue 20 messages into the second queue to demonstrate batch retrieval");
            for (int i = 0; i < 20; i++) {
                queue2.addMessage(new CloudQueueMessage(Integer.toString(i) + " - Hello World"));
            }
            System.out.println("\tSucessfully enqueued the messages.");

            // The FetchAttributes method asks the Queue service to retrieve the queue attributes, including an approximation of message count
            System.out.println("\nGet the queue length");
            queue2.downloadAttributes();
            System.out.println("\tApproximate number of messages in the second queue: " + queue2.getApproximateMessageCount());

            // Dequeue a batch of 15 messages (up to 32) and set visibility timeout to 5 minutes.
            System.out.println("\nDequeue 15 messages, allowing 5 minutes for the clients to process.");
            for (CloudQueueMessage messageItr : queue2.retrieveMessages(15, 5, null, null)) {
                System.out.println("\tProcessing & deleting message with content: " + messageItr.getMessageContentAsString());

                // Process all messages in less than 5 minutes, deleting each message after processing.
                queue2.deleteMessage(messageItr);
            }

            // Enumerate all queues starting with the prefix "queuebasics-"
            System.out.println("\nEnumerate all queues starting with the prefix \"queuebasics-\"");
            for (CloudQueue queue : queueClient.listQueues("queuebasics-")) {
                queue.downloadAttributes();
                System.out.println(String.format("\tQueue: %s. Approximate number of messages: %d.", queue.getName(), queue.getApproximateMessageCount()));
            }
        }
        catch (Throwable t) {
            printException(t);
        }
        finally {
            // Delete the queues (If you do not want to delete the queues comment out the block of code below)
            System.out.print("\nDelete the queues that we created. Press any key to continue...");
            scan.nextLine();

            if (queue1 != null && queue1.deleteIfExists() == true) {
                System.out.println(String.format("\tSuccessfully deleted the queue: %s", queue1.getName()));
            }

            if (queue2 != null && queue2.deleteIfExists() == true) {
                System.out.println(String.format("\tSuccessfully deleted the queue: %s", queue2.getName()));
            }

            // Close the scanner
            if (scan != null) {
                scan.close();
            }
        }

        System.out.println("\nAzure Storage Queue sample - Completed.\n");
    }

    /**
     * Validates the connection string and returns the storage queue client.
     * The connection string must be in the Azure connection string format.
     *
     * @return The newly created CloudQueueClient object
     *
     * @throws RuntimeException
     * @throws IOException
     * @throws URISyntaxException
     * @throws IllegalArgumentException
     * @throws InvalidKeyException
     */
    private static CloudQueueClient getQueueClientReference() throws RuntimeException, IOException, IllegalArgumentException, URISyntaxException, InvalidKeyException {

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

    /**
     * Creates and returns a queue for the sample application to use.
     *
     * @param queueClient CloudQueueClient object
     * @param queueName Name of the queue to create
     * @return The newly created CloudQueue object
     *
     * @throws StorageException
     * @throws RuntimeException
     * @throws IOException
     * @throws URISyntaxException
     * @throws IllegalArgumentException
     * @throws InvalidKeyException
     * @throws IllegalStateException
     */
    private static CloudQueue createQueue(CloudQueueClient queueClient, String queueName) throws StorageException, RuntimeException, IOException, InvalidKeyException, IllegalArgumentException, URISyntaxException, IllegalStateException {

        // Create a new queue
        CloudQueue queue = queueClient.getQueueReference(queueName);
        try {
            if (queue.createIfNotExists() == false) {
                throw new IllegalStateException(String.format("Queue with name \"%s\" already exists.", queueName));
            }
        }
        catch (StorageException s) {
            if (s.getCause() instanceof java.net.ConnectException) {
                System.out.println("Caught connection exception from the client. If running with the default configuration please make sure you have started the storage emulator.");
            }
            throw s;
        }

        return queue;
    }

    /**
     * Creates and returns a randomized name based on the prefix file for use by the sample.
     *
     * @param namePrefix The prefix string to be used in generating the name.
     * @return The randomized name
     */
    private static String createRandomName(String namePrefix) {

        return namePrefix + UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * Print the exception stack trace
     *
     * @param ex Exception to be printed
     */
    public static void printException(Throwable t) {

        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        t.printStackTrace(printWriter);
        if (t instanceof StorageException) {
            if (((StorageException) t).getExtendedErrorInformation() != null) {
                System.out.println(String.format("\nError: %s", ((StorageException) t).getExtendedErrorInformation().getErrorMessage()));
            }
        }
        System.out.println(String.format("Exception details:\n%s", stringWriter.toString()));
    }
}
