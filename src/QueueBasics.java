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
import java.util.UUID;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.*;

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
 *
 * Instructions:
 *      This sample can only be run using your Azure Storage account by updating the config.properties file with your "AccountName" and "Key".
 *
 *      To run the sample using the Storage Service
 *          1.  Open the app.config file and comment out the connection string for the emulator (UseDevelopmentStorage=True) and
 *              uncomment the connection string for the storage service (AccountName=[]...)
 *          2.  Create a Storage Account through the Azure Portal and provide your [AccountName] and [AccountKey] in the config.properties file.
 *              See https://azure.microsoft.com/en-us/documentation/articles/storage-create-storage-account/ for more information.
 *          3.  Set breakpoints and run the project.
 */
public class QueueBasics {

    protected static CloudQueue queue = null;
    protected final static String queueNamePrefix = "queuebasics";
	
     /**
     * Azure Storage Queue Sample
     *
     * @param args No input arguments are expected from users.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception 
    {
        System.out.println("Azure Storage Queue sample - Starting.\n");

        try {
            // 1. Create a queue  
            queue = createQueue();
            
            // 2. Insert a message into the queue 
            System.out.println("2. Insert a single message into a queue");
            queue.addMessage(new CloudQueueMessage("Hello World!"));
    
            // 3. Peek at the message in the front of a queue without removing it from the queue using PeekMessage
            System.out.println("3. Peek at the next message");
            CloudQueueMessage peekedMessage = queue.peekMessage(); 
            if (peekedMessage != null)
            { 
                System.out.println("The peeked message is: " + peekedMessage.toString());
            }
    
            // 4. De-queue the next message
            // You de-queue a message in two steps. Call RetrieveMessage at which point the message becomes invisible to any other code reading messages 
            // from this queue for a default period of 30 seconds. To finish removing the message from the queue, you call DeleteMessage. 
            // This two-step process ensures that if your code fails to process a message due to hardware or software failure, another instance 
            // of your code can get the same message and try again. 
            System.out.println("4. De-queue the next message");
            CloudQueueMessage message = queue.retrieveMessage();
            if (message != null)
            {
                System.out.println("Processing & deleting message with content: " + message.toString());
                queue.deleteMessage(message);
            }
            
            // 5. Insert another test message into the queue 
            System.out.println("5. Insert another test message ");
            queue.addMessage(new CloudQueueMessage("Hello World Again!"));

            // 6. Change the contents of an already queued message            
            System.out.println("6. Change the contents of a queued message");
            CloudQueueMessage msg = queue.retrieveMessage();
            msg.setMessageContent("Updated contents.");
            
            EnumSet<MessageUpdateFields> updateFields = 
                    EnumSet.of(MessageUpdateFields.CONTENT,
                    MessageUpdateFields.VISIBILITY);
            
            queue.updateMessage(
                msg, 
                0,  // For the purpose of the sample make the update visible immediately
                updateFields, null, null);

            // 7. Enqueue 20 messages by which to demonstrate batch retrieval
            System.out.println("7. Enqueue 20 messages."); 
            for (int i = 0; i < 20; i++)
            {
                queue.addMessage(new CloudQueueMessage(Integer.toString(i) + " - Hello World"));
            }
    
            // 8. The FetchAttributes method asks the Queue service to retrieve the queue attributes, including an approximation of message count 
            System.out.println("8. Get the queue length");
            queue.downloadAttributes();
            long cachedMessageCount = queue.getApproximateMessageCount();
            System.out.println("Approximate number of messages in queue: " + cachedMessageCount);
    
            // 9. Dequeue a batch of 21 messages (up to 32) and set visibility timeout to 5 minutes. Note we are dequeuing 21 messages because the earlier
            // UpdateMessage method left a message on the queue hence we are retrieving that as well. 
            System.out.println("9. Dequeue 21 messages, allowing 5 minutes for the clients to process.");
            
            for (CloudQueueMessage cqm : queue.retrieveMessages(21,5,null,null))
            {
                System.out.println("Processing & deleting message with content: " + cqm.toString());
    
                // Process all messages in less than 5 minutes, deleting each message after processing.
                queue.deleteMessage(cqm);
            }
        }
        catch (Throwable t) {
            printException(t);
        }
        finally {
            System.out.println("\nPress any key to delete queue and exit.");
            System.in.read();

            // 10. Delete a queue
            deleteQueue(queue);
        }

        System.out.println("Azure Storage Queue sample - Completed.\n");
    }
    
    /**
     * Validates the connection string and returns the storage account.
     * The connection string must be in the Azure connection string format.
     *
     * @param storageConnectionString Connection string for the storage service or the emulator
     * @return The newly created CloudStorageAccount object
     *
     * @throws URISyntaxException
     * @throws IllegalArgumentException
     * @throws InvalidKeyException
     */
    private static CloudStorageAccount getStorageAccountFromConnectionString(String storageConnectionString) throws IllegalArgumentException, URISyntaxException, InvalidKeyException 
    {
        CloudStorageAccount storageAccount;
        try {
            storageAccount = CloudStorageAccount.parse(storageConnectionString);
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

        return storageAccount;
    }

    /**
     * Creates and returns a Queue for the sample application to use.
     *
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
    private static CloudQueue createQueue() throws StorageException, RuntimeException, IOException, InvalidKeyException, IllegalArgumentException, URISyntaxException, IllegalStateException 
    {
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
        String storageConnectionString = prop.getProperty("StorageConnectionString");

        // Retrieve storage account information from connection string.
        CloudStorageAccount storageAccount = getStorageAccountFromConnectionString(storageConnectionString);

        // Create a queue client for interacting with the queue service
        CloudQueueClient queueClient = storageAccount.createCloudQueueClient();

        // Create a randomized queue name
        String queueName = queueNamePrefix + UUID.randomUUID().toString().replace("-", "");
        System.out.println(String.format("1. Create a queue with name \"%s\" for the demo", queueName));

        // Create a new queue
        CloudQueue queue = queueClient.getQueueReference(queueName);
        try {
            if (queue.createIfNotExists()) {
                System.out.println(String.format("\tSuccessfully created queue. ", queueName));
            }
            else {
                System.out.println(String.format("\tQueue already exists."));
                throw new IllegalStateException(String.format("Queue with name \"%s\" already exists.", queueName));
            }
        }
        catch (StorageException e) {
            System.out.println("\nCaught storage exception from the client.");
            System.out.println("If running with the default configuration please make sure you have started the storage emulator.");
            throw e;
        }

        return queue;
    }

    /**
     * Delete the specified queue.
     *
     * @param queue The {@link CloudQueue} object to delete
     *
     * @throws StorageException
     */
    private static void deleteQueue(CloudQueue queue) throws StorageException 
    {
        try
        {
            System.out.println(String.format("10. Delete a queue with name \"%s\" .", queue.getName()));
            
            if (queue != null) {
                queue.deleteIfExists();
                System.out.println(String.format("\tSuccessfully deleted queue. ", queue.getName()));
            }
        }
        catch (StorageException e) 
        {
            System.out.println("\nCaught storage exception from the client.");
            System.out.println("If running with the default configuration please make sure you have started the storage emulator.");
            throw e;
        }
    }

    /*
     * Print the exception stack trace
     *
     * @param ex Exception to be printed
     */
    public static void printException(Throwable ex) 
    {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        ex.printStackTrace(printWriter);
        System.out.println(String.format("Exception details:\n%s\n", stringWriter.toString()));
    }
}
