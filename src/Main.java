/*
  Copyright Microsoft Corporation

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

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
public class Main {

    /**
     * Executes the sample.
     *
     * @param args
     *            No input args are expected from users.
     */
    public static void main(String[] args) throws Exception {
        QueueBasics basicSamples = new QueueBasics();
        basicSamples.runSamples();

        QueueAdvanced advancedSamples = new QueueAdvanced();
        advancedSamples.runSamples();
    }
}