---
services: Storage
platforms: Java
author: Azure Storage
---

# Azure Storage: Queue

This sample demonstrates how to use the Queue Storage service. Queue storage provides reliable messaging for workflow processing and for communication between components of cloud services.

Note: If you don't have a Microsoft Azure subscription you can get a FREE trial account [here](http://go.microsoft.com/fwlink/?LinkId=330212)

## Running this sample

This sample can be run using either the Azure Storage Emulator (Windows) or by updating the config.properties file with your Storage account name and key.

To run the sample using the Storage Emulator (default option):

1. Download and install the Azure Storage Emulator https://azure.microsoft.com/en-us/downloads/ 
2. Start the emulator (once only) by pressing the Start button or the Windows key and searching for it by typing "Azure Storage Emulator". Select it from the list of applications to start it.
3. Set breakpoints and run the project. 

To run the sample using the Storage Service

1. Open the config.properties file and comment out the connection string for the emulator (UseDevelopmentStorage=True) and uncomment the connection string for the storage service (AccountName=[]...)
2. Create a Storage Account through the Azure Portal and provide your [AccountName] and [AccountKey] in the config.properties file. See https://azure.microsoft.com/en-us/documentation/articles/storage-create-storage-account/ for more information
3. Set breakpoints and run the project. 

## More information
- [What is a Storage Account](http://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/)
- [Getting Started with Queues](https://azure.microsoft.com/en-us/documentation/articles/storage-java-how-to-use-queue-storage/)
- [Queue Service Concepts](https://msdn.microsoft.com/library/azure/dd179353.aspx)
- [Queue Service REST API](https://msdn.microsoft.com/library/azure/dd179363.aspx)
- [Queue Service Java API](http://azure.github.io/azure-storage-java/)
- [Storage Emulator](http://msdn.microsoft.com/en-us/library/azure/hh403989.aspx)