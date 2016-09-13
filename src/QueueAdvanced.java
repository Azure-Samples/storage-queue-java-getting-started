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

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.queue.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.*;

/**
 * This sample illustrates advanced usage of the Azure queue storage service.
 */
class QueueAdvanced {

    /**
     * Executes the samples.
     *
     * @throws URISyntaxException Uri has invalid syntax
     * @throws InvalidKeyException Invalid key
     */
    void runSamples() throws InvalidKeyException, URISyntaxException, IOException {
        System.out.println();
        System.out.println();
        PrintHelper.printSampleStartInfo("Queue Advanced");

        // Create a queue service client
        CloudQueueClient queueClient = QueueClientProvider.getQueueClientReference();

        try {
            System.out.println("Service properties sample");
            serviceProperties(queueClient);
            System.out.println();

            System.out.println("CORS rules sample");
            corsRules(queueClient);
            System.out.println();

            System.out.println("Queue metadata sample");
            queueMetadata(queueClient);
            System.out.println();

            System.out.println("Queue Acl sample");
            queueAcl(queueClient);
            System.out.println();

            // This will fail unless the account is RA-GRS enabled.
//            System.out.println("Service stats sample");
//            serviceStats(queueClient);
//            System.out.println();
        }
        catch (Throwable t) {
            PrintHelper.printException(t);
        }

        PrintHelper.printSampleCompleteInfo("Queue Advanced");
    }

    /**
     * Manage the service properties including logging hour and minute metrics.
     * @param queueClient Azure Storage Queue Service
     */
    private void serviceProperties(CloudQueueClient queueClient) throws StorageException {
        System.out.println("Get service properties");
        ServiceProperties originalProps = queueClient.downloadServiceProperties();

        try {
            System.out.println("Set service properties");
            // Change service properties
            ServiceProperties props = new ServiceProperties();

            props.getLogging().setLogOperationTypes(EnumSet.allOf(LoggingOperations.class));
            props.getLogging().setRetentionIntervalInDays(2);
            props.getLogging().setVersion("1.0");

            final MetricsProperties hours = props.getHourMetrics();
            hours.setMetricsLevel(MetricsLevel.SERVICE_AND_API);
            hours.setRetentionIntervalInDays(1);
            hours.setVersion("1.0");

            final MetricsProperties minutes = props.getMinuteMetrics();
            minutes.setMetricsLevel(MetricsLevel.SERVICE);
            minutes.setRetentionIntervalInDays(1);
            minutes.setVersion("1.0");

            queueClient.uploadServiceProperties(props);

            System.out.println();
            System.out.println("Logging");
            System.out.printf("version: %s%n", props.getLogging().getVersion());
            System.out.printf("retention interval: %d%n", props.getLogging().getRetentionIntervalInDays());
            System.out.printf("operation types: %s%n", props.getLogging().getLogOperationTypes());
            System.out.println();
            System.out.println("Hour Metrics");
            System.out.printf("version: %s%n", props.getHourMetrics().getVersion());
            System.out.printf("retention interval: %d%n", props.getHourMetrics().getRetentionIntervalInDays());
            System.out.printf("operation types: %s%n", props.getHourMetrics().getMetricsLevel());
            System.out.println();
            System.out.println("Minute Metrics");
            System.out.printf("version: %s%n", props.getMinuteMetrics().getVersion());
            System.out.printf("retention interval: %d%n", props.getMinuteMetrics().getRetentionIntervalInDays());
            System.out.printf("operation types: %s%n", props.getMinuteMetrics().getMetricsLevel());
            System.out.println();
        }
        finally {
            // Revert back to original service properties
            queueClient.uploadServiceProperties(originalProps);
        }
    }

    /**
     * Set CORS rules sample.
     * @param queueClient Azure Storage Queue Service
     */
    private void corsRules(CloudQueueClient queueClient) throws StorageException {

        System.out.println("Get service properties");
        ServiceProperties originalProps = queueClient.downloadServiceProperties();

        try {
            // Setr CORS rules
            System.out.println("Set CORS rules");
            CorsRule ruleAllowAll = new CorsRule();
            ruleAllowAll.getAllowedOrigins().add("*");
            ruleAllowAll.getAllowedMethods().add(CorsHttpMethods.GET);
            ruleAllowAll.getAllowedHeaders().add("*");
            ruleAllowAll.getExposedHeaders().add("*");
            ServiceProperties props = queueClient.downloadServiceProperties();
            props.getCors().getCorsRules().add(ruleAllowAll);
            queueClient.uploadServiceProperties(props);
        }
        finally {
            // Revert back to original service properties
            queueClient.uploadServiceProperties(originalProps);
        }
    }

    /**
     * Manage queue metadata
     * @param queueClient Azure Storage Queue Service
     */
    private void queueMetadata(CloudQueueClient queueClient) throws URISyntaxException, StorageException {
        // Get a reference to a queue
        CloudQueue queue = queueClient.getQueueReference("queue"
                + UUID.randomUUID().toString().replace("-", ""));

        try {
            System.out.println("Set queue metadata");

            HashMap<String, String> metadata = new HashMap<>();
            metadata.put("key1", "value1");
            metadata.put("foo", "bar");
            queue.setMetadata(metadata);

            System.out.println("Create queue");
            // Create the queue if it does not exist
            queue.createIfNotExists();

            System.out.println("Get queue metadata:");
            metadata = queue.getMetadata();
            Iterator it = metadata.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                System.out.printf(" %s = %s%n", pair.getKey(), pair.getValue());
                it.remove();
            }
        }
        finally {
            // Delete the queue
            System.out.println("Delete queue");
            queue.deleteIfExists();
        }
    }

    /**
     * Manage queue access properties
     * @param queueClient Azure Storage Queue Service
     */
    private void queueAcl(CloudQueueClient queueClient) throws StorageException, URISyntaxException, InterruptedException {
        // Get a reference to a queue
        // The queue name must be lower case
        CloudQueue queue = queueClient.getQueueReference("queue"
                + UUID.randomUUID().toString().replace("-", ""));

        try {
            System.out.println("Create queue");
            // Create the queue if it does not exist
            queue.createIfNotExists();

            // Get permissions
            QueuePermissions permissions = queue.downloadPermissions();

            System.out.println("Set queue permissions");
            final Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
            cal.add(Calendar.MINUTE, -30);
            final Date start = cal.getTime();
            cal.add(Calendar.MINUTE, 30);
            final Date expiry = cal.getTime();

            SharedAccessQueuePolicy policy = new SharedAccessQueuePolicy();
            policy.setPermissions(EnumSet.of(SharedAccessQueuePermissions.ADD, SharedAccessQueuePermissions.READ, SharedAccessQueuePermissions.UPDATE));
            policy.setSharedAccessStartTime(start);
            policy.setSharedAccessExpiryTime(expiry);
            permissions.getSharedAccessPolicies().put("key1", policy);

            // Set queue permissions
            queue.uploadPermissions(permissions);
            System.out.println("Wait 30 seconds for the container permissions to take effect");
            Thread.sleep(30000);

            System.out.println("Get queue permissions");
            // Get queue permissions
            permissions = queue.downloadPermissions();

            HashMap<String, SharedAccessQueuePolicy> accessPolicies = permissions.getSharedAccessPolicies();
            Iterator it = accessPolicies.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                SharedAccessQueuePolicy value = (SharedAccessQueuePolicy) pair.getValue();
                System.out.printf(" %s: %n", pair.getKey());
                System.out.printf("  Permissions: %s%n", value.permissionsToString());
                System.out.printf("  Start: %s%n", value.getSharedAccessStartTime());
                System.out.printf("  Expiry: %s%n", value.getSharedAccessStartTime());
                it.remove();
            }

            System.out.println("Clear queue permissions");
            // Clear permissions
            permissions.getSharedAccessPolicies().clear();
            queue.uploadPermissions(permissions);
        }
        finally {
            // Delete the queue
            System.out.println("Delete queue");
            queue.deleteIfExists();
        }
    }

    /**
     * Retrieve statistics related to replication for the Queue service.
     * This operation is only available on the secondary location endpoint
     * when read-access geo-redundant replication is enabled for the storage account.
     * @param queueClient Azure Storage Queue Service
     */
    private void serviceStats(CloudQueueClient queueClient) throws StorageException {
        // Get service stats
        System.out.println("Service Stats:");
        ServiceStats stats = queueClient.getServiceStats();
        System.out.printf("- status: %s%n", stats.getGeoReplication().getStatus());
        System.out.printf("- last sync time: %s%n", stats.getGeoReplication().getLastSyncTime());
    }

}
