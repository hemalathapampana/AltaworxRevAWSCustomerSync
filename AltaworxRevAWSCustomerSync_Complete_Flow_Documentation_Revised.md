# Altaworx Rev.IO AWS Customer Sync Lambda - Complete Flow Documentation (Revised)

## Overview

**Lambda Name:** `AltaworxRevAWSCustomerSync`  
**Queue Name:** `RevCustomerSync_TEST`  
**Schedule:** Daily at 13:45 UTC  
**Purpose:** Synchronizes customer and related data from Rev.IO APIs to Altaworx database through staging tables

## Environment Variables

- **RevCustomerSyncQueueUrl:** SQS queue for self-triggering
- **RevGetCustomersUrl:** Rev.IO Customers API endpoint
- **RevGetBillProfilesUrl:** Rev.IO Bill Profiles API endpoint  
- **RevGetProvidersUrl:** Rev.IO Providers API endpoint
- **RevGetProductTypesUrl:** Rev.IO Product Types API endpoint
- **RevGetProductsUrl:** Rev.IO Products API endpoint
- **RevGetServiceTypesUrl:** Rev.IO Service Types API endpoint
- **RevGetServicesUrl:** Rev.IO Services API endpoint
- **RevGetServiceProductsUrl:** Rev.IO Service Products API endpoint
- **RevGetInventoryTypesUrl:** Rev.IO Inventory Types API endpoint
- **RevGetInventoryItemsUrl:** Rev.IO Inventory Items API endpoint
- **RevGetUsagePlanGroupsUrl:** Rev.IO Usage Plan Groups API endpoint
- **RevGetAgentUrl:** Rev.IO Agent API endpoint
- **RevGetPackagesUrl:** Rev.IO Packages API endpoint
- **RevGetPackageProductsUrl:** Rev.IO Package Products API endpoint
- **RevPageSize:** API pagination size (default: 1000)
- **ConnectionString:** Central database connection string
- **VerboseLogging:** Logging level flag

## 1. High-Level Sequential Function Flow

### Primary Entry and Setup Phase
1. **FunctionHandler** - Main Lambda entry point that determines execution path
2. **BaseFunctionHandler** - Initialize Lambda context wrapper with AWS and database settings
3. **Authentication Setup** - Extract authentication details and sync parameters from SQS messages

### Main Processing Orchestration Phase
4. **ProcessEventAsync** - Main orchestrator that routes to appropriate sync method based on current step

### Data Synchronization Phase (Sequential Order)
5. **Customer Sync** - Fetch and stage customer data
6. **Bill Profile Sync** - Fetch and stage billing profile data
7. **Provider Sync** - Fetch and stage provider data
8. **Product Type Sync** - Fetch and stage product type data
9. **Product Sync** - Fetch and stage product data
10. **Service Type Sync** - Fetch and stage service type data
11. **Service Sync** - Fetch and stage service data
12. **Service Product Sync** - Fetch and stage service product relationship data
13. **Inventory Type Sync** - Fetch and stage inventory type data
14. **Inventory Item Sync** - Fetch and stage inventory item data
15. **Usage Plan Group Sync** - Fetch and stage usage plan group data
16. **Agent Sync** - Fetch and stage agent data
17. **Package Sync** - Fetch and stage package data
18. **Package Product Sync** - Fetch and stage package product relationship data

### Staging to Production Transfer Phase
19. **ProcessLoadRevFromStaging** - Transfer data from staging tables to production tables

### Instance Management and Completion Phase
20. **QueueNextRevInstanceAsync** - Move to next authentication instance or complete sync
21. **SendNotificationToAmop20** - Notify AMOP 2.0 system of completion
22. **CleanUp** - Resource cleanup and finalization

## 2. Detailed Low-Level Flow

### Phase 1: Primary Entry and Setup

#### FunctionHandler - Main Lambda Entry Point

**High-Level Flow:**
- Initialize Lambda context and load environment variables
- Determine if this is a new sync or continuation from SQS message
- Route to appropriate processing method based on message type

**Low-Level Flow:**

**Step 1: Context Initialization**
- The method begins by creating a null KeySysLambdaContext variable to hold the Lambda execution context
- A try-catch block wraps the entire execution to handle any unhandled exceptions
- The BaseFunctionHandler is called to initialize the Lambda context wrapper, setting up logging, database connections, and AWS services

**Step 2: Environment Variable Loading**
- The system checks if environment variables have already been loaded in a previous execution
- If not loaded, it extracts all required environment variables from the Lambda context including API endpoints, queue URLs, page sizes, and connection strings
- Each environment variable is assigned to corresponding class properties for use throughout the execution

**Step 3: SQS Message Processing**
- The system checks if the Lambda was triggered by an SQS event containing message records
- If SQS records exist, it extracts the first record and parses message attributes to determine the current state
- Four key pieces of information are extracted: authentication ID, sync step, page number, and whether this is a staging transfer operation

**Step 4: Execution Path Determination**
- Based on the IsLastStepSync flag, the system determines whether to process staging-to-production transfer or continue data fetching
- If staging transfer is required, ProcessLoadRevFromStaging is called with the current sync step and authentication ID
- If data fetching is required, ProcessEventAsync is called with all extracted parameters

**Step 5: Direct Invocation Handling**
- When no SQS event is present (direct invocation), the system starts a fresh sync process
- It queries the database to get the first available authentication ID using the RevIoAuthenticationRepository
- The system handles three possible outcomes: valid authentication found, no authentication records, or database error
- If a valid authentication is found, it clears all staging tables and begins processing with the Customer sync step

**Step 6: Error Handling and Cleanup**
- Any exceptions during execution are caught, logged with full stack trace details
- The CleanUp method is called in the finally block to ensure proper resource disposal

#### BaseFunctionHandler - Context Initialization

**High-Level Flow:**
- Create and configure KeySysLambdaContext with AWS and database settings
- Set up logging, environment access, and connection strings

**Low-Level Flow:**

**Step 1: Context Creation**
- A new KeySysLambdaContext instance is created, passing the AWS Lambda context and optional organizational unit logic flag
- During construction, the context automatically sets up environment repository for accessing Lambda environment variables
- The KeysysLambdaLogger is initialized to handle structured logging throughout the execution

**Step 2: Service Initialization**
- Base64 service is configured for credential decoding operations
- The system determines if running in production environment by examining the Lambda function ARN
- Database connection strings are loaded from environment variables and validated

**Step 3: Additional Service Setup**
- Redis connection string is configured for caching operations if available
- Settings repository is initialized for database-based configuration retrieval
- The fully configured context is returned for use throughout the Lambda execution

#### Authentication and Message Parameter Extraction

**High-Level Flow:**
- Extract authentication ID, sync step, page number, and staging flag from SQS messages
- Validate and log extracted parameters

**Low-Level Flow:**

**Authentication ID Extraction:**
- The system checks if the SQS message contains the "CurrentIntegrationAuthenticationId" attribute
- If present, the string value is parsed to an integer representing the authentication record to process
- The extracted value is logged for audit purposes and returned, defaulting to -1 if not found

**Sync Step Extraction:**
- The "CurrentRevSyncStep" attribute is checked in the SQS message attributes
- The string value is parsed to an integer and cast to the RevSyncStep enumeration
- This determines which type of data (customers, products, services, etc.) should be processed in this execution

**Page Number Extraction:**
- The "CurrentPageNumber" attribute indicates which page of paginated results to fetch from the API
- If not present, defaults to page 1 for starting fresh pagination
- This enables the Lambda to continue pagination across multiple executions

**Staging Flag Extraction:**
- The "IsLastStepSync" attribute indicates whether this execution should transfer data from staging to production tables
- When true, the Lambda skips API fetching and processes staging table transfers
- When false or absent, the Lambda continues with normal API data fetching operations

### Phase 2: Main Processing Orchestration

#### ProcessEventAsync - Main Processing Orchestrator

**High-Level Flow:**
- Create authentication repository and API client
- Route to appropriate sync method based on current step
- Handle authentication failures and exceptions

**Low-Level Flow:**

**Step 1: Authentication Setup**
- A RevIoAuthenticationRepository instance is created with logging, Base64 service, and database connection
- The repository retrieves the complete authentication record for the specified authentication ID
- This includes API credentials, tenant information, and configuration settings needed for Rev.IO API access

**Step 2: API Client Configuration**
- An HttpClientFactory is created with the Lambda logger for HTTP request tracking
- A RevioApiClient is instantiated with the factory, request factory, authentication details, and production flag
- The API client is configured with retry policies, timeout settings, and authentication headers

**Step 3: Sync Step Routing**
- A switch statement routes execution to the appropriate sync method based on the RevSyncStep enumeration value
- Each case calls a specific sync method (SyncRevCustomersAsync, SyncRevBillProfilesAsync, etc.) with consistent parameters
- Unknown sync steps are logged as exceptions to identify configuration issues

**Step 4: Error Handling**
- If authentication record is not found, an error is logged and QueueNextRevInstanceAsync is called to skip to the next instance
- Any exceptions during processing trigger error logging and automatic progression to the next authentication instance
- This ensures that failures in one authentication don't block processing of others

### Phase 3: Data Synchronization (Using Customer Sync as Example)

#### SyncRevCustomersAsync - Customer Sync Entry Point

**High-Level Flow:**
- Create SQL retry policy for database resilience
- Call the main customer data fetching method

**Low-Level Flow:**

**Step 1: Retry Policy Creation**
- A SQL retry policy is created using the Polly library to handle transient database failures
- The policy is configured to retry SQL exceptions and invalid operation exceptions
- Exponential backoff is implemented with delays of 5, 10, and 15 seconds for successive retries

**Step 2: Method Delegation**
- The CustomersAsync method is called with the context, retry policy, API client, authentication ID, and page number
- This separation allows for consistent error handling and retry logic across all sync methods

#### CustomersAsync - Customer Data Fetching and Staging

**High-Level Flow:**
- Initialize empty customer list
- Use generic pagination handler to fetch all customer pages
- Load customers to staging table with retry policy
- Determine next action based on pagination completion

**Low-Level Flow:**

**Step 1: Data Structure Initialization**
- An empty List<RevCustomer> is created to accumulate customer records across multiple API pages
- This list will grow as pagination continues, storing all customer data before bulk database insertion

**Step 2: Pagination Processing**
- The GetPagesFromRevIOListAPI generic method is called with the customer list and GetCustomersAsync method delegate
- This method handles the complex logic of API pagination, failure tolerance, and Lambda timeout management
- It returns whether the last page was reached and the final page number for continuation

**Step 3: Database Staging**
- The LoadRevCustomersToStaging method is executed within the SQL retry policy context
- This ensures that transient database failures don't cause data loss during the bulk insert operation
- All accumulated customer records are inserted into the staging table in a single bulk operation

**Step 4: Flow Control Decision**
- CheckProcessOfSyncStep is called to determine whether to continue pagination or move to the next sync step
- If more pages exist, a continuation message is queued for the same sync step with the next page number
- If pagination is complete, staging transfer and next step messages are queued

#### GetPagesFromRevIOListAPI - Generic Pagination Handler

**High-Level Flow:**
- Continue fetching pages until completion, failure limits, or timeout
- Handle API failures with acceptable failure count
- Monitor Lambda execution time to prevent timeouts

**Low-Level Flow:**

**Step 1: Pagination Loop Initialization**
- Boolean flag isLastPage tracks whether all pages have been processed
- Counter variables track the number of pages processed and API failures encountered
- Maximum limits are set for pages per batch and acceptable failures to prevent infinite loops

**Step 2: Execution Time Monitoring**
- Before each API call, the remaining Lambda execution time is checked
- If less than 30 seconds remain, pagination stops to allow time for cleanup and message queuing
- This prevents Lambda timeouts that would lose progress and require reprocessing

**Step 3: API Call Execution**
- The provided API method delegate (like GetCustomersAsync) is called with current page parameters
- The response is validated for null values and proper structure before processing
- Successful responses add records to the accumulating list and update pagination state

**Step 4: Failure Handling**
- API failures increment the failure counter and are logged with detailed error information
- If failure count exceeds the maximum threshold, pagination stops to prevent extended failure loops
- Successful calls reset the failure counter to allow for occasional transient failures

**Step 5: Pagination State Management**
- The hasMore flag from API responses determines if additional pages exist
- Page number is incremented for the next iteration
- Batch counter prevents processing too many pages in a single Lambda execution

#### GetCustomersAsync - Rev.IO API Call

**High-Level Flow:**
- Make authenticated HTTP GET request to Rev.IO Customers API
- Handle pagination parameters and filtering
- Return structured response with customer data

**Low-Level Flow:**

**Step 1: API Request Construction**
- The RevioApiClient constructs the full API URL using the base customer endpoint and pagination parameters
- Query parameters are added for page size, page number, and any configured bill profile filtering
- Authentication headers are added based on the stored API credentials

**Step 2: HTTP Request Execution**
- The client executes the HTTP GET request with built-in retry logic for network failures
- Response validation ensures the API returned a successful status code and valid JSON
- Any HTTP errors or network failures are handled by the client's retry policy

**Step 3: Response Processing**
- The JSON response is deserialized into a RevListResponse<RevCustomer> object structure
- This includes the customer records array and pagination metadata like hasMore flag
- The structured response is returned to the pagination handler for further processing

#### LoadRevCustomersToStaging - Bulk Database Insert

**High-Level Flow:**
- Create DataTable with customer staging schema
- Map customer objects to DataTable rows
- Perform bulk insert to staging table

**Low-Level Flow:**

**Step 1: Schema Preparation**
- A DataTable is created using the RevCustomerStagingColumnNames schema definition
- This ensures the DataTable structure exactly matches the database staging table schema
- Column names, data types, and constraints are all properly configured

**Step 2: Data Mapping**
- Each customer object in the list is processed through the AddToDataRow method
- Customer properties are mapped to corresponding staging table columns with proper null handling
- The authentication ID is added to each row for tracking and cleanup purposes

**Step 3: Bulk Database Operation**
- SqlBulkCopy is used to efficiently insert all rows in a single database operation
- This is much faster than individual INSERT statements for large datasets
- Connection management and error handling are provided by the base class implementation

#### AddToDataRow - Customer Data Mapping

**High-Level Flow:**
- Create new DataTable row
- Map all customer properties to staging table columns
- Handle null values and data type conversions

**Low-Level Flow:**

**Step 1: Row Creation**
- A new DataRow is created from the DataTable schema, ensuring proper column structure
- This row will be populated with customer data and added to the DataTable for bulk insertion

**Step 2: Property Mapping**
- Each customer property (Id, Name, ParentCustomerId, Status, etc.) is mapped to its corresponding staging table column
- Null values are converted to DBNull.Value to prevent database insertion errors
- The authentication ID is added to enable filtering and cleanup operations

**Step 3: Data Type Handling**
- Date values are properly formatted for database storage
- Boolean values are converted to appropriate database representations
- String values are handled with proper null checking to prevent database constraint violations

#### CheckProcessOfSyncStep - Flow Control Decision

**High-Level Flow:**
- Determine if pagination is complete
- Queue continuation message for more pages or staging transfer message
- Queue next sync step or next authentication instance

**Low-Level Flow:**

**Step 1: Pagination Status Evaluation**
- The isLastPage flag is checked to determine if all pages have been processed for the current sync step
- If false, more pages exist and pagination should continue with the same sync step

**Step 2: Continuation Message Queuing**
- When pagination is incomplete, BuildMessageToQueue is called with the current sync step and next page number
- This creates an SQS message that will trigger another Lambda execution to continue processing the same data type
- The message includes all necessary context to resume exactly where this execution left off

**Step 3: Completion Processing**
- When pagination is complete (isLastPage = true), two messages are queued for the next phase
- First, BuildMessageLoadRevFromStagingToQueueAsync queues a staging transfer operation for the current sync step
- Second, either the next sync step is queued or QueueNextRevInstanceAsync is called if this was the final step

#### BuildMessageToQueue - SQS Message Creation

**High-Level Flow:**
- Create SQS message with sync continuation parameters
- Send message to trigger next Lambda execution

**Low-Level Flow:**

**Step 1: Message Parameter Preparation**
- The authentication ID, sync step, and page number are prepared for inclusion in the SQS message
- These parameters will be extracted by the next Lambda execution to continue processing exactly where this execution ended

**Step 2: SQS Message Dispatch**
- SendMessageToQueueAsync is called with the prepared parameters and the configured queue URL
- The message includes all necessary attributes for the receiving Lambda to understand its context and continue processing

#### SendMessageToQueueAsync - SQS Message Transmission

**High-Level Flow:**
- Create AWS SQS client with credentials
- Build message with attributes for Lambda continuation
- Send message and validate response

**Low-Level Flow:**

**Step 1: AWS Client Setup**
- AWS credentials are retrieved from the Lambda context
- An AmazonSQSClient is created with the credentials and configured for the US-East-1 region
- The client is properly disposed after use to prevent resource leaks

**Step 2: Message Construction**
- A SendMessageRequest is created with the target queue URL and descriptive message body
- Message attributes are added for CurrentIntegrationAuthenticationId, CurrentRevSyncStep, and CurrentPageNumber
- These attributes allow the receiving Lambda to extract context without parsing the message body

**Step 3: Message Transmission**
- The message is sent asynchronously to the SQS queue
- The HTTP response status is validated to ensure successful delivery
- Any delivery failures are logged as exceptions for troubleshooting

### Phase 4: Staging to Production Transfer

#### ProcessLoadRevFromStaging - Staging Transfer Orchestrator

**High-Level Flow:**
- Create SQL retry policy for database operations
- Route to appropriate stored procedure based on sync step
- Clear staging table after successful transfer

**Low-Level Flow:**

**Step 1: Retry Policy Setup**
- A SQL retry policy is created to handle transient database failures during the transfer operations
- This ensures that temporary database issues don't prevent the transfer of staged data to production tables

**Step 2: Transfer Method Routing**
- A switch statement routes to the appropriate staging transfer method based on the current sync step
- Each case calls a specific method (LoadRevCustomersFromStagingTable, LoadRevBillProfilesFromStagingTable, etc.)
- This ensures that each data type is processed with its corresponding stored procedure

**Step 3: Staging Cleanup**
- After successful data transfer, the staging table is cleared for the specific authentication ID
- This prevents data accumulation and ensures clean state for future sync operations
- The cleanup is also executed within the retry policy to handle any database issues

#### LoadRevCustomersFromStagingTable - Customer Staging Transfer

**High-Level Flow:**
- Execute usp_Update_RevCustomer stored procedure
- Handle database connection and command setup
- Process staging data transfer to production tables

**Low-Level Flow:**

**Step 1: Database Connection Setup**
- A SQL connection is established using the central database connection string
- A SQL command is created for executing the usp_Update_RevCustomer stored procedure
- Command timeout is set to accommodate large data processing operations

**Step 2: Parameter Configuration**
- The authentication ID parameter is added to the stored procedure call
- This allows the procedure to process only the staging data for the specific authentication instance
- Parameter binding prevents SQL injection and ensures proper data type handling

**Step 3: Stored Procedure Execution**
- The stored procedure is executed synchronously to ensure completion before proceeding
- The procedure handles the complex logic of merging staging data with existing production data
- Database connection is properly closed after execution to prevent connection leaks

**Stored Procedure Functionality (usp_Update_RevCustomer):**
The stored procedure performs a comprehensive merge operation between staging and production customer tables. It first identifies new customers in staging that don't exist in production and inserts them with appropriate timestamps and status flags. For existing customers, it compares all relevant fields including customer name, parent customer relationships, status, activation dates, and billing profile associations. When differences are detected, the production record is updated with staging values and audit fields are modified to track the change. The procedure also handles hierarchical customer relationships by ensuring parent-child associations remain consistent. Additionally, it manages soft deletion scenarios where customers exist in production but are no longer present in the Rev.IO system, marking them as inactive while preserving historical data. Finally, the procedure updates synchronization timestamps and logs summary statistics about the number of records inserted, updated, and deactivated during the operation.

#### ClearStagingTablesV2 - Staging Table Cleanup

**High-Level Flow:**
- Execute DELETE statement for specific staging table and authentication ID
- Handle database connection and command execution
- Ensure proper cleanup without affecting other authentication instances

**Low-Level Flow:**

**Step 1: SQL Command Construction**
- A parameterized DELETE statement is constructed to remove staging data for the specific authentication ID
- The table name is dynamically inserted into the SQL command based on the staging table parameter
- Parameter binding is used to prevent SQL injection and ensure proper data type handling

**Step 2: Database Execution**
- The DELETE command is executed to remove all staging records for the processed authentication ID
- This ensures that staging tables remain clean and don't accumulate data across multiple sync cycles
- Command timeout is set to handle large cleanup operations without timing out

**Step 3: Connection Management**
- Database connections are properly opened before execution and closed after completion
- This prevents connection leaks and ensures efficient resource utilization
- Error handling ensures that cleanup failures don't affect the overall sync process

### Phase 5: Instance Management and Completion

#### QueueNextRevInstanceAsync - Instance Progression

**High-Level Flow:**
- Get next authentication ID from database
- Send completion notification to AMOP 2.0 if next instance exists
- Queue next instance or complete entire sync process

**Low-Level Flow:**

**Step 1: Next Authentication Lookup**
- The RevIoAuthenticationRepository is used to query for the next authentication ID to process
- The GetNextRevIoAuthenticationId method returns either a valid ID, 0 for errors, or -1 for no more records
- This ensures sequential processing of all configured Rev.IO authentication instances

**Step 2: Current Instance Completion Processing**
- If a next authentication ID is found, the current instance is considered complete
- The authentication details for the current instance are retrieved to get tenant information
- This tenant information is used for sending completion notifications to dependent systems

**Step 3: AMOP 2.0 Notification**
- SendNotificationToAmop20 is called with the current tenant ID and name
- This notifies the AMOP 2.0 system that Rev.IO sync has completed for this specific tenant
- The notification enables downstream processes that depend on synchronized Rev.IO data

**Step 4: Next Instance Queuing**
- If more authentication instances exist, a new sync process is queued starting with the Customer step
- If no more instances exist, the entire sync process is complete and no further messages are queued
- This creates a self-perpetuating cycle that processes all configured authentication instances

#### SendNotificationToAmop20 - External System Notification

**High-Level Flow:**
- Get AMOP 2.0 API URL from environment variables
- Create HTTP client and build JSON payload
- Send POST request with tenant completion information

**Low-Level Flow:**

**Step 1: API Configuration Retrieval**
- The AMOP 2.0 sync update API URL is retrieved from Lambda environment variables
- If the URL is not configured, the notification is skipped with a warning log entry
- This allows the system to function even when AMOP 2.0 integration is not required

**Step 2: HTTP Request Preparation**
- An HTTP client is created with appropriate timeout and retry settings
- A JSON payload is constructed containing the sync completion key, tenant ID, and tenant name
- The payload follows the expected AMOP 2.0 API schema for sync completion notifications

**Step 3: API Request Execution**
- A POST request is sent to the AMOP 2.0 API with the JSON payload
- Response status codes are validated to ensure successful delivery
- Both success and failure responses are logged for audit and troubleshooting purposes

**Step 4: Error Handling**
- Network failures, timeout issues, and API errors are caught and logged
- Notification failures don't prevent the sync process from continuing to the next authentication instance
- This ensures that AMOP 2.0 communication issues don't block the primary sync functionality

### Phase 6: Cleanup and Finalization

#### CleanUp - Resource Cleanup

**High-Level Flow:**
- Flush logging buffers to ensure all log entries are written
- Close database connections and dispose of resources
- Disconnect from Redis cache if connected

**Low-Level Flow:**

**Step 1: Logging Finalization**
- All pending log entries are flushed to ensure complete audit trails
- Log buffers are cleared and logging resources are properly disposed
- This ensures that diagnostic information is available even if the Lambda terminates abruptly

**Step 2: Database Resource Cleanup**
- Any open database connections are properly closed and disposed
- Connection pools are cleaned up to prevent resource leaks
- Database command objects and related resources are properly disposed

**Step 3: Cache and External Resource Cleanup**
- Redis connections are closed if they were established during execution
- HTTP clients and other external service connections are properly disposed
- This ensures efficient resource utilization and prevents connection exhaustion

## Key Data Structures and Constants

### RevSyncStep Enumeration
Defines the sequential order of data synchronization with integer values for SQS message transmission:
- None = 0 (default/error state)
- Customer = 1 (first sync step)
- BillProfile = 2 (billing profile data)
- Provider = 3 (service provider data)
- ProductType = 4 (product categorization)
- Product = 5 (product catalog)
- ServiceType = 6 (service categorization)
- Service = 7 (service catalog)
- ServiceProduct = 8 (service-product relationships)
- InventoryType = 9 (inventory categorization)
- InventoryItem = 10 (inventory items)
- UsagePlanGroup = 11 (usage plan groupings)
- Agent = 12 (sales agent data)
- Package = 13 (service packages)
- PackageProduct = 14 (package-product relationships)

### RevStagingTable Constants
Maps sync steps to their corresponding staging table names in the database:
- REV_CUSTOMER_STG for customer data staging
- REV_BILLING_PROFILE_STG for billing profile staging
- REV_PROVIDER_STG for provider information staging
- REV_PRODUCT_TYPE_STG for product type categorization staging
- REV_PRODUCT_STG for product catalog staging
- REV_SERVICE_TYPE_STG for service type staging
- REV_SERVICE_STG for service catalog staging
- REV_SERVICE_PRODUCT_STG for service-product relationship staging
- REV_INVENTORY_TYPE_STG for inventory type staging
- REV_INVENTORY_ITEM_STG for inventory item staging
- REV_USAGE_PLAN_GROUP_STG for usage plan group staging
- REV_AGENT_STG for agent information staging
- REV_PACKAGE_STG for package definition staging
- REV_PACKAGE_PRODUCT_STG for package-product relationship staging

## Error Handling and Resilience Patterns

### Retry Policies and Failure Management
- **SQL Operations:** Polly retry policy with exponential backoff (5, 10, 15 seconds) for transient database failures
- **HTTP API Calls:** Built-in retry in RevioApiClient with configurable attempts and circuit breaker patterns
- **SQS Operations:** AWS SDK built-in retry with exponential backoff and dead letter queue support

### Failure Scenario Handling
- **API Failures:** System continues processing with acceptable failure threshold (maximum 5 consecutive failures per pagination batch)
- **Database Failures:** Retry operations with exponential backoff, fail Lambda execution only after exhausting retry attempts
- **Timeout Management:** Monitor Lambda remaining execution time, queue continuation messages when less than 30 seconds remain
- **Authentication Issues:** Skip to next authentication instance with error logging, preventing single tenant failures from blocking entire sync

### Monitoring and Observability
- **Comprehensive Logging:** Structured logging at each processing step with contextual information and performance metrics
- **Progress Tracking:** SQS message attributes maintain state across Lambda executions for seamless continuation
- **Error Details:** Full stack traces and error context for debugging and troubleshooting
- **Performance Metrics:** Page processing counts, API response times, and Lambda execution duration tracking

## Stored Procedures and Database Operations

### Data Transfer Stored Procedures
Each stored procedure handles the complex merge logic between staging and production tables:

1. **usp_Update_RevCustomer** - Merges customer data from staging to production by comparing all customer attributes, handling new customer insertions, updating changed records, managing parent-child relationships, and tracking audit information for all modifications.

2. **usp_Update_RevBillProfile** - Processes billing profile information by merging new profiles, updating existing profile attributes like billing addresses and payment terms, maintaining relationships with customers, and preserving billing history.

3. **usp_Update_RevProvider** - Handles service provider data synchronization by inserting new providers, updating provider contact information and service capabilities, managing provider status changes, and maintaining provider-customer relationships.

4. **usp_Update_RevProductType_FromStaging** - Manages product type categorization by creating new product categories, updating category descriptions and attributes, handling category hierarchy changes, and maintaining product-category associations.

5. **usp_Update_RevProduct_FromStaging** - Synchronizes product catalog data by adding new products, updating product specifications and pricing, managing product lifecycle status, and maintaining product-type relationships.

6. **usp_Update_RevServiceType** - Processes service type definitions by inserting new service categories, updating service type attributes, managing service classification changes, and preserving service-type associations.

7. **usp_Update_RevService** - Handles service catalog synchronization by merging new services, updating service descriptions and configurations, managing service availability status, and maintaining service-type relationships.

8. **usp_Update_RevServiceProduct_FromStaging** - Manages service-product relationship data by creating new associations, updating relationship attributes, handling relationship status changes, and maintaining referential integrity.

9. **usp_Update_RevInventoryType_FromStaging** - Processes inventory type classifications by inserting new inventory categories, updating type specifications, managing category attributes, and maintaining inventory classification consistency.

10. **usp_Update_RevInventoryItem_FromStaging** - Synchronizes inventory item data by adding new items, updating item specifications and quantities, managing item status changes, and maintaining inventory-type relationships.

11. **usp_Update_RevUsagePlanGroup_FromStaging** - Handles usage plan group data by merging new groups, updating group configurations and limits, managing group membership changes, and maintaining usage plan associations.

12. **usp_Update_RevAgent** - Processes sales agent information by inserting new agents, updating agent contact details and territories, managing agent status and commission structures, and maintaining agent-customer relationships.

13. **usp_Update_RevPackage_FromStaging** - Synchronizes service package data by creating new packages, updating package configurations and pricing, managing package availability, and maintaining package definitions.

14. **usp_Update_RevPackageProduct_FromStaging** - Manages package-product relationships by establishing new associations, updating relationship configurations, handling package composition changes, and maintaining referential integrity.

### Utility Stored Procedures

15. **usp_RevCustomerClearStagingTables** - Performs comprehensive cleanup of all staging tables by truncating staging data, resetting identity columns, cleaning up orphaned records, and preparing staging environment for next sync cycle.

16. **usp_Rev_Get_Next_Authentication** - Manages authentication instance sequencing by finding the next unprocessed authentication record, handling authentication priority ordering, managing authentication status tracking, and returning appropriate authentication details for processing.

## Performance Characteristics and Optimization

### Batch Processing and Resource Management
- **Page Limits:** Maximum 20 pages per Lambda execution to maintain execution within time constraints
- **Record Pagination:** 1000 records per API page (configurable via RevPageSize environment variable)
- **Bulk Operations:** SqlBulkCopy implementation for efficient staging table inserts with minimal database round trips
- **Memory Management:** Process data in controlled batches to prevent memory exhaustion in large datasets

### Parallel Processing and Scalability
- **Sequential Instance Processing:** Multiple authentication instances processed sequentially to prevent database conflicts
- **Concurrent Step Processing:** Each sync step processes independently with staging isolation
- **Time Management:** Continuous monitoring of Lambda remaining execution time with automatic continuation queuing
- **Failure Tolerance:** Maximum 5 API failures per pagination batch before advancing to next sync step

### Database Performance Optimization
- **Staging Table Strategy:** Bulk insert to staging tables followed by stored procedure merge operations
- **Connection Pooling:** Efficient database connection management with proper disposal patterns
- **Retry Mechanisms:** Exponential backoff retry policies for transient database failures
- **Cleanup Operations:** Automated staging table cleanup to prevent data accumulation and maintain performance

This comprehensive documentation provides both high-level understanding and detailed implementation insights for the complete Altaworx Rev.IO AWS Customer Sync Lambda function flow.