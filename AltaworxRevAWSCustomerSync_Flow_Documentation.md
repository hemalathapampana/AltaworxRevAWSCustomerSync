# Altaworx Rev.IO AWS Customer Sync Lambda Flow Documentation

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

## 1. High-Level Action Methods Flow (First to Last)

### Primary Entry Point
1. **`FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`** - Main Lambda entry point

### Initialization Phase
2. **`BaseFunctionHandler(ILambdaContext context)`** - Initialize Lambda context and settings
3. **`InitializeContext(bool skipOUSpecificLogic)`** - Setup environment variables and connections
4. **`LoadOUSettings()`** - Load optimization and provider settings

### Authentication & Queue Processing  
5. **`GetNextRevIoAuthenticationId(int currentId)`** - Get next authentication ID to process
6. **`ProcessEventAsync(KeySysLambdaContext context, int authId, RevSyncStep step, int pageNumber)`** - Main processing orchestrator

### Data Synchronization Steps (Sequential)
7. **`SyncRevCustomersAsync()`** → **`SyncRevBillProfilesAsync()`** → **`SyncRevProvidersAsync()`** → **`SyncRevProductTypesAsync()`** → **`SyncRevProductsAsync()`** → **`SyncRevServiceTypesAsync()`** → **`SyncRevServicesAsync()`** → **`SyncRevServiceProductsAsync()`** → **`SyncRevInventoryTypesAsync()`** → **`SyncRevInventoryItemsAsync()`** → **`SyncRevUsagePlanAsync()`** → **`SyncRevAgentAsync()`** → **`SyncRevPackagesAsync()`** → **`SyncRevPackageProductsAsync()`**

### Staging to Production Transfer
8. **`ProcessLoadRevFromStaging()`** - Transfer data from staging tables to production tables
9. **`QueueNextRevInstanceAsync()`** - Queue next authentication instance or complete sync
10. **`SendNotificationToAmop20()`** - Notify AMOP 2.0 system of completion
11. **`CleanUp()`** - Cleanup resources and flush logs

## 2. Low-Level Flow Details for Each Method

### `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
**Purpose:** Main Lambda entry point that determines execution path based on SQS message or direct invocation

**Low-level Flow:**
- Initialize `KeySysLambdaContext` using base function handler
- Load environment variables if not already set
- Extract message attributes from SQS event:
  - `CurrentIntegrationAuthenticationId`: Which Rev.IO authentication to use
  - `CurrentRevSyncStep`: Which data type to sync (Customer, BillProfile, etc.)
  - `CurrentPageNumber`: Current page number for pagination
  - `IsLastStepSync`: Flag indicating if this is the final staging-to-production step
- **If `IsLastStepSync = true`:** Call `ProcessLoadRevFromStaging()` to transfer staging data to production
- **If `IsLastStepSync = false`:** Call `ProcessEventAsync()` to fetch and stage data
- **If no SQS event:** Start fresh sync by getting first authentication ID and beginning with Customer step
- Handle all exceptions and ensure cleanup

### `BaseFunctionHandler(ILambdaContext context)`
**Purpose:** Initialize Lambda context wrapper with AWS and database settings

**Low-level Flow:**
- Create new `KeySysLambdaContext` instance
- Set up environment repository for accessing Lambda environment variables
- Initialize logging with KeysysLambdaLogger
- Set up Base64 service for credential decoding
- Determine if running in production based on function ARN
- Load database connection strings from environment variables
- Set up Redis connection string for caching
- Initialize settings repository for database-based configuration
- Load OU-specific settings unless skipped

### `ProcessEventAsync(KeySysLambdaContext context, int authId, RevSyncStep step, int pageNumber)`
**Purpose:** Main orchestrator that routes to appropriate sync method based on current step

**Low-level Flow:**
- Create `RevioAuthenticationRepository` to get API credentials
- Retrieve Rev.IO authentication details for current authentication ID
- Create `RevioApiClient` with authentication and HTTP factory
- **Switch on `RevSyncStep` enum to call appropriate sync method:**
  - `Customer` → `SyncRevCustomersAsync()`
  - `BillProfile` → `SyncRevBillProfilesAsync()`
  - `Provider` → `SyncRevProvidersAsync()`
  - `ProductType` → `SyncRevProductTypesAsync()`
  - `Product` → `SyncRevProductsAsync()`
  - `ServiceType` → `SyncRevServiceTypesAsync()`
  - `Service` → `SyncRevServicesAsync()`
  - `ServiceProduct` → `SyncRevServiceProductsAsync()`
  - `InventoryType` → `SyncRevInventoryTypesAsync()`
  - `InventoryItem` → `SyncRevInventoryItemsAsync()`
  - `UsagePlanGroup` → `SyncRevUsagePlanAsync()`
  - `Agent` → `SyncRevAgentAsync()`
  - `Package` → `SyncRevPackagesAsync()`
  - `PackageProduct` → `SyncRevPackageProductsAsync()`
- Handle exceptions and queue next instance on failure
- If no authentication found, queue next instance

### `SyncRev[EntityType]Async()` Methods (e.g., `SyncRevCustomersAsync()`)
**Purpose:** Sync specific entity type from Rev.IO API to staging table

**Low-level Flow (using Customers as example):**
- Create SQL retry policy for database resilience
- Call corresponding data fetching method (e.g., `CustomersAsync()`)
- **Inside data fetching method:**
  - Initialize empty list for storing entities
  - Call `GetPagesFromRevIOListAPI()` with entity-specific API method
  - Load fetched entities to staging table using `LoadRev[EntityType]ToStaging()`
  - Call `CheckProcessOfSyncStep()` to determine next action
- Handle SQL exceptions with retry logic

### `GetPagesFromRevIOListAPI<T>()` Generic Pagination Handler
**Purpose:** Handle paginated API calls with retry logic and batch processing

**Low-level Flow:**
- Initialize pagination variables (`isLastPage = false`, `pageNumber`, `counter = 0`)
- **While not last page and within limits:**
  - Check remaining Lambda execution time and page batch limits
  - Check failure count against acceptable limit (stop if too many failures)
  - Call provided API method delegate (e.g., `GetCustomersAsync()`)
  - **If API call successful:**
    - Add returned records to items list
    - Update `isLastPage` based on `HasMore` property
    - Increment page number and counter
    - Reset failure count
  - **If API call fails:**
    - Increment failure count
    - Log error details
    - Continue to next iteration or break if failure limit reached
- Return tuple of (`isLastPage`, `finalPageNumber`)

### `Get[EntityType]Async()` API Call Methods (e.g., `GetCustomersAsync()`)
**Purpose:** Make actual HTTP calls to Rev.IO APIs

**Low-level Flow:**
- Call `revApiClient.ExecuteRevGetWithRetryAsync<T>()` with:
  - Specific Rev.IO API URL
  - Page size from environment variable
  - Current page number
  - Logger instance
- Return `RevListResponse<T>` containing records and pagination info
- Built-in retry logic handles temporary API failures

### `LoadRev[EntityType]ToStaging()` Methods
**Purpose:** Bulk insert fetched data into staging tables

**Low-level Flow:**
- Create `DataTable` with appropriate schema for entity type
- **For each entity in the list:**
  - Create new `DataRow`
  - Map entity properties to table columns
  - Add `IntegrationAuthenticationId` for tracking
  - Add row to DataTable
- Use `SqlBulkCopy` to efficiently insert all rows into staging table
- Handle SQL exceptions with detailed logging

### `CheckProcessOfSyncStep()` Step Completion Handler
**Purpose:** Determine whether to continue pagination or move to next sync step

**Low-level Flow:**
- **If not last page (`!isLastPage`):**
  - Increment page number
  - Call `BuildMessageToQueue()` to queue same step with next page
- **If last page (`isLastPage`):**
  - Call `BuildMessageToQueue()` with `IsLastStepSync = true` for current step
  - Call `BuildMessageToQueue()` to start next sync step at page 1
- Handle transition from last step (PackageProduct) to completion

### `BuildMessageToQueue()` and `SendMessageToQueueAsync()`
**Purpose:** Create and send SQS messages for self-triggering Lambda continuation

**Low-level Flow:**
- Create SQS message with attributes:
  - `CurrentIntegrationAuthenticationId`: Current auth ID
  - `CurrentRevSyncStep`: Step to execute
  - `CurrentPageNumber`: Page number to process
  - `IsLastStepSync`: Whether to transfer staging to production
- **If `IsLastStepSync = true`:**
  - Set message body to indicate staging transfer
- **If `IsLastStepSync = false`:**
  - Set message body to indicate data fetching
- Send message to SQS queue using AWS SDK
- Handle SQS exceptions and retry logic

### `ProcessLoadRevFromStaging()` Staging to Production Transfer
**Purpose:** Transfer data from staging tables to production tables using stored procedures

**Low-level Flow:**
- Create SQL retry policy for resilience
- **Switch on `RevSyncStep` to call appropriate stored procedure:**
  - `Customer` → Execute `usp_Update_RevCustomer`
  - `BillProfile` → Execute `usp_Update_RevBillProfile`
  - `Provider` → Execute `usp_Update_RevProvider`
  - `ProductType` → Execute `usp_Update_RevProductType_FromStaging`
  - `Product` → Execute `usp_Update_RevProduct_FromStaging`
  - `ServiceType` → Execute `usp_Update_RevServiceType`
  - `Service` → Execute `usp_Update_RevService`
  - `ServiceProduct` → Execute `usp_Update_RevServiceProduct_FromStaging`
  - `InventoryType` → Execute `usp_Update_RevInventoryType_FromStaging`
  - `InventoryItem` → Execute `usp_Update_RevInventoryItem_FromStaging`
  - `UsagePlanGroup` → Execute `usp_Update_RevUsagePlanGroup_FromStaging`
  - `Agent` → Execute `usp_Update_RevAgent`
  - `Package` → Execute `usp_Update_RevPackage_FromStaging`
  - `PackageProduct` → Execute `usp_Update_RevPackageProduct_FromStaging`
- **After each stored procedure:**
  - Clear corresponding staging table using `ClearStagingTablesV2()` or specific delete methods
- Handle SQL exceptions with retry and detailed logging
- Queue next authentication instance when complete

### `LoadRev[EntityType]FromStagingTable()` Methods
**Purpose:** Execute stored procedures to transfer staging data to production tables

**Low-level Flow:**
- Create SQL connection to central database
- Create SQL command with appropriate stored procedure name
- Add `@IntegrationAuthenticationId` parameter
- Set command timeout to handle large data transfers
- Execute stored procedure synchronously
- Handle SQL exceptions with detailed error logging
- Ensure proper connection disposal

### `QueueNextRevInstanceAsync()` Instance Progression
**Purpose:** Move to next Rev.IO authentication instance or complete entire sync

**Low-level Flow:**
- Get next authentication ID using `RevIoAuthenticationRepository`
- **If next authentication ID found (`nextAuthId > 0`):**
  - Get tenant ID for current authentication
  - Send completion notification to AMOP 2.0 via `DailySyncAmopApiTrigger`
  - Update current authentication ID to next ID
  - Queue Customer sync step for new authentication ID
- **If no more authentication IDs:**
  - Log completion of all instances
  - Sync process ends naturally

### `SendNotificationToAmop20()` External System Integration
**Purpose:** Notify AMOP 2.0 system that Rev.IO sync has completed for a tenant

**Low-level Flow:**
- Get AMOP 2.0 sync update API URL from environment variables
- Create HTTP client with Lambda logging handler
- Build JSON payload with:
  - `key_name`: "rev_customers_sync"
  - `tenant_id`: Current tenant ID
  - `tenant_name`: Tenant name (optional)
- Send POST request to AMOP 2.0 API
- **If successful:** Log success message
- **If failed:** Log error response details
- Handle HTTP exceptions and timeouts

### `CleanUp()` Resource Management
**Purpose:** Ensure proper cleanup of resources and logging

**Low-level Flow:**
- Call `context.CleanUp()` which:
  - Flushes logger to ensure all log messages are written
  - Closes database connections if any are open
  - Disconnects from Redis cache if connected
  - Disposes of any other resources held by context
- Handle cleanup exceptions to prevent masking original errors

## Key Data Structures

### `RevSyncStep` Enum
Defines the sequential order of data synchronization:
```
None, Customer, BillProfile, Provider, ProductType, Product, 
ServiceType, Service, ServiceProduct, InventoryType, InventoryItem, 
UsagePlanGroup, Agent, Package, PackageProduct, Default
```

### `RevStagingTable` Constants
Maps sync steps to staging table names:
- `REV_CUSTOMER_STG` → "dbo.RevCustomerStaging"
- `REV_BILLING_PROFILE_STG` → "dbo.RevBillProfileStaging"
- `REV_PROVIDER_STG` → "dbo.RevProviderStaging"
- And so on for each entity type...

## Error Handling & Resilience

### Retry Policies
- **SQL Operations:** Polly retry policy with exponential backoff
- **HTTP API Calls:** Built-in retry in RevioApiClient with configurable attempts
- **SQS Operations:** AWS SDK built-in retry with exponential backoff

### Failure Scenarios
- **API Failures:** Continue with acceptable failure count, log errors, skip to next step if too many failures
- **Database Failures:** Retry with exponential backoff, fail Lambda execution if persistent
- **Timeout Handling:** Monitor Lambda remaining time, queue continuation if approaching timeout
- **Authentication Issues:** Skip to next authentication instance, log warnings

### Monitoring & Logging
- Comprehensive logging at each step with context
- Performance metrics (pages processed, time remaining)
- Error details with stack traces
- Progress tracking through SQS message attributes

## Stored Procedures Used

1. **`usp_Update_RevCustomer`** - Merge customers from staging to production
2. **`usp_Update_RevBillProfile`** - Merge bill profiles from staging  
3. **`usp_Update_RevProvider`** - Merge providers from staging
4. **`usp_Update_RevProductType_FromStaging`** - Merge product types
5. **`usp_Update_RevProduct_FromStaging`** - Merge products
6. **`usp_Update_RevServiceType`** - Merge service types
7. **`usp_Update_RevService`** - Merge services
8. **`usp_Update_RevServiceProduct_FromStaging`** - Merge service products
9. **`usp_Update_RevInventoryType_FromStaging`** - Merge inventory types
10. **`usp_Update_RevInventoryItem_FromStaging`** - Merge inventory items
11. **`usp_Update_RevUsagePlanGroup_FromStaging`** - Merge usage plan groups
12. **`usp_Update_RevAgent`** - Merge agents
13. **`usp_Update_RevPackage_FromStaging`** - Merge packages
14. **`usp_Update_RevPackageProduct_FromStaging`** - Merge package products
15. **`usp_RevCustomerClearStagingTables`** - Clear all staging tables

## Performance Characteristics

- **Batch Processing:** Maximum 20 pages per Lambda execution to stay within time limits
- **Pagination:** 1000 records per page (configurable via RevPageSize)
- **Bulk Operations:** SqlBulkCopy for efficient staging table inserts
- **Parallel Processing:** Multiple authentication instances can be processed sequentially
- **Memory Management:** Processes data in batches to avoid memory exhaustion
- **Time Management:** Monitors Lambda remaining execution time to queue continuations