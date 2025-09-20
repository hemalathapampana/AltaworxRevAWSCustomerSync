# Altaworx Rev.IO AWS Customer Sync Lambda - Complete Flow Documentation

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

## 1. Complete High-Level Sequential Function Flow

### Primary Entry Point and Initialization
1. **`FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`** - Main Lambda entry point
   - **Internal Methods:**
     - `BaseFunctionHandler(context)` - Initialize Lambda context
     - `GetCurrentIntegrationAuthenticationId()` - Extract auth ID from SQS message
     - `GetCurrentRevSyncStep()` - Extract sync step from SQS message  
     - `GetPageNumber()` - Extract page number from SQS message
     - `GetIsLastStepSyncNumber()` - Check if staging-to-production transfer
     - `ProcessLoadRevFromStaging()` OR `ProcessEventAsync()` - Route to appropriate handler

### Context and Authentication Setup
2. **`BaseFunctionHandler(ILambdaContext context, bool skipOUSpecificLogic = false)`** - Initialize Lambda wrapper
   - **Internal Methods:**
     - `new KeySysLambdaContext(context, skipOUSpecificLogic)` - Create context wrapper
     - `LoadOUSettings()` - Load organizational unit settings

3. **`GetNextRevIoAuthenticationId(int currentId)`** - Get next authentication to process
   - **Internal Methods:**
     - SQL stored procedure call: `usp_Rev_Get_Next_Authentication`

### Main Processing Orchestrator
4. **`ProcessEventAsync(KeySysLambdaContext context, int authId, RevSyncStep step, int pageNumber)`**
   - **Internal Methods:**
     - `new RevIoAuthenticationRepository()` - Create auth repository
     - `GetRevioApiAuthentication(authId)` - Get API credentials
     - `new RevioApiClient()` - Create API client
     - Route to appropriate sync method based on `RevSyncStep`

### Data Synchronization Methods (Sequential Order)
5. **`SyncRevCustomersAsync()`** → **`CustomersAsync()`**
   - **Internal Methods:**
     - `GetSqlRetryPolicy()` - Create retry policy
     - `GetPagesFromRevIOListAPI()` - Handle pagination
     - `GetCustomersAsync()` - Make API call
     - `LoadRevCustomersToStaging()` - Bulk insert to staging
     - `CheckProcessOfSyncStep()` - Determine next action

6. **`SyncRevBillProfilesAsync()`** → **`GetBillProfilesAsync()`**
   - **Internal Methods:**
     - `GetSqlRetryPolicy()` - Create retry policy
     - `GetPagesFromRevIOListAPI()` - Handle pagination
     - `GetBillProfilesAsync()` - Make API call
     - `LoadRevBillProfilesToStaging()` - Bulk insert to staging
     - `CheckProcessOfSyncStep()` - Determine next action

7. **`SyncRevProvidersAsync()`** → **`GetProvidersAsync()`**
   - **Internal Methods:**
     - `GetSqlRetryPolicy()` - Create retry policy
     - `GetPagesFromRevIOListAPI()` - Handle pagination
     - `GetProvidersAsync()` - Make API call
     - `LoadRevProvidersToStaging()` - Bulk insert to staging
     - `CheckProcessOfSyncStep()` - Determine next action

8. **`SyncRevProductTypesAsync()`** → **`GetProductTypesAsync()`**
   - **Internal Methods:**
     - `GetSqlRetryPolicy()` - Create retry policy
     - `GetPagesFromRevIOListAPI()` - Handle pagination
     - `GetProductTypesAsync()` - Make API call
     - `LoadRevProductTypesToStaging()` - Bulk insert to staging
     - `CheckProcessOfSyncStep()` - Determine next action

9. **`SyncRevProductsAsync()`** → **`GetProductsAsync()`**
   - **Internal Methods:**
     - `GetSqlRetryPolicy()` - Create retry policy
     - `GetPagesFromRevIOListAPI()` - Handle pagination
     - `GetProductsAsync()` - Make API call
     - `LoadRevProductsToStaging()` - Bulk insert to staging
     - `CheckProcessOfSyncStep()` - Determine next action

10. **`SyncRevServiceTypesAsync()`** → **`RevServiceTypesAsync()`**
    - **Internal Methods:**
      - `GetSqlRetryPolicy()` - Create retry policy
      - `GetPagesFromRevIOListAPI()` - Handle pagination
      - `GetServiceTypesAsync()` - Make API call
      - `LoadRevServiceTypesToStaging()` - Bulk insert to staging
      - `CheckProcessOfSyncStep()` - Determine next action

11. **`SyncRevServicesAsync()`** → **`GetServicesAsync()`**
    - **Internal Methods:**
      - `GetSqlRetryPolicy()` - Create retry policy
      - `GetPagesFromRevIOListAPI()` - Handle pagination
      - `GetServicesAsync()` - Make API call
      - `LoadRevServicesToStaging()` - Bulk insert to staging
      - `CheckProcessOfSyncStep()` - Determine next action

12. **`SyncRevServiceProductsAsync()`** → **`GetServiceProductsAsync()`**
    - **Internal Methods:**
      - `GetSqlRetryPolicy()` - Create retry policy
      - `GetPagesFromRevIOListAPI()` - Handle pagination
      - `GetServiceProductsAsync()` - Make API call
      - `LoadRevServiceProductsToStaging()` - Bulk insert to staging
      - `CheckProcessOfSyncStep()` - Determine next action

13. **`SyncRevInventoryTypesAsync()`** → **`GetInventoryTypesAsync()`**
    - **Internal Methods:**
      - `GetSqlRetryPolicy()` - Create retry policy
      - `GetPagesFromRevIOListAPI()` - Handle pagination
      - `GetInventoryTypesAsync()` - Make API call
      - `LoadRevInventoryTypesToStaging()` - Bulk insert to staging
      - `BuildMessageLoadRevFromStagingToQueueAsync()` - Queue staging transfer
      - `BuildMessageToQueue()` - Queue next step

14. **`SyncRevInventoryItemsAsync()`** → **`GetInventoryItemsAsync()`**
    - **Internal Methods:**
      - `GetSqlRetryPolicy()` - Create retry policy
      - `GetPagesFromRevIOListAPI()` - Handle pagination
      - `GetInventoryItemsAsync()` - Make API call
      - `LoadRevInventoryItemsToStaging()` - Bulk insert to staging
      - `BuildMessageLoadRevFromStagingToQueueAsync()` - Queue staging transfer
      - `BuildMessageToQueue()` - Queue next step

15. **`SyncRevUsagePlanAsync()`** → **`GetUsagePlanGroupsAsync()`**
    - **Internal Methods:**
      - `GetSqlRetryPolicy()` - Create retry policy
      - `GetPagesFromRevIOListAPI()` - Handle pagination
      - `GetUsagePlanGroupsAsync()` - Make API call
      - `LoadRevUsagePlanGroupsToStaging()` - Bulk insert to staging
      - `BuildMessageLoadRevFromStagingToQueueAsync()` - Queue staging transfer
      - `BuildMessageToQueue()` - Queue next step

16. **`SyncRevAgentAsync()`** → **`GetAgentAsync()`**
    - **Internal Methods:**
      - `GetSqlRetryPolicy()` - Create retry policy
      - `GetPagesFromRevIOListAPI()` - Handle pagination
      - `GetAgentAsync()` - Make API call
      - `LoadRevAgentsToStaging()` - Bulk insert to staging
      - `CheckProcessOfSyncStep()` - Determine next action

17. **`SyncRevPackagesAsync()`** → **`RevPackagesAsync()`**
    - **Internal Methods:**
      - `GetSqlRetryPolicy()` - Create retry policy
      - `GetPagesFromRevIOListAPI()` - Handle pagination
      - `GetPackagesAsync()` - Make API call
      - `LoadRevPackagesToStaging()` - Bulk insert to staging
      - `CheckProcessOfSyncStep()` - Determine next action

18. **`SyncRevPackageProductsAsync()`** → **`RevPackageProductsAsync()`**
    - **Internal Methods:**
      - `GetSqlRetryPolicy()` - Create retry policy
      - `GetPagesFromRevIOListAPI()` - Handle pagination
      - `GetPackageProductsAsync()` - Make API call
      - `LoadRevPackageProductsToStaging()` - Bulk insert to staging
      - `CheckProcessOfSyncStep()` - Determine next action

### Staging to Production Transfer
19. **`ProcessLoadRevFromStaging(RevSyncStep currentRevSyncStep, int authId)`**
    - **Internal Methods:**
      - `GetSqlRetryPolicy()` - Create retry policy
      - Route to appropriate stored procedure based on sync step:
        - `LoadRevCustomersFromStagingTable()` → `usp_Update_RevCustomer`
        - `LoadRevBillProfilesFromStagingTable()` → `usp_Update_RevBillProfile`
        - `LoadRevProvidersFromStagingTable()` → `usp_Update_RevProvider`
        - `LoadRevProductTypesFromStagingTable()` → `usp_Update_RevProductType_FromStaging`
        - `LoadRevProductsFromStagingTable()` → `usp_Update_RevProduct_FromStaging`
        - `LoadRevServiceTypesFromStagingTable()` → `usp_Update_RevServiceType`
        - `LoadRevServicesFromStagingTable()` → `usp_Update_RevService`
        - `LoadRevServiceProductsFromStagingTable()` → `usp_Update_RevServiceProduct_FromStaging`
        - `LoadRevInventoryTypesFromStagingTable()` → `usp_Update_RevInventoryType_FromStaging`
        - `LoadRevInventoryItemsFromStagingTable()` → `usp_Update_RevInventoryItem_FromStaging`
        - `LoadRevUsagePlanGroupsFromStagingTable()` → `usp_Update_RevUsagePlanGroup_FromStaging`
        - `LoadRevAgentFromStagingTable()` → `usp_Update_RevAgent`
        - `LoadRevPackageFromStagingTable()` → `usp_Update_RevPackage_FromStaging`
        - `LoadRevPackageProductFromStagingTable()` → `usp_Update_RevPackageProduct_FromStaging`
      - Clear staging tables after transfer
      - `QueueNextRevInstanceAsync()` - Queue next authentication instance

### Instance and Queue Management
20. **`QueueNextRevInstanceAsync(int currentAuthId)`**
    - **Internal Methods:**
      - `GetNextRevIoAuthenticationId()` - Get next authentication ID
      - `SendNotificationToAmop20()` - Notify AMOP 2.0 system
      - `BuildMessageToQueue()` - Queue next instance or complete

21. **`SendNotificationToAmop20()`**
    - **Internal Methods:**
      - HTTP POST to AMOP 2.0 API with completion notification

### Cleanup and Finalization
22. **`CleanUp(KeySysLambdaContext context)`**
    - **Internal Methods:**
      - `context.CleanUp()` - Flush logs and close connections

## 2. Detailed Low-Level Flow for Each Method

### `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`

**Purpose:** Main Lambda entry point that determines execution path

**Low-level Flow:**
```csharp
// Line 67-165: Method declaration and initialization
KeySysLambdaContext keysysContext = null;
try
{
    // Line 72: Initialize Lambda context wrapper
    keysysContext = base.BaseFunctionHandler(context);
    
    // Lines 73-91: Load environment variables if not already set
    if (string.IsNullOrWhiteSpace(RevGetCustomersUrl))
    {
        // Extract all environment variables from context.ClientContext.Environment
        RevCustomerSyncQueueURL = context.ClientContext.Environment["RevCustomerSyncQueueUrl"];
        RevGetCustomersUrl = context.ClientContext.Environment["RevGetCustomersUrl"];
        // ... (load all other environment variables)
        PageSize = int.Parse(context.ClientContext.Environment["RevPageSize"]);
    }

    // Lines 93-96: Initialize variables for message processing
    int currentIntegrationAuthenticationId;
    bool isLastStepSync = false;
    RevSyncStep currentRevSyncStep = RevSyncStep.None;
    int pageNumber = DEFAULT_PAGE_NUMBER;
    
    // Lines 97-115: Process SQS event if present
    if (sqsEvent?.Records != null)
    {
        // Line 99: Get first SQS record
        var currentRecord = sqsEvent.Records.First();
        
        // Lines 100-103: Extract message attributes
        currentIntegrationAuthenticationId = GetCurrentIntegrationAuthenticationId(keysysContext, currentRecord);
        currentRevSyncStep = GetCurrentRevSyncStep(keysysContext, currentRecord);
        pageNumber = GetPageNumber(keysysContext, currentRecord);
        isLastStepSync = GetIsLastStepSyncNumber(keysysContext, currentRecord);

        // Lines 106-114: Route based on message type
        if (isLastStepSync)
        {
            // Process staging to production transfer
            ProcessLoadRevFromStaging(keysysContext, currentRevSyncStep, currentIntegrationAuthenticationId);
        }
        else
        {
            // Process data fetching and staging
            await ProcessEventAsync(keysysContext, currentIntegrationAuthenticationId, currentRevSyncStep, pageNumber);
        }
    }
    // Lines 116-158: Handle direct invocation (no SQS event)
    else
    {
        currentIntegrationAuthenticationId = 0;
        
        // Lines 121-141: Get first authentication ID
        if (currentIntegrationAuthenticationId == 0)
        {
            var revIoAuthRepo = new RevIoAuthenticationRepository(keysysContext.logger, keysysContext.Base64Service, keysysContext.CentralDbConnectionString);
            var authId = revIoAuthRepo.GetNextRevIoAuthenticationId(currentIntegrationAuthenticationId);
            
            switch (authId)
            {
                case 0: // Exception occurred
                    LogInfo(keysysContext, "WARNING", $"Error Getting an auth id for Rev.IO CurrentIntegrationAuthenticationId: {currentIntegrationAuthenticationId}");
                    proceed = false;
                    break;
                case -1: // No authentication records found
                    LogInfo(keysysContext, "WARNING", "No Authentication record was found for Rev.IO.");
                    proceed = false;
                    break;
                default: // Valid authentication ID found
                    currentIntegrationAuthenticationId = authId;
                    currentRevSyncStep = RevSyncStep.Customer;
                    break;
            }
        }

        // Lines 143-152: Start sync process if valid authentication found
        if (proceed)
        {
            LogInfo(keysysContext, "CurrentIntegrationAuthenticationId", currentIntegrationAuthenticationId);
            LogInfo(keysysContext, "CurrentRevSyncStep", currentRevSyncStep);
            
            // Create SQL retry policy and clear staging tables
            var sqlRetryPolicy = GetSqlRetryPolicy(keysysContext);
            sqlRetryPolicy.Execute(() => ClearStagingTables(keysysContext));
            
            // Start processing with Customer step
            await ProcessEventAsync(keysysContext, currentIntegrationAuthenticationId, currentRevSyncStep, DEFAULT_PAGE_NUMBER);
        }
    }
}
// Lines 160-163: Exception handling
catch (Exception ex)
{
    LogInfo(keysysContext, "EXCEPTION", ex.Message + " " + ex.Source + " " + ex.StackTrace);
}
// Line 164: Cleanup resources
CleanUp(keysysContext);
```

### `BaseFunctionHandler(ILambdaContext context, bool skipOUSpecificLogic = false)`

**Purpose:** Initialize Lambda context wrapper with AWS and database settings

**Low-level Flow:**
```csharp
// Lines 40-45: Method from AwsFunctionBase class
public KeySysLambdaContext BaseFunctionHandler(ILambdaContext context, bool skipOUSpecificLogic = false)
{
    // Line 43: Create new KeySysLambdaContext instance
    // This constructor internally:
    // - Sets up environment repository for accessing Lambda environment variables
    // - Initializes logging with KeysysLambdaLogger
    // - Sets up Base64 service for credential decoding
    // - Determines if running in production based on function ARN
    // - Loads database connection strings from environment variables
    // - Sets up Redis connection string for caching
    // - Initializes settings repository for database-based configuration
    KeySysLambdaContext keySysLambdaContext = new KeySysLambdaContext(context, skipOUSpecificLogic);
    
    // Line 44: Return initialized context
    return keySysLambdaContext;
}
```

### `GetCurrentIntegrationAuthenticationId(KeySysLambdaContext context, SQSEvent.SQSMessage message)`

**Purpose:** Extract authentication ID from SQS message attributes

**Low-level Flow:**
```csharp
// Lines 188-198: Static method to extract auth ID
private static int GetCurrentIntegrationAuthenticationId(KeySysLambdaContext context, SQSEvent.SQSMessage message)
{
    // Line 190: Initialize with default invalid value
    int integrationAuthenticationId = -1;
    
    // Lines 191-194: Check if message contains auth ID attribute
    if (message.MessageAttributes.ContainsKey("CurrentIntegrationAuthenticationId"))
    {
        // Parse string value to integer
        integrationAuthenticationId = int.Parse(message.MessageAttributes["CurrentIntegrationAuthenticationId"].StringValue);
    }

    // Lines 196-197: Log extracted value and return
    LogInfo(context, "CurrentIntegrationAuthenticationId", integrationAuthenticationId);
    return integrationAuthenticationId;
}
```

### `GetCurrentRevSyncStep(KeySysLambdaContext context, SQSEvent.SQSMessage message)`

**Purpose:** Extract sync step from SQS message attributes

**Low-level Flow:**
```csharp
// Lines 200-211: Static method to extract sync step
private static RevSyncStep GetCurrentRevSyncStep(KeySysLambdaContext context, SQSEvent.SQSMessage message)
{
    // Line 202: Initialize with default None value
    RevSyncStep revSyncStep = RevSyncStep.None;
    
    // Lines 203-206: Check if message contains sync step attribute
    if (message.MessageAttributes.ContainsKey("CurrentRevSyncStep"))
    {
        // Parse string value to enum, casting from integer
        revSyncStep = (RevSyncStep)int.Parse(message.MessageAttributes["CurrentRevSyncStep"].StringValue);
    }

    // Lines 208-209: Log extracted value and return
    LogInfo(context, "CurrentRevSyncStep", revSyncStep);
    return revSyncStep;
}
```

### `GetPageNumber(KeySysLambdaContext context, SQSEvent.SQSMessage message)`

**Purpose:** Extract page number from SQS message attributes

**Low-level Flow:**
```csharp
// Lines 178-187: Method to extract page number
private int GetPageNumber(KeySysLambdaContext context, SQSEvent.SQSMessage message)
{
    // Line 180: Initialize with default first page
    var pageNumber = 1;
    
    // Lines 181-185: Check if message contains page number attribute
    if (message.MessageAttributes.ContainsKey("CurrentPageNumber"))
    {
        // Parse string value to integer
        pageNumber = int.Parse(message.MessageAttributes["CurrentPageNumber"].StringValue);
        // Log extracted value
        context.LogInfo("CurrentPageNumber", pageNumber);
    }
    
    // Line 186: Return page number
    return pageNumber;
}
```

### `GetIsLastStepSyncNumber(KeySysLambdaContext context, SQSEvent.SQSMessage message)`

**Purpose:** Check if this is a staging-to-production transfer message

**Low-level Flow:**
```csharp
// Lines 167-176: Method to check if staging transfer
private bool GetIsLastStepSyncNumber(KeySysLambdaContext context, SQSEvent.SQSMessage message)
{
    // Line 169: Initialize with default false
    var isLastStepSync = false;
    
    // Lines 170-174: Check if message contains last step sync attribute
    if (message.MessageAttributes.ContainsKey("IsLastStepSync"))
    {
        // Parse string value to boolean
        isLastStepSync = bool.Parse(message.MessageAttributes["IsLastStepSync"].StringValue);
        // Log extracted value
        context.LogInfo("IsLastStepSync", isLastStepSync);
    }
    
    // Line 175: Return boolean flag
    return isLastStepSync;
}
```

### `ProcessEventAsync(KeySysLambdaContext context, int authId, RevSyncStep step, int pageNumber)`

**Purpose:** Main orchestrator that routes to appropriate sync method based on current step

**Low-level Flow:**
```csharp
// Lines 212-290: Main processing orchestrator method
private async Task ProcessEventAsync(KeySysLambdaContext context, int currentIntegrationAuthenticationId, RevSyncStep currentRevSyncStep, int pageNumber)
{
    // Lines 214-215: Log method entry with parameters
    LogInfo(context, CommonConstants.SUB, $"ProcessEventAsync({currentIntegrationAuthenticationId}, {currentRevSyncStep}, {pageNumber})");

    try
    {
        // Lines 218-219: Create authentication repository
        var revIoAuthRepo = new RevIoAuthenticationRepository(context.logger, context.Base64Service, context.CentralDbConnectionString);
        
        // Line 220: Get API authentication details
        var revIoAuthentication = revIoAuthRepo.GetRevioApiAuthentication(currentIntegrationAuthenticationId);
        
        // Lines 222-223: Check if authentication was found
        if (revIoAuthentication != null)
        {
            // Lines 224-225: Create HTTP factory and API client
            var httpClientFactory = new HttpClientFactory(context.logger);
            var revApiClient = new RevioApiClient(httpClientFactory, _httpRequestFactory, revIoAuthentication, context.IsProduction);

            // Lines 227-276: Switch on sync step to call appropriate method
            switch (currentRevSyncStep)
            {
                case RevSyncStep.Customer:
                    await SyncRevCustomersAsync(context, revApiClient, currentIntegrationAuthenticationId, pageNumber);
                    break;
                case RevSyncStep.BillProfile:
                    await SyncRevBillProfilesAsync(context, revApiClient, currentIntegrationAuthenticationId, pageNumber);
                    break;
                case RevSyncStep.Provider:
                    await SyncRevProvidersAsync(context, revApiClient, currentIntegrationAuthenticationId, pageNumber);
                    break;
                case RevSyncStep.ProductType:
                    await SyncRevProductTypesAsync(context, revApiClient, currentIntegrationAuthenticationId, pageNumber);
                    break;
                case RevSyncStep.Product:
                    await SyncRevProductsAsync(context, revApiClient, currentIntegrationAuthenticationId, pageNumber);
                    break;
                case RevSyncStep.ServiceType:
                    await SyncRevServiceTypesAsync(context, revApiClient, currentIntegrationAuthenticationId, pageNumber);
                    break;
                case RevSyncStep.Service:
                    await SyncRevServicesAsync(context, revApiClient, currentIntegrationAuthenticationId, pageNumber);
                    break;
                case RevSyncStep.ServiceProduct:
                    await SyncRevServiceProductsAsync(context, revApiClient, currentIntegrationAuthenticationId, pageNumber);
                    break;
                case RevSyncStep.InventoryType:
                    await SyncRevInventoryTypesAsync(context, revApiClient, currentIntegrationAuthenticationId, pageNumber);
                    break;
                case RevSyncStep.InventoryItem:
                    await SyncRevInventoryItemsAsync(context, revApiClient, currentIntegrationAuthenticationId, pageNumber);
                    break;
                case RevSyncStep.UsagePlanGroup:
                    await SyncRevUsagePlanAsync(context, revApiClient, currentIntegrationAuthenticationId, pageNumber);
                    break;
                case RevSyncStep.Agent:
                    await SyncRevAgentAsync(context, revApiClient, currentIntegrationAuthenticationId, pageNumber);
                    break;
                case RevSyncStep.Package:
                    await SyncRevPackagesAsync(context, revApiClient, currentIntegrationAuthenticationId, pageNumber);
                    break;
                case RevSyncStep.PackageProduct:
                    await SyncRevPackageProductsAsync(context, revApiClient, currentIntegrationAuthenticationId, pageNumber);
                    break;
                default:
                    // Lines 274-275: Log unknown sync step
                    LogInfo(context, "EXCEPTION", $"Unknown RevSyncStep: {currentRevSyncStep}");
                    break;
            }
        }
        else
        {
            // Lines 279-281: Handle missing authentication
            LogInfo(context, "EXCEPTION", $"No RevIo Authentication found for IntegrationAuthenticationId: {currentIntegrationAuthenticationId}");
            await QueueNextRevInstanceAsync(context, currentIntegrationAuthenticationId);
        }
    }
    // Lines 283-287: Exception handling
    catch (Exception ex)
    {
        LogInfo(context, "EXCEPTION", $"ProcessEventAsync Exception: {ex.Message} {ex.StackTrace}");
        await QueueNextRevInstanceAsync(context, currentIntegrationAuthenticationId);
    }
}
```

### `SyncRevCustomersAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int authId, int pageNumber)`

**Purpose:** Sync customer data from Rev.IO API to staging table

**Low-level Flow:**
```csharp
// Lines 400-406: Customer sync method
private async Task SyncRevCustomersAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
{
    // Line 402: Log method entry
    LogInfo(context, CommonConstants.SUB, $"SyncRevCustomersAsync({currentIntegrationAuthenticationId}, {pageNumber})");
    
    // Line 403: Create SQL retry policy for database resilience
    var sqlRetryPolicy = GetSqlRetryPolicy(context);
    
    // Line 404: Call customer data fetching method with retry policy
    await CustomersAsync(context, sqlRetryPolicy, revApiClient, currentIntegrationAuthenticationId, pageNumber);
}
```

### `CustomersAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int authId, int pageNumber)`

**Purpose:** Fetch customer data from Rev.IO API and load to staging

**Low-level Flow:**
```csharp
// Lines 1306-1316: Customer data fetching method
private async Task CustomersAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
{
    // Line 1308: Log method entry with parameters
    LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");

    // Line 1310: Initialize empty list for storing customers
    List<RevCustomer> customers = new List<RevCustomer>();
    
    // Line 1311: Call generic pagination handler to fetch all pages
    (bool isLastPage, pageNumber) = await GetPagesFromRevIOListAPI(context, revApiClient, pageNumber, customers, GetCustomersAsync);

    // Line 1313: Execute staging load with retry policy
    sqlRetryPolicy.Execute(() => LoadRevCustomersToStaging(context, customers, currentIntegrationAuthenticationId));

    // Line 1315: Check if need to continue pagination or move to next step
    await CheckProcessOfSyncStep(context, sqlRetryPolicy, currentIntegrationAuthenticationId, RevSyncStep.Customer, RevSyncStep.BillProfile, RevStagingTable.REV_CUSTOMER_STG, pageNumber, isLastPage);
}
```

### `GetPagesFromRevIOListAPI<T>(context, revApiClient, pageNumber, items, apiMethod)`

**Purpose:** Generic pagination handler for Rev.IO API calls with retry logic

**Low-level Flow:**
```csharp
// This is a generic method that handles pagination for all Rev.IO list APIs
// Located in RevIOCommon class

// Initialize pagination variables
bool isLastPage = false;
int counter = 0;
int failureCount = 0;
const int MAX_ACCEPTABLE_FAILURES = 5;

// Continue until last page or limits reached
while (!isLastPage && counter < MAX_NUMBER_OF_PAGES_IN_A_BATCH)
{
    // Check remaining Lambda execution time
    if (context.GetRemainingTimeInMillis() < 30000) // Less than 30 seconds
    {
        LogInfo(context, "WARNING", "Approaching Lambda timeout, stopping pagination");
        break;
    }
    
    // Check failure count
    if (failureCount >= MAX_ACCEPTABLE_FAILURES)
    {
        LogInfo(context, "ERROR", $"Too many API failures ({failureCount}), stopping pagination");
        break;
    }
    
    try
    {
        // Call the provided API method delegate (e.g., GetCustomersAsync)
        var response = await apiMethod(context, revApiClient, pageNumber);
        
        if (response != null && response.Records != null)
        {
            // Add returned records to items list
            items.AddRange(response.Records);
            
            // Update pagination state
            isLastPage = !response.HasMore; // Check if more pages available
            pageNumber++; // Increment for next iteration
            counter++; // Increment batch counter
            failureCount = 0; // Reset failure count on success
            
            LogInfo(context, "INFO", $"Fetched page {pageNumber-1}, records: {response.Records.Count}, hasMore: {response.HasMore}");
        }
        else
        {
            // Handle null response
            LogInfo(context, "WARNING", $"Null response from API for page {pageNumber}");
            failureCount++;
        }
    }
    catch (Exception ex)
    {
        // Handle API call failures
        LogInfo(context, "ERROR", $"API call failed for page {pageNumber}: {ex.Message}");
        failureCount++;
        
        // Continue to next iteration or break if too many failures
        if (failureCount >= MAX_ACCEPTABLE_FAILURES)
        {
            break;
        }
    }
}

// Return final pagination state
return (isLastPage, pageNumber);
```

### `GetCustomersAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)`

**Purpose:** Make HTTP call to Rev.IO Customers API

**Low-level Flow:**
```csharp
// This method makes the actual API call to Rev.IO
private async Task<RevListResponse<RevCustomer>> GetCustomersAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)
{
    // Log method entry
    LogInfo(context, CommonConstants.SUB, $"({pageNumber})");

    // Call RevioApiClient with retry logic built-in
    // This internally:
    // 1. Constructs URL with pagination parameters
    // 2. Adds bill profile filtering if configured
    // 3. Sets up authentication headers
    // 4. Makes HTTP GET request with Polly retry policy
    // 5. Deserializes JSON response to RevListResponse<RevCustomer>
    return await revApiClient.ExecuteRevGetWithRetryAsync<RevListResponse<RevCustomer>>(RevGetCustomersUrl, PageSize, pageNumber, context.logger);
}
```

### `LoadRevCustomersToStaging(KeySysLambdaContext context, List<RevCustomer> customerList, int authId)`

**Purpose:** Bulk insert customer data into staging table

**Low-level Flow:**
```csharp
// Lines 1064-1080: Customer staging load method
private void LoadRevCustomersToStaging(KeySysLambdaContext context, List<RevCustomer> customerList, int currentIntegrationAuthenticationId)
{
    // Line 1066: Log method entry
    LogInfo(context, CommonConstants.SUB, $"LoadRevCustomersToStaging({customerList.Count})");

    // Line 1068: Create DataTable with customer schema
    DataTable table = RevCustomerStagingColumnNames.GetDataTable();

    // Lines 1069-1073: Populate DataTable with customer data
    foreach (var customer in customerList)
    {
        // Create DataRow and populate with customer properties
        DataRow row = AddToDataRow(table, customer, currentIntegrationAuthenticationId);
        table.Rows.Add(row);
    }

    // Lines 1075-1079: Bulk copy to staging table
    try
    {
        // Use SqlBulkCopy for efficient insertion
        AwsFunctionBase.SqlBulkCopy(context, context.CentralDbConnectionString, table, RevStagingTable.REV_CUSTOMER_STG);
    }
    catch (Exception ex)
    {
        LogInfo(context, "EXCEPTION", $"Error loading customers to staging: {ex.Message}");
        throw;
    }
}
```

### `AddToDataRow(DataTable table, RevCustomer customer, int authId)` - Customer Version

**Purpose:** Map RevCustomer object properties to DataTable row

**Low-level Flow:**
```csharp
// This method maps individual customer properties to staging table columns
private static DataRow AddToDataRow(DataTable table, RevCustomer customer, int integrationAuthenticationId)
{
    // Create new DataRow from table schema
    DataRow row = table.NewRow();
    
    // Map each customer property to corresponding column
    row[RevCustomerStagingColumnNames.RevCustomerId] = customer.Id ?? (object)DBNull.Value;
    row[RevCustomerStagingColumnNames.CustomerName] = customer.Name ?? (object)DBNull.Value;
    row[RevCustomerStagingColumnNames.ParentCustomerId] = customer.ParentCustomerId ?? (object)DBNull.Value;
    row[RevCustomerStagingColumnNames.Status] = customer.Status ?? (object)DBNull.Value;
    row[RevCustomerStagingColumnNames.ActivatedDate] = customer.ActivatedDate ?? (object)DBNull.Value;
    row[RevCustomerStagingColumnNames.CloseDate] = customer.CloseDate ?? (object)DBNull.Value;
    row[RevCustomerStagingColumnNames.TaxExemptEnabled] = customer.TaxExemptEnabled ?? (object)DBNull.Value;
    row[RevCustomerStagingColumnNames.TaxExemptTypes] = customer.TaxExemptTypes ?? (object)DBNull.Value;
    row[RevCustomerStagingColumnNames.BillProfileId] = customer.BillProfileId ?? (object)DBNull.Value;
    row[RevCustomerStagingColumnNames.AgentId] = customer.AgentId ?? (object)DBNull.Value;
    
    // Add authentication ID for tracking
    row[RevCustomerStagingColumnNames.IntegrationAuthenticationId] = integrationAuthenticationId;
    
    return row;
}
```

### `CheckProcessOfSyncStep(context, sqlRetryPolicy, authId, currentStep, nextStep, stagingTable, pageNumber, isLastPage)`

**Purpose:** Determine whether to continue pagination or move to next sync step

**Low-level Flow:**
```csharp
// Lines 1317-1341: Step completion handler
private async Task CheckProcessOfSyncStep(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, int currentIntegrationAuthenticationId, RevSyncStep currentStep, RevSyncStep nextStep, string stgTable, int pageNumber, bool isLastPage)
{
    // Lines 1319-1320: Log method entry
    LogInfo(context, CommonConstants.SUB, $"CheckProcessOfSyncStep({currentStep}, {nextStep}, {pageNumber}, {isLastPage})");

    // Lines 1322-1326: If not last page, continue with same step
    if (!isLastPage)
    {
        // Queue message to continue pagination for current step
        await BuildMessageToQueue(context, currentIntegrationAuthenticationId, currentStep, pageNumber);
    }
    else
    {
        // Lines 1328-1340: If last page, move to staging transfer and next step
        
        // Line 1330: Queue message to transfer current step from staging to production
        await BuildMessageLoadRevFromStagingToQueueAsync(context, RevCustomerSyncQueueURL, currentIntegrationAuthenticationId, currentStep);
        
        // Lines 1332-1339: Queue next step or complete sync
        if (nextStep != RevSyncStep.None)
        {
            // Queue next sync step starting from page 1
            await BuildMessageToQueue(context, currentIntegrationAuthenticationId, nextStep, DEFAULT_PAGE_NUMBER);
        }
        else
        {
            // This was the last step, queue next authentication instance
            await QueueNextRevInstanceAsync(context, currentIntegrationAuthenticationId);
        }
    }
}
```

### `BuildMessageToQueue(KeySysLambdaContext context, int authId, RevSyncStep step, int pageNumber)`

**Purpose:** Create and send SQS message for Lambda continuation

**Low-level Flow:**
```csharp
// Lines 363-376: Message building method
public async Task BuildMessageToQueue(KeySysLambdaContext context, int currentIntegrationAuthenticationId, RevSyncStep revSyncStep, int pageNumber)
{
    // Lines 365-366: Log method entry
    LogInfo(context, CommonConstants.SUB, "BuildMessageToQueue");
    LogInfo(context, "revSyncStep", revSyncStep);

    // Lines 368-375: Call message sending method with standard parameters
    await SendMessageToQueueAsync(context, RevCustomerSyncQueueURL, currentIntegrationAuthenticationId, revSyncStep, pageNumber);
}
```

### `SendMessageToQueueAsync(context, queueURL, authId, syncStep, pageNumber)`

**Purpose:** Send SQS message to trigger Lambda continuation

**Low-level Flow:**
```csharp
// Lines 1926-1977: SQS message sending method
private async Task SendMessageToQueueAsync(KeySysLambdaContext context, string revCustomerSyncQueueURL, int currentIntegrationAuthenticationId, RevSyncStep nextRevSyncStep, int pageNumber)
{
    // Lines 1928-1933: Log method parameters
    LogInfo(context, "SUB", "SendMessageToQueueAsync");
    LogInfo(context, "revCustomerSyncQueueURL", revCustomerSyncQueueURL);
    LogInfo(context, "nextRevSyncStep", nextRevSyncStep);
    LogInfo(context, "pageNumber", pageNumber);

    // Lines 1934-1938: Skip if queue URL not configured (for testing)
    if (string.IsNullOrWhiteSpace(revCustomerSyncQueueURL))
    {
        return;
    }

    // Lines 1940-1976: Create and send SQS message
    var awsCredentials = AwsCredentials(context);
    using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
    {
        // Lines 1943-1944: Create message body
        var requestMsgBody = $"Current Auth Id {currentIntegrationAuthenticationId}";
        LogInfo(context, "Sending message for", $"{requestMsgBody} to destination queue: {revCustomerSyncQueueURL}");

        // Lines 1946-1965: Create SQS message with attributes
        var request = new SendMessageRequest
        {
            MessageAttributes = new Dictionary<string, MessageAttributeValue>
            {
                {
                    "CurrentIntegrationAuthenticationId", new MessageAttributeValue
                    { DataType = "String", StringValue = currentIntegrationAuthenticationId.ToString()}
                },
                {
                    "CurrentRevSyncStep", new MessageAttributeValue
                    { DataType = "String", StringValue = ((int)nextRevSyncStep).ToString()}
                },
                {
                    "CurrentPageNumber", new MessageAttributeValue
                    { DataType = "String", StringValue = ((int)pageNumber).ToString()}
                }
            },
            MessageBody = requestMsgBody,
            QueueUrl = revCustomerSyncQueueURL
        };

        // Lines 1967-1970: Log request details
        LogInfo(context, "STATUS", "SendMessageRequest is ready!");
        LogInfo(context, "MessageBody", request.MessageBody);
        LogInfo(context, "QueueURL", request.QueueUrl);

        // Lines 1971-1976: Send message and check response
        var response = await client.SendMessageAsync(request);
        if (((int)response.HttpStatusCode < 200) || ((int)response.HttpStatusCode > 299))
        {
            LogInfo(context, "EXCEPTION", $"Error enqueuing Rev Sync next step - {nextRevSyncStep:g}: {response.HttpStatusCode:d} {response.HttpStatusCode:g}");
        }
    }
}
```

### `BuildMessageLoadRevFromStagingToQueueAsync(context, queueURL, authId, currentStep)`

**Purpose:** Send SQS message to trigger staging-to-production transfer

**Low-level Flow:**
```csharp
// Lines 1979-2021: Staging transfer message method
private async Task BuildMessageLoadRevFromStagingToQueueAsync(KeySysLambdaContext context, string revCustomerSyncQueueURL, int currentIntegrationAuthenticationId, RevSyncStep currentStep)
{
    // Lines 1981-1984: Log method entry
    LogInfo(context, "SUB", "BuildMessageLoadRevFromStagingToQueueAsync");
    LogInfo(context, "revCustomerSyncQueueURL", revCustomerSyncQueueURL);
    LogInfo(context, "currentRevSyncStep", currentStep);

    // Lines 1986-1990: Skip if queue URL not configured
    if (string.IsNullOrWhiteSpace(revCustomerSyncQueueURL))
    {
        return;
    }

    // Lines 1992-2021: Create and send staging transfer message
    var awsCredentials = AwsCredentials(context);
    using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
    {
        var requestMsgBody = $"Current Auth Id {currentIntegrationAuthenticationId}";
        LogInfo(context, "Sending message for", $"{requestMsgBody} to destination queue: {revCustomerSyncQueueURL}");

        // Create message with IsLastStepSync = true to indicate staging transfer
        var request = new SendMessageRequest
        {
            MessageAttributes = new Dictionary<string, MessageAttributeValue>
            {
                {
                    "CurrentIntegrationAuthenticationId", new MessageAttributeValue
                    { DataType = "String", StringValue = currentIntegrationAuthenticationId.ToString()}
                },
                {
                    "CurrentRevSyncStep", new MessageAttributeValue
                    { DataType = "String", StringValue = ((int)currentStep).ToString()}
                },
                {
                    "IsLastStepSync", new MessageAttributeValue
                    { DataType = "String", StringValue = "true"}
                }
            },
            MessageBody = $"Load Rev from staging for step: {currentStep}",
            QueueUrl = revCustomerSyncQueueURL
        };

        LogInfo(context, "STATUS", "SendMessageRequest for staging load is ready!");
        
        var response = await client.SendMessageAsync(request);
        if (((int)response.HttpStatusCode < 200) || ((int)response.HttpStatusCode > 299))
        {
            LogInfo(context, "EXCEPTION", $"Error enqueuing Rev staging load for step - {currentStep:g}: {response.HttpStatusCode:d} {response.HttpStatusCode:g}");
        }
    }
}
```

### `ProcessLoadRevFromStaging(KeySysLambdaContext context, RevSyncStep currentRevSyncStep, int authId)`

**Purpose:** Transfer data from staging tables to production tables using stored procedures

**Low-level Flow:**
```csharp
// Lines 291-362: Staging to production transfer orchestrator
private void ProcessLoadRevFromStaging(KeySysLambdaContext context, RevSyncStep currentRevSyncStep, int currentIntegrationAuthenticationId)
{
    // Lines 293-294: Log method entry
    LogInfo(context, CommonConstants.SUB, $"ProcessLoadRevFromStaging({currentRevSyncStep}, {currentIntegrationAuthenticationId})");

    // Line 296: Create SQL retry policy
    var sqlRetryPolicy = GetSqlRetryPolicy(context);

    try
    {
        // Lines 299-348: Switch on sync step to call appropriate stored procedure
        switch (currentRevSyncStep)
        {
            case RevSyncStep.Customer:
                sqlRetryPolicy.Execute(() => LoadRevCustomersFromStagingTable(context, currentIntegrationAuthenticationId));
                sqlRetryPolicy.Execute(() => ClearStagingTablesV2(context, RevStagingTable.REV_CUSTOMER_STG, currentIntegrationAuthenticationId));
                break;
            case RevSyncStep.BillProfile:
                sqlRetryPolicy.Execute(() => LoadRevBillProfilesFromStagingTable(context, currentIntegrationAuthenticationId));
                sqlRetryPolicy.Execute(() => ClearStagingTablesV2(context, RevStagingTable.REV_BILLING_PROFILE_STG, currentIntegrationAuthenticationId));
                break;
            case RevSyncStep.Provider:
                sqlRetryPolicy.Execute(() => LoadRevProvidersFromStagingTable(context, currentIntegrationAuthenticationId));
                sqlRetryPolicy.Execute(() => ClearStagingTablesV2(context, RevStagingTable.REV_PROVIDER_STG, currentIntegrationAuthenticationId));
                break;
            case RevSyncStep.ProductType:
                sqlRetryPolicy.Execute(() => LoadRevProductTypesFromStagingTable(context, currentIntegrationAuthenticationId));
                sqlRetryPolicy.Execute(() => ClearStagingTablesV2(context, RevStagingTable.REV_PRODUCT_TYPE_STG, currentIntegrationAuthenticationId));
                break;
            case RevSyncStep.Product:
                sqlRetryPolicy.Execute(() => LoadRevProductsFromStagingTable(context, currentIntegrationAuthenticationId));
                sqlRetryPolicy.Execute(() => ClearStagingTablesV2(context, RevStagingTable.REV_PRODUCT_STG, currentIntegrationAuthenticationId));
                break;
            case RevSyncStep.ServiceType:
                sqlRetryPolicy.Execute(() => LoadRevServiceTypesFromStagingTable(context, currentIntegrationAuthenticationId));
                sqlRetryPolicy.Execute(() => ClearStagingTablesV2(context, RevStagingTable.REV_SERVICE_TYPE_STG, currentIntegrationAuthenticationId));
                break;
            case RevSyncStep.Service:
                sqlRetryPolicy.Execute(() => LoadRevServicesFromStagingTable(context, currentIntegrationAuthenticationId));
                sqlRetryPolicy.Execute(() => ClearStagingTablesV2(context, RevStagingTable.REV_SERVICE_STG, currentIntegrationAuthenticationId));
                break;
            case RevSyncStep.ServiceProduct:
                sqlRetryPolicy.Execute(() => LoadRevServiceProductsFromStagingTable(context, currentIntegrationAuthenticationId));
                sqlRetryPolicy.Execute(() => ClearStagingTablesV2(context, RevStagingTable.REV_SERVICE_PRODUCT_STG, currentIntegrationAuthenticationId));
                break;
            case RevSyncStep.InventoryType:
                sqlRetryPolicy.Execute(() => LoadRevInventoryTypesFromStagingTable(context, currentIntegrationAuthenticationId));
                sqlRetryPolicy.Execute(() => DeleteRevInventoryTypeStagingData(context, currentIntegrationAuthenticationId));
                break;
            case RevSyncStep.InventoryItem:
                sqlRetryPolicy.Execute(() => LoadRevInventoryItemsFromStagingTable(context, currentIntegrationAuthenticationId));
                sqlRetryPolicy.Execute(() => DeleteRevInventoryItemStagingData(context, currentIntegrationAuthenticationId));
                break;
            case RevSyncStep.UsagePlanGroup:
                sqlRetryPolicy.Execute(() => LoadRevUsagePlanGroupsFromStagingTable(context, currentIntegrationAuthenticationId));
                sqlRetryPolicy.Execute(() => DeleteRevUsagePlanGroupStagingData(context, currentIntegrationAuthenticationId));
                break;
            case RevSyncStep.Agent:
                sqlRetryPolicy.Execute(() => LoadRevAgentFromStagingTable(context, currentIntegrationAuthenticationId));
                sqlRetryPolicy.Execute(() => ClearStagingTablesV2(context, RevStagingTable.REV_AGENT_STG, currentIntegrationAuthenticationId));
                break;
            case RevSyncStep.Package:
                sqlRetryPolicy.Execute(() => LoadRevPackageFromStagingTable(context, currentIntegrationAuthenticationId));
                sqlRetryPolicy.Execute(() => ClearStagingTablesV2(context, RevStagingTable.REV_PACKAGE_STG, currentIntegrationAuthenticationId));
                break;
            case RevSyncStep.PackageProduct:
                sqlRetryPolicy.Execute(() => LoadRevPackageProductFromStagingTable(context, currentIntegrationAuthenticationId));
                sqlRetryPolicy.Execute(() => ClearStagingTablesV2(context, RevStagingTable.REV_PACKAGE_PRODUCT_STG, currentIntegrationAuthenticationId));
                break;
            default:
                LogInfo(context, "EXCEPTION", $"Unknown RevSyncStep for staging load: {currentRevSyncStep}");
                break;
        }
    }
    // Lines 349-361: Exception handling and cleanup
    catch (Exception ex)
    {
        LogInfo(context, "EXCEPTION", $"ProcessLoadRevFromStaging Exception: {ex.Message} {ex.StackTrace}");
    }
}
```

### `LoadRevCustomersFromStagingTable(KeySysLambdaContext context, int authId)`

**Purpose:** Execute stored procedure to transfer customer data from staging to production

**Low-level Flow:**
```csharp
// Lines 1456-1474: Customer staging transfer method
private static void LoadRevCustomersFromStagingTable(KeySysLambdaContext context, int currentIntegrationAuthenticationId)
{
    // Line 1458: Log method entry
    LogInfo(context, "SUB", "LoadRevCustomersFromStagingTable()");

    // Lines 1460-1473: Execute stored procedure
    using (var conn = new SqlConnection(context.CentralDbConnectionString))
    {
        using (var cmd = new SqlCommand("usp_Update_RevCustomer", conn))
        {
            // Line 1463: Set command timeout for large data operations
            cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
            
            // Line 1464: Set command type to stored procedure
            cmd.CommandType = CommandType.StoredProcedure;
            
            // Line 1465: Add authentication ID parameter
            cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", currentIntegrationAuthenticationId);
            
            // Line 1466: Open database connection
            conn.Open();

            // Line 1468: Execute stored procedure synchronously
            cmd.ExecuteNonQuery();
            
            // Line 1469: Close connection
            conn.Close();
        }
    }
}
```

### `ClearStagingTablesV2(KeySysLambdaContext context, string tableName, int authId)`

**Purpose:** Clear staging table data for specific authentication ID

**Low-level Flow:**
```csharp
// Lines 879-897: Staging table cleanup method
private static void ClearStagingTablesV2(KeySysLambdaContext context, string tableName, int integrationAuthenticationId)
{
    // Lines 881-882: Log method entry
    LogInfo(context, "SUB", $"ClearStagingTablesV2({tableName}, {integrationAuthenticationId})");

    // Lines 884-896: Execute delete command
    using (var conn = new SqlConnection(context.CentralDbConnectionString))
    {
        using (var cmd = new SqlCommand($"DELETE FROM {tableName} WHERE IntegrationAuthenticationId = @IntegrationAuthenticationId", conn))
        {
            // Line 887: Set command type to text
            cmd.CommandType = CommandType.Text;
            
            // Line 888: Add parameter to prevent SQL injection
            cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", integrationAuthenticationId);
            
            // Line 889: Set command timeout
            cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
            
            // Line 890: Open connection
            conn.Open();
            
            // Line 891: Execute delete command
            cmd.ExecuteNonQuery();
        }
    }
}
```

### `QueueNextRevInstanceAsync(KeySysLambdaContext context, int currentAuthId)`

**Purpose:** Move to next Rev.IO authentication instance or complete entire sync

**Low-level Flow:**
```csharp
// Lines 377-399: Instance progression method
private async Task QueueNextRevInstanceAsync(KeySysLambdaContext context, int currentIntegrationAuthenticationId)
{
    // Lines 379-380: Log method entry
    LogInfo(context, CommonConstants.SUB, $"QueueNextRevInstanceAsync({currentIntegrationAuthenticationId})");

    // Lines 382-383: Create authentication repository
    var revIoAuthRepo = new RevIoAuthenticationRepository(context.logger, context.Base64Service, context.CentralDbConnectionString);
    
    // Line 384: Get next authentication ID
    var nextAuthId = revIoAuthRepo.GetNextRevIoAuthenticationId(currentIntegrationAuthenticationId);
    
    // Lines 386-397: Handle next authentication or completion
    if (nextAuthId > 0)
    {
        // Lines 388-389: Get tenant information for current authentication
        var revIoAuthentication = revIoAuthRepo.GetRevioApiAuthentication(currentIntegrationAuthenticationId);
        
        // Line 391: Send completion notification to AMOP 2.0
        await dailySyncAmopApiTrigger.SendNotificationToAmop20(context, revIoAuthentication?.TenantId ?? 0, revIoAuthentication?.TenantName);
        
        // Lines 393-394: Start sync for next authentication instance
        LogInfo(context, "INFO", $"Starting sync for next authentication ID: {nextAuthId}");
        await BuildMessageToQueue(context, nextAuthId, RevSyncStep.Customer, DEFAULT_PAGE_NUMBER);
    }
    else
    {
        // Line 398: Log completion of all instances
        LogInfo(context, "INFO", "All Rev.IO authentication instances have been processed");
    }
}
```

### `SendNotificationToAmop20(KeySysLambdaContext context, int tenantId, string tenantName)`

**Purpose:** Notify AMOP 2.0 system that Rev.IO sync has completed for a tenant

**Low-level Flow:**
```csharp
// This method is in DailySyncAmopApiTrigger class
public async Task SendNotificationToAmop20(KeySysLambdaContext context, int tenantId, string tenantName)
{
    try
    {
        // Get AMOP 2.0 sync update API URL from environment variables
        var amopApiUrl = context.EnvironmentRepository.GetEnvironmentVariable(context.Context, "Amop20SyncUpdateAPIURL");
        
        if (string.IsNullOrWhiteSpace(amopApiUrl))
        {
            LogInfo(context, "WARNING", "AMOP 2.0 API URL not configured");
            return;
        }

        // Create HTTP client with Lambda logging handler
        using (var httpClient = new HttpClient())
        {
            // Build JSON payload
            var payload = new
            {
                key_name = "rev_customers_sync",
                tenant_id = tenantId,
                tenant_name = tenantName ?? ""
            };
            
            var jsonContent = JsonConvert.SerializeObject(payload);
            var content = new StringContent(jsonContent, Encoding.UTF8, "application/json");
            
            LogInfo(context, "INFO", $"Sending AMOP 2.0 notification: {jsonContent}");
            
            // Send POST request to AMOP 2.0 API
            var response = await httpClient.PostAsync(amopApiUrl, content);
            
            if (response.IsSuccessStatusCode)
            {
                var responseBody = await response.Content.ReadAsStringAsync();
                LogInfo(context, "INFO", $"AMOP 2.0 notification sent successfully: {responseBody}");
            }
            else
            {
                var errorBody = await response.Content.ReadAsStringAsync();
                LogInfo(context, "ERROR", $"AMOP 2.0 notification failed: {response.StatusCode} - {errorBody}");
            }
        }
    }
    catch (Exception ex)
    {
        LogInfo(context, "EXCEPTION", $"Error sending AMOP 2.0 notification: {ex.Message}");
    }
}
```

### `GetSqlRetryPolicy(KeySysLambdaContext context)`

**Purpose:** Create Polly retry policy for SQL operations

**Low-level Flow:**
```csharp
// Lines 499-510: SQL retry policy creation
private static RetryPolicy GetSqlRetryPolicy(KeySysLambdaContext context)
{
    // Line 501: Create retry policy using Polly
    return Policy
        .Handle<SqlException>() // Handle SQL exceptions
        .Or<InvalidOperationException>() // Handle connection exceptions
        .WaitAndRetry(
            retryCount: MaxRetries, // Maximum 3 retries
            sleepDurationProvider: retryAttempt => TimeSpan.FromSeconds(RetryDelaySeconds * retryAttempt), // Exponential backoff: 5s, 10s, 15s
            onRetry: (outcome, timespan, retryCount, context) =>
            {
                // Log retry attempt
                LogInfo(context, "WARNING", $"SQL operation retry attempt {retryCount} after {timespan} seconds. Exception: {outcome.Exception?.Message}");
            });
}
```

### `ClearStagingTables(KeySysLambdaContext context)`

**Purpose:** Clear all staging tables at start of sync process

**Low-level Flow:**
```csharp
// Lines 862-878: Clear all staging tables method
private static void ClearStagingTables(KeySysLambdaContext context)
{
    // Line 864: Log method entry
    LogInfo(context, "SUB", "ClearStagingTables()");

    // Lines 866-877: Execute stored procedure to clear all staging tables
    using (var conn = new SqlConnection(context.CentralDbConnectionString))
    {
        using (var cmd = new SqlCommand("usp_RevCustomerClearStagingTables", conn))
        {
            // Line 869: Set command timeout
            cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
            
            // Line 870: Set command type
            cmd.CommandType = CommandType.StoredProcedure;
            
            // Line 871: Open connection
            conn.Open();

            // Line 873: Execute stored procedure
            cmd.ExecuteNonQuery();
            
            // Line 874: Close connection
            conn.Close();
        }
    }
}
```

### `CleanUp(KeySysLambdaContext context)`

**Purpose:** Ensure proper cleanup of resources and logging

**Low-level Flow:**
```csharp
// Lines 53-56: Cleanup method from AwsFunctionBase
public virtual void CleanUp(KeySysLambdaContext context)
{
    // Line 55: Call context cleanup which:
    // - Flushes logger to ensure all log messages are written
    // - Closes database connections if any are open
    // - Disconnects from Redis cache if connected
    // - Disposes of any other resources held by context
    context.CleanUp();
}
```

## Key Data Structures

### `RevSyncStep` Enum
Defines the sequential order of data synchronization:
```csharp
public enum RevSyncStep
{
    None = 0,
    Customer = 1,
    BillProfile = 2,
    Provider = 3,
    ProductType = 4,
    Product = 5,
    ServiceType = 6,
    Service = 7,
    ServiceProduct = 8,
    InventoryType = 9,
    InventoryItem = 10,
    UsagePlanGroup = 11,
    Agent = 12,
    Package = 13,
    PackageProduct = 14,
    Default = 15
}
```

### `RevStagingTable` Constants
Maps sync steps to staging table names:
```csharp
public static class RevStagingTable
{
    public const string REV_CUSTOMER_STG = "dbo.RevCustomerStaging";
    public const string REV_BILLING_PROFILE_STG = "dbo.RevBillProfileStaging";
    public const string REV_PROVIDER_STG = "dbo.RevProviderStaging";
    public const string REV_PRODUCT_TYPE_STG = "dbo.RevProductTypeStaging";
    public const string REV_PRODUCT_STG = "dbo.RevProductStaging";
    public const string REV_SERVICE_TYPE_STG = "dbo.RevServiceTypeStaging";
    public const string REV_SERVICE_STG = "dbo.RevServiceStaging";
    public const string REV_SERVICE_PRODUCT_STG = "dbo.RevServiceProductStaging";
    public const string REV_INVENTORY_TYPE_STG = "dbo.RevInventoryTypeStaging";
    public const string REV_INVENTORY_ITEM_STG = "dbo.RevInventoryItemStaging";
    public const string REV_USAGE_PLAN_GROUP_STG = "dbo.RevUsagePlanGroupStaging";
    public const string REV_AGENT_STG = "dbo.RevAgentStaging";
    public const string REV_PACKAGE_STG = "dbo.RevPackageStaging";
    public const string REV_PACKAGE_PRODUCT_STG = "dbo.RevPackageProductStaging";
}
```

## Error Handling & Resilience

### Retry Policies
- **SQL Operations:** Polly retry policy with exponential backoff (5s, 10s, 15s)
- **HTTP API Calls:** Built-in retry in RevioApiClient with configurable attempts
- **SQS Operations:** AWS SDK built-in retry with exponential backoff

### Failure Scenarios
- **API Failures:** Continue with acceptable failure count (max 5), log errors, skip to next step if too many failures
- **Database Failures:** Retry with exponential backoff, fail Lambda execution if persistent
- **Timeout Handling:** Monitor Lambda remaining time, queue continuation if approaching timeout (< 30 seconds)
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
16. **`usp_Rev_Get_Next_Authentication`** - Get next authentication ID to process

## Performance Characteristics

- **Batch Processing:** Maximum 20 pages per Lambda execution to stay within time limits
- **Pagination:** 1000 records per page (configurable via RevPageSize)
- **Bulk Operations:** SqlBulkCopy for efficient staging table inserts
- **Parallel Processing:** Multiple authentication instances can be processed sequentially
- **Memory Management:** Processes data in batches to avoid memory exhaustion
- **Time Management:** Monitors Lambda remaining execution time to queue continuations
- **Failure Tolerance:** Maximum 5 API failures per pagination batch before moving to next step

## Complete Method List

### Primary Methods (88 total)
1. `FunctionHandler` - Main entry point
2. `BaseFunctionHandler` - Context initialization
3. `GetIsLastStepSyncNumber` - Extract staging flag
4. `GetPageNumber` - Extract page number
5. `GetCurrentIntegrationAuthenticationId` - Extract auth ID
6. `GetCurrentRevSyncStep` - Extract sync step
7. `ProcessEventAsync` - Main orchestrator
8. `ProcessLoadRevFromStaging` - Staging transfer orchestrator
9. `BuildMessageToQueue` - Queue message builder
10. `QueueNextRevInstanceAsync` - Instance progression
11. `SyncRevCustomersAsync` - Customer sync entry
12. `SyncRevBillProfilesAsync` - Bill profile sync entry
13. `SyncRevProvidersAsync` - Provider sync entry
14. `SyncRevProductTypesAsync` - Product type sync entry
15. `SyncRevProductsAsync` - Product sync entry
16. `SyncRevServiceTypesAsync` - Service type sync entry
17. `SyncRevServicesAsync` - Service sync entry
18. `SyncRevServiceProductsAsync` - Service product sync entry
19. `SyncRevInventoryTypesAsync` - Inventory type sync entry
20. `SyncRevInventoryItemsAsync` - Inventory item sync entry
21. `SyncRevUsagePlanAsync` - Usage plan sync entry
22. `SyncRevAgentAsync` - Agent sync entry
23. `SyncRevPackagesAsync` - Package sync entry
24. `SyncRevPackageProductsAsync` - Package product sync entry
25. `GetSqlRetryPolicy` - Create retry policy
26. `LoadRevBillProfilesToStaging` - Bill profile staging load
27. `LoadRevProvidersToStaging` - Provider staging load
28. `LoadRevServiceProductsToStaging` - Service product staging load
29. `LoadRevProductsToStaging` - Product staging load
30. `LoadRevInventoryTypesToStaging` - Inventory type staging load
31. `LoadRevInventoryItemsToStaging` - Inventory item staging load
32. `LoadRevUsagePlanGroupsToStaging` - Usage plan staging load
33. `LoadRevAgentsToStaging` - Agent staging load
34. `LoadRevProductTypesToStaging` - Product type staging load
35. `LoadRevServiceTypesToStaging` - Service type staging load
36. `LoadRevPackagesToStaging` - Package staging load
37. `LoadRevPackageProductsToStaging` - Package product staging load
38. `LoadRevServicesToStaging` - Service staging load
39. `LoadRevCustomersToStaging` - Customer staging load
40. `ClearStagingTables` - Clear all staging tables
41. `ClearStagingTablesV2` - Clear specific staging table
42. `GetServicesAsync` - Service data fetching
43. `CustomersAsync` - Customer data fetching
44. `CheckProcessOfSyncStep` - Step completion handler
45. `GetBillProfilesAsync` - Bill profile data fetching
46. `GetProvidersAsync` - Provider data fetching
47. `RevServiceTypesAsync` - Service type data fetching
48. `RevPackagesAsync` - Package data fetching
49. `RevPackageProductsAsync` - Package product data fetching
50. `GetProductTypesAsync` - Product type data fetching
51. `GetProductsAsync` - Product data fetching
52. `GetServiceProductsAsync` - Service product data fetching
53. `GetInventoryTypesAsync` - Inventory type data fetching
54. `GetInventoryItemsAsync` - Inventory item data fetching
55. `GetUsagePlanGroupsAsync` - Usage plan data fetching
56. `GetAgentAsync` - Agent data fetching
57. `LoadRevCustomersFromStagingTable` - Customer staging transfer
58. `LoadRevBillProfilesFromStagingTable` - Bill profile staging transfer
59. `LoadRevProvidersFromStagingTable` - Provider staging transfer
60. `LoadRevProductTypesFromStagingTable` - Product type staging transfer
61. `LoadRevProductsFromStagingTable` - Product staging transfer
62. `LoadRevServiceTypesFromStagingTable` - Service type staging transfer
63. `LoadRevServicesFromStagingTable` - Service staging transfer
64. `LoadRevServiceProductsFromStagingTable` - Service product staging transfer
65. `LoadRevInventoryTypesFromStagingTable` - Inventory type staging transfer
66. `LoadRevInventoryItemsFromStagingTable` - Inventory item staging transfer
67. `LoadRevUsagePlanGroupsFromStagingTable` - Usage plan staging transfer
68. `LoadRevAgentFromStagingTable` - Agent staging transfer
69. `LoadRevPackageFromStagingTable` - Package staging transfer
70. `LoadRevPackageProductFromStagingTable` - Package product staging transfer
71. `DeleteRevInventoryItemStagingData` - Clear inventory item staging
72. `DeleteRevInventoryTypeStagingData` - Clear inventory type staging
73. `DeleteRevUsagePlanGroupStagingData` - Clear usage plan staging
74. `SendMessageToQueueAsync` - SQS message sender
75. `BuildMessageLoadRevFromStagingToQueueAsync` - Staging transfer message builder
76. Multiple `AddToDataRow` methods (15 variations) - Data mapping helpers
77. Multiple API call methods - Rev.IO API callers
78. `CleanUp` - Resource cleanup

### Supporting Methods from Other Classes
- `GetNextRevIoAuthenticationId` - RevIoAuthenticationRepository
- `GetRevioApiAuthentication` - RevIoAuthenticationRepository
- `ExecuteRevGetWithRetryAsync` - RevioApiClient
- `SendNotificationToAmop20` - DailySyncAmopApiTrigger
- `SqlBulkCopy` - AwsFunctionBase
- Various logging and utility methods

This comprehensive documentation covers every method in the Lambda flow with detailed line-by-line explanations of what each method does internally.