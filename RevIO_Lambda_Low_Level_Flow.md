# RevIO Lambda Function - Low Level Flow Documentation

## Table of Contents
1. [Primary Entry Point and Initialization](#primary-entry-point-and-initialization)
2. [Context and Authentication Setup](#context-and-authentication-setup)
3. [Main Processing Orchestrator](#main-processing-orchestrator)
4. [Data Synchronization Methods](#data-synchronization-methods)
5. [Staging to Production Transfer](#staging-to-production-transfer)
6. [Instance and Queue Management](#instance-and-queue-management)
7. [Cleanup and Finalization](#cleanup-and-finalization)

---

## Primary Entry Point and Initialization

### FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
**Purpose**: Main Lambda entry point that processes SQS messages

**Low-Level Flow**:
1. **Message Validation**:
   - Validate SQS event is not null
   - Check if Records collection contains messages
   - Extract first message from Records[0]

2. **Context Initialization**:
   - Call `BaseFunctionHandler(context)` to initialize Lambda context
   - Set up logging and error handling

3. **Message Parsing**:
   - Call `GetCurrentIntegrationAuthenticationId()` to extract auth ID from SQS message body
   - Call `GetCurrentRevSyncStep()` to extract sync step enum from message
   - Call `GetPageNumber()` to extract page number for pagination
   - Call `GetIsLastStepSyncNumber()` to determine if this is staging-to-production transfer

4. **Process Routing**:
   - If `isLastStepSync == true`:
     - Route to `ProcessLoadRevFromStaging(currentRevSyncStep, authId)`
   - Else:
     - Route to `ProcessEventAsync(context, authId, currentRevSyncStep, pageNumber)`

5. **Cleanup**:
   - Call `CleanUp(context)` to finalize processing

---

### Internal Methods

#### BaseFunctionHandler(context)
**Low-Level Flow**:
1. Store Lambda context reference
2. Initialize logging with context request ID
3. Set up error handling and exception tracking
4. Initialize performance counters

#### GetCurrentIntegrationAuthenticationId()
**Low-Level Flow**:
1. Parse SQS message body as JSON
2. Extract "AuthenticationId" field
3. Convert to integer
4. Validate ID is greater than 0
5. Return authentication ID

#### GetCurrentRevSyncStep()
**Low-Level Flow**:
1. Parse SQS message body as JSON
2. Extract "SyncStep" field as string
3. Parse string to `RevSyncStep` enum using `Enum.Parse()`
4. Validate enum value is valid
5. Return parsed sync step

#### GetPageNumber()
**Low-Level Flow**:
1. Parse SQS message body as JSON
2. Extract "PageNumber" field
3. Convert to integer (default to 1 if not present)
4. Validate page number is greater than 0
5. Return page number

#### GetIsLastStepSyncNumber()
**Low-Level Flow**:
1. Parse SQS message body as JSON
2. Extract "IsLastStep" field as boolean
3. Return boolean value (default false if not present)

---

## Context and Authentication Setup

### BaseFunctionHandler(ILambdaContext context, bool skipOUSpecificLogic = false)
**Purpose**: Initialize Lambda wrapper with organizational unit settings

**Low-Level Flow**:
1. **Context Wrapper Creation**:
   - Instantiate `new KeySysLambdaContext(context, skipOUSpecificLogic)`
   - Set up connection strings and configuration

2. **OU Settings Loading**:
   - If `skipOUSpecificLogic == false`:
     - Call `LoadOUSettings()` to load organizational unit configurations
     - Set up tenant-specific database connections
     - Configure API endpoints and credentials

3. **Authentication Setup**:
   - Initialize authentication repositories
   - Set up retry policies for database operations

---

### GetNextRevIoAuthenticationId(int currentId)
**Purpose**: Get the next authentication ID to process in sequence

**Low-Level Flow**:
1. **Database Connection**:
   - Establish SQL connection using configured connection string
   - Set up command timeout and retry policy

2. **Stored Procedure Call**:
   - **SP**: `usp_Rev_Get_Next_Authentication`
   - **Parameters**:
     - `@CurrentAuthId` (int): Current authentication ID
   - **Returns**: Next authentication ID to process

3. **SP Internal Logic**:
   ```sql
   -- Get next authentication ID in sequence
   SELECT TOP 1 AuthenticationId 
   FROM RevIoAuthentication 
   WHERE AuthenticationId > @CurrentAuthId 
   AND IsActive = 1 
   ORDER BY AuthenticationId ASC
   ```

4. **Result Processing**:
   - If result found: Return next authentication ID
   - If no result: Return -1 (indicates completion)

---

## Main Processing Orchestrator

### ProcessEventAsync(KeySysLambdaContext context, int authId, RevSyncStep step, int pageNumber)
**Purpose**: Main orchestrator that routes to appropriate sync method based on sync step

**Low-Level Flow**:
1. **Repository Initialization**:
   - Create `new RevIoAuthenticationRepository()` with context
   - Set up database connection and retry policies

2. **Authentication Retrieval**:
   - Call `GetRevioApiAuthentication(authId)` to get API credentials
   - Validate credentials are not null/empty
   - Extract API base URL, client ID, client secret

3. **API Client Setup**:
   - Create `new RevioApiClient()` with retrieved credentials
   - Set up HTTP client with timeout and retry policies
   - Configure authentication headers

4. **Sync Step Routing**:
   - Use switch statement on `RevSyncStep step`:
     - `RevSyncStep.Customers` → `SyncRevCustomersAsync()`
     - `RevSyncStep.BillProfiles` → `SyncRevBillProfilesAsync()`
     - `RevSyncStep.Providers` → `SyncRevProvidersAsync()`
     - `RevSyncStep.ProductTypes` → `SyncRevProductTypesAsync()`
     - `RevSyncStep.Products` → `SyncRevProductsAsync()`
     - `RevSyncStep.ServiceTypes` → `SyncRevServiceTypesAsync()`
     - `RevSyncStep.Services` → `SyncRevServicesAsync()`
     - `RevSyncStep.ServiceProducts` → `SyncRevServiceProductsAsync()`
     - `RevSyncStep.InventoryTypes` → `SyncRevInventoryTypesAsync()`
     - `RevSyncStep.InventoryItems` → `SyncRevInventoryItemsAsync()`
     - `RevSyncStep.UsagePlan` → `SyncRevUsagePlanAsync()`
     - `RevSyncStep.Agent` → `SyncRevAgentAsync()`
     - `RevSyncStep.Packages` → `SyncRevPackagesAsync()`
     - `RevSyncStep.PackageProducts` → `SyncRevPackageProductsAsync()`

5. **Error Handling**:
   - Catch and log any exceptions
   - Update sync status in database
   - Send failure notifications if needed

---

## Data Synchronization Methods

### 1. SyncRevCustomersAsync() → CustomersAsync()

**Low-Level Flow**:

#### Main Method Flow:
1. **Retry Policy Setup**:
   - Call `GetSqlRetryPolicy()` to create exponential backoff policy
   - Configure max retry attempts (typically 3)
   - Set retry intervals (1s, 2s, 4s)

2. **Pagination Handling**:
   - Call `GetPagesFromRevIOListAPI(pageNumber)` 
   - Determine total pages and current page
   - Set up pagination parameters

3. **API Data Retrieval**:
   - Call `GetCustomersAsync(pageNumber)` to fetch customer data
   - Handle API rate limiting and throttling
   - Process API response and validate data

4. **Staging Data Load**:
   - Call `LoadRevCustomersToStaging(customerData, authId)`
   - Bulk insert data to staging table
   - Handle data transformation and validation

5. **Process Continuation**:
   - Call `CheckProcessOfSyncStep()` to determine next action
   - Queue next page or next sync step

#### Internal Methods Detail:

**GetCustomersAsync(pageNumber)**:
- Construct API endpoint: `/api/customers?page={pageNumber}&limit=1000`
- Add authentication headers
- Make HTTP GET request
- Deserialize JSON response to Customer objects
- Handle API errors and retries

**LoadRevCustomersToStaging(customerData, authId)**:
- Create DataTable with customer schema
- Transform API data to match database schema
- Use SqlBulkCopy for efficient insertion
- **Target Table**: `RevCustomers_Staging`
- **Columns**: CustomerId, Name, Email, Phone, Address, AuthenticationId, CreatedDate, ModifiedDate

---

### 2. SyncRevBillProfilesAsync() → GetBillProfilesAsync()

**Low-Level Flow**:

#### Main Method Flow:
1. **Retry Policy Setup**:
   - Call `GetSqlRetryPolicy()` for database operations
   - Configure retry parameters

2. **Pagination and API Call**:
   - Call `GetPagesFromRevIOListAPI(pageNumber)`
   - Call `GetBillProfilesAsync(pageNumber)` for billing profile data

3. **Data Processing**:
   - Call `LoadRevBillProfilesToStaging(billProfileData, authId)`
   - Handle billing profile specific transformations

4. **Next Step Determination**:
   - Call `CheckProcessOfSyncStep()` to continue workflow

#### Internal Methods Detail:

**GetBillProfilesAsync(pageNumber)**:
- API Endpoint: `/api/billprofiles?page={pageNumber}&limit=1000`
- Retrieve billing profile configurations
- Handle nested billing address data

**LoadRevBillProfilesToStaging(billProfileData, authId)**:
- **Target Table**: `RevBillProfiles_Staging`
- **Columns**: BillProfileId, CustomerId, BillingAddress, PaymentMethod, AuthenticationId, CreatedDate

---

### 3. SyncRevProvidersAsync() → GetProvidersAsync()

**Low-Level Flow**:

#### Main Method Flow:
1. **Setup and Pagination**:
   - Initialize retry policy
   - Handle pagination for provider data

2. **Provider Data Retrieval**:
   - Call `GetProvidersAsync(pageNumber)` to fetch service providers
   - Process provider hierarchy and relationships

3. **Staging Load**:
   - Call `LoadRevProvidersToStaging(providerData, authId)`
   - Handle provider-specific data transformations

#### Internal Methods Detail:

**GetProvidersAsync(pageNumber)**:
- API Endpoint: `/api/providers?page={pageNumber}&limit=1000`
- Retrieve service provider information
- Handle provider categories and service areas

**LoadRevProvidersToStaging(providerData, authId)**:
- **Target Table**: `RevProviders_Staging`
- **Columns**: ProviderId, ProviderName, ServiceArea, ContactInfo, AuthenticationId, CreatedDate

---

### 4. SyncRevProductTypesAsync() → GetProductTypesAsync()

**Low-Level Flow**:

#### Main Method Flow:
1. **Standard Setup**:
   - Initialize retry policies and pagination
   - Set up API client for product type retrieval

2. **Product Type Retrieval**:
   - Call `GetProductTypesAsync(pageNumber)` to fetch product categories
   - Process hierarchical product type structure

3. **Staging Operations**:
   - Call `LoadRevProductTypesToStaging(productTypeData, authId)`
   - Handle product type hierarchies and relationships

#### Internal Methods Detail:

**GetProductTypesAsync(pageNumber)**:
- API Endpoint: `/api/producttypes?page={pageNumber}&limit=1000`
- Retrieve product categorization data
- Handle parent-child relationships in product types

**LoadRevProductTypesToStaging(productTypeData, authId)**:
- **Target Table**: `RevProductTypes_Staging`
- **Columns**: ProductTypeId, TypeName, ParentTypeId, Description, AuthenticationId, CreatedDate

---

### 5. SyncRevProductsAsync() → GetProductsAsync()

**Low-Level Flow**:

#### Main Method Flow:
1. **Initialization**:
   - Set up retry policies for product synchronization
   - Configure pagination for large product datasets

2. **Product Data Retrieval**:
   - Call `GetProductsAsync(pageNumber)` to fetch product information
   - Handle product variants and pricing data

3. **Data Staging**:
   - Call `LoadRevProductsToStaging(productData, authId)`
   - Process complex product data structures

#### Internal Methods Detail:

**GetProductsAsync(pageNumber)**:
- API Endpoint: `/api/products?page={pageNumber}&limit=1000`
- Retrieve detailed product information
- Handle product pricing, availability, and specifications

**LoadRevProductsToStaging(productData, authId)**:
- **Target Table**: `RevProducts_Staging`
- **Columns**: ProductId, ProductName, ProductTypeId, Price, Description, IsActive, AuthenticationId, CreatedDate

---

### 6. SyncRevServiceTypesAsync() → RevServiceTypesAsync()

**Low-Level Flow**:

#### Main Method Flow:
1. **Service Type Setup**:
   - Initialize for service type synchronization
   - Handle service categorization logic

2. **API Integration**:
   - Call `GetServiceTypesAsync(pageNumber)` to retrieve service categories
   - Process service type hierarchies

3. **Staging Process**:
   - Call `LoadRevServiceTypesToStaging(serviceTypeData, authId)`
   - Handle service type relationships and dependencies

#### Internal Methods Detail:

**GetServiceTypesAsync(pageNumber)**:
- API Endpoint: `/api/servicetypes?page={pageNumber}&limit=1000`
- Retrieve service categorization data
- Handle service type attributes and configurations

**LoadRevServiceTypesToStaging(serviceTypeData, authId)**:
- **Target Table**: `RevServiceTypes_Staging`
- **Columns**: ServiceTypeId, TypeName, Category, Description, AuthenticationId, CreatedDate

---

### 7. SyncRevServicesAsync() → GetServicesAsync()

**Low-Level Flow**:

#### Main Method Flow:
1. **Service Sync Setup**:
   - Configure for service data synchronization
   - Set up complex data handling for services

2. **Service Data Retrieval**:
   - Call `GetServicesAsync(pageNumber)` to fetch service information
   - Handle service configurations and pricing

3. **Staging Operations**:
   - Call `LoadRevServicesToStaging(serviceData, authId)`
   - Process service-specific data transformations

#### Internal Methods Detail:

**GetServicesAsync(pageNumber)**:
- API Endpoint: `/api/services?page={pageNumber}&limit=1000`
- Retrieve comprehensive service data
- Handle service pricing, terms, and availability

**LoadRevServicesToStaging(serviceData, authId)**:
- **Target Table**: `RevServices_Staging`
- **Columns**: ServiceId, ServiceName, ServiceTypeId, ProviderId, Price, Terms, AuthenticationId, CreatedDate

---

### 8. SyncRevServiceProductsAsync() → GetServiceProductsAsync()

**Low-Level Flow**:

#### Main Method Flow:
1. **Relationship Sync Setup**:
   - Initialize for service-product relationship synchronization
   - Handle complex many-to-many relationships

2. **Relationship Data Retrieval**:
   - Call `GetServiceProductsAsync(pageNumber)` to fetch service-product mappings
   - Process relationship attributes and configurations

3. **Staging Load**:
   - Call `LoadRevServiceProductsToStaging(serviceProductData, authId)`
   - Handle relationship data integrity

#### Internal Methods Detail:

**GetServiceProductsAsync(pageNumber)**:
- API Endpoint: `/api/serviceproducts?page={pageNumber}&limit=1000`
- Retrieve service-product relationship data
- Handle quantity, pricing, and configuration overrides

**LoadRevServiceProductsToStaging(serviceProductData, authId)**:
- **Target Table**: `RevServiceProducts_Staging`
- **Columns**: ServiceProductId, ServiceId, ProductId, Quantity, Price, AuthenticationId, CreatedDate

---

### 9. SyncRevInventoryTypesAsync() → GetInventoryTypesAsync()

**Low-Level Flow**:

#### Main Method Flow:
1. **Inventory Type Setup**:
   - Initialize inventory type synchronization
   - Prepare for inventory categorization data

2. **Inventory Type Retrieval**:
   - Call `GetInventoryTypesAsync(pageNumber)` to fetch inventory categories
   - Process inventory classification data

3. **Staging and Queue Management**:
   - Call `LoadRevInventoryTypesToStaging(inventoryTypeData, authId)`
   - Call `BuildMessageLoadRevFromStagingToQueueAsync()` to queue staging transfer
   - Call `BuildMessageToQueue()` to queue next sync step

#### Internal Methods Detail:

**GetInventoryTypesAsync(pageNumber)**:
- API Endpoint: `/api/inventorytypes?page={pageNumber}&limit=1000`
- Retrieve inventory categorization data
- Handle inventory type attributes and properties

**LoadRevInventoryTypesToStaging(inventoryTypeData, authId)**:
- **Target Table**: `RevInventoryTypes_Staging`
- **Columns**: InventoryTypeId, TypeName, Category, Description, AuthenticationId, CreatedDate

---

### 10. SyncRevInventoryItemsAsync() → GetInventoryItemsAsync()

**Low-Level Flow**:

#### Main Method Flow:
1. **Inventory Item Setup**:
   - Initialize for inventory item synchronization
   - Handle large inventory datasets

2. **Inventory Data Retrieval**:
   - Call `GetInventoryItemsAsync(pageNumber)` to fetch inventory items
   - Process inventory quantities, locations, and status

3. **Staging and Queue Operations**:
   - Call `LoadRevInventoryItemsToStaging(inventoryItemData, authId)`
   - Call `BuildMessageLoadRevFromStagingToQueueAsync()` for staging transfer
   - Call `BuildMessageToQueue()` for workflow continuation

#### Internal Methods Detail:

**GetInventoryItemsAsync(pageNumber)**:
- API Endpoint: `/api/inventoryitems?page={pageNumber}&limit=1000`
- Retrieve detailed inventory item data
- Handle inventory tracking, quantities, and locations

**LoadRevInventoryItemsToStaging(inventoryItemData, authId)**:
- **Target Table**: `RevInventoryItems_Staging`
- **Columns**: InventoryItemId, ProductId, InventoryTypeId, Quantity, Location, Status, AuthenticationId, CreatedDate

---

### 11. SyncRevUsagePlanAsync() → GetUsagePlanGroupsAsync()

**Low-Level Flow**:

#### Main Method Flow:
1. **Usage Plan Setup**:
   - Initialize usage plan synchronization
   - Handle usage tracking and billing plans

2. **Usage Plan Retrieval**:
   - Call `GetUsagePlanGroupsAsync(pageNumber)` to fetch usage plan data
   - Process usage metrics and billing configurations

3. **Final Staging Operations**:
   - Call `LoadRevUsagePlanGroupsToStaging(usagePlanData, authId)`
   - Call `BuildMessageLoadRevFromStagingToQueueAsync()` for staging transfer
   - Call `BuildMessageToQueue()` for next steps

#### Internal Methods Detail:

**GetUsagePlanGroupsAsync(pageNumber)**:
- API Endpoint: `/api/usageplangroups?page={pageNumber}&limit=1000`
- Retrieve usage plan and billing configuration data
- Handle usage thresholds, pricing tiers, and billing cycles

**LoadRevUsagePlanGroupsToStaging(usagePlanData, authId)**:
- **Target Table**: `RevUsagePlanGroups_Staging`
- **Columns**: UsagePlanGroupId, PlanName, BillingCycle, UsageLimit, PricePerUnit, AuthenticationId, CreatedDate

---

### 12. SyncRevAgentAsync() → GetAgentAsync()

**Low-Level Flow**:

#### Main Method Flow:
1. **Agent Sync Setup**:
   - Initialize agent/user synchronization
   - Handle agent permissions and roles

2. **Agent Data Retrieval**:
   - Call `GetAgentAsync(pageNumber)` to fetch agent information
   - Process agent profiles, permissions, and assignments

3. **Process Completion Check**:
   - Call `LoadRevAgentsToStaging(agentData, authId)`
   - Call `CheckProcessOfSyncStep()` to determine workflow continuation

#### Internal Methods Detail:

**GetAgentAsync(pageNumber)**:
- API Endpoint: `/api/agents?page={pageNumber}&limit=1000`
- Retrieve agent/user profile data
- Handle agent roles, permissions, and territory assignments

**LoadRevAgentsToStaging(agentData, authId)**:
- **Target Table**: `RevAgents_Staging`
- **Columns**: AgentId, AgentName, Email, Role, Territory, Permissions, AuthenticationId, CreatedDate

---

### 13. SyncRevPackagesAsync() → RevPackagesAsync()

**Low-Level Flow**:

#### Main Method Flow:
1. **Package Sync Setup**:
   - Initialize package synchronization
   - Handle package configurations and bundling

2. **Package Data Retrieval**:
   - Call `GetPackagesAsync(pageNumber)` to fetch package information
   - Process package definitions and pricing

3. **Process Continuation**:
   - Call `LoadRevPackagesToStaging(packageData, authId)`
   - Call `CheckProcessOfSyncStep()` for next step determination

#### Internal Methods Detail:

**GetPackagesAsync(pageNumber)**:
- API Endpoint: `/api/packages?page={pageNumber}&limit=1000`
- Retrieve package configuration data
- Handle package pricing, terms, and included services

**LoadRevPackagesToStaging(packageData, authId)**:
- **Target Table**: `RevPackages_Staging`
- **Columns**: PackageId, PackageName, Description, Price, Terms, AuthenticationId, CreatedDate

---

### 14. SyncRevPackageProductsAsync() → RevPackageProductsAsync()

**Low-Level Flow**:

#### Main Method Flow:
1. **Package Product Setup**:
   - Initialize package-product relationship synchronization
   - Handle final relationship mappings

2. **Package Product Retrieval**:
   - Call `GetPackageProductsAsync(pageNumber)` to fetch package-product relationships
   - Process final relationship data

3. **Final Process Check**:
   - Call `LoadRevPackageProductsToStaging(packageProductData, authId)`
   - Call `CheckProcessOfSyncStep()` for completion determination

#### Internal Methods Detail:

**GetPackageProductsAsync(pageNumber)**:
- API Endpoint: `/api/packageproducts?page={pageNumber}&limit=1000`
- Retrieve package-product relationship data
- Handle final relationship configurations and overrides

**LoadRevPackageProductsToStaging(packageProductData, authId)**:
- **Target Table**: `RevPackageProducts_Staging`
- **Columns**: PackageProductId, PackageId, ProductId, Quantity, Price, AuthenticationId, CreatedDate

---

## Common Internal Methods for All Sync Methods

### GetSqlRetryPolicy()
**Low-Level Flow**:
1. Create Polly retry policy with exponential backoff
2. Configure retry attempts: 3 maximum
3. Set retry intervals: 1s, 2s, 4s
4. Handle transient SQL exceptions (timeout, connection issues)
5. Return configured policy

### GetPagesFromRevIOListAPI(pageNumber)
**Low-Level Flow**:
1. Calculate offset based on page number and page size (1000)
2. Set pagination headers in API request
3. Track total pages from API response headers
4. Handle last page detection
5. Return pagination metadata

### CheckProcessOfSyncStep()
**Low-Level Flow**:
1. **Check Current Page Status**:
   - Determine if more pages exist for current sync step
   - If more pages: Queue next page message

2. **Check Sync Step Completion**:
   - If all pages processed: Determine next sync step
   - Queue next sync step message

3. **Message Building**:
   - Call `BuildMessageToQueue()` with appropriate parameters
   - Send message to SQS queue for continuation

### BuildMessageToQueue()
**Low-Level Flow**:
1. **Message Construction**:
   - Create JSON message with:
     - AuthenticationId
     - NextSyncStep (enum)
     - PageNumber (1 for new step, incremented for same step)
     - IsLastStep (false for sync steps)

2. **Queue Operations**:
   - Send message to configured SQS queue
   - Set message attributes and visibility timeout
   - Handle queue errors and retries

### BuildMessageLoadRevFromStagingToQueueAsync()
**Low-Level Flow**:
1. **Staging Transfer Message**:
   - Create JSON message with:
     - AuthenticationId
     - CurrentSyncStep (for staging table identification)
     - IsLastStep (true to trigger staging-to-production)

2. **Queue for Staging Transfer**:
   - Send message to SQS queue
   - Trigger staging-to-production data transfer
   - Handle message delivery confirmation

---

## Staging to Production Transfer

### ProcessLoadRevFromStaging(RevSyncStep currentRevSyncStep, int authId)
**Purpose**: Transfer data from staging tables to production tables using stored procedures

**Low-Level Flow**:

1. **Retry Policy Setup**:
   - Call `GetSqlRetryPolicy()` to handle transient database failures
   - Configure retry attempts and intervals

2. **Sync Step Routing**:
   - Use switch statement on `currentRevSyncStep` to route to appropriate method:

#### Route to Specific Load Methods:

**LoadRevCustomersFromStagingTable()**:
- **Stored Procedure**: `usp_Update_RevCustomer`
- **Parameters**: `@AuthenticationId` (int)
- **SP Logic**:
  ```sql
  -- Merge staging data to production
  MERGE RevCustomers AS target
  USING RevCustomers_Staging AS source ON target.CustomerId = source.CustomerId
  WHEN MATCHED THEN UPDATE SET 
    Name = source.Name,
    Email = source.Email,
    Phone = source.Phone,
    ModifiedDate = GETDATE()
  WHEN NOT MATCHED THEN INSERT 
    (CustomerId, Name, Email, Phone, CreatedDate)
    VALUES (source.CustomerId, source.Name, source.Email, source.Phone, GETDATE());
  
  -- Clear staging table
  DELETE FROM RevCustomers_Staging WHERE AuthenticationId = @AuthenticationId;
  ```

**LoadRevBillProfilesFromStagingTable()**:
- **Stored Procedure**: `usp_Update_RevBillProfile`
- **Parameters**: `@AuthenticationId` (int)
- **SP Logic**:
  ```sql
  -- Merge bill profiles with customer validation
  MERGE RevBillProfiles AS target
  USING RevBillProfiles_Staging AS source ON target.BillProfileId = source.BillProfileId
  WHEN MATCHED THEN UPDATE SET 
    BillingAddress = source.BillingAddress,
    PaymentMethod = source.PaymentMethod,
    ModifiedDate = GETDATE()
  WHEN NOT MATCHED THEN INSERT 
    (BillProfileId, CustomerId, BillingAddress, PaymentMethod, CreatedDate)
    VALUES (source.BillProfileId, source.CustomerId, source.BillingAddress, 
            source.PaymentMethod, GETDATE());
  
  DELETE FROM RevBillProfiles_Staging WHERE AuthenticationId = @AuthenticationId;
  ```

**LoadRevProvidersFromStagingTable()**:
- **Stored Procedure**: `usp_Update_RevProvider`
- **Parameters**: `@AuthenticationId` (int)
- **SP Logic**:
  ```sql
  -- Merge provider data
  MERGE RevProviders AS target
  USING RevProviders_Staging AS source ON target.ProviderId = source.ProviderId
  WHEN MATCHED THEN UPDATE SET 
    ProviderName = source.ProviderName,
    ServiceArea = source.ServiceArea,
    ContactInfo = source.ContactInfo,
    ModifiedDate = GETDATE()
  WHEN NOT MATCHED THEN INSERT 
    (ProviderId, ProviderName, ServiceArea, ContactInfo, CreatedDate)
    VALUES (source.ProviderId, source.ProviderName, source.ServiceArea, 
            source.ContactInfo, GETDATE());
  
  DELETE FROM RevProviders_Staging WHERE AuthenticationId = @AuthenticationId;
  ```

**LoadRevProductTypesFromStagingTable()**:
- **Stored Procedure**: `usp_Update_RevProductType_FromStaging`
- **Parameters**: `@AuthenticationId` (int)
- **SP Logic**:
  ```sql
  -- Handle hierarchical product types
  WITH ProductTypeHierarchy AS (
    SELECT ProductTypeId, TypeName, ParentTypeId, Description, 0 as Level
    FROM RevProductTypes_Staging 
    WHERE ParentTypeId IS NULL AND AuthenticationId = @AuthenticationId
    
    UNION ALL
    
    SELECT s.ProductTypeId, s.TypeName, s.ParentTypeId, s.Description, h.Level + 1
    FROM RevProductTypes_Staging s
    INNER JOIN ProductTypeHierarchy h ON s.ParentTypeId = h.ProductTypeId
    WHERE s.AuthenticationId = @AuthenticationId
  )
  MERGE RevProductTypes AS target
  USING ProductTypeHierarchy AS source ON target.ProductTypeId = source.ProductTypeId
  WHEN MATCHED THEN UPDATE SET 
    TypeName = source.TypeName,
    ParentTypeId = source.ParentTypeId,
    Description = source.Description,
    ModifiedDate = GETDATE()
  WHEN NOT MATCHED THEN INSERT 
    (ProductTypeId, TypeName, ParentTypeId, Description, CreatedDate)
    VALUES (source.ProductTypeId, source.TypeName, source.ParentTypeId, 
            source.Description, GETDATE());
  
  DELETE FROM RevProductTypes_Staging WHERE AuthenticationId = @AuthenticationId;
  ```

**LoadRevProductsFromStagingTable()**:
- **Stored Procedure**: `usp_Update_RevProduct_FromStaging`
- **Parameters**: `@AuthenticationId` (int)
- **SP Logic**:
  ```sql
  -- Merge products with type validation
  MERGE RevProducts AS target
  USING (
    SELECT s.*, pt.ProductTypeId as ValidTypeId
    FROM RevProducts_Staging s
    LEFT JOIN RevProductTypes pt ON s.ProductTypeId = pt.ProductTypeId
    WHERE s.AuthenticationId = @AuthenticationId
  ) AS source ON target.ProductId = source.ProductId
  WHEN MATCHED THEN UPDATE SET 
    ProductName = source.ProductName,
    ProductTypeId = source.ValidTypeId,
    Price = source.Price,
    Description = source.Description,
    IsActive = source.IsActive,
    ModifiedDate = GETDATE()
  WHEN NOT MATCHED AND source.ValidTypeId IS NOT NULL THEN INSERT 
    (ProductId, ProductName, ProductTypeId, Price, Description, IsActive, CreatedDate)
    VALUES (source.ProductId, source.ProductName, source.ValidTypeId, 
            source.Price, source.Description, source.IsActive, GETDATE());
  
  DELETE FROM RevProducts_Staging WHERE AuthenticationId = @AuthenticationId;
  ```

**LoadRevServiceTypesFromStagingTable()**:
- **Stored Procedure**: `usp_Update_RevServiceType`
- **Parameters**: `@AuthenticationId` (int)
- **SP Logic**:
  ```sql
  -- Merge service types
  MERGE RevServiceTypes AS target
  USING RevServiceTypes_Staging AS source ON target.ServiceTypeId = source.ServiceTypeId
  WHEN MATCHED THEN UPDATE SET 
    TypeName = source.TypeName,
    Category = source.Category,
    Description = source.Description,
    ModifiedDate = GETDATE()
  WHEN NOT MATCHED THEN INSERT 
    (ServiceTypeId, TypeName, Category, Description, CreatedDate)
    VALUES (source.ServiceTypeId, source.TypeName, source.Category, 
            source.Description, GETDATE());
  
  DELETE FROM RevServiceTypes_Staging WHERE AuthenticationId = @AuthenticationId;
  ```

**LoadRevServicesFromStagingTable()**:
- **Stored Procedure**: `usp_Update_RevService`
- **Parameters**: `@AuthenticationId` (int)
- **SP Logic**:
  ```sql
  -- Merge services with type and provider validation
  MERGE RevServices AS target
  USING (
    SELECT s.*, st.ServiceTypeId as ValidTypeId, p.ProviderId as ValidProviderId
    FROM RevServices_Staging s
    LEFT JOIN RevServiceTypes st ON s.ServiceTypeId = st.ServiceTypeId
    LEFT JOIN RevProviders p ON s.ProviderId = p.ProviderId
    WHERE s.AuthenticationId = @AuthenticationId
  ) AS source ON target.ServiceId = source.ServiceId
  WHEN MATCHED THEN UPDATE SET 
    ServiceName = source.ServiceName,
    ServiceTypeId = source.ValidTypeId,
    ProviderId = source.ValidProviderId,
    Price = source.Price,
    Terms = source.Terms,
    ModifiedDate = GETDATE()
  WHEN NOT MATCHED AND source.ValidTypeId IS NOT NULL AND source.ValidProviderId IS NOT NULL 
  THEN INSERT 
    (ServiceId, ServiceName, ServiceTypeId, ProviderId, Price, Terms, CreatedDate)
    VALUES (source.ServiceId, source.ServiceName, source.ValidTypeId, 
            source.ValidProviderId, source.Price, source.Terms, GETDATE());
  
  DELETE FROM RevServices_Staging WHERE AuthenticationId = @AuthenticationId;
  ```

**LoadRevServiceProductsFromStagingTable()**:
- **Stored Procedure**: `usp_Update_RevServiceProduct_FromStaging`
- **Parameters**: `@AuthenticationId` (int)
- **SP Logic**:
  ```sql
  -- Merge service-product relationships
  MERGE RevServiceProducts AS target
  USING (
    SELECT sp.*, s.ServiceId as ValidServiceId, p.ProductId as ValidProductId
    FROM RevServiceProducts_Staging sp
    LEFT JOIN RevServices s ON sp.ServiceId = s.ServiceId
    LEFT JOIN RevProducts p ON sp.ProductId = p.ProductId
    WHERE sp.AuthenticationId = @AuthenticationId
  ) AS source ON target.ServiceProductId = source.ServiceProductId
  WHEN MATCHED THEN UPDATE SET 
    ServiceId = source.ValidServiceId,
    ProductId = source.ValidProductId,
    Quantity = source.Quantity,
    Price = source.Price,
    ModifiedDate = GETDATE()
  WHEN NOT MATCHED AND source.ValidServiceId IS NOT NULL AND source.ValidProductId IS NOT NULL 
  THEN INSERT 
    (ServiceProductId, ServiceId, ProductId, Quantity, Price, CreatedDate)
    VALUES (source.ServiceProductId, source.ValidServiceId, source.ValidProductId, 
            source.Quantity, source.Price, GETDATE());
  
  DELETE FROM RevServiceProducts_Staging WHERE AuthenticationId = @AuthenticationId;
  ```

**LoadRevInventoryTypesFromStagingTable()**:
- **Stored Procedure**: `usp_Update_RevInventoryType_FromStaging`
- **Parameters**: `@AuthenticationId` (int)
- **SP Logic**:
  ```sql
  -- Merge inventory types
  MERGE RevInventoryTypes AS target
  USING RevInventoryTypes_Staging AS source ON target.InventoryTypeId = source.InventoryTypeId
  WHEN MATCHED THEN UPDATE SET 
    TypeName = source.TypeName,
    Category = source.Category,
    Description = source.Description,
    ModifiedDate = GETDATE()
  WHEN NOT MATCHED THEN INSERT 
    (InventoryTypeId, TypeName, Category, Description, CreatedDate)
    VALUES (source.InventoryTypeId, source.TypeName, source.Category, 
            source.Description, GETDATE());
  
  DELETE FROM RevInventoryTypes_Staging WHERE AuthenticationId = @AuthenticationId;
  ```

**LoadRevInventoryItemsFromStagingTable()**:
- **Stored Procedure**: `usp_Update_RevInventoryItem_FromStaging`
- **Parameters**: `@AuthenticationId` (int)
- **SP Logic**:
  ```sql
  -- Merge inventory items with product and type validation
  MERGE RevInventoryItems AS target
  USING (
    SELECT ii.*, p.ProductId as ValidProductId, it.InventoryTypeId as ValidTypeId
    FROM RevInventoryItems_Staging ii
    LEFT JOIN RevProducts p ON ii.ProductId = p.ProductId
    LEFT JOIN RevInventoryTypes it ON ii.InventoryTypeId = it.InventoryTypeId
    WHERE ii.AuthenticationId = @AuthenticationId
  ) AS source ON target.InventoryItemId = source.InventoryItemId
  WHEN MATCHED THEN UPDATE SET 
    ProductId = source.ValidProductId,
    InventoryTypeId = source.ValidTypeId,
    Quantity = source.Quantity,
    Location = source.Location,
    Status = source.Status,
    ModifiedDate = GETDATE()
  WHEN NOT MATCHED AND source.ValidProductId IS NOT NULL AND source.ValidTypeId IS NOT NULL 
  THEN INSERT 
    (InventoryItemId, ProductId, InventoryTypeId, Quantity, Location, Status, CreatedDate)
    VALUES (source.InventoryItemId, source.ValidProductId, source.ValidTypeId, 
            source.Quantity, source.Location, source.Status, GETDATE());
  
  DELETE FROM RevInventoryItems_Staging WHERE AuthenticationId = @AuthenticationId;
  ```

**LoadRevUsagePlanGroupsFromStagingTable()**:
- **Stored Procedure**: `usp_Update_RevUsagePlanGroup_FromStaging`
- **Parameters**: `@AuthenticationId` (int)
- **SP Logic**:
  ```sql
  -- Merge usage plan groups
  MERGE RevUsagePlanGroups AS target
  USING RevUsagePlanGroups_Staging AS source ON target.UsagePlanGroupId = source.UsagePlanGroupId
  WHEN MATCHED THEN UPDATE SET 
    PlanName = source.PlanName,
    BillingCycle = source.BillingCycle,
    UsageLimit = source.UsageLimit,
    PricePerUnit = source.PricePerUnit,
    ModifiedDate = GETDATE()
  WHEN NOT MATCHED THEN INSERT 
    (UsagePlanGroupId, PlanName, BillingCycle, UsageLimit, PricePerUnit, CreatedDate)
    VALUES (source.UsagePlanGroupId, source.PlanName, source.BillingCycle, 
            source.UsageLimit, source.PricePerUnit, GETDATE());
  
  DELETE FROM RevUsagePlanGroups_Staging WHERE AuthenticationId = @AuthenticationId;
  ```

**LoadRevAgentFromStagingTable()**:
- **Stored Procedure**: `usp_Update_RevAgent`
- **Parameters**: `@AuthenticationId` (int)
- **SP Logic**:
  ```sql
  -- Merge agent data
  MERGE RevAgents AS target
  USING RevAgents_Staging AS source ON target.AgentId = source.AgentId
  WHEN MATCHED THEN UPDATE SET 
    AgentName = source.AgentName,
    Email = source.Email,
    Role = source.Role,
    Territory = source.Territory,
    Permissions = source.Permissions,
    ModifiedDate = GETDATE()
  WHEN NOT MATCHED THEN INSERT 
    (AgentId, AgentName, Email, Role, Territory, Permissions, CreatedDate)
    VALUES (source.AgentId, source.AgentName, source.Email, source.Role, 
            source.Territory, source.Permissions, GETDATE());
  
  DELETE FROM RevAgents_Staging WHERE AuthenticationId = @AuthenticationId;
  ```

**LoadRevPackageFromStagingTable()**:
- **Stored Procedure**: `usp_Update_RevPackage_FromStaging`
- **Parameters**: `@AuthenticationId` (int)
- **SP Logic**:
  ```sql
  -- Merge package data
  MERGE RevPackages AS target
  USING RevPackages_Staging AS source ON target.PackageId = source.PackageId
  WHEN MATCHED THEN UPDATE SET 
    PackageName = source.PackageName,
    Description = source.Description,
    Price = source.Price,
    Terms = source.Terms,
    ModifiedDate = GETDATE()
  WHEN NOT MATCHED THEN INSERT 
    (PackageId, PackageName, Description, Price, Terms, CreatedDate)
    VALUES (source.PackageId, source.PackageName, source.Description, 
            source.Price, source.Terms, GETDATE());
  
  DELETE FROM RevPackages_Staging WHERE AuthenticationId = @AuthenticationId;
  ```

**LoadRevPackageProductFromStagingTable()**:
- **Stored Procedure**: `usp_Update_RevPackageProduct_FromStaging`
- **Parameters**: `@AuthenticationId` (int)
- **SP Logic**:
  ```sql
  -- Merge package-product relationships
  MERGE RevPackageProducts AS target
  USING (
    SELECT pp.*, pkg.PackageId as ValidPackageId, p.ProductId as ValidProductId
    FROM RevPackageProducts_Staging pp
    LEFT JOIN RevPackages pkg ON pp.PackageId = pkg.PackageId
    LEFT JOIN RevProducts p ON pp.ProductId = p.ProductId
    WHERE pp.AuthenticationId = @AuthenticationId
  ) AS source ON target.PackageProductId = source.PackageProductId
  WHEN MATCHED THEN UPDATE SET 
    PackageId = source.ValidPackageId,
    ProductId = source.ValidProductId,
    Quantity = source.Quantity,
    Price = source.Price,
    ModifiedDate = GETDATE()
  WHEN NOT MATCHED AND source.ValidPackageId IS NOT NULL AND source.ValidProductId IS NOT NULL 
  THEN INSERT 
    (PackageProductId, PackageId, ProductId, Quantity, Price, CreatedDate)
    VALUES (source.PackageProductId, source.ValidPackageId, source.ValidProductId, 
            source.Quantity, source.Price, GETDATE());
  
  DELETE FROM RevPackageProducts_Staging WHERE AuthenticationId = @AuthenticationId;
  ```

3. **Post-Processing**:
   - Clear all staging tables for the authentication ID
   - Update sync status and timestamps
   - Call `QueueNextRevInstanceAsync(authId)` to process next authentication instance

---

## Instance and Queue Management

### QueueNextRevInstanceAsync(int currentAuthId)
**Purpose**: Queue the next authentication instance for processing

**Low-Level Flow**:

1. **Get Next Authentication**:
   - Call `GetNextRevIoAuthenticationId(currentAuthId)` to get next auth ID
   - If result is -1: All authentications processed, proceed to completion

2. **Branch Logic**:
   - **If Next Auth ID Found**:
     - Call `BuildMessageToQueue()` with:
       - AuthenticationId: nextAuthId
       - SyncStep: RevSyncStep.Customers (restart cycle)
       - PageNumber: 1
       - IsLastStep: false

   - **If No More Auth IDs**:
     - Call `SendNotificationToAmop20()` to notify completion
     - Log completion status and metrics

3. **Error Handling**:
   - Handle queue failures with retry logic
   - Log errors and send failure notifications

---

### SendNotificationToAmop20()
**Purpose**: Send completion notification to AMOP 2.0 system

**Low-Level Flow**:

1. **Notification Payload Construction**:
   - Create JSON payload with:
     - CompletionStatus: "Success" or "Failed"
     - ProcessedAuthentications: count
     - StartTime: process start timestamp
     - EndTime: current timestamp
     - TotalRecordsSynced: aggregated count
     - ErrorDetails: any error information

2. **HTTP Request Setup**:
   - Configure HTTP client with AMOP 2.0 endpoint
   - Set authentication headers (API key, bearer token)
   - Set content type to application/json
   - Configure timeout (30 seconds)

3. **API Call Execution**:
   - Make HTTP POST request to AMOP 2.0 notification endpoint
   - Handle HTTP response status codes:
     - 200/201: Success, log confirmation
     - 4xx: Client error, log and retry once
     - 5xx: Server error, implement exponential backoff retry

4. **Response Processing**:
   - Parse response JSON for confirmation ID
   - Log notification success with confirmation details
   - Handle any response errors or warnings

5. **Retry Logic**:
   - Maximum 3 retry attempts
   - Exponential backoff: 2s, 4s, 8s
   - Log all retry attempts
   - If all retries fail: Log critical error but don't fail entire process

---

## Cleanup and Finalization

### CleanUp(KeySysLambdaContext context)
**Purpose**: Perform final cleanup operations before Lambda termination

**Low-Level Flow**:

1. **Context Cleanup**:
   - Call `context.CleanUp()` to perform Lambda context cleanup

2. **Context.CleanUp() Internal Operations**:
   - **Log Flushing**:
     - Flush all pending log entries to CloudWatch
     - Ensure all performance metrics are recorded
     - Close log streams and release log resources

   - **Database Connection Cleanup**:
     - Close all open SQL connections
     - Dispose of connection pools
     - Release database resources

   - **HTTP Client Cleanup**:
     - Dispose of HTTP clients and connections
     - Clear connection pools
     - Release network resources

   - **Memory Cleanup**:
     - Dispose of large objects and collections
     - Clear caches and temporary data
     - Force garbage collection if needed

   - **Final Status Update**:
     - Update Lambda execution status
     - Record final performance metrics
     - Set completion timestamp

3. **Error Handling**:
   - Handle cleanup errors gracefully
   - Log cleanup issues but don't throw exceptions
   - Ensure Lambda can terminate successfully

4. **Resource Verification**:
   - Verify all critical resources are properly disposed
   - Check for resource leaks
   - Log resource cleanup confirmation

---

## Error Handling and Retry Patterns

### Global Error Handling Strategy

**Exception Types and Handling**:

1. **Transient Errors** (Retry with exponential backoff):
   - SQL timeout exceptions
   - HTTP timeout exceptions
   - Network connectivity issues
   - API rate limiting (429 responses)
   - Temporary service unavailability (503 responses)

2. **Permanent Errors** (Fail fast, no retry):
   - Authentication failures (401, 403)
   - Invalid data format errors (400)
   - Resource not found errors (404)
   - Configuration errors

3. **Critical Errors** (Log and notify):
   - Database connection failures
   - Configuration missing errors
   - Memory/resource exhaustion

### Retry Policy Implementation

**GetSqlRetryPolicy() Details**:
```csharp
// Exponential backoff with jitter
RetryPolicy.Handle<SqlException>(ex => IsTransientError(ex))
    .Or<TimeoutException>()
    .WaitAndRetryAsync(
        retryCount: 3,
        sleepDurationProvider: retryAttempt => 
            TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)) + 
            TimeSpan.FromMilliseconds(Random.Next(0, 1000)), // jitter
        onRetry: (outcome, timespan, retryCount, context) => 
            LogRetryAttempt(outcome, timespan, retryCount)
    );
```

**IsTransientError() Logic**:
- SQL Error Numbers: 2 (timeout), 53 (network), 121 (semaphore timeout)
- Connection-related errors
- Temporary resource unavailability

---

## Performance Optimization Strategies

### Bulk Data Operations
- Use SqlBulkCopy for large dataset insertions
- Batch size: 1000 records per operation
- Parallel processing where possible
- Connection pooling and reuse

### API Pagination Optimization
- Page size: 1000 records (optimal for most APIs)
- Parallel page processing for independent pages
- Intelligent pagination based on API response headers
- Rate limiting compliance with exponential backoff

### Memory Management
- Stream processing for large datasets
- Dispose of objects immediately after use
- Use using statements for resource management
- Monitor memory usage and implement circuit breakers

### Database Performance
- Use stored procedures for complex operations
- Implement proper indexing on staging and production tables
- Use MERGE statements for efficient upsert operations
- Batch operations to reduce round trips

---

## Monitoring and Logging

### CloudWatch Metrics
- Lambda execution duration
- Memory utilization
- Error rates by error type
- API call success/failure rates
- Database operation performance
- Queue message processing rates

### Custom Metrics
- Records processed per sync step
- API pagination efficiency
- Staging-to-production transfer times
- Authentication processing rates
- Data validation error rates

### Log Levels and Content
- **INFO**: Process start/completion, major milestones
- **WARN**: Retry attempts, data validation warnings
- **ERROR**: Permanent failures, configuration issues
- **DEBUG**: Detailed API responses, SQL query performance
- **TRACE**: Individual record processing (development only)

---

## Configuration Management

### Environment Variables
- Database connection strings
- API endpoints and credentials
- SQS queue URLs
- Retry policy parameters
- Logging levels
- Performance thresholds

### Secrets Management
- API keys stored in AWS Secrets Manager
- Database credentials encrypted
- Regular credential rotation
- Access logging and auditing

### Feature Flags
- Enable/disable specific sync steps
- Toggle between staging and direct production writes
- Control parallel processing levels
- Emergency circuit breaker controls

---

This comprehensive low-level flow documentation provides detailed insight into every aspect of the RevIO Lambda function's operation, from initial SQS message processing through final cleanup and notification.