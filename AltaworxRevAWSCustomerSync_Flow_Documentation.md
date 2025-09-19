# AltaworxRevAWSCustomerSync Lambda - Comprehensive Flow Documentation

## Lambda Configuration Details

### Basic Information
- **Lambda Name**: `AltaworxRevAWSCustomerSync`
- **Queue Name**: `RevCustomerSync_TEST` (DEV Environment)
- **Queue URL**: `https://sqs.us-east-1.amazonaws.com/130265568833/RevCustomerSync_DEV`
- **Scheduled Time**: Runs daily at 13:45 UTC (1:45 PM UTC)
- **Sync Frequency**: Daily execution with next 10 scheduled runs shown in configuration

### Environment Variables
| Variable | Value |
|----------|--------|
| **Amop20SyncUpdateAPIURL** | `https://qa-api.amop.services/migration_management_qa` |
| **BaseMultiTenantConnectionString** | `data source=multitenantportal.cvax8ohayigx.us-east-1.rds.amazonaws.com;initial catalog=basemultitenant_dev;persist security info=True;user id=BaseMultiTenant_TestUser;password=9ZadmvQj3QU%pyQg;multipleactiveresultsets=True;Encrypt=False;` |
| **ConnectionString** | `data source=altaworx-test.cd98i7zb3ml3.us-east-1.rds.amazonaws.com;initial catalog=AltaWorxCentral_DEV;persist security info=True;user id=altaworx;password=2lvAQnQ0vBwNZNjpONhy;multipleactiveresultsets=True;connect timeout=90;application name=EntityFramework" providerName = "System.Data.SqlClient;Encrypt=False;` |
| **EnvName** | `Development` |
| **RevCustomerSyncQueueUrl** | `https://sqs.us-east-1.amazonaws.com/130265568833/RevCustomerSync_DEV` |
| **RevGetAgentUrl** | `https://api.revioapi.com/v1/Agents` |
| **RevGetBillProfilesUrl** | `https://api.revioapi.com/v1/BillProfiles` |
| **RevGetCustomersUrl** | `https://api.revioapi.com/v1/Customers` |
| **RevGetInventoryItemsUrl** | `https://api.revioapi.com/v1/InventoryItem` |
| **RevGetInventoryTypesUrl** | `https://api.revioapi.com/v1/InventoryTypes` |
| **RevGetPackageProductsUrl** | `https://api.revioapi.com/v1/PackageProducts` |
| **RevGetPackagesUrl** | `https://api.revioapi.com/v1/Packages` |
| **RevGetProductTypesUrl** | `https://api.revioapi.com/v1/ProductTypes` |
| **RevGetProductsUrl** | `https://api.revioapi.com/v1/Products` |
| **RevGetProvidersUrl** | `https://api.revioapi.com/v1/Providers` |
| **RevGetServiceProductsUrl** | `https://api.revioapi.com/v1/ServiceProduct` |
| **RevGetServiceTypesUrl** | `https://api.revioapi.com/v1/ServiceTypes` |
| **RevGetServicesUrl** | `https://api.revioapi.com/v1/Services` |
| **RevGetUsagePlanGroupsUrl** | `https://api.revioapi.com/v1/UsagePlanGroups` |
| **RevPageSize** | `1000` |
| **VerboseLogging** | `false` |

## High-Level Flow Overview

### 1. Lambda Initialization
```
Lambda Trigger (SQS/Scheduled) → BaseFunctionHandler() → Initialize KeySysLambdaContext
```

**Key Components Initialized:**
- **AwsFunctionBase**: Base class providing common AWS Lambda functionality
- **KeySysLambdaContext**: Context with database connections, logging, and settings
- **Environment Variables**: Loaded from Lambda configuration
- **Database Connections**: Central DB connection string established
- **Logging**: Verbose logging configuration applied

### 2. Authentication & Instance Management
```
GetNextRevIoAuthenticationId() → Determine Current Integration Authentication ID → Process Event
```

**Authentication Flow:**
- **RevIoAuthenticationRepository**: Manages Rev.IO API authentication credentials
- **Integration Authentication ID**: Identifies which Rev.IO tenant to sync
- **Stored Procedure**: `usp_Rev_Get_Next_Authentication` retrieves next authentication record
- **Authentication Cycling**: Processes multiple Rev.IO tenants in sequence

### 3. Sync Step Processing (Sequential)
```
RevSyncStep Enumeration:
1. Customer → 2. BillProfile → 3. Provider → 4. ProductType → 5. Product → 
6. ServiceType → 7. Service → 8. ServiceProduct → 9. InventoryType → 
10. InventoryItem → 11. UsagePlanGroup → 12. Agent → 13. Package → 14. PackageProduct
```

### 4. Data Flow Pattern (Per Step)
```
Rev.IO API Call → Staging Table → Main Table → Site/Customer Updates
```

## Detailed Low-Level Flow

### A. FunctionHandler Entry Point

**Method**: `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`

1. **Context Initialization**
   - Creates `KeySysLambdaContext` via `BaseFunctionHandler()`
   - Loads environment variables and database connections
   - Initializes logging with verbose flag: `VerboseLogging = false`

2. **Message Processing Logic**
   ```csharp
   if (sqsEvent?.Records != null) {
       // Process SQS message with existing sync step
       currentIntegrationAuthenticationId = GetCurrentIntegrationAuthenticationId()
       currentRevSyncStep = GetCurrentRevSyncStep()
       pageNumber = GetPageNumber()
       isLastStepSync = GetIsLastStepSyncNumber()
   } else {
       // Initial run - start from first authentication
       currentIntegrationAuthenticationId = 0
   }
   ```

### B. Authentication Management

**Repository**: `RevIoAuthenticationRepository`
**Stored Procedure**: `usp_Rev_Get_Next_Authentication`

**Authentication Retrieval Process:**
1. **Input**: Current Integration Authentication ID (0 for initial)
2. **Logic**: Retrieves next available Rev.IO authentication record
3. **Returns**: 
   - `> 0`: Valid authentication ID
   - `0`: Exception occurred
   - `-1`: No more authentication records
4. **Authentication Object**: Contains BaseUrl, Username, Password (Base64 encoded), APIKey

### C. Sync Step Processing (Detailed)

#### Step 1: Customer Sync (`RevSyncStep.Customer`)

**Method**: `SyncRevCustomersAsync()`
**API Endpoint**: `https://api.revioapi.com/v1/Customers`
**Page Size**: `1000` records per page
**Constants Used**: `CommonConstants.NUMBER_OF_REV_IO_RETRIES = 5`

**Process:**
1. **API Call with Retry Logic**
   ```csharp
   RevioApiClient.ExecuteRevGetWithRetryAsync<RevCustomerList>(
       RevGetCustomersUrl, PageSize=1000, pageNumber
   )
   ```

2. **Data Transformation**
   - Maps `RevCustomer` API response to `RevCustomerSyncTable`
   - Handles billing cycle date parsing: `customer.RevFinance?.BillingCycleDate`
   - Extracts company name from service_address or listing_address
   - Processes tax exemption flags and bill profile associations

3. **Staging Table Population**
   - **Table**: `dbo.RevCustomerStaging`
   - **Bulk Insert**: Using `SqlBulkCopy` with `BatchSize = 1000`
   - **Timeout**: `TimeoutSeconds = 800`

4. **Pagination Logic**
   - **Max Pages Per Instance**: `MAX_NUMBER_OF_PAGES_IN_A_BATCH = 20`
   - **Time Check**: Monitors remaining Lambda execution time
   - **Queue Next**: If more pages exist, queues next Lambda instance

5. **Final Step Processing**
   - When `isLastStepSync = true`, calls `LoadRevCustomersFromStagingTable()`
   - Executes stored procedure: `usp_Update_RevCustomer`

#### Step 2-14: Similar Pattern for Other Entities

Each sync step follows the same pattern:
- **BillProfile**: `usp_Update_RevBillProfile` → `dbo.RevBillProfileStaging`
- **Provider**: `usp_Update_RevProvider` → `dbo.RevProviderStaging`
- **ProductType**: `usp_Update_RevProductType` → `dbo.RevProductTypeStaging`
- **Product**: `usp_Update_RevProduct` → `dbo.RevProductStaging`
- **ServiceType**: `usp_Update_RevServiceType` → `dbo.RevServiceTypeStaging`
- **Service**: `usp_Update_RevService` → `dbo.RevServiceStaging`
- **ServiceProduct**: `usp_Update_RevServiceProduct` → `dbo.RevServiceProductStaging`
- **InventoryType**: `usp_Update_RevInventoryType` → `dbo.RevInventoryTypeStaging`
- **InventoryItem**: `usp_Update_RevInventoryItem` → `dbo.RevInventoryItemStaging`
- **UsagePlanGroup**: `usp_Update_RevUsagePlanGroup` → `dbo.RevUsagePlanGroupStaging`
- **Agent**: `usp_Update_RevAgent` → `dbo.RevAgentStaging`
- **Package**: `usp_Update_RevPackage` → `dbo.RevPackageStaging`
- **PackageProduct**: `usp_Update_RevPackageProduct` → `dbo.RevPackageProductStaging`

### D. Queue Management & Continuation

**SQS Message Structure:**
```json
{
  "MessageAttributes": {
    "CurrentIntegrationAuthenticationId": { "StringValue": "123" },
    "CurrentRevSyncStep": { "StringValue": "2" },
    "CurrentPageNumber": { "StringValue": "1" },
    "IsLastStepSync": { "StringValue": "false" }
  }
}
```

**Queue Continuation Logic:**
1. **Next Page**: Same step, increment page number
2. **Next Step**: Increment RevSyncStep enum, reset page to 1
3. **Next Instance**: Next Integration Authentication ID, reset to Customer step
4. **Completion**: No more authentication records, process ends

### E. Error Handling & Retry Logic

**SQL Retry Policy:**
```csharp
Policy.Handle<SqlException>()
    .WaitAndRetry(CommonConstants.DEFAULT_SQL_RETRY_COUNT = 3)
```

**HTTP Retry Policy:**
```csharp
Policy.Handle<HttpRequestException>()
    .WaitAndRetry(CommonConstants.NUMBER_OF_REV_IO_RETRIES = 5)
```

**Exception Scenarios:**
- **API Timeout**: Retry with exponential backoff
- **SQL Deadlock**: Retry with delay
- **Authentication Failure**: Log and skip to next instance
- **Rate Limiting**: Wait based on `Retry-After` header

## Database Operations

### Staging Tables Structure

All staging tables follow similar pattern with these common columns:
- **IntegrationAuthenticationId**: Links to specific Rev.IO tenant
- **CreatedDate**: Timestamp of record creation
- **ModifiedDate**: Timestamp of last update
- **IsDeleted**: Soft delete flag
- **IsActive**: Active status flag

### Primary Stored Procedure: `usp_Update_RevCustomer`

**Purpose**: Merges RevCustomerStaging data into RevCustomer main table

**Key Operations:**
1. **MERGE Statement**: Updates existing, inserts new customers
2. **Parent-Child Relationships**: Links customers via `RevParentCustomerId`
3. **Child Count Updates**: Calculates number of child customers
4. **Site Record Management**: Creates/updates Site records with naming convention: `CustomerName (RevCustomerId)`
5. **Bill Period Updates**: Sets `CustomerBillPeriodEndDay` and `CustomerBillPeriodEndHour`
6. **Status Management**: Handles CLOSED status by setting `IsActive=0, IsDeleted=1`
7. **Device Reassignment**: Moves devices to default site when customer is closed
8. **Customer Group Assignment**: Auto-assigns child accounts to customer groups

**Site Creation Logic:**
```sql
INSERT INTO [dbo].[Site] (
    [TenantId], [Name], [RevCustomerId], [CreatedDate], [CreatedBy], [IsDeleted], [IsActive]
)
SELECT DISTINCT
    [IntegrationAuthentication].[TenantId],
    [RevCustomer].[CustomerName] + ' (' + [RevCustomer].[RevCustomerId] + ')' AS [Name],
    [RevCustomer].[Id] AS [RevCustomerId],
    GETUTCDATE() AS [CreatedDate],
    'RevSync' AS [CreatedBy],
    0, 1
FROM [dbo].[RevCustomer]
```

**Device Reassignment for Closed Customers:**
```sql
UPDATE [Device_Tenant]
SET [SiteId] = CASE WHEN [RevCustomer].[Status] = 'CLOSED' THEN [Site].[id] ELSE [Device_Tenant].[SiteId] END
FROM [dbo].[Device_Tenant]
INNER JOIN [dbo].[RevCustomer] ON [RevCustomer].[RevCustomerId] = [Device_Tenant].AccountNumber
INNER JOIN [dbo].[Site] ON [IsSystemDefault] = 1
WHERE [RevCustomer].[Status] = 'CLOSED'
```

### Additional Database Tables Affected

**Core Tables:**
- **RevCustomer**: Main customer records
- **Site**: Customer site associations
- **Device_Tenant**: Device assignments
- **MobilityDevice_Tenant**: Mobile device assignments
- **SiteGroup_Site**: Customer group memberships

**Staging Tables:**
- **RevCustomerStaging**: Temporary customer data from API
- **RevBillProfileStaging**: Bill profile staging
- **RevProviderStaging**: Provider staging
- **RevProductStaging**: Product staging
- **RevServiceStaging**: Service staging
- **RevServiceProductStaging**: Service product staging
- **RevInventoryTypeStaging**: Inventory type staging
- **RevInventoryItemStaging**: Inventory item staging
- **RevUsagePlanGroupStaging**: Usage plan group staging
- **RevAgentStaging**: Agent staging
- **RevPackageStaging**: Package staging
- **RevPackageProductStaging**: Package product staging

### Staging Table Cleanup

**Method**: `ClearStagingTables()` and `ClearStagingTablesV2()`
**Stored Procedure**: `usp_RevCustomerClearStagingTables`

**Cleanup Process:**
1. **Initial Cleanup**: Truncates all staging tables at start of sync
2. **Step-by-Step Cleanup**: Clears specific staging table after processing
3. **Integration-Specific**: Only clears data for current Integration Authentication ID

## AMOP 2.0 Integration

**Trigger Method**: `DailySyncAmopApiTrigger.SendNotificationToAmop20()`
**API URL**: `https://qa-api.amop.services/migration_management_qa`

**Notification Payload:**
```json
{
  "data": {
    "key_name": "rev_customers_sync",
    "tenant_id": "123",
    "tenant_name": "Customer Name"
  }
}
```

**Trigger Points:**
- After completing sync for each tenant (Integration Authentication ID)
- Only sent for the last instance of each tenant's sync cycle

## Performance Optimization

### Constants Used

**From SQLConstant.cs:**
- **TimeoutSeconds**: `800` seconds for stored procedures
- **ShortTimeoutSeconds**: `360` seconds for quick operations
- **BatchSize**: `1000` records per bulk insert
- **PageSize**: `1000` records per API call

**From CommonConstants.cs:**
- **NUMBER_OF_REV_IO_RETRIES**: `5` retry attempts for API calls
- **SQS_SENDING_MESSAGE_DELAY_IN_SECONDS**: `5` seconds delay between queue messages
- **LAMBA_RUN_TIME_LIMIT_IN_SECONDS**: `900` seconds (15 minutes) Lambda timeout

### Pagination Strategy

**Per Lambda Instance:**
- Maximum `20` pages processed (`MAX_NUMBER_OF_PAGES_IN_A_BATCH`)
- Each page contains `1000` records (`RevPageSize`)
- Total maximum records per instance: `20,000`

**Time Management:**
- Monitors remaining Lambda execution time
- Queues continuation if approaching timeout
- Uses `REMAINING_TIME_CUT_OFF = 180` seconds safety margin

### Bulk Operations

**SQL Bulk Copy Configuration:**
```csharp
bulkCopy.BulkCopyTimeout = 800; // SQLConstant.TimeoutSeconds
bulkCopy.BatchSize = 1000;      // SQLConstant.BatchSize
```

**Staging Table Strategy:**
- Bulk insert into staging tables for speed
- Single stored procedure call to merge all staged data
- Minimizes individual database transactions

## Error Scenarios & Recovery

### Common Error Patterns

1. **API Rate Limiting**
   - **Detection**: HTTP 429 status code
   - **Response**: Wait based on `Retry-After` header
   - **Retry**: Up to 5 attempts with exponential backoff

2. **Database Deadlocks**
   - **Detection**: SQL deadlock exception
   - **Response**: Wait and retry up to 3 times
   - **Recovery**: Uses Polly retry policy

3. **Authentication Expiry**
   - **Detection**: HTTP 401 status code
   - **Response**: Log error and move to next integration
   - **Recovery**: Next scheduled run will retry

4. **Lambda Timeout**
   - **Detection**: Remaining time < 180 seconds
   - **Response**: Queue continuation message
   - **Recovery**: Next instance continues from last position

### Monitoring & Logging

**Log Types** (from LogCommonStrings.cs):
- **INFO**: General information and progress
- **WARNING**: Non-critical issues
- **ERROR**: Recoverable errors
- **EXCEPTION**: Critical failures with stack traces

**Key Monitoring Points:**
- Integration Authentication ID processing
- API call success/failure rates
- Staging table record counts
- Stored procedure execution times
- Queue message processing

## Conclusion

The AltaworxRevAWSCustomerSync Lambda implements a robust, scalable synchronization system that:

1. **Handles Multiple Tenants**: Processes multiple Rev.IO integrations sequentially
2. **Manages Large Datasets**: Uses pagination and staging tables for efficiency  
3. **Provides Fault Tolerance**: Implements comprehensive retry and recovery mechanisms
4. **Maintains Data Integrity**: Uses MERGE operations and transaction management
5. **Integrates with AMOP 2.0**: Provides completion notifications for downstream processing
6. **Scales Horizontally**: Queues continuation instances to handle large data volumes

The system processes customer data through a 14-step pipeline, ensuring all Rev.IO entities are synchronized with the AMOP platform while maintaining referential integrity and handling business logic requirements such as customer hierarchies, site management, and device assignments.