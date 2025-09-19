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

### 1. Lambda Initialization & Context Setup
```
Lambda Trigger (SQS/Scheduled) → BaseFunctionHandler() → Initialize KeySysLambdaContext
```

**Key Components Initialized:**
- **AwsFunctionBase**: Base class providing common AWS Lambda functionality with SQL bulk copy operations and retry policies
- **KeySysLambdaContext**: Context with database connections (`ConnectionString`), logging (`VerboseLogging = false`), and settings
- **Environment Variables**: Loaded from Lambda configuration for Rev.IO API endpoints and database connections
- **Database Connections**: Central DB connection string established for AltaWorxCentral_DEV database
- **Logging**: Verbose logging configuration applied with KeysysLambdaLogger

### 2. Authentication & Instance Management
```
GetNextRevIoAuthenticationId() → Determine Current Integration Authentication ID → Process Event
```

**Authentication Flow:**
- **RevIoAuthenticationRepository**: Manages Rev.IO API authentication credentials with Base64 decoding
- **Integration Authentication ID**: Identifies which Rev.IO tenant to sync (starts with 0 for initial)
- **Stored Procedure**: `usp_Rev_Get_Next_Authentication` retrieves next authentication record
- **Authentication Cycling**: Processes multiple Rev.IO tenants in sequence (returns -1 when complete)
- **RevioApiAuthentication**: Contains BaseUrl, Username, Password (Base64 encoded), APIKey, ProductionURL, SandboxURL

### 3. Sync Step Processing (Sequential - 14 Steps)
```
RevSyncStep Enumeration:
1. Customer → 2. BillProfile → 3. Provider → 4. ProductType → 5. Product → 
6. ServiceType → 7. Service → 8. ServiceProduct → 9. InventoryType → 
10. InventoryItem → 11. UsagePlanGroup → 12. Agent → 13. Package → 14. PackageProduct
```

### 4. Data Flow Pattern (Per Step)
```
Rev.IO API Call → Staging Table → Main Table → Site/Customer Updates → AMOP 2.0 Notification
```

## Detailed Low-Level Flow

### A. FunctionHandler Entry Point

**Method**: `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`

1. **Context Initialization**
   - Creates `KeySysLambdaContext` via `BaseFunctionHandler()`
   - Loads environment variables and database connections using `EnvironmentRepository`
   - Initializes logging with verbose flag: `VerboseLogging = false`
   - Sets up Base64Service for credential decoding

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

3. **Constants Applied**:
   - **MAX_NUMBER_OF_PAGES_IN_A_BATCH**: `20` pages per Lambda instance
   - **DEFAULT_PAGE_NUMBER**: `1` for initial page
   - **PageSize**: `1000` records per page (from `RevPageSize` environment variable)

### B. Authentication Management

**Repository**: `RevIoAuthenticationRepository`
**Stored Procedure**: `usp_Rev_Get_Next_Authentication`

**Authentication Retrieval Process:**
1. **Input**: Current Integration Authentication ID (0 for initial)
2. **Logic**: Retrieves next available Rev.IO authentication record from database
3. **Returns**: 
   - `> 0`: Valid authentication ID
   - `0`: Exception occurred during retrieval
   - `-1`: No more authentication records to process
4. **Authentication Object**: Contains BaseUrl, Username, Password (Base64 encoded), APIKey, ProductionURL, SandboxURL, RevBillProfile

### C. Staging Table Cleanup

**Method**: `ClearStagingTables()`
**Stored Procedure**: `usp_RevCustomerClearStagingTables`

**Cleanup Process:**
1. **SQL Retry Policy**: Uses `CommonConstants.DEFAULT_SQL_RETRY_COUNT = 3` retries
2. **Staging Tables Cleared**:
   - `dbo.RevCustomerStaging` (`REV_CUSTOMER_STG`)
   - `dbo.RevProviderStaging` (`REV_PROVIDER_STG`)
   - `dbo.RevServiceTypeStaging` (`REV_SERVICE_TYPE_STG`)
   - `dbo.RevServiceStaging` (`REV_SERVICE_STG`)
   - `dbo.RevProductTypeStaging` (`REV_PRODUCT_TYPE_STG`)
   - `dbo.RevProductStaging` (`REV_PRODUCT_STG`)
   - `dbo.RevServiceProductStaging` (`REV_SERVICE_PRODUCT_STG`)
   - `dbo.RevAgentStaging` (`REV_AGENT_STG`)
   - `dbo.RevBillProfileStaging` (`REV_BILLING_PROFILE_STG`)
   - `dbo.RevInventoryTypeStaging` (`REV_INVENTORY_TYPE_STG`)
   - `dbo.RevInventoryItemStaging` (`REV_INVENTORY_ITEM_STG`)
   - `dbo.RevPackageStaging` (`REV_PACKAGE_STG`)
   - `dbo.RevPackageProductStaging` (`REV_PACKAGE_PRODUCT_STG`)

### D. Sync Step Processing (Detailed)

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

2. **Data Transformation** (`RevCustomerSyncTable`)
   - Maps `RevCustomer` API response to staging table structure
   - **Customer Name Extraction**: Uses `customer.service_address?.company_name` or `customer.listing_address?.company_name`
   - **Billing Cycle Processing**: Parses `customer.RevFinance?.BillingCycleDate` to extract day and hour
   - **Tax Exemption**: Processes `customer.RevFinance?.TaxExemptEnabled` and `TaxExemptType`
   - **Parent-Child Relationships**: Handles `customer.parent_customer_id` for customer hierarchies
   - **Status Handling**: Processes customer status (ACTIVE, CLOSED, etc.)
   - **Agent Association**: Links to `customer.agent_id` (defaults to 0 if null)

3. **Staging Table Population**
   - **Table**: `dbo.RevCustomerStaging`
   - **Bulk Insert**: Using `SqlBulkCopy` with `BatchSize = 1000` (`SQLConstant.BatchSize`)
   - **Timeout**: `TimeoutSeconds = 800` (`SQLConstant.TimeoutSeconds`)
   - **Columns**: RevCustomerId, CustomerName, ParentCustomerId, IntegrationAuthenticationId, Status, ActivatedDate, CloseDate, TaxExemptEnabled, TaxExemptTypes, BillProfileId, AgentId, CustomerBillPeriodEndDay, CustomerBillPeriodEndHour

4. **Pagination Logic**
   - **Max Pages Per Instance**: `MAX_NUMBER_OF_PAGES_IN_A_BATCH = 20`
   - **Time Check**: Monitors remaining Lambda execution time using `CommonConstants.REMAINING_TIME_CUT_OFF = 180` seconds
   - **Queue Next**: If more pages exist, queues next Lambda instance with incremented page number

5. **Final Step Processing**
   - When `isLastStepSync = true`, calls `LoadRevCustomersFromStagingTable()`
   - Executes stored procedure: `usp_Update_RevCustomer`

#### Step 2-14: Similar Pattern for Other Entities

Each sync step follows the same pattern with entity-specific details:

**Step 2: BillProfile** (`RevSyncStep.BillProfile`)
- **API**: `https://api.revioapi.com/v1/BillProfiles`
- **Staging**: `dbo.RevBillProfileStaging`
- **SP**: `usp_Update_RevBillProfile`
- **Fields**: bill_profile_id, description, active

**Step 3: Provider** (`RevSyncStep.Provider`)
- **API**: `https://api.revioapi.com/v1/Providers`
- **Staging**: `dbo.RevProviderStaging`
- **SP**: `usp_Update_RevProvider`
- **Fields**: provider_id, description, provider_code, active, created_date, bill_profile_id, order_types

**Step 4: ProductType** (`RevSyncStep.ProductType`)
- **API**: `https://api.revioapi.com/v1/ProductTypes`
- **Staging**: `dbo.RevProductTypeStaging`
- **SP**: `usp_Update_RevProductType`
- **Fields**: product_type_id, product_type_code, description, tax_class_id, active

**Step 5: Product** (`RevSyncStep.Product`)
- **API**: `https://api.revioapi.com/v1/Products`
- **Staging**: `dbo.RevProductStaging`
- **SP**: `usp_Update_RevProduct`
- **Fields**: product_id, product_type_id, description, code_1, code_2, rate, cost, buy_rate, active, creates_order, provider_id, bills_in_arrears, prorates, customer_class, long_description, ledger_code, free_months, automatic_expiration_months, order_completion_billing, tax_class_id, wholesale_description

**Step 6: ServiceType** (`RevSyncStep.ServiceType`)
- **API**: `https://api.revioapi.com/v1/ServiceTypes`
- **Staging**: `dbo.RevServiceTypeStaging`
- **SP**: `usp_Update_RevServiceType`
- **Fields**: service_type_id, description, active

**Step 7: Service** (`RevSyncStep.Service`)
- **API**: `https://api.revioapi.com/v1/Services`
- **Staging**: `dbo.RevServiceStaging`
- **SP**: `usp_Update_RevService`
- **Fields**: number, service_type_id, service_id, customer_id, provider_id, activated_date, disconnected_date, description, fields (custom fields array)

**Step 8: ServiceProduct** (`RevSyncStep.ServiceProduct`)
- **API**: `https://api.revioapi.com/v1/ServiceProduct`
- **Staging**: `dbo.RevServiceProductStaging`
- **SP**: `usp_Update_RevServiceProduct`
- **Fields**: service_product_id, customer_id, product_id, package_id, service_id, description, code_1, code_2, rate, billed_through_date, canceled_date, status, status_date, status_user_id, activated_date, cost, wholesale_description, free_start_date, free_end_date, quantity, contract_start_date, contract_end_date, created_date, tax_included, group_on_bill, itemized

**Step 9: InventoryType** (`RevSyncStep.InventoryType`)
- **API**: `https://api.revioapi.com/v1/InventoryTypes`
- **Staging**: `dbo.RevInventoryTypeStaging`
- **SP**: `usp_Update_RevInventoryType`
- **Fields**: inventory_type_id, category, identifier, name, requires_product, description, status, format, ratable

**Step 10: InventoryItem** (`RevSyncStep.InventoryItem`)
- **API**: `https://api.revioapi.com/v1/InventoryItem`
- **Staging**: `dbo.RevInventoryItemStaging`
- **SP**: `usp_Update_RevInventoryItem`
- **Fields**: inventory_item_id, inventory_type_id, identifier, customer_id, created_date, created_by, status, inventory_item_unavailable_reason_id

**Step 11: UsagePlanGroup** (`RevSyncStep.UsagePlanGroup`)
- **API**: `https://api.revioapi.com/v1/UsagePlanGroups`
- **Staging**: `dbo.RevUsagePlanGroupStaging`
- **SP**: `usp_Update_RevUsagePlanGroup`
- **Fields**: usage_plan_group_id, description, long_description, active, created_date, created_by

**Step 12: Agent** (`RevSyncStep.Agent`)
- **API**: `https://api.revioapi.com/v1/Agents`
- **Staging**: `dbo.RevAgentStaging`
- **SP**: `usp_Update_RevAgent`
- **Fields**: agent_id, parent_agent_id, name, status

**Step 13: Package** (`RevSyncStep.Package`)
- **API**: `https://api.revioapi.com/v1/Packages`
- **Staging**: `dbo.RevPackageStaging`
- **SP**: `usp_Update_RevPackage`
- **Fields**: package_id, provider_id, currency_code, description, description_on_bill, long_description, created_date, created_by, active, usage_plan_group_id, service_type_id, package_category_id, exempt_from_spiff_commission, restrict_class_flag, class, restrict_bill_profile_flag, bill_profile_id

**Step 14: PackageProduct** (`RevSyncStep.PackageProduct`)
- **API**: `https://api.revioapi.com/v1/PackageProducts`
- **Staging**: `dbo.RevPackageProductStaging`
- **SP**: `usp_Update_RevPackageProduct`
- **Fields**: package_product_id, product_id, package_id, description, code_1, code_2, rate, cost, buy_rate, quantity, tax_included, group_on_bill, itemize, credit

### E. Queue Management & Continuation

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
1. **Next Page**: Same step, increment page number if more pages available
2. **Next Step**: Increment RevSyncStep enum, reset page to 1, set IsLastStepSync=true for final page
3. **Next Instance**: Next Integration Authentication ID, reset to Customer step
4. **Completion**: No more authentication records, process ends

**Queue Delay**: `CommonConstants.SQS_SENDING_MESSAGE_DELAY_IN_SECONDS = 5` seconds between messages

### F. Error Handling & Retry Logic

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
- **API Rate Limiting**: HTTP 429 status code, wait based on `Retry-After` header
- **SQL Deadlock**: Retry with delay using Polly retry policy
- **Authentication Failure**: HTTP 401, log error and move to next integration
- **Lambda Timeout**: Remaining time < `REMAINING_TIME_CUT_OFF = 180` seconds, queue continuation

## Database Operations

### Primary Stored Procedure: `usp_Update_RevCustomer`

**Purpose**: Merges RevCustomerStaging data into RevCustomer main table with comprehensive business logic

**Key Operations:**

1. **MERGE Statement**: Updates existing, inserts new customers using RevCustomerId and IntegrationAuthenticationId as keys

2. **Customer Status Processing**:
   ```sql
   [TARGET].[IsActive] = CASE WHEN [SOURCE].[Status] = 'CLOSED' THEN 0 ELSE 1 END,
   [TARGET].[IsDeleted] = CASE WHEN [SOURCE].[Status] = 'CLOSED' THEN 1 ELSE 0 END
   ```

3. **Parent-Child Relationship Resolution**:
   ```sql
   UPDATE [RevCustomer]
   SET [ParentCustomerId] = [ParentRevCustomer].[id]
   FROM [RevCustomer]
   LEFT JOIN [RevCustomer] [ParentRevCustomer] 
   ON [RevCustomer].[RevParentCustomerId] = [ParentRevCustomer].[RevCustomerId] 
   AND [RevCustomer].[IntegrationAuthenticationId] = [ParentRevCustomer].[IntegrationAuthenticationId]
   ```

4. **Child Count Updates**:
   ```sql
   UPDATE [RevCustomer]
   SET [ChildCount] = (
       SELECT COUNT(1) FROM [RevCustomer] 
       WHERE [RevCustomer].[RevCustomerId] = [RevParentCustomerId] 
       AND [Status] <> 'CLOSED'
   )
   ```

5. **Site Record Management**: Creates Site records with naming convention: `CustomerName (RevCustomerId)`
   ```sql
   INSERT INTO [dbo].[Site] ([TenantId], [Name], [RevCustomerId], [CreatedDate], [CreatedBy], [IsDeleted], [IsActive])
   SELECT DISTINCT
       [IntegrationAuthentication].[TenantId],
       [RevCustomer].[CustomerName] + ' (' + [RevCustomer].[RevCustomerId] + ')' AS [Name],
       [RevCustomer].[Id] AS [RevCustomerId],
       GETUTCDATE(), 'RevSync', 0, 1
   ```

6. **Bill Period Management**: Sets `CustomerBillPeriodEndDay` and `CustomerBillPeriodEndHour` from staging data

7. **Device Reassignment for Closed Customers**:
   ```sql
   UPDATE [Device_Tenant]
   SET [SiteId] = CASE WHEN [RevCustomer].[Status] = 'CLOSED' THEN [Site].[id] ELSE [Device_Tenant].[SiteId] END
   FROM [dbo].[Device_Tenant]
   INNER JOIN [dbo].[RevCustomer] ON [RevCustomer].[RevCustomerId] = [Device_Tenant].AccountNumber
   INNER JOIN [dbo].[Site] ON [IsSystemDefault] = 1
   WHERE [RevCustomer].[Status] = 'CLOSED'
   ```

8. **Customer Group Assignment**: Auto-assigns child accounts to customer groups based on parent relationships

### Database Tables Affected

**Core Tables:**
- **RevCustomer**: Main customer records with parent-child relationships
- **Site**: Customer site associations with unique naming
- **Device_Tenant**: Device assignments with automatic reassignment for closed customers
- **MobilityDevice_Tenant**: Mobile device assignments
- **SiteGroup_Site**: Customer group memberships

**Staging Tables** (All cleared and repopulated per sync):
- **RevCustomerStaging**: Temporary customer data from API
- **RevBillProfileStaging**: Bill profile staging data
- **RevProviderStaging**: Provider staging data
- **RevProductTypeStaging**: Product type staging data
- **RevProductStaging**: Product staging data
- **RevServiceTypeStaging**: Service type staging data
- **RevServiceStaging**: Service staging data
- **RevServiceProductStaging**: Service product staging data
- **RevInventoryTypeStaging**: Inventory type staging data
- **RevInventoryItemStaging**: Inventory item staging data
- **RevUsagePlanGroupStaging**: Usage plan group staging data
- **RevAgentStaging**: Agent staging data
- **RevPackageStaging**: Package staging data
- **RevPackageProductStaging**: Package product staging data

## AMOP 2.0 Integration

**Trigger Method**: `DailySyncAmopApiTrigger.SendNotificationToAmop20()`
**API URL**: `https://qa-api.amop.services/migration_management_qa` (from `Amop20SyncUpdateAPIURL`)

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
- Uses `LambdaLoggingHandler` for HTTP client logging

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
- **REMAINING_TIME_CUT_OFF**: `180` seconds safety margin before timeout

### Pagination Strategy

**Per Lambda Instance:**
- Maximum `20` pages processed (`MAX_NUMBER_OF_PAGES_IN_A_BATCH`)
- Each page contains `1000` records (`RevPageSize`)
- Total maximum records per instance: `20,000`

**Time Management:**
- Monitors remaining Lambda execution time
- Queues continuation if approaching timeout
- Uses safety margin to prevent abrupt termination

### Bulk Operations

**SQL Bulk Copy Configuration:**
```csharp
bulkCopy.BulkCopyTimeout = 800; // SQLConstant.TimeoutSeconds
bulkCopy.BatchSize = 1000;      // SQLConstant.BatchSize
```

**Staging Table Strategy:**
- Bulk insert into staging tables for maximum speed
- Single stored procedure call to merge all staged data
- Minimizes individual database transactions

## Error Scenarios & Recovery

### Common Error Patterns

1. **API Rate Limiting**
   - **Detection**: HTTP 429 status code
   - **Response**: Wait based on `Retry-After` header
   - **Retry**: Up to `NUMBER_OF_REV_IO_RETRIES = 5` attempts with exponential backoff

2. **Database Deadlocks**
   - **Detection**: SQL deadlock exception
   - **Response**: Wait and retry up to `DEFAULT_SQL_RETRY_COUNT = 3` times
   - **Recovery**: Uses Polly retry policy with exponential backoff

3. **Authentication Expiry**
   - **Detection**: HTTP 401 status code
   - **Response**: Log error and move to next integration authentication
   - **Recovery**: Next scheduled run will retry with fresh authentication

4. **Lambda Timeout**
   - **Detection**: Remaining time < `REMAINING_TIME_CUT_OFF = 180` seconds
   - **Response**: Queue continuation message with current state
   - **Recovery**: Next instance continues from last position

### Monitoring & Logging

**Log Types** (from LogCommonStrings.cs):
- **INFO**: General information and progress tracking
- **WARNING**: Non-critical issues that don't stop processing
- **ERROR**: Recoverable errors with retry attempts
- **EXCEPTION**: Critical failures with full stack traces

**Key Monitoring Points:**
- Integration Authentication ID processing status
- API call success/failure rates with retry counts
- Staging table record counts per sync step
- Stored procedure execution times and timeouts
- Queue message processing and continuation logic
- AMOP 2.0 notification delivery status

## File Structure & Dependencies

### Core Lambda Files
- **AltaworxRevAWSCustomerSync.cs**: Main Lambda function with sync orchestration
- **AwsFunctionBase.cs**: Base class with common Lambda functionality, SQL operations, and logging
- **KeySysLambdaContext.cs**: Context management with database connections and environment variables

### Sync Step Management
- **RevSyncStep.cs**: Enumeration defining 14-step sync process
- **RevStagingTable.cs**: Constants for staging table names
- **RevCustomerSyncTable.cs**: Customer-specific staging table operations

### Rev.IO Integration
- **RevIOCommon.cs**: Data models for Rev.IO API responses and request structures
- **RevIOAuthentication.cs**: Authentication handling with Base64 encoding
- **RevioApiClient.cs**: HTTP client with retry logic and API communication
- **RevIOHelper.cs**: Utility methods for billing period calculations
- **RevioAuthenticationRepository.cs**: Database operations for authentication management

### Constants & Configuration
- **SQLConstant.cs**: Database timeout, batch size, and stored procedure name constants
- **CommonConstants.cs**: Retry counts, delays, and general application constants
- **CommonColumnNames.cs**: Database column name constants
- **LogCommonStrings.cs**: Standardized logging message templates
- **EnvironmentVariableKeyConstants.cs**: Environment variable key definitions

### Support Classes
- **Common.cs**: Application-specific constants and stored procedure names
- **DailySyncAmopApiTrigger.cs**: AMOP 2.0 notification integration
- **EnvironmentRepository.cs**: Environment variable access and management
- **RevPackageStagingColumnNames.cs**: Package-specific column name constants
- **RevPackageProductStagingColumnNames.cs**: Package product column name constants

## Conclusion

The AltaworxRevAWSCustomerSync Lambda implements a comprehensive, fault-tolerant synchronization system that:

1. **Handles Multiple Tenants**: Processes multiple Rev.IO integrations sequentially using authentication cycling
2. **Manages Large Datasets**: Uses pagination (1000 records/page, 20 pages/instance) and staging tables for efficiency
3. **Provides Fault Tolerance**: Implements comprehensive retry policies (5 retries for API, 3 for SQL) and recovery mechanisms
4. **Maintains Data Integrity**: Uses MERGE operations and transaction management with proper parent-child relationship handling
5. **Integrates with AMOP 2.0**: Provides completion notifications for downstream processing
6. **Scales Horizontally**: Queues continuation instances to handle large data volumes without timeout issues
7. **Ensures Business Logic**: Handles customer status changes, site creation, device reassignment, and customer group management
8. **Optimizes Performance**: Uses bulk operations, connection pooling, and efficient staging strategies

The system processes customer and related entity data through a 14-step pipeline, ensuring all Rev.IO entities are synchronized with the AMOP platform while maintaining referential integrity and handling complex business requirements such as customer hierarchies, site management, device assignments, and billing period calculations.