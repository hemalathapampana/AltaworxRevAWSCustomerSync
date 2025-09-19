using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Helpers.Constants;
using Altaworx.AWS.Core.Models;
using Altaworx.AWS.Core.Repositories.RevIo;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amop.Core.Constants;
using Amop.Core.Enumerations;
using Amop.Core.Helpers;
using Amop.Core.Logger;
using Amop.Core.Models.Revio;
using Amop.Core.Repositories.Environment;
using Amop.Core.Repositories.Revio;
using Amop.Core.Services.Base64Service;
using Amop.Core.Services.Http;
using Amop.Core.Services.Revio;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore.SqlServer.Storage.Internal;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;
using static Altaworx.AWS.Core.RevIOCommon;
using static AltaworxRevAWSCustomerSync.Common;
using AMOPSQLConstant = Amop.Core.Constants.SQLConstant;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxRevAWSCustomerSync
{
    public class Function : AwsFunctionBase
    {
        private string RevCustomerSyncQueueURL = Environment.GetEnvironmentVariable("RevCustomerSyncQueueUrl");

        private string RevGetCustomersUrl = Environment.GetEnvironmentVariable("RevGetCustomersUrl");
        private string RevGetServiceTypesUrl = Environment.GetEnvironmentVariable("RevGetServiceTypesUrl");
        private string RevGetServicesUrl = Environment.GetEnvironmentVariable("RevGetServicesUrl");
        private string RevGetProductTypesUrl = Environment.GetEnvironmentVariable("RevGetProductTypesUrl");
        private string RevGetProductsUrl = Environment.GetEnvironmentVariable("RevGetProductsUrl");
        private string RevGetServiceProductsUrl = Environment.GetEnvironmentVariable("RevGetServiceProductsUrl");
        private string RevGetProvidersUrl = Environment.GetEnvironmentVariable("RevGetProvidersUrl");
        private string RevGetBillProfilesUrl = Environment.GetEnvironmentVariable("RevGetBillProfilesUrl");
        private string RevGetInventoryTypesUrl = Environment.GetEnvironmentVariable("RevGetInventoryTypesUrl");
        private string RevGetInventoryItemsUrl = Environment.GetEnvironmentVariable("RevGetInventoryItemsUrl");
        private string RevGetUsagePlanGroupsUrl = Environment.GetEnvironmentVariable("RevGetUsagePlanGroupsUrl");
        private string RevGetAgentUrl = Environment.GetEnvironmentVariable("RevGetAgentUrl");
        private string RevGetPackagesUrl = Environment.GetEnvironmentVariable("RevGetPackagesUrl");
        private string RevGetPackageProductsUrl = Environment.GetEnvironmentVariable("RevGetPackageProductsUrl");
        private readonly HttpRequestFactory _httpRequestFactory = new HttpRequestFactory();
        private const int MaxRetries = 3;
        private const int RetryDelaySeconds = 5;
        private static readonly string RevPageSize = Environment.GetEnvironmentVariable("RevPageSize");
        private int PageSize = !string.IsNullOrEmpty(RevPageSize) ? int.Parse(RevPageSize) : 0;
        private const int MAX_NUMBER_OF_PAGES_IN_A_BATCH = 20;
        private const int DEFAULT_PAGE_NUMBER = 1;
        private DailySyncAmopApiTrigger dailySyncAmopApiTrigger = new DailySyncAmopApiTrigger();
        public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            KeySysLambdaContext keysysContext = null;
            try
            {
                keysysContext = base.BaseFunctionHandler(context);
                if (string.IsNullOrWhiteSpace(RevGetCustomersUrl))
                {
                    RevCustomerSyncQueueURL = context.ClientContext.Environment["RevCustomerSyncQueueUrl"];
                    RevGetCustomersUrl = context.ClientContext.Environment["RevGetCustomersUrl"];
                    RevGetServiceTypesUrl = context.ClientContext.Environment["RevGetServiceTypesUrl"];
                    RevGetServicesUrl = context.ClientContext.Environment["RevGetServicesUrl"];
                    RevGetProductTypesUrl = context.ClientContext.Environment["RevGetProductTypesUrl"];
                    RevGetProductsUrl = context.ClientContext.Environment["RevGetProductsUrl"];
                    RevGetServiceProductsUrl = context.ClientContext.Environment["RevGetServiceProductsUrl"];
                    RevGetProvidersUrl = context.ClientContext.Environment["RevGetProvidersUrl"];
                    RevGetBillProfilesUrl = context.ClientContext.Environment["RevGetBillProfilesUrl"];
                    RevGetInventoryTypesUrl = context.ClientContext.Environment["RevGetInventoryTypesUrl"];
                    RevGetInventoryItemsUrl = context.ClientContext.Environment["RevGetInventoryItemsUrl"];
                    RevGetUsagePlanGroupsUrl = context.ClientContext.Environment["RevGetUsagePlanGroupsUrl"];
                    RevGetAgentUrl = context.ClientContext.Environment["RevGetAgentUrl"];
                    RevGetPackagesUrl = context.ClientContext.Environment["RevGetPackagesUrl"];
                    RevGetPackageProductsUrl = context.ClientContext.Environment["RevGetPackageProductsUrl"];
                    PageSize = int.Parse(context.ClientContext.Environment["RevPageSize"]);
                }

                int currentIntegrationAuthenticationId;
                bool isLastStepSync = false;
                RevSyncStep currentRevSyncStep = RevSyncStep.None;
                int pageNumber = DEFAULT_PAGE_NUMBER;
                if (sqsEvent?.Records != null)
                {
                    var currentRecord = sqsEvent.Records.First();
                    currentIntegrationAuthenticationId = GetCurrentIntegrationAuthenticationId(keysysContext, currentRecord);
                    currentRevSyncStep = GetCurrentRevSyncStep(keysysContext, currentRecord);
                    pageNumber = GetPageNumber(keysysContext, currentRecord);
                    isLastStepSync = GetIsLastStepSyncNumber(keysysContext, currentRecord);

                    // Is merge staging 
                    if (isLastStepSync)
                    {
                        // load Rev from staging table and clear staging
                        ProcessLoadRevFromStaging(keysysContext, currentRevSyncStep, currentIntegrationAuthenticationId);
                    }
                    else
                    {
                        await ProcessEventAsync(keysysContext, currentIntegrationAuthenticationId, currentRevSyncStep, pageNumber);
                    }
                }
                else
                {
                    currentIntegrationAuthenticationId = 0;
                    try
                    {
                        bool proceed = true;
                        if (currentIntegrationAuthenticationId == 0) /* 0: initial value*/
                        {
                            var revIoAuthRepo = new RevIoAuthenticationRepository(keysysContext.logger, keysysContext.Base64Service, keysysContext.CentralDbConnectionString);
                            var authId = revIoAuthRepo.GetNextRevIoAuthenticationId(currentIntegrationAuthenticationId);
                            switch (authId)
                            {
                                case 0: /* Exception */
                                    LogInfo(keysysContext, "WARNING", $"Error Getting an auth id for Rev.IO CurrentIntegrationAuthenticationId: {currentIntegrationAuthenticationId}");
                                    proceed = false;
                                    break;
                                case -1:
                                    LogInfo(keysysContext, "WARNING", "No Authentication record was found for Rev.IO.");
                                    proceed = false;
                                    break;
                                default:
                                    currentIntegrationAuthenticationId = authId;
                                    currentRevSyncStep = RevSyncStep.Customer;
                                    break;
                            }
                        }

                        if (proceed)
                        {
                            LogInfo(keysysContext, "CurrentIntegrationAuthenticationId", currentIntegrationAuthenticationId);
                            LogInfo(keysysContext, "CurrentRevSyncStep", currentRevSyncStep);
                            var sqlRetryPolicy = GetSqlRetryPolicy(keysysContext);

                            sqlRetryPolicy.Execute(() => ClearStagingTables(keysysContext));
                            await ProcessEventAsync(keysysContext, currentIntegrationAuthenticationId, currentRevSyncStep, DEFAULT_PAGE_NUMBER);

                        }
                    }
                    catch (Exception ex)
                    {
                        LogInfo(keysysContext, "EXCEPTION", ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                LogInfo(keysysContext, "EXCEPTION", ex.Message + " " + ex.Source + " " + ex.StackTrace);
            }
            CleanUp(keysysContext);
        }

        private bool GetIsLastStepSyncNumber(KeySysLambdaContext context, SQSEvent.SQSMessage message)
        {
            var isLastStepSync = false;
            if (message.MessageAttributes.ContainsKey("IsLastStepSync"))
            {
                isLastStepSync = bool.Parse(message.MessageAttributes["IsLastStepSync"].StringValue);
                context.LogInfo("IsLastStepSync", isLastStepSync);
            }
            return isLastStepSync;
        }

        private int GetPageNumber(KeySysLambdaContext context, SQSEvent.SQSMessage message)
        {
            var pageNumber = 1;
            if (message.MessageAttributes.ContainsKey("CurrentPageNumber"))
            {
                pageNumber = int.Parse(message.MessageAttributes["CurrentPageNumber"].StringValue);
                context.LogInfo("CurrentPageNumber", pageNumber);
            }
            return pageNumber;
        }
        private static int GetCurrentIntegrationAuthenticationId(KeySysLambdaContext context, SQSEvent.SQSMessage message)
        {
            int integrationAuthenticationId = -1;
            if (message.MessageAttributes.ContainsKey("CurrentIntegrationAuthenticationId"))
            {
                integrationAuthenticationId = int.Parse(message.MessageAttributes["CurrentIntegrationAuthenticationId"].StringValue);
            }

            LogInfo(context, "CurrentIntegrationAuthenticationId", integrationAuthenticationId);
            return integrationAuthenticationId;
        }

        private static RevSyncStep GetCurrentRevSyncStep(KeySysLambdaContext context, SQSEvent.SQSMessage message)
        {
            RevSyncStep revSyncStep = RevSyncStep.None;
            if (message.MessageAttributes.ContainsKey("CurrentRevSyncStep"))
            {
                revSyncStep = (RevSyncStep)int.Parse(message.MessageAttributes["CurrentRevSyncStep"].StringValue);
            }

            LogInfo(context, "CurrentRevSyncStep", revSyncStep);
            return revSyncStep;
        }

        private async Task ProcessEventAsync(KeySysLambdaContext context, int currentIntegrationAuthenticationId, RevSyncStep currentRevSyncStep, int pageNumber)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({currentRevSyncStep})");
            var revIOAuthenticationRepository = new RevioAuthenticationRepository(context.CentralDbConnectionString, context.Base64Service);

            var revIOAuthentication = revIOAuthenticationRepository.GetRevioApiAuthentication(currentIntegrationAuthenticationId);
            var revApiClient = new RevioApiClient(new SingletonHttpClientFactory(), _httpRequestFactory, revIOAuthentication,
                context.IsProduction);
            if (revIOAuthentication != null)
            {
                RevSyncStep nextRevSyncStep = RevSyncStep.Default;
                try
                {
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
                    }
                }
                catch (Exception ex)
                {
                    LogInfo(context, LogTypeConstant.Exception, "Error Syncing: " + ex.Message + " " + ex.Source);
                    nextRevSyncStep = RevSyncStep.None;
                }

                //run in case have an exception
                if (nextRevSyncStep == RevSyncStep.None)
                {
                    //queue next instance
                    await QueueNextRevInstanceAsync(context, currentIntegrationAuthenticationId);
                }
            }
            else
            {
                LogInfo(context, LogTypeConstant.Error, "No Rev.IO Auth Info");
                await QueueNextRevInstanceAsync(context, currentIntegrationAuthenticationId);
            }
        }

        private void ProcessLoadRevFromStaging(KeySysLambdaContext context, RevSyncStep currentRevSyncStep, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({currentRevSyncStep})");

            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            try
            {
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
                        DeleteRevInventoryTypeStagingData(context, currentIntegrationAuthenticationId);
                        break;
                    case RevSyncStep.InventoryItem:
                        sqlRetryPolicy.Execute(() => LoadRevInventoryItemsFromStagingTable(context, currentIntegrationAuthenticationId));
                        DeleteRevInventoryItemStagingData(context, currentIntegrationAuthenticationId);
                        break;
                    case RevSyncStep.UsagePlanGroup:
                        sqlRetryPolicy.Execute(() => LoadRevUsagePlanGroupsFromStagingTable(context, currentIntegrationAuthenticationId));
                        DeleteRevUsagePlanGroupStagingData(context, currentIntegrationAuthenticationId);
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
                }
            }
            catch (Exception ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"Load Rev ({currentRevSyncStep}) From Staging" + ex.Message + " " + ex.Source);
            }
        }
        public async Task BuildMessageToQueue(KeySysLambdaContext context, int currentIntegrationAuthenticationId, RevSyncStep revSyncStep, int pageNumber)
        {
            if (revSyncStep == RevSyncStep.None)
            {
                //queue next instance
                await QueueNextRevInstanceAsync(context, currentIntegrationAuthenticationId);
            }
            else
            {
                // queue next step
                await SendMessageToQueueAsync(context, RevCustomerSyncQueueURL, currentIntegrationAuthenticationId, revSyncStep, pageNumber);
            }
        }

        private async Task QueueNextRevInstanceAsync(KeySysLambdaContext context, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "QueueNextRevInstanceAsync()");
            // get next rev.io instance
            var revIoAuthRepo = new RevIoAuthenticationRepository(context.logger, context.Base64Service, context.CentralDbConnectionString);
            var nextAuthId = revIoAuthRepo.GetNextRevIoAuthenticationId(currentIntegrationAuthenticationId);
            if (nextAuthId > 0)
            {
                //trigger will be sent to 2.0 only for the last instance for each tenant of the sync
                var tenantId = await GetTenantIdByIntegrationAuthenticationId(context, currentIntegrationAuthenticationId);
                dailySyncAmopApiTrigger.SendNotificationToAmop20(context, context.Context, "rev_customers_sync", tenantId, null);

                currentIntegrationAuthenticationId = nextAuthId;
                await SendMessageToQueueAsync(context, RevCustomerSyncQueueURL, currentIntegrationAuthenticationId, RevSyncStep.Customer, DEFAULT_PAGE_NUMBER);
            }
            //else
            //{
            //    var tenantId = await GetTenantIdByIntegrationAuthenticationId(context, currentIntegrationAuthenticationId);
            //    //trigger will be sent to 2.0 only for the last instance of the sync
            //    dailySyncAmopApiTrigger.SendNotificationToAmop20(context, context.Context, "rev_customers_sync");
            //}
        }

        private async Task SyncRevCustomersAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            await CustomersAsync(context, sqlRetryPolicy, revApiClient, currentIntegrationAuthenticationId, pageNumber);
        }

        private async Task SyncRevBillProfilesAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            await GetBillProfilesAsync(context, sqlRetryPolicy, revApiClient, currentIntegrationAuthenticationId, pageNumber);
        }

        private async Task SyncRevProvidersAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            await GetProvidersAsync(context, sqlRetryPolicy, revApiClient, currentIntegrationAuthenticationId, pageNumber);
        }

        private async Task SyncRevProductTypesAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            await GetProductTypesAsync(context, sqlRetryPolicy, revApiClient, currentIntegrationAuthenticationId, pageNumber);
        }

        private async Task SyncRevProductsAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            await GetProductsAsync(context, sqlRetryPolicy, revApiClient, currentIntegrationAuthenticationId, pageNumber);
        }

        private async Task SyncRevServiceTypesAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            await RevServiceTypesAsync(context, sqlRetryPolicy, revApiClient, currentIntegrationAuthenticationId, pageNumber);
        }
        private async Task SyncRevServicesAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            await GetServicesAsync(context, sqlRetryPolicy, revApiClient, currentIntegrationAuthenticationId, pageNumber);
        }

        private async Task SyncRevServiceProductsAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            await GetServiceProductsAsync(context, sqlRetryPolicy, revApiClient, currentIntegrationAuthenticationId, pageNumber);
        }

        private async Task SyncRevInventoryTypesAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            await GetInventoryTypesAsync(context, sqlRetryPolicy, revApiClient, currentIntegrationAuthenticationId, pageNumber);
        }

        private async Task SyncRevInventoryItemsAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            await GetInventoryItemsAsync(context, sqlRetryPolicy, revApiClient, currentIntegrationAuthenticationId, pageNumber);
        }

        private async Task SyncRevUsagePlanAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            await GetUsagePlanGroupsAsync(context, sqlRetryPolicy, revApiClient, currentIntegrationAuthenticationId, pageNumber);
        }

        private async Task SyncRevAgentAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            await GetAgentAsync(context, sqlRetryPolicy, revApiClient, currentIntegrationAuthenticationId, pageNumber);
        }

        private async Task SyncRevPackagesAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            await RevPackagesAsync(context, sqlRetryPolicy, revApiClient, currentIntegrationAuthenticationId, pageNumber);
        }

        private async Task SyncRevPackageProductsAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            await RevPackageProductsAsync(context, sqlRetryPolicy, revApiClient, currentIntegrationAuthenticationId, pageNumber);
        }


        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "EF1001:Internal EF Core API usage.", Justification = "Despite the warning, referring to the package version for the time being is better than the alternative of copying and maintaining the code, license, etc.")]
        private static RetryPolicy GetSqlRetryPolicy(KeySysLambdaContext context)
        {
            var sqlTransientRetryPolicy = Policy
                .Handle<SqlException>(SqlServerTransientExceptionDetector.ShouldRetryOn)
                .Or<TimeoutException>()
                .WaitAndRetry(MaxRetries,
                    retryAttempt => TimeSpan.FromSeconds(RetryDelaySeconds),
                    (exception, timeSpan, retryCount, sqlContext) => LogInfo(context, "STATUS",
                        $"Encountered transient SQL error - delaying for {timeSpan.TotalMilliseconds}ms, then making retry {retryCount}. Exception: {exception?.Message}"));
            return sqlTransientRetryPolicy;
        }

        private void LoadRevBillProfilesToStaging(KeySysLambdaContext context, List<RevBillProfile> billProfileList, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevBillProfilesToStaging()");

            DataTable table = new DataTable();

            table.Columns.Add("revBillProfileStagingId");
            table.Columns.Add("BillProfileId");
            table.Columns.Add("Description");
            table.Columns.Add("IsActive");
            table.Columns.Add("IntegrationAuthenticationId");

            if (billProfileList.Count > 0)
            {
                foreach (var billProfile in billProfileList.GroupBy(x => x.bill_profile_id))
                {
                    DataRow dr = AddToDataRow(table, billProfile.First(), currentIntegrationAuthenticationId);
                    table.Rows.Add(dr);
                }
            }

            SqlBulkCopy(context, context.CentralDbConnectionString, table, "RevBillProfileStaging");
        }

        private static DataRow AddToDataRow(DataTable table, RevBillProfile billProfile, int integrationAuthenticationId)
        {
            var dr = table.NewRow();
            dr[1] = billProfile.bill_profile_id;
            dr[2] = billProfile.description;
            dr[3] = billProfile.active;
            dr[4] = integrationAuthenticationId;
            return dr;
        }

        private void LoadRevProvidersToStaging(KeySysLambdaContext context, List<RevProvider> providerList, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevProvidersToStaging()");

            DataTable table = new DataTable();

            table.Columns.Add("revProviderStagingId");
            table.Columns.Add("ProviderId");
            table.Columns.Add("Description");
            table.Columns.Add("ProviderCode");
            table.Columns.Add("IsActive");
            table.Columns.Add("DateCreated");
            table.Columns.Add("BillProfileId");
            table.Columns.Add("HasCnamOrderType");
            table.Columns.Add("HasConversionOrderType");
            table.Columns.Add("HasDenyOrderType");
            table.Columns.Add("HasDisconnectOrderType");
            table.Columns.Add("HasE911OrderType");
            table.Columns.Add("HasLongDistanceBlockOrderType");
            table.Columns.Add("HasPortOrderType");
            table.Columns.Add("HasRestoreOrderType");
            table.Columns.Add("HasTransferOrderType");
            table.Columns.Add("IntegrationAuthenticationId");

            if (providerList.Count > 0)
            {
                foreach (var provider in providerList.GroupBy(x => x.provider_id))
                {
                    DataRow dr = AddToDataRow(table, provider.First(), currentIntegrationAuthenticationId);
                    table.Rows.Add(dr);
                }
            }

            SqlBulkCopy(context, context.CentralDbConnectionString, table, "RevProviderStaging");
        }

        private static DataRow AddToDataRow(DataTable table, RevProvider provider, int integrationAuthenticationId)
        {
            var dr = table.NewRow();
            dr[1] = provider.provider_id;
            dr[2] = provider.description;
            dr[3] = provider.provider_code;
            dr[4] = provider.active;
            dr[5] = provider.created_date;
            dr[6] = provider.bill_profile_id;
            if (provider.order_types != null)
            {
                dr[7] = provider.order_types.cnam;
                dr[8] = provider.order_types.conversion;
                dr[9] = provider.order_types.deny;
                dr[10] = provider.order_types.disconnect;
                dr[11] = provider.order_types.e911;
                dr[12] = provider.order_types.ld_block;
                dr[13] = provider.order_types.port;
                dr[14] = provider.order_types.restore;
                dr[15] = provider.order_types.transfer;
            }
            else
            {
                dr[7] = false;
                dr[8] = false;
                dr[9] = false;
                dr[10] = false;
                dr[11] = false;
                dr[12] = false;
                dr[13] = false;
                dr[14] = false;
                dr[15] = false;
            }

            dr[16] = integrationAuthenticationId;
            return dr;
        }

        private DataRow AddToDataRow(DataTable table, RevServiceType serviceType, int integrationAuthenticationId)
        {
            var dr = table.NewRow();
            dr[1] = serviceType.service_type_id;
            dr[2] = serviceType.description;
            dr[3] = serviceType.active;
            dr[4] = integrationAuthenticationId;
            return dr;
        }

        private void LoadRevServiceProductsToStaging(KeySysLambdaContext context, List<RevServiceProduct> serviceProductList, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevServiceProductsToStaging()");

            DataTable table = new DataTable();

            table.Columns.Add("revServiceProductStagingId");
            table.Columns.Add("ServiceProductId");
            table.Columns.Add("CustomerId");
            table.Columns.Add("ProductId");
            table.Columns.Add("PackageId");
            table.Columns.Add("ServiceId");
            table.Columns.Add("Description");
            table.Columns.Add("Code1");
            table.Columns.Add("Code2");
            table.Columns.Add("Rate");
            table.Columns.Add("BilledThroughDate");
            table.Columns.Add("CanceledDate");
            table.Columns.Add("Status");
            table.Columns.Add("StatusDate");
            table.Columns.Add("StatusUserId");
            table.Columns.Add("ActivatedDate");
            table.Columns.Add("Cost");
            table.Columns.Add("WholesaleDescription");
            table.Columns.Add("FreeStartDate");
            table.Columns.Add("FreeEndDate");
            table.Columns.Add("Quantity");
            table.Columns.Add("ContractStartDate");
            table.Columns.Add("ContractEndDate");
            table.Columns.Add("CreatedDate");
            table.Columns.Add("TaxIncluded");
            table.Columns.Add("GroupOnBill");
            table.Columns.Add("Itemized");
            table.Columns.Add("IntegrationAuthenticationId");

            if (serviceProductList.Count > 0)
            {
                foreach (var serviceProduct in serviceProductList)
                {
                    var dr = AddToDataRow(table, serviceProduct, currentIntegrationAuthenticationId);
                    table.Rows.Add(dr);
                }
            }

            if (table.Rows.Count > 0)
            {
                SqlBulkCopy(context, context.CentralDbConnectionString, table, "RevServiceProductStaging");
            }
        }

        private void LoadRevProductsToStaging(KeySysLambdaContext context, List<RevProduct> productList, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevProductsToStaging()");

            DataTable table = new DataTable();

            table.Columns.Add("revProductStagingId");
            table.Columns.Add("ProductId");
            table.Columns.Add("ProductTypeId");
            table.Columns.Add("Description");
            table.Columns.Add("Code1");
            table.Columns.Add("Code2");
            table.Columns.Add("Rate");
            table.Columns.Add("Cost");
            table.Columns.Add("BuyRate");
            table.Columns.Add("CreatedDate");
            table.Columns.Add("CreatedBy");
            table.Columns.Add("Active");
            table.Columns.Add("CreatesOrder");
            table.Columns.Add("ProviderId");
            table.Columns.Add("BillsInArrears");
            table.Columns.Add("Prorates");
            table.Columns.Add("CustomerClass");
            table.Columns.Add("LongDescription");
            table.Columns.Add("LedgerCode");
            table.Columns.Add("FreeMonths");
            table.Columns.Add("AutomaticExpirationMonths");
            table.Columns.Add("OrderCompletionBilling");
            table.Columns.Add("TaxClassId");
            table.Columns.Add("WholesaleDescription");
            table.Columns.Add("MSISDN");
            table.Columns.Add("ICCID");
            table.Columns.Add("IMEI");
            table.Columns.Add("IntegrationAuthenticationId");

            if (productList.Count > 0)
            {
                foreach (var product in productList)
                {
                    var dr = AddToDataRow(table, product, currentIntegrationAuthenticationId);
                    table.Rows.Add(dr);
                }
            }

            if (table.Rows.Count > 0)
            {
                SqlBulkCopy(context, context.CentralDbConnectionString, table, "RevProductStaging");
            }
        }

        private void LoadRevInventoryTypesToStaging(KeySysLambdaContext context, List<RevInventoryType> inventoryTypeList, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevInventoryTypesToStaging()");

            DataTable table = new DataTable();

            table.Columns.Add("id");
            table.Columns.Add("InventoryTypeId");
            table.Columns.Add("Category");
            table.Columns.Add("Identifier");
            table.Columns.Add("Name");
            table.Columns.Add("RequiresProduct");
            table.Columns.Add("Description");
            table.Columns.Add("Status");
            table.Columns.Add("Format");
            table.Columns.Add("Ratable");
            table.Columns.Add("IntegrationAuthenticationId");

            if (inventoryTypeList.Count > 0)
            {
                foreach (var inventoryType in inventoryTypeList)
                {
                    var dr = AddToDataRow(table, inventoryType, currentIntegrationAuthenticationId);
                    table.Rows.Add(dr);
                }
            }

            if (table.Rows.Count > 0)
            {
                SqlBulkCopy(context, context.CentralDbConnectionString, table, "RevInventoryTypeStaging");
            }
        }

        private void LoadRevInventoryItemsToStaging(KeySysLambdaContext context, List<RevInventoryItem> inventoryItemList, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevInventoryItemsToStaging()");

            DataTable table = new DataTable();

            table.Columns.Add("id");
            table.Columns.Add("InventoryItemId");
            table.Columns.Add("InventoryTypeId");
            table.Columns.Add("CustomerId");
            table.Columns.Add("Identifier");
            table.Columns.Add("RevCreatedBy");
            table.Columns.Add("RevCreatedDate");
            table.Columns.Add("Status");
            table.Columns.Add("InventoryItemUnavailableReasonId");
            table.Columns.Add("IntegrationAuthenticationId");

            if (inventoryItemList.Count > 0)
            {
                foreach (var inventoryItem in inventoryItemList)
                {
                    var dr = AddToDataRow(table, inventoryItem, currentIntegrationAuthenticationId);
                    table.Rows.Add(dr);
                }
            }

            if (table.Rows.Count > 0)
            {
                SqlBulkCopy(context, context.CentralDbConnectionString, table, "RevInventoryItemStaging");
            }
        }

        private void LoadRevUsagePlanGroupsToStaging(KeySysLambdaContext context, List<RevUsagePlanGroup> usagePlanGroupList, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevUsagePlanGroupsToStaging()");

            DataTable table = new DataTable();

            table.Columns.Add("id");
            table.Columns.Add("UsagePlanGroupId");
            table.Columns.Add("Description");
            table.Columns.Add("LongDescription");
            table.Columns.Add("RevActive");
            table.Columns.Add("RevCreatedBy");
            table.Columns.Add("RevCreatedDate");
            table.Columns.Add("IntegrationAuthenticationId");

            if (usagePlanGroupList.Count > 0)
            {
                foreach (var usagePlanGroup in usagePlanGroupList)
                {
                    var dr = AddToDataRow(table, usagePlanGroup, currentIntegrationAuthenticationId);
                    table.Rows.Add(dr);
                }
            }

            if (table.Rows.Count > 0)
            {
                SqlBulkCopy(context, context.CentralDbConnectionString, table, "RevUsagePlanGroupStaging");
            }
        }

        private void LoadRevAgentsToStaging(KeySysLambdaContext context, List<RevAgent> revAgentList, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevAgentsToStaging()");

            DataTable table = new DataTable();

            table.Columns.Add("RevAgentId");
            table.Columns.Add("AgentName");
            table.Columns.Add("ParentAgentId");
            table.Columns.Add("Status");
            table.Columns.Add("IntegrationAuthenticationId");

            if (revAgentList.Count > 0)
            {
                foreach (var revAgent in revAgentList)
                {
                    var dr = AddToDataRow(table, revAgent, currentIntegrationAuthenticationId);
                    table.Rows.Add(dr);
                }
            }

            if (table.Rows.Count > 0)
            {
                SqlBulkCopy(context, context.CentralDbConnectionString, table, "RevAgentStaging");
            }
        }

        private static DataRow AddToDataRow(DataTable table, RevAgent revAgent, int integrationAuthenticationId)
        {
            var dr = table.NewRow();
            dr[0] = revAgent.AgentId;
            dr[1] = revAgent.AgentName;
            dr[2] = revAgent.ParentAgentId;
            dr[3] = revAgent.Status;
            dr[4] = integrationAuthenticationId;
            return dr;
        }

        private static void ClearStagingTables(KeySysLambdaContext context)
        {
            LogInfo(context, LogTypeConstant.Sub, "");

            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand(StoredProcedureName.ClearStagingTables, conn))
                {
                    cmd.CommandTimeout = AMOPSQLConstant.ShortTimeoutSeconds;
                    cmd.CommandType = CommandType.StoredProcedure;
                    conn.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static void ClearStagingTablesV2(KeySysLambdaContext context, string tableName, int integrationAuthenticationId)
        {
            LogInfo(context, "SUB", "ClearStagingTables()");

            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand($"DELETE FROM {tableName} WHERE IntegrationAuthenticationId = @IntegrationAuthenticationId", conn))
                {
                    cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", integrationAuthenticationId);
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
                    conn.Open();

                    cmd.ExecuteNonQuery();
                    conn.Close();
                }
            }
        }

        private void LoadRevProductTypesToStaging(KeySysLambdaContext context, List<RevProductType> productTypeList, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevProductTypesToStaging()");

            DataTable table = new DataTable();

            table.Columns.Add("revProductTypeStagingId");
            table.Columns.Add("productTypeId");
            table.Columns.Add("productTypeCode");
            table.Columns.Add("description");
            table.Columns.Add("taxClassId");
            table.Columns.Add("IsActive");
            table.Columns.Add("IntegrationAuthenticationId");

            if (productTypeList.Count > 0)
            {
                foreach (var productType in productTypeList)
                {
                    var dr = AddToDataRow(table, productType, currentIntegrationAuthenticationId);
                    table.Rows.Add(dr);
                }
            }

            if (table.Rows.Count > 0)
            {
                SqlBulkCopy(context, context.CentralDbConnectionString, table, "RevProductTypeStaging");
            }
        }

        private void LoadRevServiceTypesToStaging(KeySysLambdaContext context, List<RevServiceType> serviceTypeList, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevServiceTypesToStaging()");

            DataTable table = new DataTable();

            table.Columns.Add("revServiceTypeStagingId");
            table.Columns.Add("serviceTypeId");
            table.Columns.Add("description");
            table.Columns.Add("IsActive");
            table.Columns.Add("IntegrationAuthenticationId");

            if (serviceTypeList.Count > 0)
            {
                foreach (var serviceType in serviceTypeList)
                {
                    var dr = AddToDataRow(table, serviceType, currentIntegrationAuthenticationId);
                    table.Rows.Add(dr);
                }
            }

            if (table.Rows.Count > 0)
            {
                SqlBulkCopy(context, context.CentralDbConnectionString, table, "RevServiceTypeStaging");
            }
        }

        private void LoadRevPackagesToStaging(KeySysLambdaContext context, List<RevPackage> packageList, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({currentIntegrationAuthenticationId})");

            DataTable table = new DataTable();

            table.Columns.Add(RevPackageStagingColumnNames.RevPackageStagingId);
            table.Columns.Add(RevPackageStagingColumnNames.PackageId);
            table.Columns.Add(RevPackageStagingColumnNames.ProviderId);
            table.Columns.Add(RevPackageStagingColumnNames.CurrencyCode);
            table.Columns.Add(RevPackageStagingColumnNames.Description);
            table.Columns.Add(RevPackageStagingColumnNames.DescriptionOnBill);
            table.Columns.Add(RevPackageStagingColumnNames.LongDescription);
            table.Columns.Add(RevPackageStagingColumnNames.CreatedDate);
            table.Columns.Add(RevPackageStagingColumnNames.CreatedBy);
            table.Columns.Add(RevPackageStagingColumnNames.Active);
            table.Columns.Add(RevPackageStagingColumnNames.UsagePlanGroupId);
            table.Columns.Add(RevPackageStagingColumnNames.ServiceTypeId);
            table.Columns.Add(RevPackageStagingColumnNames.PackageCategoryId);
            table.Columns.Add(RevPackageStagingColumnNames.ExemptFromSpiffCommission);
            table.Columns.Add(RevPackageStagingColumnNames.RestrictClassFlag);
            table.Columns.Add(RevPackageStagingColumnNames.Class);
            table.Columns.Add(RevPackageStagingColumnNames.RestrictBillProfileFlag);
            table.Columns.Add(RevPackageStagingColumnNames.BillProfileId);
            table.Columns.Add(RevPackageStagingColumnNames.IntegrationAuthenticationId);

            if (packageList.Count > 0)
            {
                foreach (var package in packageList)
                {
                    var dataRow = AddToDataRow(table, package, currentIntegrationAuthenticationId);
                    table.Rows.Add(dataRow);
                }
            }

            if (table.Rows.Count > 0)
            {
                SqlBulkCopy(context, context.CentralDbConnectionString, table, DatabaseTableNames.RevPackageStaging);
            }
        }

        private void LoadRevPackageProductsToStaging(KeySysLambdaContext context, List<RevPackageProduct> packageProductList, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({currentIntegrationAuthenticationId})");

            DataTable table = new DataTable();

            table.Columns.Add(RevPackageProductStagingColumnNames.RevPackageProductStagingId);
            table.Columns.Add(RevPackageProductStagingColumnNames.PackageProductId);
            table.Columns.Add(RevPackageProductStagingColumnNames.ProductId);
            table.Columns.Add(RevPackageProductStagingColumnNames.PackageId);
            table.Columns.Add(RevPackageProductStagingColumnNames.Description);
            table.Columns.Add(RevPackageProductStagingColumnNames.PrimaryProvisioningCode);
            table.Columns.Add(RevPackageProductStagingColumnNames.SecondaryProvisioningCode);
            table.Columns.Add(RevPackageProductStagingColumnNames.Rate);
            table.Columns.Add(RevPackageProductStagingColumnNames.Cost);
            table.Columns.Add(RevPackageProductStagingColumnNames.BuyRate);
            table.Columns.Add(RevPackageProductStagingColumnNames.Quantity);
            table.Columns.Add(RevPackageProductStagingColumnNames.TaxIncluded);
            table.Columns.Add(RevPackageProductStagingColumnNames.GroupOnBill);
            table.Columns.Add(RevPackageProductStagingColumnNames.Itemized);
            table.Columns.Add(RevPackageProductStagingColumnNames.Credit);
            table.Columns.Add(RevPackageProductStagingColumnNames.IntegrationAuthenticationId);

            if (packageProductList.Count > 0)
            {
                foreach (var packageProduct in packageProductList)
                {
                    var dataRow = AddToDataRow(table, packageProduct, currentIntegrationAuthenticationId);
                    table.Rows.Add(dataRow);
                }
            }

            if (table.Rows.Count > 0)
            {
                SqlBulkCopy(context, context.CentralDbConnectionString, table, DatabaseTableNames.RevPackageProductStaging);
            }
        }

        private void LoadRevServicesToStaging(KeySysLambdaContext context, List<RevService> serviceList, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevServicesToStaging()");

            DataTable table = new DataTable();
            table.Columns.Add("RevCustomerId");
            table.Columns.Add("Number");
            table.Columns.Add("RevServiceId");
            table.Columns.Add("RatePlanCode");
            table.Columns.Add("ServiceTypeId");
            table.Columns.Add("ActivatedDate");
            table.Columns.Add("DisconnectedDate");
            table.Columns.Add("IntegrationAuthenticationId");
            table.Columns.Add("RevProviderId");
            table.Columns.Add("Description");
            if (serviceList.Count > 0)
            {

                foreach (var service in serviceList)
                {
                    var dr = AddToDataRow(table, service, currentIntegrationAuthenticationId);
                    table.Rows.Add(dr);
                }
            }

            if (table.Rows.Count > 0)
            {
                SqlBulkCopy(context, context.CentralDbConnectionString, table, "RevServiceStaging");
            }
        }

        private void LoadRevCustomersToStaging(KeySysLambdaContext context, List<RevCustomer> customerList, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevCustomersToStaging()");

            var table = new RevCustomerSyncTable();

            if (customerList.Count > 0)
            {
                foreach (var customer in customerList.GroupBy(x => x.customer_id))
                {
                    table.AddCustomerRow(customer.First(), currentIntegrationAuthenticationId);
                }
            }

            SqlBulkCopy(context, context.CentralDbConnectionString, table.DataTable, "RevCustomerStaging");
        }

        private static DataRow AddToDataRow(DataTable table, RevServiceProduct serviceProduct, int currentIntegrationAuthenticationId)
        {
            var dr = table.NewRow();

            dr[1] = serviceProduct.ServiceProductId;
            dr[2] = serviceProduct.CustomerId;
            dr[3] = serviceProduct.ProductId;
            dr[4] = serviceProduct.PackageId;
            dr[5] = serviceProduct.ServiceId;
            dr[6] = serviceProduct.Description;
            dr[7] = serviceProduct.Code1;
            dr[8] = serviceProduct.Code2;
            dr[9] = serviceProduct.Rate;
            dr[10] = (serviceProduct.BilledThroughDate <= (DateTime)SqlDateTime.MinValue) ? null : serviceProduct.BilledThroughDate;
            dr[11] = (serviceProduct.CanceledDate <= (DateTime)SqlDateTime.MinValue) ? null : serviceProduct.CanceledDate;
            dr[12] = serviceProduct.Status;
            dr[13] = (serviceProduct.StatusDate <= (DateTime)SqlDateTime.MinValue) ? null : serviceProduct.StatusDate;
            dr[14] = serviceProduct.StatusUserId;
            dr[15] = (serviceProduct.ActivatedDate <= (DateTime)SqlDateTime.MinValue) ? null : serviceProduct.ActivatedDate;
            dr[16] = serviceProduct.Cost;
            dr[17] = serviceProduct.WholesaleDescription;
            dr[18] = (serviceProduct.FreeStartDate <= (DateTime)SqlDateTime.MinValue) ? null : serviceProduct.FreeStartDate;
            dr[19] = (serviceProduct.FreeEndDate <= (DateTime)SqlDateTime.MinValue) ? null : serviceProduct.FreeEndDate;
            dr[20] = serviceProduct.Quantity;
            dr[21] = (serviceProduct.ContractStartDate <= (DateTime)SqlDateTime.MinValue) ? null : serviceProduct.ContractStartDate;
            dr[22] = (serviceProduct.ContractEndDate <= (DateTime)SqlDateTime.MinValue) ? null : serviceProduct.ContractEndDate;
            dr[23] = (serviceProduct.CreatedDate <= (DateTime)SqlDateTime.MinValue) ? null : serviceProduct.CreatedDate;
            dr[24] = serviceProduct.TaxIncluded;
            dr[25] = serviceProduct.GroupOnBill;
            dr[26] = serviceProduct.Itemized;
            dr[27] = currentIntegrationAuthenticationId;

            return dr;
        }

        private static DataRow AddToDataRow(DataTable table, RevPackage package, int currentIntegrationAuthenticationId)
        {
            var dataRow = table.NewRow();

            dataRow[RevPackageStagingColumnNames.PackageId] = package.PackageId;
            dataRow[RevPackageStagingColumnNames.ProviderId] = package.ProviderId;
            dataRow[RevPackageStagingColumnNames.CurrencyCode] = package.CurrencyCode;
            dataRow[RevPackageStagingColumnNames.Description] = package.Description;
            dataRow[RevPackageStagingColumnNames.DescriptionOnBill] = package.DescriptionOnBill;
            dataRow[RevPackageStagingColumnNames.LongDescription] = package.LongDescription;
            dataRow[RevPackageStagingColumnNames.CreatedDate] = (package.CreatedDate <= (DateTime)SqlDateTime.MinValue) ? null : package.CreatedDate;
            dataRow[RevPackageStagingColumnNames.CreatedBy] = package.CreatedBy;
            dataRow[RevPackageStagingColumnNames.Active] = package.Active;
            dataRow[RevPackageStagingColumnNames.UsagePlanGroupId] = package.UsagePlanGroupId;
            dataRow[RevPackageStagingColumnNames.ServiceTypeId] = package.ServiceTypeId;
            dataRow[RevPackageStagingColumnNames.PackageCategoryId] = package.PackageCategoryId;
            dataRow[RevPackageStagingColumnNames.ExemptFromSpiffCommission] = package.ExemptFromSpiffCommission;
            dataRow[RevPackageStagingColumnNames.RestrictClassFlag] = package.RestrictClassFlag;
            dataRow[RevPackageStagingColumnNames.Class] = package.Class;
            dataRow[RevPackageStagingColumnNames.RestrictBillProfileFlag] = package.RestrictBillProfileFlag;
            dataRow[RevPackageStagingColumnNames.BillProfileId] = package.BillProfileId;
            dataRow[RevPackageStagingColumnNames.IntegrationAuthenticationId] = currentIntegrationAuthenticationId;

            return dataRow;
        }

        private static DataRow AddToDataRow(DataTable table, RevPackageProduct packageProduct, int currentIntegrationAuthenticationId)
        {
            var dataRow = table.NewRow();

            dataRow[RevPackageProductStagingColumnNames.PackageProductId] = packageProduct.PackageProductId;
            dataRow[RevPackageProductStagingColumnNames.ProductId] = packageProduct.ProductId;
            dataRow[RevPackageProductStagingColumnNames.PackageId] = packageProduct.PackageId;
            dataRow[RevPackageProductStagingColumnNames.Description] = packageProduct.Description;
            dataRow[RevPackageProductStagingColumnNames.PrimaryProvisioningCode] = packageProduct.PrimaryProvisioningCode;
            dataRow[RevPackageProductStagingColumnNames.SecondaryProvisioningCode] = packageProduct.SecondaryProvisioningCode;
            dataRow[RevPackageProductStagingColumnNames.Rate] = packageProduct.Rate;
            dataRow[RevPackageProductStagingColumnNames.Cost] = packageProduct.Cost;
            dataRow[RevPackageProductStagingColumnNames.BuyRate] = packageProduct.BuyRate;
            dataRow[RevPackageProductStagingColumnNames.Quantity] = packageProduct.Quantity;
            dataRow[RevPackageProductStagingColumnNames.TaxIncluded] = packageProduct.TaxIncluded;
            dataRow[RevPackageProductStagingColumnNames.GroupOnBill] = packageProduct.GroupOnBill;
            dataRow[RevPackageProductStagingColumnNames.Itemized] = packageProduct.Itemized;
            dataRow[RevPackageProductStagingColumnNames.Credit] = packageProduct.Credit;
            dataRow[RevPackageProductStagingColumnNames.IntegrationAuthenticationId] = currentIntegrationAuthenticationId;

            return dataRow;
        }

        private static DataRow AddToDataRow(DataTable table, RevProduct product, int currentIntegrationAuthenticationId)
        {
            string msisdn = product.Fields != null &&
                            product.Fields.Exists(x => x.Label == "MSISDN")
                            ? product.Fields.First(x => x.Label == "MSISDN").Value : null;

            string iccid = product.Fields != null &&
                            product.Fields.Exists(x => x.Label == "ICCID")
                            ? product.Fields.First(x => x.Label == "ICCID").Value : null;

            string imei = product.Fields != null &&
                            product.Fields.Exists(x => x.Label == "IMEI")
                            ? product.Fields.First(x => x.Label == "IMEI").Value : null;

            var dr = table.NewRow();

            dr[1] = product.ProductId;
            dr[2] = product.ProductTypeId;
            dr[3] = product.Description;
            dr[4] = product.Code1;
            dr[5] = product.Code2;
            dr[6] = product.Rate;
            dr[7] = product.Cost;
            dr[8] = product.BuyRate;
            dr[9] = product.CreatedDate;
            dr[10] = product.CreatedBy;
            dr[11] = product.Active;
            dr[12] = product.CreatesOrder;
            dr[13] = product.ProviderId;
            dr[14] = product.BillsInArrears;
            dr[15] = product.Prorates;
            dr[16] = product.CustomerClass;
            dr[17] = product.LongDescription;
            dr[18] = product.LedgerCode;
            dr[19] = product.FreeMonths;
            dr[20] = product.AutomaticExpirationMonths;
            dr[21] = product.OrderCompletionBilling;
            dr[22] = product.TaxClassId;
            dr[23] = product.WholesaleDescription;
            dr[24] = msisdn;
            dr[25] = iccid;
            dr[26] = imei;
            dr[27] = currentIntegrationAuthenticationId;

            return dr;
        }

        private static DataRow AddToDataRow(DataTable table, RevProductType productType, int currentIntegrationAuthenticationId)
        {
            var dr = table.NewRow();
            dr[1] = productType.product_type_id;
            dr[2] = productType.product_type_code;
            dr[3] = productType.description;
            dr[4] = productType.tax_class_id;
            dr[5] = productType.active;
            dr[6] = currentIntegrationAuthenticationId;

            return dr;
        }

        private static DataRow AddToDataRow(DataTable table, RevService service, int currentIntegrationAuthenticationId)
        {
            var ratePlanCodeField = service.Fields.FirstOrDefault(x => x.Label == "Rate Code");
            string ratePlanCode = ratePlanCodeField != null ? ratePlanCodeField.Value : string.Empty;
            var dr = table.NewRow();
            dr[0] = service.CustomerId;
            dr[1] = service.Number;
            dr[2] = service.ServiceId;
            dr[3] = ratePlanCode;
            dr[4] = service.ServiceTypeId;
            dr[5] = service.ActivatedDate;
            dr[6] = service.DisconnectedDate;
            dr[7] = currentIntegrationAuthenticationId;
            dr[8] = service.ProviderId;
            dr[9] = service.Description;
            return dr;
        }

        private static DataRow AddToDataRow(DataTable table, RevInventoryType inventoryType, int currentIntegrationAuthenticationId)
        {
            var dr = table.NewRow();
            dr[1] = inventoryType.InventoryTypeId;
            dr[2] = inventoryType.Category;
            dr[3] = inventoryType.Identifier;
            dr[4] = inventoryType.Name;
            dr[5] = inventoryType.RequiresProduct;
            dr[6] = inventoryType.Description;
            dr[7] = inventoryType.Status;
            dr[8] = inventoryType.Format;
            dr[9] = inventoryType.Ratable;
            dr[10] = currentIntegrationAuthenticationId;
            return dr;
        }

        private static DataRow AddToDataRow(DataTable table, RevInventoryItem inventoryItem, int currentIntegrationAuthenticationId)
        {
            var dr = table.NewRow();
            dr[1] = inventoryItem.InventoryItemId;
            dr[2] = inventoryItem.InventoryTypeId;
            dr[3] = inventoryItem.CustomerId;
            dr[4] = inventoryItem.Identifier;
            dr[5] = inventoryItem.CreatedBy;
            dr[6] = inventoryItem.CreatedDate;
            dr[7] = inventoryItem.Status;
            dr[8] = inventoryItem.InventoryItemUnavailableReasonId;
            dr[9] = currentIntegrationAuthenticationId;
            return dr;
        }

        private static DataRow AddToDataRow(DataTable table, RevUsagePlanGroup usagePlanGroup, int currentIntegrationAuthenticationId)
        {
            var dr = table.NewRow();
            dr[1] = usagePlanGroup.UsagePlanGroupId;
            dr[2] = usagePlanGroup.Description;
            dr[3] = usagePlanGroup.LongDescription;
            dr[4] = usagePlanGroup.Active;
            dr[5] = usagePlanGroup.RevCreatedBy;
            dr[6] = usagePlanGroup.RevCreatedDate;
            dr[7] = currentIntegrationAuthenticationId;
            return dr;
        }

        private async Task GetServicesAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");

            var services = new List<RevService>();
            (bool isLastPage, pageNumber) = await GetPagesFromRevIOListAPI(context, revApiClient, pageNumber, services, GetServicesAsync);

            sqlRetryPolicy.Execute(() => LoadRevServicesToStaging(context, services, currentIntegrationAuthenticationId));

            await CheckProcessOfSyncStep(context, sqlRetryPolicy, currentIntegrationAuthenticationId, RevSyncStep.Service, RevSyncStep.ServiceProduct, RevStagingTable.REV_SERVICE_STG, pageNumber, isLastPage);
        }

        private async Task<RevListResponse<RevService>> GetServicesAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({pageNumber})");

            return await revApiClient.ExecuteRevGetWithRetryAsync<RevListResponse<RevService>>(RevGetServicesUrl, PageSize, pageNumber, context.logger);
        }

        private async Task CustomersAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");
            List<RevCustomer> customers = new List<RevCustomer>();
            (bool isLastPage, pageNumber) = await GetPagesFromRevIOListAPI(context, revApiClient, pageNumber, customers, GetCustomersAsync);

            sqlRetryPolicy.Execute(() => LoadRevCustomersToStaging(context, customers, currentIntegrationAuthenticationId));

            await CheckProcessOfSyncStep(context, sqlRetryPolicy, currentIntegrationAuthenticationId, RevSyncStep.Customer, RevSyncStep.BillProfile, RevStagingTable.REV_CUSTOMER_STG, pageNumber, isLastPage);
        }

        private async Task CheckProcessOfSyncStep(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, int currentIntegrationAuthenticationId, RevSyncStep currentStep, RevSyncStep nextStep, string stgTable, int pageNumber, bool isLastPage)
        {
            if (!isLastPage)
            {
                //continue to queue current step
                //sqlRetryPolicy.Execute(() => ClearStagingTablesV2(context, stgTable));
                await BuildMessageToQueue(context, currentIntegrationAuthenticationId, currentStep, pageNumber);
            }
            else
            {
                // send message to this lambda to load rev from staging
                await BuildMessageLoadRevFromStagingToQueueAsync(context, RevCustomerSyncQueueURL, currentIntegrationAuthenticationId, currentStep);

                //queue next step
                await BuildMessageToQueue(context, currentIntegrationAuthenticationId, nextStep, DEFAULT_PAGE_NUMBER);
            }
        }

        private async Task<RevListResponse<RevCustomer>> GetCustomersAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({pageNumber})");

            return await revApiClient.ExecuteRevGetWithRetryAsync<RevListResponse<RevCustomer>>(RevGetCustomersUrl, PageSize, pageNumber, context.logger);
        }

        private async Task GetBillProfilesAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");

            List<RevBillProfile> billProfiles = new List<RevBillProfile>();
            (bool isLastPage, pageNumber) = await GetPagesFromRevIOListAPI(context, revApiClient, pageNumber, billProfiles, GetBillProfilesAsync);

            sqlRetryPolicy.Execute(() => LoadRevBillProfilesToStaging(context, billProfiles, currentIntegrationAuthenticationId));

            await CheckProcessOfSyncStep(context, sqlRetryPolicy, currentIntegrationAuthenticationId, RevSyncStep.BillProfile, RevSyncStep.Provider, RevStagingTable.REV_BILLING_PROFILE_STG, pageNumber, isLastPage);
        }

        private async Task<RevListResponse<RevBillProfile>> GetBillProfilesAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({pageNumber})");

            return await revApiClient.ExecuteRevGetWithRetryAsync<RevListResponse<RevBillProfile>>(RevGetBillProfilesUrl, PageSize, pageNumber, context.logger);
        }

        private async Task GetProvidersAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");

            List<RevProvider> providers = new List<RevProvider>();
            (bool isLastPage, pageNumber) = await GetPagesFromRevIOListAPI(context, revApiClient, pageNumber, providers, GetProvidersAsync);

            sqlRetryPolicy.Execute(() => LoadRevProvidersToStaging(context, providers, currentIntegrationAuthenticationId));

            await CheckProcessOfSyncStep(context, sqlRetryPolicy, currentIntegrationAuthenticationId, RevSyncStep.Provider, RevSyncStep.ProductType, RevStagingTable.REV_PROVIDER_STG, pageNumber, isLastPage);
        }

        private async Task<RevListResponse<RevProvider>> GetProvidersAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({pageNumber})");

            return await revApiClient.ExecuteRevGetWithRetryAsync<RevListResponse<RevProvider>>(RevGetProvidersUrl, PageSize, pageNumber, context.logger);
        }

        private async Task RevServiceTypesAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");

            List<RevServiceType> serviceTypes = new List<RevServiceType>();
            (bool isLastPage, pageNumber) = await GetPagesFromRevIOListAPI(context, revApiClient, pageNumber, serviceTypes, GetServiceTypesAsync);

            sqlRetryPolicy.Execute(() => LoadRevServiceTypesToStaging(context, serviceTypes, currentIntegrationAuthenticationId));

            await CheckProcessOfSyncStep(context, sqlRetryPolicy, currentIntegrationAuthenticationId, RevSyncStep.ServiceType, RevSyncStep.Service, RevStagingTable.REV_SERVICE_TYPE_STG, pageNumber, isLastPage);
        }

        private async Task<RevListResponse<RevServiceType>> GetServiceTypesAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({pageNumber})");

            return await revApiClient.ExecuteRevGetWithRetryAsync<RevListResponse<RevServiceType>>(RevGetServiceTypesUrl, PageSize, pageNumber, context.logger);
        }

        private async Task RevPackagesAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");

            List<RevPackage> packages = new List<RevPackage>();
            (bool isLastPage, pageNumber) = await GetPagesFromRevIOListAPI(context, revApiClient, pageNumber, packages, GetPackagesAsync);

            sqlRetryPolicy.Execute(() => LoadRevPackagesToStaging(context, packages, currentIntegrationAuthenticationId));

            await CheckProcessOfSyncStep(context, sqlRetryPolicy, currentIntegrationAuthenticationId, RevSyncStep.Package, RevSyncStep.PackageProduct, RevStagingTable.REV_PACKAGE_STG, pageNumber, isLastPage);
        }

        private async Task RevPackageProductsAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");

            List<RevPackageProduct> packageProducts = new List<RevPackageProduct>();
            (bool isLastPage, pageNumber) = await GetPagesFromRevIOListAPI(context, revApiClient, pageNumber, packageProducts, GetPackageProductsAsync);

            sqlRetryPolicy.Execute(() => LoadRevPackageProductsToStaging(context, packageProducts, currentIntegrationAuthenticationId));

            await CheckProcessOfSyncStep(context, sqlRetryPolicy, currentIntegrationAuthenticationId, RevSyncStep.PackageProduct, RevSyncStep.None, RevStagingTable.REV_PACKAGE_PRODUCT_STG, pageNumber, isLastPage);
        }

        private async Task<RevListResponse<RevPackage>> GetPackagesAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({pageNumber})");

            return await revApiClient.ExecuteRevGetWithRetryAsync<RevListResponse<RevPackage>>(RevGetPackagesUrl, PageSize, pageNumber, context.logger);
        }

        private async Task<RevListResponse<RevPackageProduct>> GetPackageProductsAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({pageNumber})");

            return await revApiClient.ExecuteRevGetWithRetryAsync<RevListResponse<RevPackageProduct>>(RevGetPackageProductsUrl, PageSize, pageNumber, context.logger);
        }

        private async Task GetProductTypesAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");

            List<RevProductType> productTypes = new List<RevProductType>();
            (bool isLastPage, pageNumber) = await GetPagesFromRevIOListAPI(context, revApiClient, pageNumber, productTypes, GetProductTypesAsync);

            sqlRetryPolicy.Execute(() => LoadRevProductTypesToStaging(context, productTypes, currentIntegrationAuthenticationId));

            await CheckProcessOfSyncStep(context, sqlRetryPolicy, currentIntegrationAuthenticationId, RevSyncStep.ProductType, RevSyncStep.Product, RevStagingTable.REV_PRODUCT_TYPE_STG, pageNumber, isLastPage);
        }

        private async Task<RevListResponse<RevProductType>> GetProductTypesAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({pageNumber})");

            return await revApiClient.ExecuteRevGetWithRetryAsync<RevListResponse<RevProductType>>(RevGetProductTypesUrl, PageSize, pageNumber, context.logger);
        }

        private static void LoadRevCustomersFromStagingTable(KeySysLambdaContext context, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevCustomersFromStagingTable()");

            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Update_RevCustomer", conn))
                {
                    cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", currentIntegrationAuthenticationId);
                    conn.Open();

                    cmd.ExecuteNonQuery();
                    conn.Close();
                }
            }
        }

        private static void LoadRevBillProfilesFromStagingTable(KeySysLambdaContext context, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevBillProfilesFromStagingTable()");

            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Update_RevBillProfile", conn))
                {
                    cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", currentIntegrationAuthenticationId);
                    conn.Open();

                    cmd.ExecuteNonQuery();
                    conn.Close();
                }
            }
        }

        private static void LoadRevProvidersFromStagingTable(KeySysLambdaContext context, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevProvidersFromStagingTable()");

            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Update_RevProvider", conn))
                {
                    cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", currentIntegrationAuthenticationId);
                    conn.Open();

                    cmd.ExecuteNonQuery();
                    conn.Close();
                }
            }
        }

        private async Task GetProductsAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");

            List<RevProduct> products = new List<RevProduct>();
            (bool isLastPage, pageNumber) = await GetPagesFromRevIOListAPI(context, revApiClient, pageNumber, products, GetProductsAsync);

            sqlRetryPolicy.Execute(() => LoadRevProductsToStaging(context, products, currentIntegrationAuthenticationId));

            await CheckProcessOfSyncStep(context, sqlRetryPolicy, currentIntegrationAuthenticationId, RevSyncStep.Product, RevSyncStep.ServiceType, RevStagingTable.REV_PRODUCT_STG, pageNumber, isLastPage);
        }

        private async Task<RevListResponse<RevProduct>> GetProductsAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({pageNumber})");

            return await revApiClient.ExecuteRevGetWithRetryAsync<RevListResponse<RevProduct>>(RevGetProductsUrl, PageSize, pageNumber, context.logger);
        }

        private async Task GetServiceProductsAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");

            List<RevServiceProduct> serviceProducts = new List<RevServiceProduct>();

            (bool isLastPage, pageNumber) = await GetPagesFromRevIOListAPI(context, revApiClient, pageNumber, serviceProducts, GetServiceProductsAsync);

            sqlRetryPolicy.Execute(() => LoadRevServiceProductsToStaging(context, serviceProducts, currentIntegrationAuthenticationId));

            await CheckProcessOfSyncStep(context, sqlRetryPolicy, currentIntegrationAuthenticationId, RevSyncStep.ServiceProduct, RevSyncStep.InventoryType, RevStagingTable.REV_SERVICE_PRODUCT_STG,
                pageNumber, isLastPage);
        }

        private async Task<RevListResponse<RevServiceProduct>> GetServiceProductsAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({pageNumber})");

            return await revApiClient.ExecuteRevGetWithRetryAsync<RevListResponse<RevServiceProduct>>(RevGetServiceProductsUrl, PageSize, pageNumber, context.logger);
        }

        private async Task GetInventoryTypesAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");

            List<RevInventoryType> revInventoryTypes = new List<RevInventoryType>();

            (bool isLastPage, pageNumber) = await GetPagesFromRevIOListAPI(context, revApiClient, pageNumber, revInventoryTypes, GetInventoryTypesAsync);

            sqlRetryPolicy.Execute(() => LoadRevInventoryTypesToStaging(context, revInventoryTypes, currentIntegrationAuthenticationId));

            if (!isLastPage)
            {
                await BuildMessageToQueue(context, currentIntegrationAuthenticationId, RevSyncStep.InventoryType, pageNumber);
            }
            else
            {
                // send message to this lambda to load rev from staging
                await BuildMessageLoadRevFromStagingToQueueAsync(context, RevCustomerSyncQueueURL, currentIntegrationAuthenticationId, RevSyncStep.InventoryType);
                await BuildMessageToQueue(context, currentIntegrationAuthenticationId, RevSyncStep.InventoryItem, DEFAULT_PAGE_NUMBER);
            }
        }

        private async Task<RevListResponse<RevInventoryType>> GetInventoryTypesAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({pageNumber})");

            return await revApiClient.ExecuteRevGetWithRetryAsync<RevListResponse<RevInventoryType>>(RevGetInventoryTypesUrl, PageSize, pageNumber, context.logger);
        }

        private async Task GetInventoryItemsAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");

            List<RevInventoryItem> revInventoryItems = new List<RevInventoryItem>();
            (bool isLastPage, pageNumber) = await GetPagesFromRevIOListAPI(context, revApiClient, pageNumber, revInventoryItems, GetInventoryItemsAsync);

            sqlRetryPolicy.Execute(() => LoadRevInventoryItemsToStaging(context, revInventoryItems, currentIntegrationAuthenticationId));

            if (!isLastPage)
            {
                await BuildMessageToQueue(context, currentIntegrationAuthenticationId, RevSyncStep.InventoryItem, pageNumber);
            }
            else
            {
                // send message to this lambda to load rev from staging
                await BuildMessageLoadRevFromStagingToQueueAsync(context, RevCustomerSyncQueueURL, currentIntegrationAuthenticationId, RevSyncStep.InventoryItem);
                await BuildMessageToQueue(context, currentIntegrationAuthenticationId, RevSyncStep.UsagePlanGroup, DEFAULT_PAGE_NUMBER);
            }
        }

        private async Task<RevListResponse<RevInventoryItem>> GetInventoryItemsAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({pageNumber})");

            return await revApiClient.ExecuteRevGetWithRetryAsync<RevListResponse<RevInventoryItem>>(RevGetInventoryItemsUrl, PageSize, pageNumber, context.logger);
        }

        private async Task GetUsagePlanGroupsAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");

            List<RevUsagePlanGroup> revUsagePlanGroups = new List<RevUsagePlanGroup>();
            (bool isLastPage, pageNumber) = await GetPagesFromRevIOListAPI(context, revApiClient, pageNumber, revUsagePlanGroups, GetUsagePlanGroupsAsync);

            sqlRetryPolicy.Execute(() => LoadRevUsagePlanGroupsToStaging(context, revUsagePlanGroups, currentIntegrationAuthenticationId));

            if (!isLastPage)
            {
                await BuildMessageToQueue(context, currentIntegrationAuthenticationId, RevSyncStep.UsagePlanGroup, pageNumber);
            }
            else
            {
                // send message to this lambda to load rev from staging
                await BuildMessageLoadRevFromStagingToQueueAsync(context, RevCustomerSyncQueueURL, currentIntegrationAuthenticationId, RevSyncStep.UsagePlanGroup);
                await BuildMessageToQueue(context, currentIntegrationAuthenticationId, RevSyncStep.Agent, DEFAULT_PAGE_NUMBER);
            }
        }
        private async Task GetAgentAsync(KeySysLambdaContext context, RetryPolicy sqlRetryPolicy, RevioApiClient revApiClient, int currentIntegrationAuthenticationId, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({currentIntegrationAuthenticationId}, {pageNumber})");

            List<RevAgent> revAgents = new List<RevAgent>();
            (bool isLastPage, pageNumber) = await GetPagesFromRevIOListAPI(context, revApiClient, pageNumber, revAgents, GetAgentAsync);

            sqlRetryPolicy.Execute(() => LoadRevAgentsToStaging(context, revAgents, currentIntegrationAuthenticationId));

            await CheckProcessOfSyncStep(context, sqlRetryPolicy, currentIntegrationAuthenticationId, RevSyncStep.Agent, RevSyncStep.Package, RevStagingTable.REV_CUSTOMER_STG, pageNumber, isLastPage);
        }

        private async Task<RevListResponse<RevUsagePlanGroup>> GetUsagePlanGroupsAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({pageNumber})");

            return await revApiClient.ExecuteRevGetWithRetryAsync<RevListResponse<RevUsagePlanGroup>>(RevGetUsagePlanGroupsUrl, PageSize, pageNumber, context.logger);
        }
        private async Task<RevListResponse<RevAgent>> GetAgentAsync(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber)
        {
            LogInfo(context, CommonConstants.SUB, $"({pageNumber})");

            return await revApiClient.ExecuteRevGetWithRetryAsync<RevListResponse<RevAgent>>(RevGetAgentUrl, PageSize, pageNumber, context.logger);
        }
        private static void LoadRevAgentFromStagingTable(KeySysLambdaContext context, int integrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevAgentFromStagingTable()");

            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Update_RevAgent", conn))
                {
                    cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", integrationAuthenticationId);
                    conn.Open();

                    cmd.ExecuteNonQuery();
                    conn.Close();
                }
            }
        }

        private static void LoadRevServiceTypesFromStagingTable(KeySysLambdaContext context, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevServiceTypesFromStagingTable()");

            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Update_RevServiceType", conn))
                {
                    cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", currentIntegrationAuthenticationId);
                    conn.Open();

                    cmd.ExecuteNonQuery();
                    conn.Close();
                }
            }
        }

        private static void LoadRevServicesFromStagingTable(KeySysLambdaContext context, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevServicesFromStagingTable()");

            using (var jasperConn = new SqlConnection(context.GeneralProviderSettings.JasperDbConnectionString))
            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Update_RevService", conn))
                {
                    cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", currentIntegrationAuthenticationId);
                    conn.Open();

                    cmd.Parameters.AddWithValue("@JasperDbName", jasperConn.Database);

                    cmd.ExecuteNonQuery();
                    conn.Close();
                }
            }
        }

        private static void LoadRevProductTypesFromStagingTable(KeySysLambdaContext context, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevProductTypesFromStagingTable()");

            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Update_RevProductType_FromStaging", conn))
                {
                    cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", currentIntegrationAuthenticationId);
                    conn.Open();

                    cmd.ExecuteNonQuery();
                    conn.Close();
                }
            }
        }

        private static void LoadRevProductsFromStagingTable(KeySysLambdaContext context, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevProductsFromStagingTable()");

            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Update_RevProduct_FromStaging", conn))
                {
                    cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", currentIntegrationAuthenticationId);
                    conn.Open();

                    cmd.ExecuteNonQuery();
                    conn.Close();
                }
            }
        }

        private static void LoadRevServiceProductsFromStagingTable(KeySysLambdaContext context, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevServiceProductsFromStagingTable()");

            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Update_RevServiceProduct_FromStaging", conn))
                {
                    cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", currentIntegrationAuthenticationId);
                    conn.Open();

                    cmd.ExecuteNonQuery();
                    conn.Close();
                }
            }
        }

        private static void LoadRevInventoryTypesFromStagingTable(KeySysLambdaContext context, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevInventoryTypesFromStagingTable()");

            using var conn = new SqlConnection(context.CentralDbConnectionString);
            using var cmd = new SqlCommand("usp_Update_RevInventoryType_FromStaging", conn)
            {
                CommandTimeout = AMOPSQLConstant.TimeoutSeconds,
                CommandType = CommandType.StoredProcedure,
            };
            cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", currentIntegrationAuthenticationId);
            conn.Open();

            cmd.ExecuteNonQuery();
        }

        private static void LoadRevInventoryItemsFromStagingTable(KeySysLambdaContext context, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevInventoryItemsFromStagingTable()");

            using var conn = new SqlConnection(context.CentralDbConnectionString);
            using var cmd = new SqlCommand("usp_Update_RevInventoryItem_FromStaging", conn)
            {
                CommandTimeout = AMOPSQLConstant.TimeoutSeconds,
                CommandType = CommandType.StoredProcedure
            };
            cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", currentIntegrationAuthenticationId);
            conn.Open();

            cmd.ExecuteNonQuery();
        }

        private static void LoadRevUsagePlanGroupsFromStagingTable(KeySysLambdaContext context, int integrationAuthenticationId)
        {
            LogInfo(context, "SUB", "LoadRevUsagePlanGroupsFromStagingTable()");

            using var conn = new SqlConnection(context.CentralDbConnectionString);
            using var cmd = new SqlCommand("usp_Update_RevUsagePlanGroup_FromStaging", conn)
            {
                CommandTimeout = AMOPSQLConstant.TimeoutSeconds,
                CommandType = CommandType.StoredProcedure
            };
            cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", integrationAuthenticationId);
            conn.Open();

            cmd.ExecuteNonQuery();
        }

        private static void LoadRevPackageFromStagingTable(KeySysLambdaContext context, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, LogTypeConstant.Sub, "");
            try
            {
                using (var conn = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var cmd = new SqlCommand(AMOPSQLConstant.StoredProcedureName.usp_Update_RevPackage_FromStaging, conn))
                    {
                        cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", currentIntegrationAuthenticationId);
                        conn.Open();

                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (SqlException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"Exception when executing stored procedure: {ex.Message}, ErrorCode:{ex.ErrorCode}-{ex.Number}");
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"Exception when connecting to database: {ex.Message}");
            }
            catch (Exception ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"Exception: {ex.Message}");
            }

        }

        private static void LoadRevPackageProductFromStagingTable(KeySysLambdaContext context, int currentIntegrationAuthenticationId)
        {
            LogInfo(context, LogTypeConstant.Sub, "");
            try
            {
                using (var conn = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var cmd = new SqlCommand(AMOPSQLConstant.StoredProcedureName.usp_Update_RevPackageProduct_FromStaging, conn))
                    {
                        cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", currentIntegrationAuthenticationId);
                        conn.Open();

                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (SqlException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"Exception when executing stored procedure: {ex.Message}, ErrorCode:{ex.ErrorCode}-{ex.Number}");
            }
            catch (InvalidOperationException ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"Exception when connecting to database: {ex.Message}");
            }
            catch (Exception ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"Exception: {ex.Message}");
            }
        }

        private static void DeleteRevInventoryItemStagingData(KeySysLambdaContext context, int integrationAuthenticationId)
        {
            LogInfo(context, "SUB", "DeleteRevInventoryItemStagingData()");
            using var conn = new SqlConnection(context.CentralDbConnectionString);
            using var cmd = new SqlCommand("DELETE FROM RevInventoryItemStaging WHERE IntegrationAuthenticationId = @IntegrationAuthenticationId", conn)
            {
                CommandType = CommandType.Text
            };
            cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", integrationAuthenticationId);
            cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
            conn.Open();
            cmd.ExecuteNonQuery();
        }

        private static void DeleteRevInventoryTypeStagingData(KeySysLambdaContext context, int integrationAuthenticationId)
        {
            LogInfo(context, "SUB", "DeleteRevInventoryTypeStagingData()");
            using var conn = new SqlConnection(context.CentralDbConnectionString);
            using var cmd = new SqlCommand("DELETE FROM RevInventoryTypeStaging WHERE IntegrationAuthenticationId = @IntegrationAuthenticationId", conn)
            {
                CommandType = CommandType.Text
            };
            cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", integrationAuthenticationId);
            cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
            conn.Open();
            cmd.ExecuteNonQuery();
        }

        private static void DeleteRevUsagePlanGroupStagingData(KeySysLambdaContext context, int integrationAuthenticationId)
        {
            LogInfo(context, "SUB", "DeleteRevUsagePlanGroupStagingData()");
            using var conn = new SqlConnection(context.CentralDbConnectionString);
            using var cmd = new SqlCommand("DELETE FROM RevUsagePlanGroupStaging WHERE IntegrationAuthenticationId = @IntegrationAuthenticationId", conn)
            {
                CommandType = CommandType.Text
            };
            cmd.Parameters.AddWithValue("@IntegrationAuthenticationId", integrationAuthenticationId);
            cmd.CommandTimeout = AMOPSQLConstant.TimeoutSeconds;
            conn.Open();
            cmd.ExecuteNonQuery();
        }

        private async Task SendMessageToQueueAsync(KeySysLambdaContext context, string revCustomerSyncQueueURL, int currentIntegrationAuthenticationId, RevSyncStep nextRevSyncStep, int pageNumber)
        {
            LogInfo(context, "SUB", "SendMessageToQueueAsync");

            LogInfo(context, "revCustomerSyncQueueURL", revCustomerSyncQueueURL);
            LogInfo(context, "nextRevSyncStep", nextRevSyncStep);
            LogInfo(context, "pageNumber", pageNumber);

            if (string.IsNullOrWhiteSpace(revCustomerSyncQueueURL))
            {
                // so we don't have to queue a message during a test
                return;
            }

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
            {
                var requestMsgBody = $"Current Auth Id {currentIntegrationAuthenticationId}";
                LogInfo(context, "Sending message for", $"{requestMsgBody} to destination queue: {revCustomerSyncQueueURL}");

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

                LogInfo(context, "STATUS", "SendMessageRequest is ready!");
                LogInfo(context, "MessageBody", request.MessageBody);
                LogInfo(context, "QueueURL", request.QueueUrl);

                var response = await client.SendMessageAsync(request);
                if (((int)response.HttpStatusCode < 200) || ((int)response.HttpStatusCode > 299))
                {
                    LogInfo(context, "EXCEPTION", $"Error enqueuing Rev Sync next step - {nextRevSyncStep:g}: {response.HttpStatusCode:d} {response.HttpStatusCode:g}");
                }
            }
        }

        private async Task BuildMessageLoadRevFromStagingToQueueAsync(KeySysLambdaContext context, string revCustomerSyncQueueURL, int currentIntegrationAuthenticationId, RevSyncStep currentStep)
        {
            LogInfo(context, "SUB", "BuildMessageLoadRevFromStagingToQueueAsync");

            LogInfo(context, "revCustomerSyncQueueURL", revCustomerSyncQueueURL);
            LogInfo(context, "currentRevSyncStep", currentStep);

            if (string.IsNullOrWhiteSpace(revCustomerSyncQueueURL))
            {
                // so we don't have to queue a message during a test
                return;
            }

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
            {
                var requestMsgBody = $"Current Auth Id {currentIntegrationAuthenticationId}";
                LogInfo(context, "Sending message for", $"{requestMsgBody} to destination queue: {revCustomerSyncQueueURL}");

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
                            { DataType = "String", StringValue = true.ToString()}
                        }
                    },
                    MessageBody = requestMsgBody,
                    QueueUrl = revCustomerSyncQueueURL
                };

                LogInfo(context, "STATUS", "SendMessageRequest is ready!");
                LogInfo(context, "MessageBody", request.MessageBody);
                LogInfo(context, "QueueURL", request.QueueUrl);

                var response = await client.SendMessageAsync(request);
                if (((int)response.HttpStatusCode < 200) || ((int)response.HttpStatusCode > 299))
                {
                    LogInfo(context, "EXCEPTION", $"Error enqueuing Rev Sync next step - {currentStep:g}: {response.HttpStatusCode:d} {response.HttpStatusCode:g}");
                }
            }
        }

        private static bool CheckShouldQueueAnotherSyncInstance(KeySysLambdaContext context, int counter, int maxPagesPerBatch)
        {
            // Queue up next lambda instance if we reach the number of pages per batch, reach the fail limit or we do not enough time for another page
            return counter == maxPagesPerBatch
                    || context.Context.RemainingTime < TimeSpan.FromSeconds(CommonConstants.DEFAULT_LAMBDA_INSTANCE_REMAINING_SECONDS_LIMIT);
        }

        private async Task<(bool, int)> GetPagesFromRevIOListAPI<T>(KeySysLambdaContext context, RevioApiClient revApiClient, int pageNumber, List<T> items, Func<KeySysLambdaContext, RevioApiClient, int, Task<RevListResponse<T>>> callListApi) where T : class
        {
            bool isLastPage = false;
            int counter = 0;
            int failCount = 0;
            bool shouldStopSyncStep = false;
            while (!isLastPage)
            {
                LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.DETAILED_INFO_PER_PAGE_SYNCED, pageNumber, counter, context.Context.RemainingTime.TotalSeconds));
                shouldStopSyncStep = failCount >= CommonConstants.REV_IO_SYNC_FAIL_ACCEPTABLE_LIMIT;
                if (CheckShouldQueueAnotherSyncInstance(context, counter, MAX_NUMBER_OF_PAGES_IN_A_BATCH))
                {
                    LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.NOT_ENOUGH_REMAINING_TIME_OR_PAGE_LIMIT_REACHED, MAX_NUMBER_OF_PAGES_IN_A_BATCH, typeof(T).Name));
                    break;
                }
                // Separated if for logging
                if (shouldStopSyncStep)
                {
                    // Log then continue with next sync step
                    LogInfo(context, CommonConstants.ERROR, string.Format(LogCommonStrings.CALLING_API_FAILED_FOR_PAGES, typeof(T).Name, failCount));
                    break;
                }
                else
                {
                    var listResponse = await callListApi(context, revApiClient, pageNumber);
                    if (listResponse != null && listResponse.Records != null)
                    {
                        isLastPage = !listResponse.HasMore;
                        items.AddRange(listResponse.Records);
                    }
                    else
                    {
                        failCount++;
                    }
                    pageNumber++;
                    counter++;
                }
            }

            // The API calls fail in this instance is over a certain limit, should consider this sync instance as bad result and stop the sync
            if (shouldStopSyncStep)
            {
                // Mark as true to continue with next step
                isLastPage = true;
            }

            return (isLastPage, pageNumber);
        }

        private async Task<int?> GetTenantIdByIntegrationAuthenticationId(KeySysLambdaContext context, int currentIntegration)
        {
            int? tenantId = null;
            try
            {
                using (var connection = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var command = new SqlCommand(@"
                            SELECT TOP 1 TenantId
		                    FROM
			                    [dbo].[Integration_Authentication] [IntegrationAuthentication]
		                    WHERE
			                    [IntegrationAuthentication].[IntegrationId] = 3
			                    AND
			                    [TokenValue] IS NOT NULL
			                    AND
			                    [IntegrationAuthentication].[IsActive] = 1
			                    AND
			                    [IntegrationAuthentication].[IsDeleted] = 0
		                    ORDER BY 
			                    [IntegrationAuthentication].[id]", connection))
                    {
                        command.CommandType = CommandType.Text;
                        command.Parameters.AddWithValue("@currentIntegrationAuthenticationId", currentIntegration);
                        command.CommandTimeout = Amop.Core.Constants.SQLConstant.TimeoutSeconds120;
                        connection.Open();

                        var reader = await command.ExecuteReaderAsync();
                        if (await reader.ReadAsync())
                        {
                            tenantId = Convert.ToInt32(reader["TenantId"]);
                        }
                        connection.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                LogInfo(context, "EXCEPTION", $"Could not retrieve top TenantId: {ex.Message}");
            }
            return tenantId;
        }
    }
}
