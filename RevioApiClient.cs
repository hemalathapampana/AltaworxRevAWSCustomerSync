using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Logger;
using Amop.Core.Models;
using Amop.Core.Models.DeviceBulkChange;
using Amop.Core.Models.Revio;
using Amop.Core.Services.Http;
using Microsoft.AspNetCore.JsonPatch;
using Newtonsoft.Json;
using Polly;

namespace Amop.Core.Services.Revio
{
    public class RevioApiClient
    {
        private readonly IHttpClientFactory httpClientFactory;
        private readonly IHttpRequestFactory httpRequestFactory;
        private readonly RevioApiAuthentication authentication;
        private readonly bool isProduction;
        private readonly int totalRetries;

        public RevioApiClient(IHttpClientFactory httpClientFactory, IHttpRequestFactory httpRequestFactory, RevioApiAuthentication authentication, bool isProduction, int retryCount = CommonConstants.NUMBER_OF_REV_IO_RETRIES)
        {
            this.httpClientFactory = httpClientFactory;
            this.httpRequestFactory = httpRequestFactory;
            this.authentication = authentication;
            this.isProduction = isProduction;
            this.totalRetries = retryCount;
        }

        public async Task<DeviceChangeResult<string, UpdateResponse>> UpdateServiceProductQuantityAsync(int serviceProductId, int quantity, Action<string> logFunction)
        {
            var baseUri = isProduction ? authentication.ProductionURL : authentication.SandboxURL;
            var uri = new Uri($"{baseUri.TrimEnd('/')}/v1/ServiceProduct/{serviceProductId}");
            var patch = new JsonPatchDocument();
            patch.Replace("/quantity", quantity);
            var json = JsonConvert.SerializeObject(patch);
            var requestHeader = BuildRequestHeader();
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));
            var response = await RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync().ExecuteAsync(async () =>
            {
                var request = httpRequestFactory.BuildRequestMessage(authentication, new HttpMethod("PATCH"), uri,
                    requestHeader,
                    new StringContent(json, Encoding.UTF8, CommonConstants.APPLICATION_JSON_PATCH));
                return await httpClientFactory.GetClient().SendAsync(request);
            });
            var responseBody = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                return new DeviceChangeResult<string, UpdateResponse>()
                {
                    ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                    RequestObject = json,
                    HasErrors = false,
                    ResponseObject = JsonConvert.DeserializeObject<UpdateResponse>(responseBody)
                };
            }

            var errors = JsonConvert.DeserializeObject<UpdateResponse[]>(responseBody);

            return new DeviceChangeResult<string, UpdateResponse>()
            {
                ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                RequestObject = json,
                HasErrors = true,
                ResponseObject = errors?.FirstOrDefault()
            };
        }

        public async Task<DeviceChangeResult<string, CreateResponse>> CreateCustomerAsync(CreateCustomerBody bodyParams, Action<string> logFunction)
        {
            var baseUri = isProduction ? authentication.ProductionURL : authentication.SandboxURL;
            var uri = new Uri($"{baseUri.TrimEnd('/')}/v1/Customers");
            var requestHeader = BuildRequestHeader();
            var requestContent = BuildContentRequest(bodyParams);
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));

            var response = await RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync().ExecuteAsync(async () =>
            {
                var request = httpRequestFactory.BuildRequestMessage(authentication, new HttpMethod(CommonConstants.METHOD_POST), uri, requestHeader, requestContent);
                return await httpClientFactory.GetClient().SendAsync(request);
            });
            var responseBody = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                return new DeviceChangeResult<string, CreateResponse>()
                {
                    ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                    RequestObject = requestContent.ToString(),
                    HasErrors = false,
                    ResponseObject = JsonConvert.DeserializeObject<CreateResponse>(responseBody)
                };
            }

            var errors = JsonConvert.DeserializeObject<CreateResponse[]>(responseBody);
            return new DeviceChangeResult<string, CreateResponse>()
            {
                ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                RequestObject = requestContent.ToString(),
                HasErrors = true,
                ResponseObject = errors?.FirstOrDefault()
            };
        }

        public async Task<DeviceChangeResult<string, CreateResponse>> CreateServiceLineAsync(CreateServiceLineBody bodyParams, Action<string> logFunction, IKeysysLogger logger = null)
        {
            var baseUri = isProduction ? authentication.ProductionURL : authentication.SandboxURL;
            var uri = new Uri($"{baseUri.TrimEnd('/')}/v1/Services");
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));
            logger?.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));
            var requestContent = BuildContentRequest(bodyParams);
            var requestHeader = BuildRequestHeader();
            var response = await RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync(logger).ExecuteAsync(async () =>
            {
                var request = httpRequestFactory.BuildRequestMessage(authentication,
                    new HttpMethod(CommonConstants.METHOD_POST), uri, requestHeader, requestContent);

                return await httpClientFactory.GetClient().SendAsync(request);
            });
            var responseBody = await response.Content.ReadAsStringAsync();
            var requestBody = JsonConvert.SerializeObject(bodyParams, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore
            });
            if (response.IsSuccessStatusCode)
            {
                return new DeviceChangeResult<string, CreateResponse>()
                {
                    ActionText = $"{CommonConstants.METHOD_POST} {uri.AbsoluteUri}",
                    RequestObject = requestBody,
                    HasErrors = false,
                    ResponseObject = JsonConvert.DeserializeObject<CreateResponse>(responseBody)
                };
            }

            // If API request not successful, return the error response.
            logger?.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.REQUEST_BODY_TO_CREATE_SERVICE, requestBody));
            logger?.LogInfo(CommonConstants.ERROR, string.Format(LogCommonStrings.REQUEST_FAILED_RESPONSE_ERROR_MESSAGE, uri.AbsoluteUri, responseBody));

            return new DeviceChangeResult<string, CreateResponse>()
            {
                ActionText = $"{CommonConstants.METHOD_POST} {uri.AbsoluteUri}",
                RequestObject = requestBody,
                HasErrors = true,
                ResponseObject = new CreateResponse()
                {
                    Error = responseBody,
                    Message = responseBody,
                }
            };
        }

        private CreateResponse[] ParseResponseBody(IKeysysLogger logger, string responseBody)
        {
            var customResponse = new List<CreateResponse>();
            try
            {
                return JsonConvert.DeserializeObject<CreateResponse[]>(responseBody);
            }
            catch (Exception ex)
            {
                var errorMessage = string.Format(LogCommonStrings.ERROR_WHILE_PARSING_RESPONSE_BODY, responseBody);
                logger?.LogInfo(CommonConstants.ERROR, $"{errorMessage}. Ex: {ex.Message}.");
                customResponse.Add(new CreateResponse
                {
                    Error = errorMessage,
                    Message = errorMessage,
                    Ok = false
                });
            }

            return customResponse.ToArray();
        }

        public async Task<DeviceChangeResult<string, CreateResponse>> CreateServiceProductAsync(CreateServiceProductBody bodyParams, Action<string> logFunction, IKeysysLogger logger = null)
        {
            var baseUri = isProduction ? authentication.ProductionURL : authentication.SandboxURL;
            var uri = new Uri($"{baseUri.TrimEnd('/')}/v1/ServiceProduct");
            logger?.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));
            var requestContent = BuildContentRequest(bodyParams);
            var requestHeader = BuildRequestHeader();
            var response = await RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync(logger).ExecuteAsync(async () =>
            {
                var request = httpRequestFactory.BuildRequestMessage(authentication, new HttpMethod(CommonConstants.METHOD_POST), uri, requestHeader, requestContent);

                return await httpClientFactory.GetClient().SendAsync(request);
            });
            var responseBody = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                return new DeviceChangeResult<string, CreateResponse>()
                {
                    ActionText = $"{CommonConstants.METHOD_POST} {uri.AbsoluteUri}",
                    RequestObject = requestContent.ToString(),
                    HasErrors = false,
                    ResponseObject = JsonConvert.DeserializeObject<CreateResponse>(responseBody)
                };
            }

            logger?.LogInfo(CommonConstants.INFO, $"Request Body to create ServiceProduct: {requestContent.ToString()}");
            var errors = ParseResponseBody(logger, responseBody);
            return new DeviceChangeResult<string, CreateResponse>()
            {
                ActionText = $"{CommonConstants.METHOD_POST} {uri.AbsoluteUri}",
                RequestObject = requestContent.ToString(),
                HasErrors = true,
                ResponseObject = errors?.FirstOrDefault()
            };
        }

        public async Task<DeviceChangeResult<string, CreateResponse>> AssociateCustomerAsync(AssignCustomerRequest assignCustomerRequest, Action<string> logFunction)
        {
            var baseUri = isProduction ? authentication.ProductionURL : authentication.SandboxURL;
            var uri = new Uri($"{baseUri.TrimEnd('/')}/v1/ServiceProduct");
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));

            var requestContent = BuildContentRequest(assignCustomerRequest);
            var requestHeader = BuildRequestHeader();
            var response = await RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync().ExecuteAsync(async () =>
            {
                var request = httpRequestFactory.BuildRequestMessage(authentication, new HttpMethod(CommonConstants.METHOD_PATCH), uri, requestHeader, requestContent);

                return await httpClientFactory.GetClient().SendAsync(request);
            });
            var responseBody = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                return new DeviceChangeResult<string, CreateResponse>()
                {
                    ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                    RequestObject = requestContent.ToString(),
                    HasErrors = false,
                    ResponseObject = JsonConvert.DeserializeObject<CreateResponse>(responseBody)
                };
            }

            var errors = JsonConvert.DeserializeObject<CreateResponse[]>(responseBody);
            return new DeviceChangeResult<string, CreateResponse>()
            {
                ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                RequestObject = requestContent.ToString(),
                HasErrors = true,
                ResponseObject = errors?.FirstOrDefault()
            };
        }

        public async Task<DeviceChangeResult<string, CreateResponse>> CreateRevInventoryItemAsync(CreateInventoryItemRequest createInventoryItemRequest, Action<string> logFunction)
        {
            var baseUri = isProduction ? authentication.ProductionURL : authentication.SandboxURL;
            var uri = new Uri($"{baseUri.TrimEnd('/')}/v1/InventoryItem");
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));
            var client = httpClientFactory.GetClient();
            var requestContent = "";
            var requestHeader = BuildRequestHeader();

            var response = RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync().ExecuteAsync(async () =>
            {
                var content = BuildContentRequest(createInventoryItemRequest);
                requestContent = content.ToString();
                var request = httpRequestFactory.BuildRequestMessage(authentication, new HttpMethod(CommonConstants.METHOD_POST), uri, requestHeader, content);
                var res = client.SendAsync(request);
                res.Wait(CommonConstants.WAIT_IN_30_SECONDS);

                return res.Result;
            });
            response.Wait(CommonConstants.WAIT_IN_30_SECONDS);
            var responseBody = await response.Result.Content.ReadAsStringAsync();
            if (response.Result.IsSuccessStatusCode)
            {
                return new DeviceChangeResult<string, CreateResponse>()
                {
                    ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                    RequestObject = requestContent.ToString(),
                    HasErrors = false,
                    ResponseObject = JsonConvert.DeserializeObject<CreateResponse>(responseBody)
                };
            }

            var errors = JsonConvert.DeserializeObject<CreateResponse[]>(responseBody);
            return new DeviceChangeResult<string, CreateResponse>()
            {
                ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                RequestObject = requestContent.ToString(),
                HasErrors = true,
                ResponseObject = errors?.FirstOrDefault()
            };
        }

        public async Task<InventoryItemResponse> GetInventoryItemAsync(string identifier, Action<string> logFunction)
        {
            var baseUri = isProduction ? authentication.ProductionURL : authentication.SandboxURL;
            var uri = new Uri($"{baseUri.TrimEnd('/')}/v1/InventoryItem?search.identifier={identifier}");
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));
            var client = httpClientFactory.GetClient();
            var requestHeader = BuildRequestHeader();

            var response = RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync().ExecuteAsync(async () =>
            {
                var request = httpRequestFactory.BuildRequestMessage(authentication, new HttpMethod(CommonConstants.METHOD_GET), uri, requestHeader);
                var res = client.SendAsync(request);
                res.Wait(CommonConstants.WAIT_IN_30_SECONDS);

                return res.Result;
            });
            response.Wait(CommonConstants.WAIT_IN_30_SECONDS);
            var responseBody = await response.Result.Content.ReadAsStringAsync();
            if (response.Result.IsSuccessStatusCode)
            {
                return JsonConvert.DeserializeObject<InventoryItemResponse>(responseBody);
            }

            var errors = JsonConvert.DeserializeObject<InventoryItemResponse[]>(responseBody);
            return errors?.FirstOrDefault();
        }

        public async Task<DeviceChangeResult<string, CreateResponse>> AssignInventoryToService(AssignInventoryToServiceRequest assignInventoryToServiceRequest, Action<string> logFunction)
        {
            var baseUri = isProduction ? authentication.ProductionURL : authentication.SandboxURL;
            var uri = new Uri($"{baseUri.TrimEnd('/')}/assignService");
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));
            var requestContent = BuildContentRequest(assignInventoryToServiceRequest);
            var requestHeader = BuildRequestHeader();

            var response = await RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync().ExecuteAsync(async () =>
            {
                var request = httpRequestFactory.BuildRequestMessage(authentication, new HttpMethod(CommonConstants.METHOD_PATCH), uri, requestHeader, requestContent);
                return await httpClientFactory.GetClient().SendAsync(request);
            });
            var responseBody = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                return new DeviceChangeResult<string, CreateResponse>()
                {
                    ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                    RequestObject = requestContent.ToString(),
                    HasErrors = false,
                    ResponseObject = JsonConvert.DeserializeObject<CreateResponse>(responseBody)
                };
            }

            var errors = JsonConvert.DeserializeObject<CreateResponse[]>(responseBody);
            return new DeviceChangeResult<string, CreateResponse>()
            {
                ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                RequestObject = requestContent.ToString(),
                HasErrors = true,
                ResponseObject = errors?.FirstOrDefault()
            };
        }

        public async Task<bool> DisconnectServiceProductAsync(int serviceProductId, DateTime? effectDate, bool generateProduction, Action<string> logFunction)
        {
            var baseUri = isProduction ? authentication.ProductionURL : authentication.SandboxURL;
            var uri = new Uri($"{baseUri.TrimEnd('/')}/v1/serviceProduct");
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));
            object parameter = new
            {
                service_product_id = serviceProductId,
                effective_date = effectDate == null ? "" : ((DateTime)effectDate).ToString(CommonConstants.AMOP_YEAR_MONTH_TIME_FORMAT),
                generate_proration = generateProduction
            };
            var requestContent = BuildContentRequest(parameter);
            var requestHeader = BuildRequestHeader();
            var response = await RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync().ExecuteAsync(async () =>
            {
                var request = httpRequestFactory.BuildRequestMessage(authentication, HttpMethod.Delete, uri, requestHeader, requestContent);
                return await httpClientFactory.GetClient().SendAsync(request);
            });
            var responseBody = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                return true;
            }
            return false;
        }
        public async Task<DeviceChangeResult<string, CreateResponse>> UnAssignInventoryItem(int inventoryItemId, Action<string> logFunction)
        {
            var baseUri = isProduction ? authentication.ProductionURL : authentication.SandboxURL;
            var uri = new Uri($"{baseUri.TrimEnd('/')}/UnassignCustomer");
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));

            var json = JsonConvert.SerializeObject(new { id = inventoryItemId }, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore
            });
            var requestContent = BuildContentRequest(new { id = inventoryItemId });
            var requestHeader = BuildRequestHeader();
            var response = await RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync().ExecuteAsync(async () =>
            {
                var request = httpRequestFactory.BuildRequestMessage(authentication, new HttpMethod("PATCH"), uri, requestHeader, requestContent);
                return await httpClientFactory.GetClient().SendAsync(request);
            });
            var responseBody = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                return new DeviceChangeResult<string, CreateResponse>()
                {
                    ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                    RequestObject = json,
                    HasErrors = false,
                    ResponseObject = JsonConvert.DeserializeObject<CreateResponse>(responseBody)
                };
            }

            var errors = JsonConvert.DeserializeObject<CreateResponse[]>(responseBody);
            return new DeviceChangeResult<string, CreateResponse>()
            {
                ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                RequestObject = json,
                HasErrors = true,
                ResponseObject = errors?.FirstOrDefault()
            };
        }

        public async Task<T> ExecuteRevGetWithRetryAsync<T>(string baseUri, int pageSize, int pageNumber, IKeysysLogger logger = null) where T : class
        {
            var url = $"{baseUri}?search.page_size={pageSize}&search.page={pageNumber}";
            if (!string.IsNullOrWhiteSpace(authentication.RevBillProfile))
            {
                var revBillProfileIds = authentication.RevBillProfile.Split(',')
                                                .Select(p => p.Trim());
                foreach (var billProfileId in revBillProfileIds)
                {
                    url += $"&search.bill_profile_id={Convert.ToInt32(billProfileId)}";
                }
            }
            try
            {
                Uri endPoint = new Uri(url);
                var response = await RetryPolicyHelper.PollyRetryHttpRequestAsync(logger, totalRetries).ExecuteAsync(async () =>
                {
                    logger?.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.REQUEST_URL, endPoint));
                    var requestHeader = BuildRequestHeader();
                    var request = httpRequestFactory.BuildRequestMessage(authentication, HttpMethod.Get, endPoint, requestHeader);
                    return await httpClientFactory.GetClient().SendAsync(request);
                });
                var responseBody = await response.Content.ReadAsStringAsync();
                if (response.IsSuccessStatusCode)
                {
                    if (string.IsNullOrWhiteSpace(responseBody))
                    {
                        logger?.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.EMPTY_API_RESPONSE_ERROR, url));
                        return null;
                    }
                    return JsonConvert.DeserializeObject<T>(responseBody);
                }

                // Have a response body but invalid status code
                logger?.LogInfo(CommonConstants.ERROR, string.Format(LogCommonStrings.REQUEST_FAILED_RESPONSE_AT_FINAL_RETRIES, url, responseBody));
                return null;
            }
            catch (Exception ex)
            {
                logger?.LogInfo(CommonConstants.EXCEPTION, string.Format(LogCommonStrings.REQUEST_FAILED_RESPONSE_ERROR_MESSAGE, url, ex.Message));
                return null;
            }
        }

        public async Task<T> GetServicesAsync<T>(string number, IKeysysLogger logger = null) where T : class
        {
            var url = new Uri(string.Format(URLConstants.REV_DEVICE_SERVICE_LOOK_UP_URL, number));
            var response = await RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync(logger, totalRetries).ExecuteAsync(async () =>
            {
                logger?.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.REQUEST_URL, url.AbsoluteUri));
                var requestHeader = BuildRequestHeader();
                var request = httpRequestFactory.BuildRequestMessage(authentication, HttpMethod.Get, url, requestHeader);
                return await httpClientFactory.GetClient().SendAsync(request);
            });
            var responseBody = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode || (int)response.StatusCode == CommonConstants.TOO_MANY_REQUEST_HTTP_STATUS_CODE)
            {
                return JsonConvert.DeserializeObject<T>(responseBody);
            }
            logger?.LogInfo(CommonConstants.ERROR, string.Format(LogCommonStrings.REQUEST_FAILED_RESPONSE_AT_FINAL_RETRIES, url.AbsoluteUri, responseBody));
            return null;
        }

        public async Task<RevIoCreateChargeResult> AddChargeAsync(string requestString, IAsyncPolicy<HttpResponseMessage> retryPolicy, IKeysysLogger logger = null)
        {
            var url = new Uri(URLConstants.REV_CREATE_CHARGES_URL);
            var response = await retryPolicy.ExecuteAsync(async () =>
            {
                logger?.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.REQUEST_URL, URLConstants.REV_CREATE_CHARGES_URL));
                var requestHeader = BuildRequestHeader();
                var request = httpRequestFactory.BuildRequestMessage(authentication, HttpMethod.Post, url, requestHeader,
                    new StringContent(requestString, Encoding.ASCII, CommonConstants.TEXT_JSON));
                var responseMessage = await httpClientFactory.GetClient().SendAsync(request);
                // If the response status is 500, do not retry
                if (responseMessage.StatusCode == HttpStatusCode.InternalServerError)
                {
                    logger?.LogInfo(CommonConstants.ERROR, $"Skipping retry for 500 response: {await responseMessage.Content.ReadAsStringAsync()}");
                    return responseMessage;
                }
                return responseMessage;
            });
            var responseBody = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                return JsonConvert.DeserializeObject<RevIoCreateChargeResult>(responseBody);
            }
            logger?.LogInfo(CommonConstants.ERROR, string.Format(LogCommonStrings.REQUEST_FAILED_RESPONSE_ERROR_MESSAGE, url, responseBody));
            return new RevIoCreateChargeResult()
            {
                StatusCode = ((int)response.StatusCode).ToString(),
                Message = ((int)response.StatusCode) == (int)HttpStatusCode.InternalServerError ? LogCommonStrings.INTERNAL_SERVER_ERROR : LogCommonStrings.ERROR_WHEN_UPLOADING_CHARGE_AT_FINAL_RETRY
            };

        }

        public async Task SendMailSummaryCustomerChargeForMultipleInstances(string proxyUrl, IAsyncPolicy<HttpResponseMessage> retryPolicy, List<CustomerChargeQueueOfInstance> customerChargeQueueIdList, List<RevCustomerModel> lstCustomer, int tenantId, string bucketName, bool isNonRev, IKeysysLogger logger = null)
        {
            logger?.LogInfo(CommonConstants.SUB, $"{customerChargeQueueIdList.Count}, {bucketName}, {tenantId}, {isNonRev}");
            if (customerChargeQueueIdList.Count > 0)
            {
                var jsonContent = new RevCustomerChargeEmailModel
                {
                    customerChargeQueueIdList = customerChargeQueueIdList,
                    lstCustomer = lstCustomer,
                    TenantId = tenantId,
                    IsNonRev = isNonRev,
                    BucketName = bucketName

                };
                if (!string.IsNullOrWhiteSpace(proxyUrl))
                {
                    var payload = new PayloadModel()
                    {
                        JsonContent = JsonConvert.SerializeObject(jsonContent),
                        Password = null,
                        Token = null,
                        Username = null,
                        IsOptCustomerSendEmail = false
                    };

                    var url = new Uri(proxyUrl);
                    var response = await retryPolicy.ExecuteAsync(async () =>
                    {
                        var requestHeader = BuildProxyRequestHeader();
                        var request = httpRequestFactory.BuildRequestMessage(null, HttpMethod.Post, url, requestHeader, BuildContentRequest(payload));
                        return await httpClientFactory.GetClient().SendAsync(request);
                    });
                    var responseBody = await response.Content.ReadAsStringAsync();
                    logger.LogInfo(CommonConstants.INFO, $"{responseBody}");
                }

            }
        }

        public async Task UploadDeviceCustomerChargeByProxy(IAsyncPolicy<HttpResponseMessage> retryPolicy, byte[] fileByte, int tenantId, string fileName, string proxyUrl, IKeysysLogger logger = null)
        {
            logger?.LogInfo(CommonConstants.SUB, $"{fileName}, {tenantId}");
            var jsonContent = new CustomerChargeUploadModel
            {
                FileBytes = fileByte,
                TenantId = tenantId,
                FileName = fileName
            };
            if (!string.IsNullOrWhiteSpace(proxyUrl))
            {
                var payload = new PayloadModel()
                {
                    JsonContent = JsonConvert.SerializeObject(jsonContent),
                    Password = null,
                    Token = null,
                    Username = null,
                    IsOptCustomerSendEmail = false
                };

                var url = new Uri(proxyUrl);
                var response = await retryPolicy.ExecuteAsync(async () =>
                {
                    var requestHeader = BuildProxyRequestHeader();
                    var request = httpRequestFactory.BuildRequestMessage(null, HttpMethod.Post, url, requestHeader, BuildContentRequest(payload));
                    return await httpClientFactory.GetClient().SendAsync(request);
                });
                var responseBody = await response.Content.ReadAsStringAsync();
                logger?.LogInfo(CommonConstants.INFO, $"{responseBody}");
            }
        }

        public async Task<ServiceProductListResponse> GetServiceProductsByServiceAsync(string serviceId, Action<string> logFunction)
        {
            var baseUri = isProduction ? authentication.ProductionURL : authentication.SandboxURL;
            var uri = new Uri($"{baseUri.TrimEnd('/')}/v1/ServiceProduct?search.service_id={serviceId}&search.status={CommonConstants.SERVICE_PRODUCT_ACTIVE_STATUS}");
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));
            var client = httpClientFactory.GetClient();
            var requestHeader = BuildRequestHeader();
            var response = RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync().ExecuteAsync(async () =>
            {
                var request = httpRequestFactory.BuildRequestMessage(authentication, new HttpMethod(CommonConstants.METHOD_GET), uri, requestHeader);
                var responseAync = client.SendAsync(request);
                responseAync.Wait(CommonConstants.WAIT_IN_30_SECONDS);

                return responseAync.Result;
            });
            response.Wait(CommonConstants.WAIT_IN_30_SECONDS);
            var responseBody = await response.Result.Content.ReadAsStringAsync();
            if (response.Result.IsSuccessStatusCode)
            {
                return JsonConvert.DeserializeObject<ServiceProductListResponse>(responseBody);
            }

            var errors = JsonConvert.DeserializeObject<ServiceProductListResponse[]>(responseBody);
            return errors?.FirstOrDefault();
        }

        public async Task<DeviceChangeResult<string, CreateResponse>> AssignInventoryItemToService(AssignInventoryToServiceRequest assignInventoryToServiceRequest, Action<string> logFunction)
        {
            var baseUri = isProduction ? authentication.ProductionURL : authentication.SandboxURL;
            var uri = new Uri($"{baseUri.TrimEnd('/')}/v1/InventoryItem/assignService");
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));
            var requestContent = "";
            var requestHeader = BuildRequestHeader();

            var response = RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync().ExecuteAsync(async () =>
            {
                var content = BuildContentRequest(assignInventoryToServiceRequest);
                requestContent = content.ToString();
                var request = httpRequestFactory.BuildRequestMessage(authentication, new HttpMethod(CommonConstants.METHOD_PATCH), uri, requestHeader, content);
                var res = httpClientFactory.GetClient().SendAsync(request);
                res.Wait(CommonConstants.WAIT_IN_30_SECONDS);

                return res.Result;
            });
            response.Wait(CommonConstants.WAIT_IN_30_SECONDS);
            var responseBody = await response.Result.Content.ReadAsStringAsync();
            if (response.Result.IsSuccessStatusCode)
            {
                return new DeviceChangeResult<string, CreateResponse>()
                {
                    ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                    RequestObject = requestContent.ToString(),
                    HasErrors = false,
                    ResponseObject = JsonConvert.DeserializeObject<CreateResponse>(responseBody)
                };
            }

            var errors = JsonConvert.DeserializeObject<CreateResponse[]>(responseBody);
            return new DeviceChangeResult<string, CreateResponse>()
            {
                ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                RequestObject = requestContent.ToString(),
                HasErrors = true,
                ResponseObject = errors?.FirstOrDefault()
            };
        }

        public async Task<DeviceChangeResult<string, UpdateResponse>> UpdateServiceProductQuantityAndEffectiveDateAsync(int serviceProductId, int quantity, DateTime effectiveDate, Action<string> logFunction)
        {
            var baseUri = isProduction ? authentication.ProductionURL : authentication.SandboxURL;
            var uri = new Uri($"{baseUri.TrimEnd('/')}/v1/ServiceProduct/{serviceProductId}");
            var patch = new JsonPatchDocument();
            patch.Replace("/quantity", quantity);
            patch.Replace("/effective_date", effectiveDate);
            var json = JsonConvert.SerializeObject(patch);
            var requestHeader = BuildRequestHeader();
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));
            var response = RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync().ExecuteAsync(async () =>
            {
                var request = httpRequestFactory.BuildRequestMessage(authentication, new HttpMethod("PATCH"), uri,
                    requestHeader,
                    new StringContent(json, Encoding.UTF8, CommonConstants.APPLICATION_JSON_PATCH));
                var res = httpClientFactory.GetClient().SendAsync(request);
                res.Wait(CommonConstants.WAIT_IN_30_SECONDS);

                return res.Result;
            });
            response.Wait(CommonConstants.WAIT_IN_30_SECONDS);
            var responseBody = await response.Result.Content.ReadAsStringAsync();
            if (response.Result.IsSuccessStatusCode)
            {
                return new DeviceChangeResult<string, UpdateResponse>()
                {
                    ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                    RequestObject = json,
                    HasErrors = false,
                    ResponseObject = JsonConvert.DeserializeObject<UpdateResponse>(responseBody)
                };
            }

            var errors = JsonConvert.DeserializeObject<UpdateResponse[]>(responseBody);

            return new DeviceChangeResult<string, UpdateResponse>()
            {
                ActionText = $"{CommonConstants.METHOD_PATCH} {uri.AbsoluteUri}",
                RequestObject = json,
                HasErrors = true,
                ResponseObject = errors?.FirstOrDefault()
            };
        }

        public async Task<InventoryItemResponse> GetDIDsByServiceLine(string serviceId, Action<string> logFunction)
        {
            var baseUri = isProduction ? authentication.ProductionURL : authentication.SandboxURL;
            var uri = new Uri($"{baseUri.TrimEnd('/')}/v1/ServiceInventory?search.service_id={serviceId}");
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));
            var client = httpClientFactory.GetClient();
            var requestHeader = BuildRequestHeader();
            var response = RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync().ExecuteAsync(async () =>
            {
                var request = httpRequestFactory.BuildRequestMessage(authentication, new HttpMethod(CommonConstants.METHOD_GET), uri, requestHeader);
                var responseAync = client.SendAsync(request);
                responseAync.Wait(CommonConstants.WAIT_IN_30_SECONDS);

                return responseAync.Result;
            });
            response.Wait(CommonConstants.WAIT_IN_30_SECONDS);
            var responseBody = await response.Result.Content.ReadAsStringAsync();
            if (response.Result.IsSuccessStatusCode)
            {
                return JsonConvert.DeserializeObject<InventoryItemResponse>(responseBody);
            }

            var errors = JsonConvert.DeserializeObject<InventoryItemResponse[]>(responseBody);
            return errors?.FirstOrDefault();
        }

        public async Task<InventoryItemResponse> GetDIDsByCustomerId(string customerId, int pageSize, Action<string> logFunction)
        {
            var baseUri = isProduction ? authentication.ProductionURL : authentication.SandboxURL;
            var uri = new Uri($"{baseUri.TrimEnd('/')}/v1/ServiceInventory?search.customer_id={customerId}&search.page_size={pageSize}");
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, uri.AbsoluteUri));
            var client = httpClientFactory.GetClient();
            var requestHeader = BuildRequestHeader();
            var response = RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync().ExecuteAsync(async () =>
            {
                var request = httpRequestFactory.BuildRequestMessage(authentication, new HttpMethod(CommonConstants.METHOD_GET), uri, requestHeader);
                var responseAync = client.SendAsync(request);
                responseAync.Wait(CommonConstants.WAIT_IN_30_SECONDS);

                return responseAync.Result;
            });
            response.Wait(CommonConstants.WAIT_IN_30_SECONDS);
            var responseBody = await response.Result.Content.ReadAsStringAsync();
            if (response.Result.IsSuccessStatusCode)
            {
                return JsonConvert.DeserializeObject<InventoryItemResponse>(responseBody);
            }

            var errors = JsonConvert.DeserializeObject<InventoryItemResponse[]>(responseBody);
            return errors?.FirstOrDefault();
        }

        private Dictionary<string, string> BuildRequestHeader()
        {
            var requestHeader = new Dictionary<string, string>();
            requestHeader.Add(CommonConstants.ACCEPT, CommonConstants.APPLICATION_JSON);
            requestHeader.Add(CommonConstants.OCP_APIM_SUBSCRIPTION_KEY, authentication.APIKey);
            return requestHeader;
        }

        private StringContent BuildContentRequest(object parameter)
        {
            var json = JsonConvert.SerializeObject(parameter, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore
            });
            return new StringContent(json, null, CommonConstants.APPLICATION_JSON);
        }
        private Dictionary<string, string> BuildProxyRequestHeader()
        {
            var requestHeader = new Dictionary<string, string>();
            requestHeader.Add(CommonConstants.ACCEPT, CommonConstants.APPLICATION_JSON);
            requestHeader.Add(CommonConstants.ACCESS_TOKEN_HEADER_KEY, CommonConstants.AMOP_PROXY_ACCESS_TOKEN);
            return requestHeader;
        }

        public async Task<T> GetRevServicesAsync<T>(int serviceId, Action<string> logFunction) where T : class
        {
            var url = new Uri(string.Format(URLConstants.REV_GET_SERVICE_DETAIL_URL, serviceId));
            var response = await RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync().ExecuteAsync(async () =>
            {
                logFunction(string.Format(LogCommonStrings.REQUEST_URL, url.AbsoluteUri));
                var requestHeader = BuildRequestHeader();
                var request = httpRequestFactory.BuildRequestMessage(authentication, HttpMethod.Get, url, requestHeader);
                return await httpClientFactory.GetClient().SendAsync(request);
            });
            var responseBody = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                return JsonConvert.DeserializeObject<T>(responseBody);
            }
            logFunction(string.Format(LogCommonStrings.REQUEST_FAILED_RESPONSE_AT_FINAL_RETRIES, url.AbsoluteUri, responseBody));
            return null;
        }

        public async Task<DeviceChangeResult<string, UpdateResponse>> UpdateServiceCustomFieldAsync(int serviceId, Dictionary<int, string> fieldsToUpdate, Action<string> logFunction)
        {
            var url = new Uri(string.Format(URLConstants.REV_GET_SERVICE_DETAIL_URL, serviceId));
            logFunction(string.Format(LogCommonStrings.REQUEST_URL, url.AbsoluteUri));

            var json = "";

            foreach (var field in fieldsToUpdate)
            {
                var fieldPatch = new JsonPatchDocument();
                //-1 for Description which is in root level in Get Rev Service API Response
                if (field.Key == -1)
                {
                    fieldPatch.Replace($"/{CommonColumnNames.Description.ToLowerInvariant()}", field.Value);
                }
                else
                {
                    fieldPatch.Replace($"fields/{field.Key}/value", field.Value);
                }
                var jsonFieldPatch = JsonConvert.SerializeObject(fieldPatch);
                if (string.IsNullOrWhiteSpace(json))
                {
                    json = jsonFieldPatch;
                }
                else
                {
                    json = json.TrimEnd(']') + "," + jsonFieldPatch.TrimStart('[');
                }
            }

            var requestHeader = BuildRequestHeader();
            var response = await RetryPolicyHelper.PollyRetryRevIOHttpRequestAsync().ExecuteAsync(async () =>
            {
                var request = httpRequestFactory.BuildRequestMessage(authentication, new HttpMethod("PATCH"), url,
                    requestHeader,
                    new StringContent(json, Encoding.UTF8, CommonConstants.APPLICATION_JSON_PATCH));
                return await httpClientFactory.GetClient().SendAsync(request);
            });
            var responseBody = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                return new DeviceChangeResult<string, UpdateResponse>()
                {
                    ActionText = $"{CommonConstants.METHOD_PATCH} {url.AbsoluteUri}",
                    RequestObject = json,
                    HasErrors = false,
                    ResponseObject = JsonConvert.DeserializeObject<UpdateResponse>(responseBody)
                };
            }

            var errors = JsonConvert.DeserializeObject<UpdateResponse[]>(responseBody);

            return new DeviceChangeResult<string, UpdateResponse>()
            {
                ActionText = $"{CommonConstants.METHOD_PATCH} {url.AbsoluteUri}",
                RequestObject = json,
                HasErrors = true,
                ResponseObject = errors?.FirstOrDefault()
            };
        }
    }
}
