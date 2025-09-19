using System;
using System.Collections.Generic;
using System.Linq;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Models;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;

namespace Altaworx.AWS.Core
{
    public static class RevIOCommon
    {
        public class DeviceCustomerChargeQueueRecord
        {
            public long Id { get; set; }
            public string ICCID { get; set; }
            public string MSISDN { get; set; }
            public decimal UsageMB { get; set; }
            public decimal BaseRate { get; set; }
            public decimal _3GSurcharge { get; set; }
            public decimal PlanMB { get; set; }
            public string RatePlanCode { get; set; }
            public string RatePlanName { get; set; }
            public bool IsProcessed { get; set; }
            public int? ChargeId { get; set; }
            public decimal? ChargeAmount { get; set; }
            public DateTime? ModifiedDate { get; set; }
            public decimal RateChargeAmount { get; set; }
            public decimal? DisplayRate { get; set; }
            public decimal DataPerOverageCharge { get; set; }
            public decimal OverageRateCost { get; set; }
            public int? RevProductTypeId { get; set; }
            public bool HasErrors { get; set; }
            public string ErrorMessage { get; set; }
            public string RevServiceNumber { get; set; }
            public DateTime? BillingStartDate { get; set; }
            public DateTime? BillingEndDate { get; set; }
            public string Description { get; set; }
            public long SmsUsage { get; set; }
            public decimal SmsChargeAmount { get; set; }
            public int? SmsChargeId { get; set; }
            public int? SmsRevProductTypeId { get; set; }
            public decimal SmsRate { get; set; }
            //Rate charge + Overage
            public decimal DeviceCharge => ChargeAmount.GetValueOrDefault(0.0M);
            public bool IsBillInAdvance { get; set; }

            //public decimal OverageCharge
            //{
            //    get
            //    {
            //        // need to account for overage, but no overage rates are in the data
            //        if (UsageMB <= PlanMB)
            //        {
            //            return 0.0M;
            //        }

            //        return Math.Ceiling((UsageMB - PlanMB) / DataPerOverageCharge) * OverageRateCost;
            //    }
            //}
            public decimal CalculatedRateCharge { get; set; }
            public decimal CalculatedOverageCharge { get; set; }
            public decimal CalculatedBaseRate { get; set; }
            public int? OverageRevProductTypeId { get; set; }
            public int? ServiceProviderId { get; set; }
            public int? RevProductId { get; set; }
            public int? SmsRevProductId { get; set; }
            public int? OverageRevProductId { get; set; }
            public string RevAccountNumber { get; set; }

            public DeviceCustomerChargeQueueRecord() { }

            public DeviceCustomerChargeQueueRecord(SqlDataReader dataReader)
            {
                var columns = dataReader.GetColumnsFromReader();
                Id = dataReader.LongFromReader(columns, CommonColumnNames.Id);
                ICCID = dataReader.StringFromReader(columns, CommonColumnNames.ICCID);
                MSISDN = dataReader.StringFromReader(columns, CommonColumnNames.MSISDN);
                UsageMB = dataReader.DecimalFromReader(columns, CommonColumnNames.UsageMB);
                RatePlanCode = dataReader.StringFromReader(columns, CommonColumnNames.RatePlanCode);
                RatePlanName = dataReader.StringFromReader(columns, CommonColumnNames.RatePlanName);
                BaseRate = dataReader.DecimalFromReader(columns, CommonColumnNames.BaseRate);
                _3GSurcharge = dataReader.DecimalFromReader(columns, CommonColumnNames.Surcharge3G);
                PlanMB = dataReader.DecimalFromReader(columns, CommonColumnNames.PlanMB);
                IsProcessed = dataReader.BooleanFromReader(columns, CommonColumnNames.IsProcessed);
                ChargeId = dataReader.IntFromReader(columns, CommonColumnNames.ChargeId);
                ChargeAmount = dataReader.DecimalFromReader(columns, CommonColumnNames.ChargeAmount);
                ModifiedDate = dataReader.DateTimeFromReader(columns, CommonColumnNames.ModifiedDate, true);
                RateChargeAmount = dataReader.DecimalFromReader(columns, CommonColumnNames.RateChargeAmt);
                DisplayRate = dataReader.DecimalFromReader(columns, CommonColumnNames.DisplayRate);
                DataPerOverageCharge = dataReader.DecimalFromReader(columns, CommonColumnNames.DataPerOverageCharge);
                OverageRateCost = dataReader.DecimalFromReader(columns, CommonColumnNames.OverageRateCost);
                RevProductTypeId = dataReader.IntFromReader(columns, CommonColumnNames.RevProductTypeId);
                RevServiceNumber = dataReader.StringFromReader(columns, CommonColumnNames.RevServiceNumber);
                HasErrors = dataReader.BooleanFromReader(columns, CommonColumnNames.HasErrors);
                ErrorMessage = dataReader.StringFromReader(columns, CommonColumnNames.ErrorMessage);
                SmsRevProductTypeId = dataReader.IntFromReader(columns, CommonColumnNames.SmsRevProductTypeId);
                SmsChargeAmount = dataReader.DecimalFromReader(columns, CommonColumnNames.SmsChargeAmount);
                SmsChargeId = dataReader.IntFromReader(columns, CommonColumnNames.SmsChargeId);
                SmsRate = dataReader.DecimalFromReader(columns, CommonColumnNames.SmsRate);
                SmsUsage = dataReader.LongFromReader(columns, CommonColumnNames.SmsUsage);
                CalculatedBaseRate = dataReader.DecimalFromReader(columns, CommonColumnNames.BaseRateAmount);
                ServiceProviderId = dataReader.IntFromReader(columns, CommonColumnNames.ServiceProviderId);
                IsBillInAdvance = dataReader.BooleanFromReader(columns, CommonColumnNames.IsBillInAdvance);
                CalculatedOverageCharge = dataReader.DecimalFromReader(columns, CommonColumnNames.OverageChargeAmount);
                CalculatedRateCharge = dataReader.DecimalFromReader(columns, CommonColumnNames.RateChargeAmount);
                OverageRevProductTypeId = dataReader.IntFromReader(columns, CommonColumnNames.OverageRevProductTypeId);
                RevAccountNumber = dataReader.StringFromReader(columns, CommonColumnNames.RevAccountNumber);
            }
        }

        public class CreateDeviceChargeRequest
        {
            public CreateDeviceChargeRequest(DeviceCustomerChargeQueueRecord device, RevService serviceRecord,
                DateTime billingPeriodStart, DateTime billingPeriodEnd, TimeZoneInfo billingTimeZone, int integrationId, bool isSmsCharge = false, bool useNewLogicCustomerCharge = false, bool isOverageCharge = false, bool isRateCharge = false)
            {
                if (device != null && serviceRecord != null)
                {
                    if (useNewLogicCustomerCharge)
                    {
                        if (isSmsCharge)
                        {
                            Description = string.Format(LogCommonStrings.BILLABLE_SMS_USAGE, device.SmsUsage);
                            ProductAmount = device.SmsChargeAmount;
                            Amount = device.SmsChargeAmount;
                            Rate = device.SmsChargeAmount;
                            if (device.SmsRevProductId > 0)
                            {
                                ProductId = device.SmsRevProductId.Value;
                            }
                            else if (device.SmsRevProductTypeId > 0)
                            {
                                ProductTypeId = device.SmsRevProductTypeId.Value;
                            }
                        }
                        else if (isOverageCharge)
                        {
                            ProductAmount = device.CalculatedOverageCharge;
                            Amount = device.CalculatedOverageCharge;
                            Rate = device.CalculatedOverageCharge;
                            Description = string.Format(LogCommonStrings.OVERAGE_CHARGE_DATA_USAGE, device.UsageMB, Amount);
                            ProductId = 0;
                            ProductTypeId = 0;
                            if (device.OverageRevProductId > 0)
                            {
                                ProductId = device.OverageRevProductId.Value;
                            }
                            else if (device.OverageRevProductTypeId > 0)
                            {
                                ProductTypeId = device.OverageRevProductTypeId.Value;
                            }
                        }
                        else if (isRateCharge)
                        {
                            Description = $"{device.RatePlanName} - {device.UsageMB}";
                            ProductAmount = device.CalculatedRateCharge;
                            Amount = device.CalculatedRateCharge;
                            Rate = device.CalculatedRateCharge;
                            if (device.RevProductId > 0)
                            {
                                ProductId = device.RevProductId.Value;
                            }
                            else if (device.RevProductTypeId > 0)
                            {
                                ProductTypeId = device.RevProductTypeId.Value;
                            }
                        }
                    }
                    else
                    {
                        if (isSmsCharge)
                        {
                            if (device.SmsRevProductId > 0)
                            {
                                ProductId = device.SmsRevProductId.Value;
                            }
                            else if (device.SmsRevProductTypeId > 0)
                            {
                                ProductTypeId = device.SmsRevProductTypeId.Value;
                            }
                            Description = string.Format(LogCommonStrings.BILLABLE_SMS_USAGE, device.SmsUsage);
                            Amount = device.SmsChargeAmount;
                            ProductAmount = device.SmsChargeAmount;
                            Rate = device.SmsChargeAmount;
                        }
                        else
                        {
                            if (device.RevProductId > 0)
                            {
                                ProductId = device.RevProductId.Value;
                            }
                            else if (device.RevProductTypeId > 0)
                            {
                                ProductTypeId = device.RevProductTypeId.Value;
                            }
                            Description = string.Format(LogCommonStrings.BILLABLE_DATA_USAGE, device.UsageMB);
                            Amount = device.DeviceCharge;
                            ProductAmount = device.DeviceCharge;
                            Rate = device.DeviceCharge;
                        }
                    }

                    CustomerId = serviceRecord.CustomerId;
                    ServiceId = serviceRecord.ServiceId;
                    IsTaxIncluded = false;
                    Quantity = CommonConstants.DEFAULT_QUANTITY;
                    IsCommissionExempted = false;
                    Cost = CommonConstants.DEFAULT_COST;
                    IsProrated = false;
                    var billingPeriodDay = RevIOHelper.BuildBillingPeriodDay(integrationId, billingPeriodStart, billingPeriodEnd, device.IsBillInAdvance);
                    StartDate = billingPeriodDay.Item1;
                    EndDate = billingPeriodDay.Item2;
                }
                else
                {
                    throw new ArgumentException(LogCommonStrings.DEVICE_AND_REV_SERVICE_CANNOT_BE_NULL_OR_EMPTY);
                }
            }

            public CreateDeviceChargeRequest(DeviceCustomerChargeQueueRecord device, RevService serviceRecord, int integrationId, bool isSmsCharge = false, bool useNewLogicCustomerCharge = false, bool isOverageCharge = false, bool isRateCharge = false)
            {
                if (device != null && serviceRecord != null)
                {
                    if (useNewLogicCustomerCharge)
                    {
                        if (isSmsCharge)
                        {
                            ProductAmount = device.SmsChargeAmount;
                            Amount = device.SmsChargeAmount;
                            Rate = device.SmsChargeAmount;
                            if (device.SmsRevProductId > 0)
                            {
                                ProductId = device.SmsRevProductId.Value;
                            }
                            else if (device.SmsRevProductTypeId > 0)
                            {
                                ProductTypeId = device.SmsRevProductTypeId.Value;
                            }
                        }
                        else if (isOverageCharge)
                        {
                            ProductAmount = device.CalculatedOverageCharge;
                            Amount = device.CalculatedOverageCharge;
                            Rate = device.CalculatedOverageCharge;
                            ProductId = 0;
                            ProductTypeId = 0;
                            if (device.OverageRevProductId > 0)
                            {
                                ProductId = device.OverageRevProductId.Value;
                            }
                            else if (device.OverageRevProductTypeId > 0)
                            {
                                ProductTypeId = device.OverageRevProductTypeId.Value;
                            }
                        }
                        else if (isRateCharge)
                        {
                            ProductAmount = device.CalculatedRateCharge;
                            Amount = device.CalculatedRateCharge;
                            Rate = device.CalculatedRateCharge;
                            if (device.RevProductId > 0)
                            {
                                ProductId = device.RevProductId.Value;
                            }
                            else if (device.RevProductTypeId > 0)
                            {
                                ProductTypeId = device.RevProductTypeId.Value;
                            }
                        }
                    }
                    else
                    {
                        if (isSmsCharge)
                        {
                            if (device.SmsRevProductId > 0)
                            {
                                ProductId = device.SmsRevProductId.Value;
                            }
                            else if (device.SmsRevProductTypeId > 0)
                            {
                                ProductTypeId = device.SmsRevProductTypeId.Value;
                            }
                            Amount = device.SmsChargeAmount;
                            ProductAmount = device.SmsChargeAmount;
                            Rate = device.SmsChargeAmount;

                        }
                        else
                        {
                            if (device.RevProductId > 0)
                            {
                                ProductId = device.RevProductId.Value;
                            }
                            else if (device.RevProductTypeId > 0)
                            {
                                ProductTypeId = device.RevProductTypeId.Value;
                            }
                            Amount = device.DeviceCharge;
                            ProductAmount = device.DeviceCharge;
                            Rate = device.DeviceCharge;
                        }
                    }
                    CustomerId = serviceRecord.CustomerId;
                    ServiceId = serviceRecord.ServiceId;
                    IsTaxIncluded = false;
                    Description = device.Description;
                    Quantity = CommonConstants.DEFAULT_QUANTITY;
                    IsCommissionExempted = false;
                    Cost = CommonConstants.DEFAULT_COST;
                    IsProrated = false;
                    var billingPeriodDay = RevIOHelper.BuildBillingPeriodDay(integrationId, device.BillingStartDate, device.BillingEndDate, device.IsBillInAdvance);
                    StartDate = billingPeriodDay.Item1;
                    EndDate = billingPeriodDay.Item2;
                }
                else
                {
                    throw new ArgumentException(LogCommonStrings.DEVICE_AND_REV_SERVICE_CANNOT_BE_NULL_OR_EMPTY);
                }
            }

            [JsonProperty(PropertyName = "product_id")]
            public int ProductId { get; set; }

            [JsonProperty(PropertyName = "product_type_id")]
            public int ProductTypeId { get; set; }

            [JsonProperty(PropertyName = "customer_id")]
            public int CustomerId { get; set; }

            [JsonProperty(PropertyName = "service_id")]
            public int ServiceId { get; set; }

            [JsonProperty(PropertyName = "is_tax_included")]
            public bool IsTaxIncluded { get; set; }

            [JsonProperty(PropertyName = "description")]
            public string Description { get; set; }

            [JsonProperty(PropertyName = "amount")]
            public decimal Amount { get; set; }

            [JsonProperty(PropertyName = "product_amount")]
            public decimal ProductAmount { get; set; }

            [JsonProperty(PropertyName = "quantity")]
            public int Quantity { get; set; }

            [JsonProperty(PropertyName = "is_commission_exempted")]
            public bool IsCommissionExempted { get; set; }

            [JsonProperty(PropertyName = "cost")]
            public decimal Cost { get; set; }

            [JsonProperty(PropertyName = "rate")]
            public decimal Rate { get; set; }

            [JsonProperty(PropertyName = "is_prorated")]
            public bool IsProrated { get; set; }

            [JsonProperty(PropertyName = "start_date")]
            public string StartDate { get; set; }

            [JsonProperty(PropertyName = "end_date")]
            public string EndDate { get; set; }
        }

        public class RevResponseRootObjectBase
        {
            public bool ok { get; set; }
            public bool has_more { get; set; }
            public bool record_count { get; set; }
        }

        public class RevCustomerList : RevResponseRootObjectBase
        {
            public List<RevCustomer> records { get; set; }
        }

        public class RevCustomer
        {
            public string customer_id { get; set; }
            public string parent_customer_id { get; set; }
            public string status { get; set; }
            public DateTime? activated_date { get; set; }
            public DateTime? close_date { get; set; }
            public RevAddress listing_address { get; set; }
            public RevAddress service_address { get; set; }
            [JsonProperty("finance")]
            public RevFinance RevFinance { get; set; }
            public string agent_id { get; set; }
        }

        public class RevBillProfileList : RevResponseRootObjectBase
        {
            public List<RevBillProfile> records { get; set; }
        }

        public class RevBillProfile
        {
            public string bill_profile_id { get; set; }
            public string description { get; set; }
            public bool active { get; set; }
        }

        public class RevProviderList : RevResponseRootObjectBase
        {
            public List<RevProvider> records { get; set; }
        }

        public class RevProvider
        {
            public string provider_id { get; set; }
            public string description { get; set; }
            public string provider_code { get; set; }
            public bool active { get; set; }
            public DateTime? created_date { get; set; }
            public int? bill_profile_id { get; set; }
            public RevProviderOrderTypes order_types { get; set; }
        }

        public class RevProviderOrderTypes
        {
            public bool cnam { get; set; }
            public bool conversion { get; set; }
            public bool deny { get; set; }
            public bool disconnect { get; set; }
            public bool e911 { get; set; }
            public bool ld_block { get; set; }
            public bool port { get; set; }
            public bool restore { get; set; }
            public bool transfer { get; set; }
        }

        public class RevServiceTypeList : RevResponseRootObjectBase
        {
            public List<RevServiceType> records { get; set; }
        }

        public class RevServiceType
        {
            public string service_type_id { get; set; }
            public string description { get; set; }
            public bool active { get; set; }
        }

        public class RevServiceList
        {
            [JsonProperty(PropertyName = "ok")]
            public bool OK { get; set; }

            [JsonProperty(PropertyName = "has_more")]
            public bool HasMore { get; set; }

            [JsonProperty(PropertyName = "record_count")]
            public int RecordCount { get; set; }

            [JsonProperty(PropertyName = "records")]
            public List<RevService> Records { get; set; }
            [JsonProperty(PropertyName = "status_code")]
            public int StatusCode { get; set; }
        }

        public class RevService
        {
            [JsonProperty(PropertyName = "number")]
            public string Number { get; set; }

            [JsonProperty(PropertyName = "service_type_id")]
            public int ServiceTypeId { get; set; }

            [JsonProperty(PropertyName = "service_id")]
            public int ServiceId { get; set; }

            [JsonProperty(PropertyName = "customer_id")]
            public int CustomerId { get; set; }

            [JsonProperty(PropertyName = "provider_id")]
            public int? ProviderId { get; set; }

            [JsonProperty(PropertyName = "activated_date")]
            public string ActivatedDate { get; set; }

            [JsonProperty(PropertyName = "disconnected_date")]
            public string DisconnectedDate { get; set; }

            [JsonProperty(PropertyName = "fields")]
            public List<RevServiceField> Fields { get; set; }
            [JsonProperty(PropertyName = "description")]
            public string Description { get; set; }
        }

        public class RevServiceField
        {
            [JsonProperty(PropertyName = "field_id")]
            public int FieldId { get; set; }

            [JsonProperty(PropertyName = "label")]
            public string Label { get; set; }

            [JsonProperty(PropertyName = "value")]
            public string Value { get; set; }
        }

        public class RevProductTypeList : RevResponseRootObjectBase
        {
            public List<RevProductType> records { get; set; }
        }

        public class RevProductType
        {
            public int product_type_id { get; set; }
            public string product_type_code { get; set; }
            public string description { get; set; }
            public string tax_class_id { get; set; }
            public bool active { get; set; }
        }

        public class RevProductResponseRootObject : RevResponseRootObjectBase
        {
            [JsonProperty(PropertyName = "records")]
            public List<RevProduct> Records { get; set; }
        }

        public class RevProduct
        {
            [JsonProperty(PropertyName = "product_id")]
            public int ProductId { get; set; }

            [JsonProperty(PropertyName = "product_type_id")]
            public int? ProductTypeId { get; set; }

            [JsonProperty(PropertyName = "description")]
            public string Description { get; set; }

            [JsonProperty(PropertyName = "code_1")]
            public string Code1 { get; set; }

            [JsonProperty(PropertyName = "code_2")]
            public string Code2 { get; set; }

            [JsonProperty(PropertyName = "rate")]
            public decimal? Rate { get; set; }

            [JsonProperty(PropertyName = "cost")]
            public decimal? Cost { get; set; }

            [JsonProperty(PropertyName = "buy_rate")]
            public decimal? BuyRate { get; set; }

            [JsonProperty(PropertyName = "created_date")]
            public DateTime? CreatedDate { get; set; }

            [JsonProperty(PropertyName = "created_by")]
            public int? CreatedBy { get; set; }

            [JsonProperty(PropertyName = "active")]
            public bool Active { get; set; }

            [JsonProperty(PropertyName = "creates_order")]
            public bool CreatesOrder { get; set; }

            [JsonProperty(PropertyName = "provider_id")]
            public int? ProviderId { get; set; }

            [JsonProperty(PropertyName = "bills_in_arrears")]
            public bool BillsInArrears { get; set; }

            [JsonProperty(PropertyName = "prorates")]
            public bool Prorates { get; set; }

            [JsonProperty(PropertyName = "customer_class")]
            public string CustomerClass { get; set; }

            [JsonProperty(PropertyName = "long_description")]
            public string LongDescription { get; set; }

            [JsonProperty(PropertyName = "ledger_code")]
            public string LedgerCode { get; set; }

            [JsonProperty(PropertyName = "free_months")]
            public int? FreeMonths { get; set; }

            [JsonProperty(PropertyName = "automatic_expiration_months")]
            public int? AutomaticExpirationMonths { get; set; }

            [JsonProperty(PropertyName = "order_completion_billing")]
            public bool OrderCompletionBilling { get; set; }

            [JsonProperty(PropertyName = "tax_class_id")]
            public string TaxClassId { get; set; }

            [JsonProperty(PropertyName = "wholesale_description")]
            public string WholesaleDescription { get; set; }

            [JsonProperty(PropertyName = "fields")]
            public List<CustomeFields> Fields { get; set; }
        }

        public class CustomeFields
        {
            [JsonProperty(PropertyName = "field_id")]
            public string FieldId { get; set; }

            [JsonProperty(PropertyName = "label")]
            public string Label { get; set; }

            [JsonProperty(PropertyName = "value")]
            public string Value { get; set; }
        }

        public class RevServiceProductResponseRootObject : RevResponseRootObjectBase
        {
            [JsonProperty(PropertyName = "records")]
            public List<RevServiceProduct> Records { get; set; }
        }

        public class RevServiceProduct
        {
            [JsonProperty(PropertyName = "service_product_id")]
            public int ServiceProductId { get; set; }

            [JsonProperty(PropertyName = "customer_id")]
            public int? CustomerId { get; set; }

            [JsonProperty(PropertyName = "product_id")]
            public int? ProductId { get; set; }

            [JsonProperty(PropertyName = "package_id")]
            public int? PackageId { get; set; }

            [JsonProperty(PropertyName = "service_id")]
            public int? ServiceId { get; set; }

            [JsonProperty(PropertyName = "description")]
            public string Description { get; set; }

            [JsonProperty(PropertyName = "code_1")]
            public string Code1 { get; set; }

            [JsonProperty(PropertyName = "code_2")]
            public string Code2 { get; set; }

            [JsonProperty(PropertyName = "rate")]
            public decimal? Rate { get; set; }

            [JsonProperty(PropertyName = "billed_through_date")]
            public DateTime? BilledThroughDate { get; set; }

            [JsonProperty(PropertyName = "canceled_date")]
            public DateTime? CanceledDate { get; set; }

            [JsonProperty(PropertyName = "status")]
            public string Status { get; set; }

            [JsonProperty(PropertyName = "status_date")]
            public DateTime? StatusDate { get; set; }

            [JsonProperty(PropertyName = "status_user_id")]
            public int? StatusUserId { get; set; }

            [JsonProperty(PropertyName = "activated_date")]
            public DateTime? ActivatedDate { get; set; }

            [JsonProperty(PropertyName = "cost")]
            public decimal? Cost { get; set; }

            [JsonProperty(PropertyName = "wholesale_description")]
            public string WholesaleDescription { get; set; }

            [JsonProperty(PropertyName = "free_start_date")]
            public DateTime? FreeStartDate { get; set; }

            [JsonProperty(PropertyName = "free_end_date")]
            public DateTime? FreeEndDate { get; set; }

            [JsonProperty(PropertyName = "quantity")]
            public int? Quantity { get; set; }

            [JsonProperty(PropertyName = "contract_start_date")]
            public DateTime? ContractStartDate { get; set; }

            [JsonProperty(PropertyName = "contract_end_date")]
            public DateTime? ContractEndDate { get; set; }

            [JsonProperty(PropertyName = "created_date")]
            public DateTime? CreatedDate { get; set; }

            [JsonProperty(PropertyName = "tax_included")]
            public bool TaxIncluded { get; set; }

            [JsonProperty(PropertyName = "group_on_bill")]
            public bool GroupOnBill { get; set; }

            [JsonProperty(PropertyName = "itemized")]
            public bool Itemized { get; set; }
        }

        public class RevAddress
        {
            public string address_id { get; set; }
            public string company_name { get; set; }
        }

        public class RevFinance
        {
            [JsonProperty("tax_exempt_enabled")]
            public bool TaxExemptEnabled { get; set; }
            [JsonProperty("tax_exempt_types")]
            public string TaxExemptType { get; set; }
            [JsonProperty("bill_profile_id")]
            public int? BillProfileId { get; set; }
            [JsonProperty("cycle_date")]
            public string BillingCycleDate { get; set; }
        }

        public class RevInventoryType
        {
            [JsonProperty(PropertyName = "inventory_type_id")]
            public int InventoryTypeId { get; set; }

            [JsonProperty(PropertyName = "category")]
            public string Category { get; set; }

            [JsonProperty(PropertyName = "identifier")]
            public string Identifier { get; set; }

            [JsonProperty(PropertyName = "name")]
            public string Name { get; set; }

            [JsonProperty(PropertyName = "requires_product")]
            public bool RequiresProduct { get; set; }

            [JsonProperty(PropertyName = "description")]
            public string Description { get; set; }

            [JsonProperty(PropertyName = "status")]
            public string Status { get; set; }

            [JsonProperty(PropertyName = "format")]
            public string Format { get; set; }

            [JsonProperty(PropertyName = "ratable")]
            public bool Ratable { get; set; }
        }

        public class RevInventoryTypeResponseRootObject : RevResponseRootObjectBase
        {
            [JsonProperty(PropertyName = "records")]
            public List<RevInventoryType> Records { get; set; }
        }

        public class RevInventoryItem
        {
            [JsonProperty(PropertyName = "inventory_item_id")]
            public int InventoryItemId { get; set; }

            [JsonProperty(PropertyName = "inventory_type_id")]
            public int InventoryTypeId { get; set; }

            [JsonProperty(PropertyName = "identifier")]
            public string Identifier { get; set; }

            [JsonProperty(PropertyName = "customer_id")]
            public int? CustomerId { get; set; }

            private DateTime? _createdDate;
            [JsonProperty(PropertyName = "created_date")]
            public DateTime? CreatedDate
            {
                get => _createdDate;
                set
                {
                    if (value.Equals(DateTime.MinValue))
                    {
                        _createdDate = null;
                    }
                    else
                    {
                        _createdDate = value;
                    }
                }
            }

            [JsonProperty(PropertyName = "created_by")]
            public int? CreatedBy { get; set; }

            [JsonProperty(PropertyName = "status")]
            public string Status { get; set; }

            [JsonProperty(PropertyName = "inventory_item_unavailable_reason_id")]
            public int? InventoryItemUnavailableReasonId { get; set; }
        }

        public class RevInventoryItemResponseRootObject : RevResponseRootObjectBase
        {
            [JsonProperty(PropertyName = "records")]
            public List<RevInventoryItem> Records { get; set; }
        }

        public class RevUsagePlanGroup
        {
            [JsonProperty(PropertyName = "usage_plan_group_id")]
            public int UsagePlanGroupId { get; set; }

            [JsonProperty(PropertyName = "description")]
            public string Description { get; set; }

            [JsonProperty(PropertyName = "long_description")]
            public string LongDescription { get; set; }

            [JsonProperty(PropertyName = "active")]
            public bool Active { get; set; }

            [JsonProperty(PropertyName = "created_date")]
            public DateTime RevCreatedDate { get; set; }

            [JsonProperty(PropertyName = "created_by")]
            public int RevCreatedBy { get; set; }
        }

        public class RevAgent
        {
            [JsonProperty(PropertyName = "agent_id")]
            public int AgentId { get; set; }

            [JsonProperty(PropertyName = "parent_agent_id")]
            public int ParentAgentId { get; set; }

            [JsonProperty(PropertyName = "name")]
            public string AgentName { get; set; }

            [JsonProperty(PropertyName = "status")]
            public string Status { get; set; }

        }

        // This is non-standard but we are using snake_case here so that our JSON properties directly match the Rev.io API response variables which returns variables in snake_case
        public class RevPackage
        {
            [JsonProperty(PropertyName = "package_id")]
            public int PackageId { get; set; }

            [JsonProperty(PropertyName = "provider_id")]
            public int ProviderId { get; set; }

            [JsonProperty(PropertyName = "currency_code")]
            public string CurrencyCode { get; set; }

            [JsonProperty(PropertyName = "description")]
            public string Description { get; set; }

            [JsonProperty(PropertyName = "description_on_bill")]
            public string DescriptionOnBill { get; set; }

            [JsonProperty(PropertyName = "long_description")]
            public string LongDescription { get; set; }

            [JsonProperty(PropertyName = "created_date")]
            public DateTime CreatedDate { get; set; }

            [JsonProperty(PropertyName = "created_by")]
            public int? CreatedBy { get; set; }

            [JsonProperty(PropertyName = "active")]
            public bool Active { get; set; }

            [JsonProperty(PropertyName = "usage_plan_group_id")]
            public int UsagePlanGroupId { get; set; }

            [JsonProperty(PropertyName = "service_type_id")]
            public int? ServiceTypeId { get; set; }

            [JsonProperty(PropertyName = "package_category_id")]
            public int? PackageCategoryId { get; set; }

            [JsonProperty(PropertyName = "exempt_from_spiff_commission")]
            public bool ExemptFromSpiffCommission { get; set; }

            [JsonProperty(PropertyName = "restrict_class_flag")]
            public bool RestrictClassFlag { get; set; }

            [JsonProperty(PropertyName = "class")]
            public string Class { get; set; }

            [JsonProperty(PropertyName = "restrict_bill_profile_flag")]
            public bool RestrictBillProfileFlag { get; set; }

            [JsonProperty("bill_profile_id")]
            public int? BillProfileId { get; set; }

        }

        // This is non-standard but we are using snake_case here so that our JSON properties directly match the Rev.io API response variables which returns variables in snake_case
        public class RevPackageProduct
        {
            [JsonProperty(PropertyName = "package_product_id")]
            public int PackageProductId { get; set; }

            [JsonProperty(PropertyName = "product_id")]
            public int ProductId { get; set; }

            [JsonProperty(PropertyName = "package_id")]
            public int PackageId { get; set; }

            [JsonProperty(PropertyName = "description")]
            public string Description { get; set; }

            [JsonProperty(PropertyName = "code_1")]
            public string PrimaryProvisioningCode { get; set; }

            [JsonProperty(PropertyName = "code_2")]
            public string SecondaryProvisioningCode { get; set; }

            [JsonProperty(PropertyName = "rate")]
            public decimal? Rate { get; set; }

            [JsonProperty(PropertyName = "cost")]
            public decimal? Cost { get; set; }

            [JsonProperty(PropertyName = "buy_rate")]
            public decimal? BuyRate { get; set; }

            [JsonProperty(PropertyName = "quantity")]
            public int? Quantity { get; set; }

            [JsonProperty(PropertyName = "tax_included")]
            public bool TaxIncluded { get; set; }

            [JsonProperty(PropertyName = "group_on_bill")]
            public bool GroupOnBill { get; set; }

            [JsonProperty(PropertyName = "itemize")]
            public bool Itemized { get; set; }

            [JsonProperty(PropertyName = "credit")]
            public bool Credit { get; set; }
        }

        public class RevUsagePlanGroupResponseRootObject : RevResponseRootObjectBase
        {
            [JsonProperty(PropertyName = "records")]
            public List<RevUsagePlanGroup> Records { get; set; }
        }

        public class RevAgentResponseRootObject : RevResponseRootObjectBase
        {
            [JsonProperty(PropertyName = "records")]
            public List<RevAgent> Records { get; set; }
        }

        public class RevPackageList : RevResponseRootObjectBase
        {
            [JsonProperty(PropertyName = "records")]
            public List<RevPackage> Records { get; set; }
        }

        public class RevPackageProductList : RevResponseRootObjectBase
        {
            [JsonProperty(PropertyName = "records")]
            public List<RevPackageProduct> Records { get; set; }
        }

        // The Json property name is based on Rev.IO response model which is in lowercase with underscore so the casing is expected
        public class RevListResponse<T>
        {
            [JsonProperty(PropertyName = "ok")]
            public bool OK { get; set; }

            [JsonProperty(PropertyName = "has_more")]
            public bool HasMore { get; set; }

            [JsonProperty(PropertyName = "record_count")]
            public int RecordCount { get; set; }

            [JsonProperty(PropertyName = "records")]
            public List<T> Records { get; set; }
        }

    }
}
