using Altaworx.AWS.Core.Models;
using Amop.Core.Constants;
using System;
using System.Data;
using System.Data.SqlTypes;
using static Altaworx.AWS.Core.RevIOCommon;

namespace AltaworxRevAWSCustomerSync
{
    public class RevCustomerSyncTable
    {
        public DataTable DataTable { get; }
        private bool _hasColumns;

        public RevCustomerSyncTable()
        {
            DataTable = new DataTable();
        }

        public void AddCustomerRow(RevCustomer customer, int integrationId)
        {
            if (!_hasColumns)
            {
                AddCustomerColumns();
                _hasColumns = true;
            }

            int? billCycleEndDay = null;
            int? billCycleEndHour = null;
            if (DateTime.TryParse(customer.RevFinance?.BillingCycleDate, out DateTime billCycleDate))
            {
                billCycleEndDay = billCycleDate.Day;
                billCycleEndHour = billCycleDate.Hour;
            }

            var dataRow = DataTable.NewRow();
            dataRow[CommonColumnNames.RevCustomerId] = customer.customer_id;
            dataRow[CommonColumnNames.CustomerName] = customer.service_address != null ? customer.service_address.company_name :
                customer.listing_address != null ? customer.listing_address.company_name : string.Empty;
            dataRow[CommonColumnNames.ParentCustomerId] = !string.IsNullOrWhiteSpace(customer.parent_customer_id) ? customer.parent_customer_id : null;
            dataRow[CommonColumnNames.IntegrationAuthenticationId] = integrationId;
            dataRow[CommonColumnNames.Status] = customer.status;
            dataRow[CommonColumnNames.ActivatedDate] = (customer.activated_date <= (DateTime)SqlDateTime.MinValue) ? null : customer.activated_date;
            dataRow[CommonColumnNames.CloseDate] = (customer.close_date <= (DateTime)SqlDateTime.MinValue) ? null : customer.close_date;
            dataRow[CommonColumnNames.TaxExemptEnabled] = customer.RevFinance?.TaxExemptEnabled;
            dataRow[CommonColumnNames.TaxExemptTypes] = customer.RevFinance?.TaxExemptType;
            dataRow[CommonColumnNames.BillProfileId] = customer.RevFinance?.BillProfileId;
            dataRow[CommonColumnNames.AgentId] = string.IsNullOrWhiteSpace(customer.agent_id) ? 0 : Int32.Parse(customer.agent_id);
            dataRow[CommonColumnNames.CustomerBillPeriodEndDay] = billCycleEndDay;
            dataRow[CommonColumnNames.CustomerBillPeriodEndHour] = billCycleEndHour;

            DataTable.Rows.Add(dataRow);
        }

        private void AddCustomerColumns()
        {
            DataTable.Columns.Add(CommonColumnNames.RevCustomerId);
            DataTable.Columns.Add(CommonColumnNames.CustomerName);
            DataTable.Columns.Add(CommonColumnNames.ParentCustomerId);
            DataTable.Columns.Add(CommonColumnNames.IntegrationAuthenticationId);
            DataTable.Columns.Add(CommonColumnNames.Status);
            DataTable.Columns.Add(CommonColumnNames.ActivatedDate);
            DataTable.Columns.Add(CommonColumnNames.CloseDate);
            DataTable.Columns.Add(CommonColumnNames.TaxExemptEnabled);
            DataTable.Columns.Add(CommonColumnNames.TaxExemptTypes);
            DataTable.Columns.Add(CommonColumnNames.BillProfileId);
            DataTable.Columns.Add(CommonColumnNames.AgentId);
            DataTable.Columns.Add(CommonColumnNames.CustomerBillPeriodEndDay);
            DataTable.Columns.Add(CommonColumnNames.CustomerBillPeriodEndHour);
        }
    }
}
