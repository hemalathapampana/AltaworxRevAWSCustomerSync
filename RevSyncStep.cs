using System;
using System.Collections.Generic;
using System.Text;

namespace AltaworxRevAWSCustomerSync
{
    public enum RevSyncStep
    {
        None,
        Customer,
        BillProfile,
        Provider,
        Product,
        Service,
        ServiceProduct,
        InventoryType,
        InventoryItem,
        UsagePlanGroup,
        Agent,
        ProductType,
        ServiceType,
        Package,
        PackageProduct,
        Default
    }
}
