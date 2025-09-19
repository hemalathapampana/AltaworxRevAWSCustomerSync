using System;
using System.Collections.Generic;
using System.Text;
using Amop.Core.Models;

namespace Amop.Core.Helpers
{
    public class RevIOHelper
    {
        public static Tuple<string, string> BuildBillingPeriodDay(int? integragionId, DateTime? BillingStartDate, DateTime? BillingEndDate, bool IsBillInAdvance = false)
        {
            var billingStartDate = new DateTime();
            var billingEndDate = new DateTime();
            if (integragionId != null && integragionId.Value > 0)
            {
                switch (integragionId.Value)
                {
                    case (int)IntegrationType.Pond:
                        billingStartDate = BillingStartDate.GetValueOrDefault();
                        billingEndDate = BillingEndDate.GetValueOrDefault();
                        break;
                    case (int)IntegrationType.Telegence:
                        billingStartDate = BillingStartDate.GetValueOrDefault();
                        billingEndDate = BillingEndDate.GetValueOrDefault().AddDays(-1);
                        break;
                    default:
                        billingStartDate = BillingStartDate.GetValueOrDefault().AddDays(1);
                        billingEndDate = BillingEndDate.GetValueOrDefault();
                        break;
                }
                if (IsBillInAdvance)
                {
                    billingStartDate = billingStartDate.AddMonths(1);
                    billingEndDate = billingEndDate.AddMonths(1);
                }
            }
            return new Tuple<string, string>(billingStartDate.ToString("yyyy-M-d"), billingEndDate.ToString("yyyy-M-d"));
        }
    }
}
