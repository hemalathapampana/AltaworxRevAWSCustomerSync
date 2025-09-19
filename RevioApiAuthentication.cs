using System;
using Amop.Core.Services.Base64Service;
using Amop.Core.Services.Http;

namespace Amop.Core.Models.Revio
{
    public class RevioApiAuthentication : IHttpAuthentication
    {
        private readonly IBase64Service base64Service;
        private string password;
        private string apiKey { get; set; }


        public RevioApiAuthentication(IBase64Service base64Service)
        {
            this.base64Service = base64Service;
        }

        public int IntegrationAuthenticationId { get; set; }
        public string BaseUrl { get; set; }
        public string Username { get; set; }
        public string ProductionURL { get; set; }
        public string SandboxURL { get; set; }
        public string RevBillProfile { get; set; }

        public string Password
        {
            get
            {
                if (password != null)
                {
                    try
                    {
                        return base64Service.Base64Decode(password);
                    }
                    catch (FormatException)
                    {
                        // intentionally left blank
                    }
                }

                return password;
            }
            set
            {
                password = value;
            }
        }

        public string AuthorizationValue => $"Basic {base64Service.Base64Encode($"{Username}:{Password}")}";
        public string APIKey
        {
            get
            {
                if (!string.IsNullOrWhiteSpace(apiKey))
                {
                    return base64Service.Base64Decode(apiKey);
                }
                return apiKey;
            }
            set
            {
                apiKey = value;
            }
        }
    }
}
