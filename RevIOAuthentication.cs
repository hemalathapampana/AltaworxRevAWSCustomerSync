using System;
using System.Text;
using Amop.Core.Services.Http;

namespace Altaworx.AWS.Core
{
    public class RevIOAuthentication : IHttpAuthentication
    {
        public int RevIOAuthenticationId { get; set; }
        public string BaseUrl { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }

        public string AuthorizationValue
        {
            get
            {
                var passwordRaw = Encoding.UTF8.GetString(Convert.FromBase64String(Password));
                string base64basic = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{Username}:{passwordRaw}"));
                return $"Basic {base64basic}";
            }
        }
    }
}
