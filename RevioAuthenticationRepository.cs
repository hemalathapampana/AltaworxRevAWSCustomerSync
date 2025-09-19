using System;
using System.Data;
using Microsoft.Data.SqlClient;
using Amop.Core.Logger;
using Amop.Core.Repositories.Revio;
using Amop.Core.Services.Base64Service;
using Amop.Core.Models.Revio;

namespace Altaworx.AWS.Core.Repositories.RevIo
{
    public class RevIoAuthenticationRepository : RevioAuthenticationRepository, IRevIoAuthenticationRepository
    {
        private readonly string _connectionString;

        public RevIoAuthenticationRepository(IKeysysLogger logger, IBase64Service base64Service, string connectionString) :
            base(connectionString, base64Service, logger)
        {
            _connectionString = connectionString;
        }

        public new RevioApiAuthentication GetRevioApiAuthentication(int integrationAuthenticationId)
        {
            return base.GetRevioApiAuthentication(integrationAuthenticationId);
        }

        public int GetNextRevIoAuthenticationId(int currentIntegrationAuthenticationId)
        {
            int nextAuthId = 0;

            try
            {
                using (var Conn = new SqlConnection(_connectionString))
                {
                    using (var Cmd = new SqlCommand("usp_Rev_Get_Next_Authentication", Conn))
                    {
                        Cmd.CommandType = CommandType.StoredProcedure;
                        Cmd.Parameters.AddWithValue("@integrationAuthenticationId", currentIntegrationAuthenticationId);
                        Conn.Open();

                        nextAuthId = (int)Cmd.ExecuteScalar();

                        Conn.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.Message);
            }

            return nextAuthId;
        }
    }
}
