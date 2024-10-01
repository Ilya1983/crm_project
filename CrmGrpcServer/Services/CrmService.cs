using CrmGrpcServer;
using Grpc.Core;
using Microsoft.Rest;
using ServiceReference;
using System.ServiceModel;

namespace CrmGrpcServer.Services
{
    public class CrmService : CrmServer.CrmServerBase
    {
        private readonly ILogger<CrmService> _logger;
        private readonly AddUserServiceClient _client;


        public CrmService(ILogger<CrmService> logger)
        {
            _logger = logger;
            BasicHttpBinding binding = new BasicHttpBinding();
            EndpointAddress endpoint = new EndpointAddress("http://localhost:54402/Service.svc");
            _client = new AddUserServiceClient(binding, endpoint);
        }

        public override Task<AddUserReply> AddUser(AddUserRequest request, ServerCallContext context)
        {
            bool success = _client.AddUserAsync(request.FullName, request.Age).Result;

            return Task.FromResult(new AddUserReply()
            {
                Success = success,
            });
        }
    }
}