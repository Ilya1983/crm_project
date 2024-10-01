import grpc
import greet_pb2 as pb2
import greet_pb2_grpc as pb2_grpc

def run():
    # Open a gRPC channel to your server (adjust the host and port accordingly)
    with grpc.insecure_channel('localhost:5009') as channel:
        stub = pb2_grpc.CrmServerStub(channel)
        
        # Create an AddUserRequest object with test data
        request = pb2.AddUserRequest(fullName="test user", age=25)
        
        # Call the AddUser RPC method
        response = stub.AddUser(request)
        
        # Print the response
        print(f"AddUser response: success={response.success}")

if __name__ == '__main__':
    run()