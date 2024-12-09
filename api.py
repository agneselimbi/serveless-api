import boto3
from botocore.exceptions import ClientError
import json
import requests
from pprint import pprint

logger = logging.getLogger(__name__)

class ApigwtoSvce:
    """
    Contains all the functions needed to deploy a REST API and integrate with DynamoDb
    """

    def __init__(self,api_client):
        """
        :param api_client : a boto3 API Gateway client
        """
        self.api_client = api_client
        self.api_id= None
        self.root_id=None
        self.stage=None

    def create_rest_api(self,api_name,description):
        """
        Creates a REST API on API Gateway
        :param api_client : a boto3 API Gateway client
        :return api id of the newly created REST API
        """
        try:
            response = self.api_client.create_rest_api(
                name=api_name,
                description=description)
            self.api_id = response['id']
            logger.info(f"Created REST API {api_name} with id {self.api_id} ")
            
        except ClientError:
            logger.exception(f"Could not create REST API {api_name}")
            raise
        try:
            result = self.api_client.get_resources(restApiId=self.api_id)
            self.root_id = next(
                item for item in result["items"] if item["path"] == "/"
            )["id"]
        except ClientError:
            logger.exception("Couldn't get resources for API %s.", self.api_id)
            raise
        except StopIteration as err:
            logger.exception("No root resource found in API %s.", self.api_id)
            raise ValueError from err
        
        return self.api_id



    def add_rest_resource(self,parent_id,resource_path):
        """
        Adds a new resource to the REST API
        :param parent_id : the id of the parent resource 
        :param resource_path: path of the new resource 
        :return id of the new resource
        """

        try:
            response = self.api_client.create_resource(
                restApiId = self.api_id,
                parentId=parent_id,
                pathPart=resource_path
            )
            resource_id = response["id"]
            logger.info(f"Create resource with path {resource_path}")
        except ClientError:
            logger.exception(f"Could not create resource {resource_path}")
            raise
        else:
            return resource_id
        
    def add_integration_method(
        self,
        resource_id,
        rest_method,
        service_endpoint_prefix,
        service_action,
        service_method,
        role_arn,
        mapping_template,
    ):
        """
        Adds an integration method to a REST API. In the backend we have some DynamoDB running
        :param resource_id : id of the resource attached to the method
        :param rest method: action verb to be accomplished by the method e.g GET,PUT,POST ...
        :param service_endpoint_prefix: endpoint of the AWS service in the backend that is integrated with the method
        :param service_action: action that is called on the service 
        :param service_method: http method of the service request
        :param role_arn: role to be assumed by the api gateway to use a certain service 
        :param mapping_template: defines what transformation to operate on the request so we can communicate with the backend service

        """

        service_uri=(
            f"arn:aws:apigateway:{self.api_client.meta.region_name}:{service_endpoint_prefix}:action/{service_action}"
        )
        try:
            self.api_client.put_method(
                restApiId=self.api_id,
                resourceId = resource_id,
                httpMethod= rest_method,
                authorizationType='NONE'
            )
            self.api_client.put_method_response(
                restApiId=self.api_id,
                resourceId=resource_id,
                httpMethod=rest_method,
                statusCode='200',
                responseModels={"application/json": "Empty"} # need to define the response Model for this should i add a parameter
            )       
            logger.info(f"Created {rest_method} method for resource {resource_id}")
        except ClientError:
            logger.exception(f"Unable to create {rest_method} for resource {resource_id}")
            raise
        
        try:
            self.api_client.put_integration(
                restApiId=self.api_id,
                resourceId=resource_id,
                httpMethod=rest_method,
                type="AWS",
                integrationHttpMethod=service_method,
                credentials=role_arn, #we are actually going to assume this role to interact with the integration
                requestTemplates={"application/json": json.dumps(mapping_template)},
                uri=service_uri,
                passthroughBehavior='WHEN_NO_TEMPLATES',
    
            )

            self.api_client.put_integration_response(
                restApiId=self.api_id,
                resourceId=resource_id,
                httpMethod=rest_method,
                statusCode='200',
                responseTemplates={"application/json": ""}  # need to define the response Model for this should i add a parameter  
            )
            logger.info(f"Created  integration for resource {resource_id} to service url {service_uri}")
        except ClientError:
            logger.exception(f"Unable to create integration for resource {resource_id} to service url {service_uri}")
            raise

    def deploy_api(self,stage_name):
        """Deploys a REST API 
        :param stage_name : the stage of the API to deploy
        :return the Url of the deployed REST Api
        """
        try:
            self.api_client.create_deployment(
                restApiId= self.api_id, stageName=stage_name
            )
            self.stage=stage_name
            logger.info(f"Deployed stage {self.stage}")
        except ClientError:
            logger.exception(f"Could not deploy stage {self.stage}")
            raise
        else:
            return self.api_url()
        
    def api_url(self,resource=None):
        """
        Builds API from its parts
        :param resource : the resource path ot append to the base URL 
        :return the API of the specified resource 
        """
        url=(
            f"https://{self.api_id}.execute-api.{self.api_client.meta.region_name}"
            f".amazonaws.com/{self.stage}"
        )
        if resource is not None:
            url=f"{url}/{resource}"
        return url

if __name__=="__main__":
    boto3.setup_default_session(profile_name="serverless")
    role_name="dynamodb-read-access-serverless-role"
    rest_api_name="movies"
    table_name="Movies"
    description="api that displays movies and movies by year"
    gateway = ApigwtoSvce(boto3.client("apigateway"))
    role = boto3.resource("iam").Role(role_name)
    year = 1992
    print("Creating REST API in API Gateway.")
    gateway.create_rest_api(rest_api_name,description=description)
    print("Adding resources to the REST API.")
    getmovies= gateway.add_rest_resource(gateway.root_id, "getmovies")
    getmoviesbyYear = gateway.add_rest_resource(getmovies, "{year}")
                        
    # The DynamoDB service requires that all integration requests use POST.
    print("Adding integration methods to return all the movies in the DynamoDb.")
    gateway.add_integration_method(getmovies,
                                   "GET",
                                   "dynamodb",
                                   "Scan",
                                   "POST",
                                   role.arn,
                                    {"TableName": table_name})
    print("Adding integration methods to return all the movies by given year in the DynamoDb.")
    gateway.add_integration_method(getmoviesbyYear,
                                   "GET",
                                   "dynamodb",
                                   "Query",
                                   "POST",
                                   role.arn,
                                    {"TableName": table_name,
                                    "ExpressionAttributeValues":{':yr': {'N': "$method.request.path.year"} },
                                    "KeyConditionExpression" : 'releaseYear = :yr'
                                     })
    stage = "test"
    print(f"Deploying the {stage} stage.")
    gateway.deploy_api(stage)
    getmovies_url=gateway.api_url("getmovies")
    print("Getting the list of movies from the REST API.")
    returned_movies = requests.get(getmovies_url).json()
    pprint(returned_movies)
    
    print("Getting the list of movies by year from the REST API.")
    returned_movies_by_year = requests.get(f"{getmovies_url}/{year}").json()
    pprint(returned_movies_by_year)

