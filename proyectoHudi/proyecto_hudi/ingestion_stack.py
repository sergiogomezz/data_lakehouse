from constructs import Construct
from aws_cdk import (
    Duration,
    Stack,
    RemovalPolicy,
    # core,
    aws_iam as _iam,
    aws_sqs as _sqs,
    aws_iot as _iot,
    aws_lambda as _lambda,
    aws_dynamodb as _dynamodb,
    aws_lambda_event_sources as aws_lambda_event_sources
)
from aws_cdk.aws_iot import CfnTopicRule
import aws_cdk as cdk

# This stack creates the first part of the project, up to storage in DynamoDB

class IngestionStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create SQS queue
        queue = _sqs.Queue(
            self, "ProyectoHudiQueue",
            queue_name='ProyectoHudiQueue',
            visibility_timeout=Duration.seconds(300),
        )

        # Create a DynamoDB table
        telemetry_data_table = _dynamodb.Table(
            self, "TelemetryDataTable",
            table_name='TelemetryDataTable',
            partition_key=_dynamodb.Attribute(
                name="device_id",
                type=_dynamodb.AttributeType.STRING
            ),
            sort_key=_dynamodb.Attribute(
                name="timestamp",
                type=_dynamodb.AttributeType.STRING
            ),
            billing_mode=_dynamodb.BillingMode.PAY_PER_REQUEST,  # Adjust billing mode as needed
            removal_policy=RemovalPolicy.DESTROY
        )

        # Export table information to make it accessible from other stacks
        # cdk.CfnOutput(self, 'TableArn',
        #             value=telemetry_data_table.table_arn,
        #             export_name='TableArn')
        # cdk.CfnOutput(self, 'TableName',
        #             value=telemetry_data_table.table_name,
        #             export_name='TableName')

        # Create role for Lambda
        lambda_role = _iam.Role(
            self, 'LambdaExecutionRole',
            role_name='LambdaRole',
            description='Role for Lambda to be able to send data to a DynamoDB instance',
            assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"),
            inline_policies={
                'LambdaDynamoDBPolicy': _iam.PolicyDocument(
                    statements=[
                        _iam.PolicyStatement(
                            actions=[
                                'dynamodb:PutItem',
                                'dynamodb:GetItem',
                                'dynamodb:Query',
                                'dynamodb:Scan',
                                'dynamodb:UpdateItem',
                                'dynamodb:DeleteItem'
                            ],
                            effect=_iam.Effect.ALLOW,
                            resources=[telemetry_data_table.table_arn]
                        )
                    ]
                ),
                'LambdaSQSPolicy': _iam.PolicyDocument(
                    statements=[
                        _iam.PolicyStatement(
                            actions=[
                                'sqs:ReceiveMessage',
                                'sqs:ChangeMessageVisibility',
                                'sqs:GetQueueUrl',
                                'sqs:DeleteMessage',
                                'sqs:GetQueueAttributes'
                            ],
                            effect=_iam.Effect.ALLOW,
                            resources=[queue.queue_arn]
                        )
                    ]
                )
            }
        )
        
        # Add AWSLambdaBasicExecutionRole and AWSLambdaVPCAccessExecutionRole to lambda_role
        lambda_role.add_managed_policy(_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"))
        lambda_role.add_managed_policy(_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole"))

        # Create role for IoT Core
        iot_sqs_role = _iam.Role(
            self, 'IoTSQSRole',
            role_name='IoTRole',
            description='Role for IoTCore to be able to send data to an SQS',
            assumed_by=_iam.ServicePrincipal('iot.amazonaws.com'),
            inline_policies={
                'SQSPolicy': _iam.PolicyDocument(
                    statements=[
                        _iam.PolicyStatement(
                            actions=['sqs:SendMessage'],
                            effect=_iam.Effect.ALLOW,
                            resources=[queue.queue_arn]
                        )
                    ]
                )
            }
        )

        # Create Lambda function
        sqs_lambda = _lambda.Function(self, "SQSLambda",
                                    function_name='SQSLambda',
                                    handler='lambda_handler.handler',
                                    runtime=_lambda.Runtime.PYTHON_3_10,
                                    code=_lambda.Code.from_asset('./lambda'),
                                    environment={
                                        'TableName': telemetry_data_table.table_name,
                                        'TableArn': telemetry_data_table.table_arn
                                    },
                                    role=lambda_role)
        
        # Create an event source that triggers Lambda when messages enter the SQS queue
        sqs_event_source = aws_lambda_event_sources.SqsEventSource(queue)
        sqs_lambda.add_event_source(sqs_event_source)

        # Create an AWS IoT rule to route messages to SQS
        topic_rule = CfnTopicRule(
            self, "MyTopicRule",
            rule_name='MyTopicRule',
            topic_rule_payload={
                "rule_name": "MyMQTTRule",
                "sql": "SELECT * FROM 'telemetry_data'",
                "actions": [
                    {
                        "sqs": {
                            "roleArn": iot_sqs_role.role_arn,
                            "queueUrl": queue.queue_url
                        }
                    }
                ]
            }
        )
