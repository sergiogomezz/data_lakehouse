from constructs import Construct
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    # core,
    aws_iam as _iam,
    aws_s3 as _s3,
    aws_sqs as _sqs,
    aws_iot as _iot,
    aws_lambda as _lambda,
    aws_glue as _glue,
    aws_dynamodb as _dynamodb,
    aws_s3_deployment as s3deploy,
    aws_s3_notifications as s3_notifications,
    aws_lakeformation as _lakeformation,
    aws_events as _events,
    aws_lambda_event_sources as aws_lambda_event_sources
)
from aws_cdk.aws_iot import CfnTopicRule
import aws_cdk as cdk


class ProyectoHudiStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create SQS queue
        queue = _sqs.Queue(
            self, "ProyectoHudiQueue",
            name='ProyectoHudiQueue',
            visibility_timeout=Duration.seconds(300),
        )

        # Create buckets: one for the data lake and one for ETL scripts
        datalake_bucket = _s3.Bucket(
            self, 'DatalakeBucket',
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        scripts_bucket = _s3.Bucket(
            self, 'ScriptsBucket',
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        # Upload the Glue script to the corresponding bucket
        s3deploy.BucketDeployment(self, 'deployment',
                                  sources=[s3deploy.Source.asset('./assets')],
                                  destination_bucket=scripts_bucket)

        # Create a DynamoDB table
        telemetry_data_table = _dynamodb.Table(
            self, "TelemetryDataTable",
            name='TelemetryDataTable',
            partition_key=_dynamodb.Attribute(
                name="device_id",
                type=_dynamodb.AttributeType.STRING
            ),
        sort_key=_dynamodb.Attribute(
                name="timestamp",
                type=_dynamodb.AttributeType.STRING
            ),
        billing_mode=_dynamodb.BillingMode.PAY_PER_REQUEST,  # Adjust billing mode as needed
        )

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
                )
            }
        )

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

        # Create role for Glue Crawler and Glue Job
        glue_role = _iam.Role(
            self, 'GlueRole',
            role_name='GlueRole',
            description='Rol for Glue services to access S3',
            assumed_by=_iam.ServicePrincipal('glue.amazonaws.com'),
            inline_policies={
                'GluePolicy:': _iam.PolicyDocument(
                    statements=[
                        _iam.PolicyStatement(
                            actions=[   
                                's3:*',
                                'glue.*',
                                'iam:*'
                                'logs:*',
                                'cloudwatch:*',
                                'sqs:*'
                            ],
                            effect=_iam.Effect.ALLOW,
                            resources=['*'] 
                        )
                    ]
                )
            }
        )


        # Create Lambda function
        sqs_lambda = _lambda.Function(self, "SQSLambda",
                                    name='SQSLambda',
                                    handler='lambda_handler.handler',
                                    runtime=_lambda.Runtime.PYTHON_3_10,
                                    code=_lambda.Code.from_asset('./lambda'),
                                    role=lambda_role)
        
        # Create an event source that triggers Lambda when messages enter the SQS queue
        sqs_event_source = aws_lambda_event_sources.SqsEventSource(queue)
        sqs_lambda.add_event_source(sqs_event_source)
        

        # Create an AWS IoT rule to route messages to SQS
        topic_rule = CfnTopicRule(
            self, "MyTopicRule",
            name='MyTopicRule',
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


        # Create a Glue database
        glue_database = _glue.CfnDatabase(self, 'glue_database',
                                          catalog_id=cdk.Aws.ACCOUNT_ID,
                                          database_input=_glue.CfnDatabase.DatabaseInputProperty(
                                              name='telemetry_glue_database',
                                              description='Database to store telemetry data'
                                          ))
        _lakeformation.CfnPermissions(self, 'lakeformation_permission',
                                      data_lake_principal=_lakeformation.CfnPermissions.DataLakePrincipalProperty(
                                          data_lake_principal_identifier=glue_role.role_arn),
                                      resource=_lakeformation.CfnPermissions.ResourceProperty(
                                          database_resource=_lakeformation.CfnPermissions.DatabaseResourceProperty(
                                              catalog_id=glue_database.catalog_id,
                                              name='telemetry_glue_database')),
                                      permissions=['ALL'])
        
        # Create Glue Crawler (crawlers replicate table metadata)
        glue_crawler = _glue.CfnCrawler(
            self, 'GlueCrawler',
            name='GlueCrawler',
            role=glue_role.role_arn,
            database_name='telemetry_glue_database',
            description='Glue crawler for Dynamo Table',
            targets={
                'dynamodb': {
                    'path': telemetry_data_table.table_name,
                    'scanAll': True
                }
            },
            recrawl_policy=_glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior='CRAWL_EVERYTHING'
            )
        )

        # Create Glue Job
        glue_job = _glue.CfnJob(
            self, 'GlueJob',
            name='GlueJob',
            role=glue_role.role_arn,
            command=_glue.CfnJob.JobCommandProperty(
                name='glueetl',
                python_version='3.10',
                script_location=f's3://{scripts_bucket.bucket_name}/glue_job.py'
                ),
            glue_version='4.0',
            timeout=3,
            default_arguments={
                '--datalake-formats': 'hudi'
               # '--extra-jars': just if it uses a different Hudi's version that AWS Glue doesnt support (datalake parameter must be removed)
            }
        )

        # Create Glue Workflow (could be improved later with Airflow)
        glue_workflow = _glue.CfnWorkflow(
            self, 'GlueWorkflow',
            name='GlueWorkflow',
            description='Workflow to process telemetry data'
        )

        # create Glue tiggers
        _glue.CfnTrigger(
            self, 'GlueJobTrigger',
            name='GlueJobTrigger',
            description='Trigger that activates the Glue Job when the Crawler finish',
            actions=[_glue.CfnTrigger.ActionProperty(
                job_name=glue_job.name,
                notification_property=_glue.CfnTrigger.NotificationPropertyProperty(notify_delay_after=3),
                timeout=3
                )
            ],
            type='CONDITIONAL',
            start_on_creation=True,
            workflow_name=glue_workflow.name,
            predicate=_glue.CfnTrigger.PredicateProperty(
                conditions=[_glue.CfnTrigger.ConditionProperty(
                    crawler_name=glue_crawler.name,
                    logical_operator='EQUALS',
                    crawl_state='SUCCEEDED'
                )]
            )
        )

        _glue.CfnTrigger(
            self, 'CrawlerTrigger',
            name='CrawlerTrigger',
            description='Trigger the crawler',
            actions=[_glue.CfnTrigger.ActionProperty(
                crawler_name='glue_crawler',
                notification_property=_glue.CfnTrigger.NotificationPropertyProperty(notify_delay_after=3),
                name='glue_crawler_trigger',
                timeout=3)],
            type='EVENT',
            workflow_name=glue_workflow.name
        )

        # create EventBridge rules to trigger crawlers and role for it
        rule_role = _iam.Role(
            self, 'rule_role',
            role_name='EventBridgeRole',
            description='Role for EventBridge to trigger Glue workflows.',
            assumed_by=_iam.ServicePrincipal('events.amazonaws.com'),
            inline_policies={
                'eventbridge_policy': _iam.PolicyDocument(
                    statements=[
                        _iam.PolicyStatement(
                            effect=_iam.Effect.ALLOW,
                            actions=['events:*', 'glue:*'],
                            resources=['*']
                        )
                    ]
                )
            }
        )

        rule_glue_daily = _events.Rule(
            self, 'RuleGlueDaily',
            rule_name='RuleGlueDaily',
            description='It activates the workflow everyday at 20:00',
            role_arn=rule_role.role_arn,
            schedule=_events.Schedule.cron(hour=20, minute='00'),
            targets=[
                _events.CfnRule.TargetProperty(
                    arn=f'arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:workflow/glue_workflow',
                    role_arn=rule_role.role_arn,
                    id=cdk.Aws.ACCOUNT_ID
                )
            ]
        )

        rule_changes = _events.Rule(
            self, 'RuleChanges',
            name='RuleChanges',
            description='It activates when there are changes in the Dynamno',
            role_arn=rule_role.role_arn,
            event_pattern={
                "source": ["aws.dynamodb"],
                "detail-type": ["DynamoDB Update Stream Event"],
                "detail": {
                    "eventSourceARN": [telemetry_data_table.table_arn]
                }
            },
            targets=[
                _events.CfnRule.TargetProperty(
                    arn=f'arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:workflow/glue_workflow',
                    role_arn=rule_role.role_arn,
                    id=cdk.Aws.ACCOUNT_ID
                )
            ]
        )
