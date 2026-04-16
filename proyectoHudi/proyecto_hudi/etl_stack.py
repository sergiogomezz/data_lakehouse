from constructs import Construct
from aws_cdk import (
    RemovalPolicy,
    Stack,
    # core,
    aws_iam as _iam,
    aws_s3 as _s3,
    aws_glue as _glue,
    aws_s3_deployment as s3deploy,
    aws_s3_notifications as s3_notifications,
    aws_lakeformation as _lakeformation,
    aws_events as _events
)
import os
import aws_cdk as cdk

# Global variables
dynamodb_table_arn = 'arn:aws:dynamodb:eu-west-1:178395378273:table/TelemetryDataTable'
dynammodb_table_name = os.environ.get('TableName')

# This stack creates the ETL, all buckets, and the Glue implementation

class ETLStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create buckets: one for the data lake and one for ETL scripts
        datalake_bucket = _s3.Bucket(
            self, 'datalake-bucket',
            bucket_name=f'datalake-bucket-{cdk.Aws.ACCOUNT_ID}',
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        scripts_bucket = _s3.Bucket(
            self, 'scripts-bucket',
            bucket_name=f'scripts-bucket-{cdk.Aws.ACCOUNT_ID}',
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY
        )

        # Upload the Glue script to the corresponding bucket
        s3deploy.BucketDeployment(self, 'deployment',
                                  sources=[s3deploy.Source.asset('./assets')],
                                  destination_bucket=scripts_bucket)

        
        # Create role for Glue Crawler and Glue Job (TODO: reduce to least privilege)
        glue_role = _iam.Role(
            self, 'GlueRole',
            role_name='GlueRole',
            description='Rol for Glue services to access S3',
            assumed_by=_iam.ServicePrincipal('glue.amazonaws.com'),
            inline_policies={
                'GluePolicy': _iam.PolicyDocument(
                    statements=[
                        _iam.PolicyStatement(
                            actions=[   
                                's3:*',
                                'dynamodb:*',
                                'glue:*',
                                'iam:*',
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

        # Create a Glue database
        glue_database = _glue.CfnDatabase(self, 'telemetry_glue_database',
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
            self, 'glue_crawler',
            name='glue_crawler',
            role=glue_role.role_arn,
            database_name='telemetry_glue_database',
            description='Glue crawler for Dynamo Table',
            targets=_glue.CfnCrawler.TargetsProperty(
                dynamo_db_targets=[_glue.CfnCrawler.DynamoDBTargetProperty(
                    path='TelemetryDataTable'
                    )
                ]
            ),
            recrawl_policy=_glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior='CRAWL_EVERYTHING'
            )
        )

        # Create Glue Job
        glue_job = _glue.CfnJob(
            self, 'glue_job',
            name='glue_job',
            role=glue_role.role_arn,
            command=_glue.CfnJob.JobCommandProperty(
                name='glueetl',
                python_version='3',
                script_location=f's3://{scripts_bucket.bucket_name}/glue_job.py'
                ),
            glue_version='4.0',
            timeout=3,
            default_arguments={
                '--datalake-formats': 'hudi',
                '--conf': 'spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false',
                "--job-language": "python",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "true",
                "--enable-glue-datacatalog": "true",
                "--job-bookmark-option":"job-bookmark-disable"                
                # '-- additional-python-modules'
                # '--extra-jars': just if it uses a different Hudi's version that AWS Glue doesnt support (datalake parameter must be removed)
            }
        )

        # Create Glue Workflow (could be improved later with Airflow)
        glue_workflow = _glue.CfnWorkflow(
            self, 'glue_workflow',
            name='glue_workflow',
            description='Workflow to process telemetry data'
        )

        # create Glue tiggers
        _glue.CfnTrigger(
            self, 'GlueJobTrigger',
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
            description='Trigger the crawler when rules are activated',
            actions=[_glue.CfnTrigger.ActionProperty(
                crawler_name=glue_crawler.name,
                notification_property=_glue.CfnTrigger.NotificationPropertyProperty(notify_delay_after=3),
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

        rule_glue_daily = _events.CfnRule(
            self, 'RuleGlueDaily',
            description='It activates the workflow everyday at 20:00 from Monday to Friday',
            schedule_expression='cron(0 20 ? * MON-FRI *)', 
            role_arn=rule_role.role_arn,
            targets=[
                _events.CfnRule.TargetProperty(
                    arn=f'arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:workflow/{glue_workflow.name}',
                    role_arn=rule_role.role_arn,
                    id='GlueWorkflowTarget'
                )
            ]
        )

        rule_changes = _events.CfnRule(
            self, 'RuleChanges',
            name='RuleChanges',
            role_arn=rule_role.role_arn,
            description='It activates when there are changes in the Dynamno',
            event_pattern={
                "source": ["aws.dynamodb"],
                "detail-type": ["DynamoDB Update Stream Event"],
                "detail": {
                    "eventSourceARN": [dynamodb_table_arn]
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
