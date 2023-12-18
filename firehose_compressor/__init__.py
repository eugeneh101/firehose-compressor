import os

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_logs as logs,
    aws_kinesisfirehose as firehose,
    aws_lambda as _lambda,
    aws_s3 as s3,
)
from constructs import Construct


class FirehoseCompressorStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.firehose_write_to_s3_role = iam.Role(
            self,
            "FirehoseWriteToS3Role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("firehose.amazonaws.com"),
                iam.ServicePrincipal("lambda.amazonaws.com"),
            ),
        )
        self.firehose_write_to_s3_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    # "s3:AbortMultipartUpload",
                    # "s3:GetBucketLocation",
                    # "s3:GetObject",
                    # "s3:ListBucket",
                    # "s3:ListBucketMultipartUploads",
                    "s3:PutObject",
                    # "logs:PutLogEvents",
                ],
                resources=[f"arn:aws:s3:::{environment['S3_BUCKET_NAME']}/*"],
            ),
        )

        self.s3_bucket = s3.Bucket(
            self,
            "S3Bucket",
            bucket_name=environment["S3_BUCKET_NAME"],
            event_bridge_enabled=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        self.eventbridge_rules_every_minute = {}
        self.eventbridge_rules_file_created = {}
        self.producer_lambdas = {}
        self.send_to_firehose_lambdas = {}
        for process_details in environment["DETAILS_ON_PROCESSES"]:
            process_name = process_details["PROCESS_NAME"]
            s3_bucket = process_details["S3_BUCKET"]
            s3_folder_path = process_details["S3_FOLDER_PATH"]

            eventbridge_rule_every_minute = events.Rule(
                self,
                f"RunEveryMinuteFor{process_name}",
                rule_name=f"run-every-minute--{process_name}",  # hard coded
                schedule=events.Schedule.rate(duration=Duration.minutes(1)),
            )
            self.eventbridge_rules_every_minute[
                process_name
            ] = eventbridge_rule_every_minute
            eventbridge_rule_file_created = events.Rule(
                self,
                f"FileCreatedFor{process_name}",
                rule_name=f"file-created--{process_name}",  # hard coded
                event_pattern=events.EventPattern(
                    source=["aws.s3"],
                    detail_type=["Object Created"],
                    detail={
                        "bucket": {"name": [s3_bucket]},
                        "object": {"key": [{"prefix": s3_folder_path}]},
                    },
                ),
            )
            self.eventbridge_rules_file_created[
                process_name
            ] = eventbridge_rule_file_created

            producer_lambda_function_name = environment["PRODUCER_LAMBDA_NAME"].format(
                PROCESS_NAME=process_name
            )
            producer_lambda = _lambda.Function(
                self,
                f"ProducerLambdaFor{process_name}",
                function_name=producer_lambda_function_name,
                handler="handler.lambda_handler",
                memory_size=128,  # should use very little RAM
                timeout=Duration.seconds(60),  # can run up to a minute
                runtime=_lambda.Runtime.PYTHON_3_9,
                environment={
                    "S3_BUCKET_NAME": environment["S3_BUCKET_NAME"],
                    "S3_BUCKET_PREFIX_FOR_PRODUCER": environment[
                        "S3_BUCKET_PREFIX_FOR_PRODUCER"
                    ].format(PROCESS_NAME=process_name),
                },
                code=_lambda.Code.from_asset(
                    "lambda_code/producer_lambda",
                    exclude=[".venv/*"],
                ),
            )
            self.producer_lambdas[process_name] = producer_lambda
            log_group = logs.LogGroup(
                self,
                f"ProducerLambdaLogGroup{process_name}",
                log_group_name=f"/aws/lambda/{producer_lambda_function_name}",
                retention=logs.RetentionDays.ONE_MONTH,
                removal_policy=RemovalPolicy.DESTROY,
            )
            producer_lambda.node.add_dependency(log_group)

            send_to_firehose_lambda_function_name = environment[
                "SEND_TO_FIREHOSE_LAMBDA_NAME"
            ].format(PROCESS_NAME=process_name)
            send_to_firehose_lambda = _lambda.Function(
                self,
                f"SendToFirehoseLambda--{process_name}",
                function_name=send_to_firehose_lambda_function_name,
                handler="handler.lambda_handler",
                memory_size=1024,  # files should be small
                timeout=Duration.seconds(3),  # should be very fast
                runtime=_lambda.Runtime.PYTHON_3_9,
                environment={
                    "S3_BUCKET_NAME": environment["S3_BUCKET_NAME"],
                    "S3_BUCKET_PREFIX_FOR_PRODUCER": environment[
                        "S3_BUCKET_PREFIX_FOR_PRODUCER"
                    ].format(PROCESS_NAME=process_name),
                    "FIREHOSE_NAME": environment["FIREHOSE_NAME"].format(
                        PROCESS_NAME=process_name
                    ),
                },
                code=_lambda.Code.from_asset(
                    "lambda_code/send_to_firehose_lambda",
                    exclude=[".venv/*"],
                ),
            )
            self.send_to_firehose_lambdas[process_name] = send_to_firehose_lambda
            log_group = logs.LogGroup(
                self,
                f"SendToFirehoseLambdaLogGroup{process_name}",
                log_group_name=f"/aws/lambda/{send_to_firehose_lambda_function_name}",
                retention=logs.RetentionDays.ONE_MONTH,
                removal_policy=RemovalPolicy.DESTROY,
            )
            send_to_firehose_lambda.node.add_dependency(log_group)

        # connect AWS resources together
        self.firehoses_with_s3_target = {}
        for process_details in environment["DETAILS_ON_PROCESSES"]:
            process_name = process_details["PROCESS_NAME"]
            firehose_name = environment["FIREHOSE_NAME"].format(
                PROCESS_NAME=process_name
            )
            validate_and_transform_json = process_details["VALIDATE_AND_TRANSFORM_JSON"]
            firehose_buffer_interval_in_seconds = process_details[
                "FIREHOSE_BUFFER_INTERVAL_IN_SECONDS"
            ]
            firehose_buffer_size_in_mbs = process_details["FIREHOSE_BUFFER_SIZE_IN_MBS"]

            self.eventbridge_rules_every_minute[process_name].add_target(
                events_targets.LambdaFunction(self.producer_lambdas[process_name])
            )
            self.eventbridge_rules_file_created[process_name].add_target(
                events_targets.LambdaFunction(
                    self.send_to_firehose_lambdas[process_name]
                )
            )

            self.s3_bucket.grant_write(self.producer_lambdas[process_name].role)
            self.s3_bucket.grant_read(self.send_to_firehose_lambdas[process_name].role)

            vatjl_function_name = f"validate-and-transform-json--{process_name}"
            validate_and_transform_json_lambda = _lambda.Function(
                self,
                f"ValidateAndTransformJson--{process_name}",
                function_name=vatjl_function_name,
                runtime=_lambda.Runtime.PYTHON_3_9,
                code=_lambda.Code.from_asset(
                    "lambda_code/validate_and_transform_json_lambda",
                    exclude=[".venv/*"],
                ),
                handler="handler.lambda_handler",
                timeout=Duration.seconds(5),  # depends on number of messages
                memory_size=1024,  # depends on number of messages
                environment={"PROCESS_NAME": process_name},
                retry_attempts=0,
                role=self.firehose_write_to_s3_role,  # connect AWS resource
            )
            log_group = logs.LogGroup(
                self,
                f"ValidateAndTransformJsonLambda{process_name}",
                log_group_name=f"/aws/lambda/{vatjl_function_name}",
                retention=logs.RetentionDays.ONE_MONTH,
                removal_policy=RemovalPolicy.DESTROY,
            )
            validate_and_transform_json_lambda.node.add_dependency(log_group)
            processor = firehose.CfnDeliveryStream.ProcessorProperty(
                type="Lambda",
                parameters=[
                    firehose.CfnDeliveryStream.ProcessorParameterProperty(
                        parameter_name="LambdaArn",  # there are also "Delimiter" and "NumberOfRetries"
                        parameter_value=validate_and_transform_json_lambda.function_arn,  # connect AWS resource
                    ),
                    # the properties below are optional
                    firehose.CfnDeliveryStream.ProcessorParameterProperty(
                        parameter_name="BufferIntervalInSeconds",
                        parameter_value=str(firehose_buffer_interval_in_seconds),
                    ),
                    firehose.CfnDeliveryStream.ProcessorParameterProperty(
                        parameter_name="BufferSizeInMBs",
                        parameter_value=str(firehose_buffer_size_in_mbs),
                    ),
                ],
            )
            extended_s3_destination_configuration = firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=self.s3_bucket.bucket_arn,  # connect AWS resource
                role_arn=self.firehose_write_to_s3_role.role_arn,  # connect AWS resource
                # the properties below are optional
                prefix=environment["S3_BUCKET_PREFIX_FOR_FIREHOSE"].format(
                    PROCESS_NAME=f"{process_name}/"
                ),
                error_output_prefix=f"error/topic={process_name}/",
                processing_configuration=firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                    enabled=validate_and_transform_json,
                    processors=[processor] if validate_and_transform_json else [],
                ),
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=firehose_buffer_interval_in_seconds,
                    size_in_m_bs=firehose_buffer_size_in_mbs,
                ),
                cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                    enabled=True,
                    log_group_name=f"/aws/kinesisfirehose/{firehose_name}",  # hard coded
                    log_stream_name="DestinationDelivery",  # hard coded
                ),
                # compression_format="compressionFormat",
                # s3_backup_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(...),
                # s3_backup_mode="s3BackupMode",
            )
            firehose_with_s3_target = firehose.CfnDeliveryStream(
                self,
                f"FirehoseToS3ForProcess--{process_name}",
                extended_s3_destination_configuration=extended_s3_destination_configuration,
                delivery_stream_name=firehose_name,
            )
            self.firehoses_with_s3_target[process_name] = firehose_with_s3_target
            log_group = logs.LogGroup(
                self,
                f"FirehoseLogGroup{process_name}",
                log_group_name=f"/aws/kinesisfirehose/{firehose_name}",  # hard coded
                retention=logs.RetentionDays.ONE_MONTH,
                removal_policy=RemovalPolicy.DESTROY,
            )
            log_stream = logs.LogStream(
                self,
                f"LogStreamFor{process_name}",
                log_group=log_group,
                log_stream_name="DestinationDelivery",  # hard coded
                removal_policy=RemovalPolicy.DESTROY,
            )
            firehose_with_s3_target.node.add_dependency(log_group)

            self.send_to_firehose_lambdas[process_name].role.add_to_policy(
                statement=iam.PolicyStatement(
                    actions=[
                        "firehose:PutRecord",
                        "firehose:PutRecordBatch",
                    ],
                    resources=[self.firehoses_with_s3_target[process_name].attr_arn],
                ),
            )
            self.firehose_write_to_s3_role.add_to_policy(
                statement=iam.PolicyStatement(
                    actions=[
                        "logs:PutLogEvents",
                        # "logs:CreateLogGroup",  # extra permission
                        # "logs:CreateLogStream",  # extra permission
                    ],
                    resources=[log_group.log_group_arn],
                ),
            )
            self.firehose_write_to_s3_role.add_to_policy(
                statement=iam.PolicyStatement(  # for ExtendedS3DestinationConfigurationProperty
                    actions=["lambda:InvokeFunction"],
                    resources=[
                        f"arn:aws:lambda:{environment['AWS_REGION']}:"
                        f"{self.account}:function:{vatjl_function_name}"
                    ],
                ),
            )
            self.firehose_write_to_s3_role.add_to_policy(
                statement=iam.PolicyStatement(
                    actions=[
                        # "logs:CreateLogGroup",  # extra permission
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                    ],
                    resources=[
                        f"arn:aws:logs:{environment['AWS_REGION']}:{self.account}:"
                        f"log-group:/aws/lambda/{vatjl_function_name}:*"
                    ],
                ),
            )
