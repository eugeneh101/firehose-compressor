import os

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
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
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
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

        self.eventbridge_rule_every_minute = events.Rule(
            self,
            "RunEveryMinute",
            rule_name="run-every-minute",  # hard coded
            schedule=events.Schedule.rate(duration=Duration.minutes(1)),
        )
        self.eventbridge_rule_file_created = events.Rule(
            self,
            "FileCreated",
            rule_name="file-created",  # hard coded
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [environment["S3_BUCKET_NAME"]]},
                    "object": {
                        "key": [
                            {"prefix": environment["S3_BUCKET_PREFIX_FOR_PRODUCER"]}
                        ]
                    },
                },
            ),
        )

        self.producer_lambda = _lambda.Function(
            self,
            "ProducerLambda",
            function_name=environment["PRODUCER_LAMBDA_NAME"],
            handler="handler.lambda_handler",
            memory_size=128,  # should use very little RAM
            timeout=Duration.seconds(60),  # can run up to a minute
            runtime=_lambda.Runtime.PYTHON_3_9,
            environment={
                "S3_BUCKET_NAME": environment["S3_BUCKET_NAME"],
                "S3_BUCKET_PREFIX_FOR_PRODUCER": environment[
                    "S3_BUCKET_PREFIX_FOR_PRODUCER"
                ],
            },
            code=_lambda.Code.from_asset(
                "lambda_code/producer_lambda",
                exclude=[".venv/*"],
            ),
        )
        self.send_to_firehose_lambda = _lambda.Function(
            self,
            "SendToFirehoseLambda",
            function_name=environment["SEND_TO_FIREHOSE_LAMBDA_NAME"],
            handler="handler.lambda_handler",
            memory_size=1024,  # files should be small
            timeout=Duration.seconds(3),  # should be very fast
            runtime=_lambda.Runtime.PYTHON_3_9,
            environment={
                "S3_BUCKET_NAME": environment["S3_BUCKET_NAME"],
                "S3_BUCKET_PREFIX_FOR_PRODUCER": environment[
                    "S3_BUCKET_PREFIX_FOR_PRODUCER"
                ],
                "FIREHOSE_NAME": environment["FIREHOSE_NAME"],
            },
            code=_lambda.Code.from_asset(
                "lambda_code/send_to_firehose_lambda",
                exclude=[".venv/*"],
            ),
        )

        # connect AWS resources together
        self.eventbridge_rule_every_minute.add_target(
            events_targets.LambdaFunction(self.producer_lambda)
        )
        self.eventbridge_rule_file_created.add_target(
            events_targets.LambdaFunction(self.send_to_firehose_lambda)
        )

        self.s3_bucket.grant_write(self.producer_lambda.role)
        self.s3_bucket.grant_read(self.send_to_firehose_lambda.role)

        s3_destination_configuration_property = firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
            bucket_arn=self.s3_bucket.bucket_arn,  # connect AWS resource
            role_arn=self.firehose_write_to_s3_role.role_arn,  # connect AWS resource
            # the properties below are optional
            buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                interval_in_seconds=60, size_in_m_bs=1  ### parametrize
            ),
            cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                enabled=True,
                log_group_name=f"/aws/kinesisfirehose/{environment['FIREHOSE_NAME']}",  # hard coded
                log_stream_name="DestinationDelivery",  # hard coded
            ),
            prefix=environment["S3_BUCKET_PREFIX_FOR_FIREHOSE"],
            error_output_prefix=os.path.join(
                environment["S3_BUCKET_ERROR_PREFIX_FOR_FIREHOSE"],
                environment["FIREHOSE_NAME"],
            ),
            # do we need processor?
            # compression_format="compressionFormat",
        )
        self.firehose_with_s3_target = firehose.CfnDeliveryStream(
            self,
            "FirehoseToS3",
            s3_destination_configuration=s3_destination_configuration_property,
            delivery_stream_name=environment["FIREHOSE_NAME"],
        )

        self.send_to_firehose_lambda.role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "firehose:PutRecord",
                    "firehose:PutRecordBatch",
                ],
                resources=[
                    self.firehose_with_s3_target.attr_arn
                ],  # connect AWS resource
            ),
        )


# Lambda: firehose transformer
#  "TRANSFORM_LAMBDA_NAME": ...,
