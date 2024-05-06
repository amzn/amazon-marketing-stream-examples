# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

from constructs import Construct
import json
import os
from aws_cdk import (
    Environment,
    Fn as fn,
    Duration,
    Stack,
    Tags,
    CfnOutput,
    aws_iam as iam,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_events,
    aws_s3 as s3,
    aws_kinesisfirehose_alpha as firehose,
    aws_kinesisfirehose_destinations_alpha as destinations,
    aws_lambda_event_sources as lambda_event_source,
)


class DataSetScopedConstruct(Construct):
    """
    Base construct which has scoped to dataset
    """

    def __init__(self, scope: Construct, construct_id: str, ambassadors_config, dataset_config):
        super().__init__(scope, construct_id)
        self.ambassadors_config = ambassadors_config
        self.dataset_config = dataset_config


class AmzStreamAmbassadorsInfra(DataSetScopedConstruct):

    def __init__(self, scope: Construct, construct_id: str, ambassadors_config, dataset_config) -> None:
        super().__init__(scope, construct_id, ambassadors_config, dataset_config)

        self.reviewer_principal = iam.Role.from_role_arn(
            self, "Reviewer", self.ambassadors_config["reviewerArn"], mutable=False
        )

    def grant_review(self, queue: sqs.Queue):
        queue.grant(self.reviewer_principal, "sqs:GetQueueAttributes")


class AmzStreamStreamDeliveryInfra(DataSetScopedConstruct):

    def __init__(self, scope, construct_id, ambassadors_config, dataset_config) -> None:
        super().__init__(scope, construct_id, ambassadors_config, dataset_config)

    def grant_stream_delivery(self, queue: sqs.Queue):
        stream_delivery_principal = iam.PrincipalWithConditions(
            iam.ServicePrincipal("sns.amazonaws.com"),
            {"ArnLike": {"aws:SourceArn": self.dataset_config["snsSourceArn"]}},
        )
        queue.grant_send_messages(stream_delivery_principal)


class StreamIngress(DataSetScopedConstruct):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        ambassadors_config,
        dataset_config,
        visibility_timeout_s: int = 60,
        receive_message_wait_time_s: int = 20,
        retention_period_s: int = 1209600,
        max_receive_count: int = 10,
    ) -> None:
        super().__init__(scope, construct_id, ambassadors_config, dataset_config)

        self.ambassadors = AmzStreamAmbassadorsInfra(
            self, "AmzStreamAmbassadorsInfra", ambassadors_config, dataset_config
        )
        self.stream_delivery_infra = AmzStreamStreamDeliveryInfra(
            self, "AmzStreamStreamDeliveryInfra", ambassadors_config, dataset_config
        )

        self.ingress_dlq = sqs.Queue(
            self,
            "Dlq",
            visibility_timeout=Duration.seconds(visibility_timeout_s),
            retention_period=Duration.seconds(retention_period_s),
        )

        self.ingress_queue = sqs.Queue(
            self,
            "Queue",
            visibility_timeout=Duration.seconds(visibility_timeout_s),
            receive_message_wait_time=Duration.seconds(receive_message_wait_time_s),
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=max_receive_count, queue=self.ingress_dlq),
        )

        self.ingress_queue_stack_output = CfnOutput(self, "IngressQueue", value=self.ingress_queue.queue_arn)

        self.ambassadors.grant_review(self.ingress_queue)
        self.stream_delivery_infra.grant_stream_delivery(self.ingress_queue)


class StreamFanout(DataSetScopedConstruct):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        ambassadors_config,
        dataset_config,
        visibility_timeout_s: int = 60,
        max_receive_count: int = 10,
    ) -> None:
        super().__init__(scope, construct_id, ambassadors_config, dataset_config)

        self.data_fanout_topic = sns.Topic(self, "DataTopic")

        self.subscription_confirmation_dlq = sqs.Queue(
            self,
            "SubsConfirmationDlq",
            visibility_timeout=Duration.seconds(visibility_timeout_s),
        )

        self.subscription_confirmation_queue = sqs.Queue(
            self,
            "SubsConfirmationQueue",
            visibility_timeout=Duration.seconds(visibility_timeout_s),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=max_receive_count,
                queue=self.subscription_confirmation_dlq,
            ),
        )

        self.fanout_lambda = _lambda.Function(
            self,
            "Lambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="stream_fanout_lambda.handler",
            code=_lambda.Code.from_asset(path="lambda"),
            environment={
                "DATA_FANOUT_TOPIC_ARN": self.data_fanout_topic.topic_arn,
                "SUBSCRIPTION_CONFIRMATION_QUEUE_URL": self.subscription_confirmation_queue.queue_url,
            },
        )
        self.data_fanout_topic.grant_publish(self.fanout_lambda)
        self.subscription_confirmation_queue.grant_send_messages(self.fanout_lambda)

    def subscribe_to_stream(self, stream_ingress: StreamIngress):
        invoke_event_source = lambda_events.SqsEventSource(stream_ingress.ingress_queue)
        self.fanout_lambda.add_event_source(invoke_event_source)


class StreamLanding(DataSetScopedConstruct):
    def __init__(self, scope: Construct, construct_id: str, ambassadors_config, dataset_config) -> None:
        super().__init__(scope, construct_id, ambassadors_config, dataset_config)

        self.lz_bucket = s3.Bucket(self, "LZ")
        self.lz_bucket_output = CfnOutput(self, "LandingZoneBucket", value=self.lz_bucket.bucket_arn)

        prefix = "year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
        self.firehose = firehose.DeliveryStream(
            self,
            "Firehose",
            destinations=[
                destinations.S3Bucket(
                    self.lz_bucket,
                    data_output_prefix=f"{
                        dataset_config['dataSetId']}/{prefix}",
                    error_output_prefix=f"errors/{
                        dataset_config['dataSetId']}/",
                )
            ],
        )
        CfnOutput(
            self,
            "DeliveryStreamArn",
            value=self.firehose.delivery_stream_arn,
            export_name="DeliveryStreamArn",
        )

        self.sns_subscriptions_role = iam.Role(
            self, "SnsSubsRole", assumed_by=iam.ServicePrincipal("sns.amazonaws.com")
        )
        self.firehose.grant_put_records(self.sns_subscriptions_role)

    def subscribe_to_fanout(self, stream_fanout: StreamFanout):

        self.firehose_subscription = sns.Subscription(
            self,
            "FirehoseSub",
            topic=stream_fanout.data_fanout_topic,
            endpoint=self.firehose.delivery_stream_arn,
            protocol=sns.SubscriptionProtocol.FIREHOSE,
            subscription_role_arn=self.sns_subscriptions_role.role_arn,
            raw_message_delivery=True,
        )


class SubscriptionConfirmation(DataSetScopedConstruct):
    def __init__(self, scope: Construct, construct_id: str, ambassadors_config, dataset_config) -> None:
        super().__init__(scope, construct_id, ambassadors_config, dataset_config)

        self.confirmation_lambda = _lambda.Function(
            self,
            "Lambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="subscription_confirmation_lambda.handler",
            code=_lambda.Code.from_asset(path="lambda"),
        )

        self.confirmation_lambda.add_to_role_policy(
            iam.PolicyStatement(actions=["sns:ConfirmSubscription"], resources=["*"])
        )

    def subscribe_to_fanout(self, stream_fanout: StreamFanout):
        sqs_event_source = lambda_event_source.SqsEventSource(stream_fanout.subscription_confirmation_queue)
        self.confirmation_lambda.add_event_source(sqs_event_source)


class AmzStreamConsumerStack(Stack):

    def __init__(
        self,
        scope: Construct,
        advertising_region,
        installation_region,
        dataset_config,
        ambassadors_config,
        **kwargs,
    ) -> None:
        super().__init__(
            scope,
            f"AmzStream-{advertising_region}-{dataset_config['dataSetId']}",
            description=f"Amazon Marketing Stream Consumer "
            f"for Advertising region: {advertising_region} Dataset: {
                dataset_config['dataSetId']}",
            env=Environment(region=installation_region),
            **kwargs,
        )

        self.stream_ingress = StreamIngress(self, "Ingress", ambassadors_config, dataset_config)

        self.stream_fanout = StreamFanout(self, "Fanout", ambassadors_config, dataset_config)
        self.stream_fanout.subscribe_to_stream(self.stream_ingress)

        self.stream_storage = StreamLanding(self, "Storage", ambassadors_config, dataset_config)
        self.stream_storage.subscribe_to_fanout(self.stream_fanout)

        self.subscription_confirmation = SubscriptionConfirmation(
            self, "SubsConfirmation", ambassadors_config, dataset_config
        )
        self.subscription_confirmation.subscribe_to_fanout(self.stream_fanout)

        Tags.of(self).add("data_set_id", dataset_config["dataSetId"])
