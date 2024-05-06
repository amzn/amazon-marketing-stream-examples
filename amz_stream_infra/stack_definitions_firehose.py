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

import aws_cdk
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


# Creating Subscription role and its policies


class AmzStreamFirehoseSubscriptionRoleInfra(DataSetScopedConstruct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        ambassadors_config,
        dataset_config,
        firehose: firehose.DeliveryStream,
        advertising_region,
    ) -> None:
        super().__init__(scope, construct_id, ambassadors_config, dataset_config)

        self.firehose_subscription_role = iam.Role(
            self,
            "FirehoseSubscriptionRole",
            role_name="-".join([dataset_config["dataSetId"][:8], advertising_region, "subscription"]),
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("sns.amazonaws.com"),
                iam.ArnPrincipal(ambassadors_config["reviewerArn"]),
            ),
        )

        firehose_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "firehose:DescribeDeliveryStream",
                "firehose:ListTagsForDeliveryStream",
                "firehose:ListDeliveryStreams",
                "firehose:PutRecord",
                "firehose:PutRecordBatch",
            ],
            resources=[firehose.delivery_stream_arn],
        )
        self.firehose_subscription_role_output = CfnOutput(self, "Arn", value=self.firehose_subscription_role.role_arn)
        self.firehose_subscription_role.add_to_principal_policy(firehose_statement)

    def get_firehose_subscription_role_arn(self) -> str:
        return self.firehose_subscription_role.role_arn


# Creating Subscriber role and its policies


class AmzStreamFirehoseSubscriberRoleInfra(DataSetScopedConstruct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        ambassadors_config,
        dataset_config,
        advertising_region,
        firehose_subscription_role_arn,
    ) -> None:
        super().__init__(scope, construct_id, ambassadors_config, dataset_config)

        advertising_region = advertising_region[:2]

        self.firehose_subscriber_role = iam.Role(
            self,
            "FirehoseSubscriberRole",
            role_name="-".join([dataset_config["dataSetId"][:8], advertising_region, "subscriber"]),
            assumed_by=iam.ArnPrincipal(ambassadors_config["subscriberRoleArn"]).with_session_tags(),
        )

        subscribe_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["sns:Subscribe", "sns:Unsubscribe"],
            resources=[dataset_config["snsSourceArn"]],
        )

        pass_role_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["iam:PassRole"],
            resources=[firehose_subscription_role_arn],
        )

        self.firehose_subscriber_role.add_to_principal_policy(subscribe_statement)
        self.firehose_subscriber_role.add_to_principal_policy(pass_role_statement)

        self.firehose_subscriber_role_output = CfnOutput(self, "Arn", value=self.firehose_subscriber_role.role_arn)

    def get_firehose_subscriber_role_arn(self) -> str:
        return self.firehose_subscriber_role.role_arn

    # Firehose and S3 bucket


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
        CfnOutput(self, "DeliveryStreamArn", value=self.firehose.delivery_stream_arn)


class AmzStreamConsumerStackFirehose(Stack):

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

        self.stream_storage = StreamLanding(self, "Storage", ambassadors_config, dataset_config)

        self.firehose_subscription_role_infra = AmzStreamFirehoseSubscriptionRoleInfra(
            self,
            "FirehoseSubscriptionRoleInfra",
            ambassadors_config,
            dataset_config,
            self.stream_storage.firehose,
            advertising_region,
        )

        firehose_subscription_role_arn = self.firehose_subscription_role_infra.get_firehose_subscription_role_arn()

        self.firehose_subscriber_role_infra = AmzStreamFirehoseSubscriberRoleInfra(
            self,
            "FirehoseSubscriberRoleInfra",
            ambassadors_config,
            dataset_config,
            advertising_region,
            firehose_subscription_role_arn,
        )

        Tags.of(self).add("data_set_id", dataset_config["dataSetId"])
