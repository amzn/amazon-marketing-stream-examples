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

import aws_cdk as core
import aws_cdk.assertions as assertions
from amz_stream_infra.stack_definitions import AmzStreamConsumerStack

AMBASSADOR_CONFIG = {"reviewerArn": "arn:aws:iam::926844853897:role/ReviewerRole"}

DATASET_CONFIG = {
    "NA": [
        {
            "dataSetId": "sp-traffic",
            "snsSourceArn": "arn:aws:sns:us-east-1:906013806264:*",
        }
    ]
}


def test_sqs_queue_created():
    app = core.App()
    stack = AmzStreamConsumerStack(app, "NA", "us-east-1", DATASET_CONFIG["NA"][0], AMBASSADOR_CONFIG)
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::SQS::Queue", {"VisibilityTimeout": 60})


def test_sns_topic_created():
    app = core.App()
    stack = AmzStreamConsumerStack(app, "NA", "us-east-1", DATASET_CONFIG["NA"][0], AMBASSADOR_CONFIG)
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::SNS::Topic", 1)
