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

import json
import os
import aws_clients
import sqs_consuming_lambda as sqs_lambda


def on_route_to_sns(messages_batch, batch_failures, error_handler, destination_topic_arn):
    batch_to_publish = [
        {
            "Id": str(i),
            "Message": message["body"] + '\n',
        }
        for i, message in enumerate(messages_batch)
    ]

    response = aws_clients.sns_client.publish_batch(
        TopicArn=destination_topic_arn,
        PublishBatchRequestEntries=batch_to_publish
    )
    failures = response.get("Failed", [])
    if failures:
        error_handler(
            f"Partial batch failure from SNS, {len(failures)} failed out of {len(messages_batch)}",
            json.dumps(failures)
        )

    batch_failures.extend([messages_batch[int(failure["Id"])] for failure in failures])


def on_route_to_sqs(messages_batch, batch_failures, error_handler, destination_queue_url):
    for message in messages_batch:
        try:
            aws_clients.sqs_client.send_message(
                QueueUrl=destination_queue_url,
                MessageBody=message["body"]
            )
        except Exception as error:
            batch_failures.append(message)
            error_handler(error, json.dumps(message))


def on_entire_batch(all_messages, batch_failures):
    fanout_config = [
        (
            lambda x: not sqs_lambda.is_subscription_confirmation(x),
            lambda x, y, z: on_route_to_sns(x, y, z, os.environ["DATA_FANOUT_TOPIC_ARN"])
        ),
        (
            sqs_lambda.is_subscription_confirmation,
            lambda x, y, z: on_route_to_sqs(x, y, z, os.environ["SUBSCRIPTION_CONFIRMATION_QUEUE_URL"])
        )
    ]

    for messages_filter, batch_callback in fanout_config:
        sqs_lambda.process_messages_in_batches(
            all_messages,
            messages_filter,
            batch_callback,
            batch_failures,
            max_batch_size=10
        )


def handler(event, context):
    return sqs_lambda.batch_handler(event, on_entire_batch)
