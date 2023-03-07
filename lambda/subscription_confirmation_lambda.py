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

import aws_clients
import json
import sqs_consuming_lambda as sqs_lambda


def on_confirm_subscription(messages_batch, batch_failures, error_handler):
    for message in messages_batch:
        try:
            body = message["body"]
            print(f"Confirmation request: {body}")

            confirmation_request = json.loads(body)
            topic_arn = confirmation_request["TopicArn"]
            subs_token = confirmation_request["Token"]

            aws_clients.sns_client.confirm_subscription(
                TopicArn=topic_arn,
                Token=subs_token
            )
            print(f"Confirmed: {body}")
        except Exception as error:
            batch_failures.append(message)
            error_handler(error, json.dumps(message))


def on_entire_batch(all_messages, batch_failures):
    sqs_lambda.process_messages_in_batches(
        all_messages,
        lambda x: True,
        on_confirm_subscription,
        batch_failures,
        max_batch_size=10
    )


def handler(event, context):
    return sqs_lambda.batch_handler(event, on_entire_batch)
