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
import logging as log
import batch


def is_subscription_confirmation(message):
    body = json.loads(message["body"])
    return body.get("Type", "") == "SubscriptionConfirmation"


def get_messages_list(event):
    return event.get("Records", [])


def get_message_body(message):
    return json.loads(message["body"])


def as_error_id(message):
    return {"itemIdentifier": message.get("messageId")}


def default_batch_error_handler(error, context=None):
    log.error("%s, additional info: %s", error, context)


def process_messages_in_batches(
        all_messages,
        messages_filter,
        batch_callback,
        batch_failures,
        max_batch_size,
        error_handler=default_batch_error_handler,
):
    filtered_messages = list(filter(messages_filter, all_messages))

    for next_batch in batch.batch_of(filtered_messages, max_batch_size):
        try:
            batch_callback(next_batch, batch_failures, error_handler)
        except Exception as error:
            # failure in callback, fail entire micro-batch
            batch_failures.extend(next_batch)
            error_handler(error, json.dumps(next_batch))


def batch_handler(event, entire_batch_callback):
    all_messages = get_messages_list(event)
    batch_failures = []

    entire_batch_callback(all_messages, batch_failures)

    return {'batchItemFailures': [as_error_id(i) for i in batch_failures]}



