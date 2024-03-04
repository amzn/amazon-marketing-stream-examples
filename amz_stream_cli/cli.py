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

from ad_api.api import Stream
from amz_stream_cli import __app_name__, __version__
from amz_stream_cli.stream_api import AdvertisingApiRegion, DataSet, SubscriptionUpdateEntityStatus
from rich.console import Console
from rich.table import Table
from typing import Optional
import json
import typer


app = typer.Typer()
console = Console()


def _version_callback(value: bool) -> None:
    if value:
        console.print(f"{__app_name__} v{__version__}")
        raise typer.Exit()


def _subscription_to_table(subscription: dict) -> Optional[Table]:
    table = Table("Field", "Value")
    for field, value in subscription.items():
        table.add_row(field, value)
    return table


def _check_for_error_message_from_api(payload: dict) -> None:
    if 'message' in payload:
        console.print("Error message received from API:")
        console.print(payload['message'])
        raise typer.Exit(-1)


@app.command(name="create",
             short_help="Creates Amazon Marketing Stream subscription.",
             help="""
             Example usage:\n
             python -m amz_stream_cli create 
             --destination-arn  arn:aws:sqs:us-east-1:xxxxxxxxxxxx:AmzStream-NA-sp-traffic-IngressQueuexxxxx 
             -client-request-token my-unique-idempotency-string-token 
             --data-set-id sp-traffic 
             --notes "This is a marketing stream subscription"
             """)
def create_subscription(
        api_region: AdvertisingApiRegion = typer.Option(AdvertisingApiRegion.NA, "--api-region", "-a", help="Advertising API region to use. Default is NA."),
        destination_arn: str = typer.Option(..., "--destination-arn", "-e", help="AWS ARN of the destination endpoint associated with the subscription. Supported destination types: SQS"),
        client_request_token: str = typer.Option(..., "--client-request-token", "-c", help="Unique value supplied by the caller used to track identical API requests. Should request be re-tried, "
                                                                                      "the caller should supply the same value."),
        notes: str = typer.Option(None, "--notes", "-n", help="Additional details associated with the subscription."),
        data_set_id: DataSet = typer.Option(..., "--data-set-id", "-d", help="DataSet ID to use for the subscription.")
) -> None:
    create_subscription_dict = {
        "destinationArn": destination_arn,
        "clientRequestToken": client_request_token,
        "dataSetId": data_set_id.value
    }
    if notes is not None:
        create_subscription_dict["notes"] = notes

    response = Stream(marketplace=AdvertisingApiRegion.get_marketplace(api_region)) \
        .create_subscription(body=json.dumps(create_subscription_dict))
    _check_for_error_message_from_api(response.payload)
    table = _subscription_to_table(response.payload)
    console.print("Subscription has been created!")
    console.print(table)


@app.command(name="update",
             short_help="Updates specific Amazon Marketing Stream subscription by ID.",
             help="""
             Example usage:\n
             python -m amz_stream_cli update 
             --subscription-id amzn1.fead.xxxx.xxxxxxxxxxxx 
             --status ARCHIVED
             --notes "Subscription archived from CLI tool" 
             """)
def update_subscription(
        api_region: AdvertisingApiRegion = typer.Option(AdvertisingApiRegion.NA, "--api-region", "-a", help="Advertising API region to use. Default is NA."),
        subscription_id: str = typer.Option(..., "--subscription-id", "-s", help="Subscription ID of the subscription that will be updated."),
        status: SubscriptionUpdateEntityStatus = typer.Option(..., "--status", "-t", help="Status to use for the subscription."),
        notes: str = typer.Option(None, "--notes", "-n", help="Notes for the subscription update."),
) -> None:
    update_subscription_dict = {
        "status": status.value
    }
    if notes is not None:
        update_subscription_dict["notes"] = notes

    response = Stream(marketplace=AdvertisingApiRegion.get_marketplace(api_region)) \
        .update_subscription(subscription_id=subscription_id, body=json.dumps(update_subscription_dict))

    _check_for_error_message_from_api(response.payload)
    console.print("Subscription ID {} has been {}!".format(subscription_id, status.value))


@app.command(name="get",
             short_help="Gets information on specific Amazon Marketing Stream subscription by ID.",
             help="""
             Example usage:\n
             python -m amz_stream_cli get
             --subscription-id amzn1.fead.xxxx.xxxxxxxxxxxx 
             """)
def get_subscription(
        api_region: AdvertisingApiRegion = typer.Option(AdvertisingApiRegion.NA, "--api-region", "-a", help="Advertising API region to use. Default is NA."),
        subscription_id: str = typer.Option(..., "--subscription-id", "-s", help="Subscription ID of the subscription to be fetched."),
) -> None:
    response = Stream(marketplace=AdvertisingApiRegion.get_marketplace(api_region)) \
        .get_subscription(subscription_id=subscription_id)
    _check_for_error_message_from_api(response.payload)
    subscription = response.payload['subscription']
    table = _subscription_to_table(subscription)
    console.print(table)


@app.command(name="list",
             short_help="Lists all Amazon Marketing Stream subscriptions associated with your Amazon Advertising API account.",
             help="""
             Example usage:\n
             python -m amz_stream_cli list \n
             """)
def list_subscriptions(
        api_region: AdvertisingApiRegion = typer.Option(AdvertisingApiRegion.NA, "--api-region", "-a", help="Advertising API region to use. Default is NA.")
) -> None:
    response = Stream(marketplace=AdvertisingApiRegion.get_marketplace(api_region)).list_subscriptions()
    _check_for_error_message_from_api(response.payload)
    if 'subscriptions' in response.payload:
        subscriptions = sorted(response.payload['subscriptions'], key=lambda d: d['status'])
        for subscription in subscriptions:
            console.print(_subscription_to_table(subscription))
    else:
        console.print("No subscriptions found!")
        raise typer.Exit()


@app.callback()
def main(version: Optional[bool] = typer.Option(
    None,
    "--version",
    "-v",
    help="Show the application's version and exit.",
    callback=_version_callback,
    is_eager=True,
)
) -> None:
    return
