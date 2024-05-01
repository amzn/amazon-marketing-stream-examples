import yaml
import aws_cdk as cdk
from amz_stream_infra import infra_rollout

try:
    with open("stream_infrastructure_config.yml", "r") as f:
        config = yaml.safe_load(f)
except yaml.YAMLError as e:
    print("Error in configuration file:", e)
    raise e
except FileNotFoundError as e:
    print("Configuration file not found:", e)
    raise e
except Exception as e:
    print("Unknown exception while loading config:", e)
    raise e

app = cdk.App()

# Get the delivery_type parameter from the CDK context
delivery_type = app.node.try_get_context("delivery_type") or "sqs"


# Call the function to configure the stream infrastructure
infra_rollout.rollout_stacks(app, config, delivery_type)

app.synth()