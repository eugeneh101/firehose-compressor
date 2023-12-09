import aws_cdk as cdk

from firehose_compressor import FirehoseCompressorStack


app = cdk.App()
environment = app.node.try_get_context("environment")
FirehoseCompressorStack(
    app,
    "FirehoseCompressorStack",
    env=cdk.Environment(region=environment["AWS_REGION"]),
    environment=environment,
)
app.synth()
