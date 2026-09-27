#!/bin/bash
# LocalStack "ready" hook: provisions the SQS queues, the redrive policy and
# the queue access policies.
#
#   wager-transactions.fifo      inbound operations (providers -> service)
#   wager-transactions-dlq.fifo  dead letters of the inbound queue
#   wallet-events.fifo           outbound integration events (outbox relay)
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT="000000000000"
VISIBILITY="${SQS_VISIBILITY_TIMEOUT_SECONDS:-30}"
MAX_RECEIVE="${SQS_MAX_RECEIVE_COUNT:-5}"

q() { awslocal --region "$REGION" sqs "$@"; }

q create-queue --queue-name wager-transactions-dlq.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false","MessageRetentionPeriod":"1209600"}' >/dev/null
DLQ_URL=$(q get-queue-url --queue-name wager-transactions-dlq.fifo --query QueueUrl --output text)
DLQ_ARN=$(q get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query Attributes.QueueArn --output text)

REDRIVE="{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"${MAX_RECEIVE}\\\"}"
q create-queue --queue-name wager-transactions.fifo \
  --attributes "{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"VisibilityTimeout\":\"${VISIBILITY}\",\"DeduplicationScope\":\"messageGroup\",\"FifoThroughputLimit\":\"perMessageGroupId\",\"RedrivePolicy\":\"${REDRIVE}\"}" >/dev/null

q create-queue --queue-name wallet-events.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false","DeduplicationScope":"messageGroup","FifoThroughputLimit":"perMessageGroupId"}' >/dev/null

# Access policies: providers may only send to the inbound queue; only the
# wallet service may consume it, handle the DLQ and publish events.
# (LocalStack community stores but does not enforce IAM; see ARCHITECTURE.md.)
set_policy() {
  local url="$1" policy="$2"
  q set-queue-attributes --queue-url "$url" --attributes "{\"Policy\":$(printf '%s' "$policy" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}"
}
IN_URL=$(q get-queue-url --queue-name wager-transactions.fifo --query QueueUrl --output text)
EV_URL=$(q get-queue-url --queue-name wallet-events.fifo --query QueueUrl --output text)
IN_ARN="arn:aws:sqs:${REGION}:${ACCOUNT}:wager-transactions.fifo"
EV_ARN="arn:aws:sqs:${REGION}:${ACCOUNT}:wallet-events.fifo"

set_policy "$IN_URL" "{\"Version\":\"2012-10-17\",\"Statement\":[
 {\"Sid\":\"ProvidersSend\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"arn:aws:iam::${ACCOUNT}:user/provider-a\",\"arn:aws:iam::${ACCOUNT}:user/provider-b\"]},\"Action\":\"sqs:SendMessage\",\"Resource\":\"${IN_ARN}\"},
 {\"Sid\":\"ServiceConsumes\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::${ACCOUNT}:user/wallet-service\"},\"Action\":[\"sqs:ReceiveMessage\",\"sqs:DeleteMessage\",\"sqs:ChangeMessageVisibility\",\"sqs:GetQueueAttributes\",\"sqs:GetQueueUrl\"],\"Resource\":\"${IN_ARN}\"}]}"
set_policy "$DLQ_URL" "{\"Version\":\"2012-10-17\",\"Statement\":[
 {\"Sid\":\"ServiceDLQ\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::${ACCOUNT}:user/wallet-service\"},\"Action\":[\"sqs:SendMessage\",\"sqs:ReceiveMessage\",\"sqs:DeleteMessage\",\"sqs:GetQueueUrl\"],\"Resource\":\"${DLQ_ARN}\"}]}"
set_policy "$EV_URL" "{\"Version\":\"2012-10-17\",\"Statement\":[
 {\"Sid\":\"ServicePublishes\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::${ACCOUNT}:user/wallet-service\"},\"Action\":[\"sqs:SendMessage\",\"sqs:GetQueueUrl\"],\"Resource\":\"${EV_ARN}\"}]}"

echo "SQS queues provisioned: wager-transactions.fifo (redrive -> DLQ after ${MAX_RECEIVE}), wager-transactions-dlq.fifo, wallet-events.fifo"
