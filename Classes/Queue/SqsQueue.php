<?php
namespace Netlogix\JobQueue\Sqs\Queue;

use Aws\Sqs\SqsClient;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\Flow\Utility\Algorithms;

/**
 * A queue implementation using Sqs as the queue backend
 */
class SqsQueue implements QueueInterface
{

	/**
	 * @var string
	 */
	protected $name;

	/**
	 * @var array
	 */
	protected $clientSettings;

	/**
	 * @var int
	 */
	protected $defaultTimeout = 60;

	/**
	 * @var int
	 */
	protected $defaultVisibilityTimeout = 300;

	/**
	 * @var string
	 */
	protected $queueUrl;

	/**
	 * @var SqsClient
	 */
	protected $sqsClient;

	public function __construct($name, array $options = [])
	{
		$this->name = $name;
		if (isset($options['defaultTimeout'])) {
			$this->defaultTimeout = $options['defaultTimeout'];
		}
		if (isset($options['defaultVisibilityTimeout'])) {
			$this->defaultVisibilityTimeout = $options['defaultVisibilityTimeout'];
		}
		$this->clientSettings = isset($options['client']) ? $options['client'] : [];
		$this->sqsClient = new SqsClient($this->clientSettings);
		if (!isset($this->clientSettings['queueUrl'])) {
			$result = $this->sqsClient->createQueue([
				'QueueName' => $this->name,
				'Attributes' => [
					'VisibilityTimeout' => $this->defaultVisibilityTimeout
				]
			]);
			$this->queueUrl = $result->get('QueueUrl');
		} else {
			$this->queueUrl = $this->clientSettings['queueUrl'];
		}
	}

	/**
	 * @inheritdoc
	 */
	public function setUp(): void
	{
		// TODO: Configure RedrivePolicy of Queue?
	}

	/**
	 * @inheritdoc
	 */
	public function getName(): string
	{
		return $this->name;
	}

	/**
	 * @inheritdoc
	 */
	public function submit($payload, array $options = []): string
	{
		$messageId = Algorithms::generateUUID();
		$this->request('sendMessage', [
			'Id' => $messageId,
			'MessageBody' => json_encode(['payload' => $payload])
		]);

		return $messageId;
	}

	/**
	 * @inheritdoc
	 */
	public function waitAndTake(?int $timeout = null): ?Message
	{
		if ($timeout > 20) {
			$timeout = 20;
		}
		$result = $this->request('receiveMessage', [
			'MaxNumberOfMessages' => 1,
			'WaitTimeSeconds' => $timeout,
			'VisibilityTimeout' => $this->defaultVisibilityTimeout,
			'AttributeNames' => [
				'ApproximateReceiveCount'
			]
		]);
		if (!$result->get('Messages')) {
			return null;
		}
		$message = current($result->get('Messages'));
		$this->request('deleteMessage', [
			'ReceiptHandle' => $message['ReceiptHandle']
		]);

		return $this->transformSqsMessage($message);
	}

	/**
	 * @inheritdoc
	 */
	public function waitAndReserve(?int $timeout = null): ?Message
	{
		if ($timeout === null) {
			$timeout = $this->defaultVisibilityTimeout;
		}
		$result = $this->request('receiveMessage', [
			'MaxNumberOfMessages' => 1,
			'WaitTimeSeconds' => 20,
			'VisibilityTimeout' => $timeout,
			'AttributeNames' => [
				'ApproximateReceiveCount'
			]
		]);
		if (!$result->get('Messages')) {
			return null;
		}
		$message = current($result->get('Messages'));

		return $this->transformSqsMessage($message);
	}

	/**
	 * @inheritdoc
	 */
	public function release(string $receiptHandle, array $options = []): void
	{
		if (!isset($options['delay'])) {
			$options['delay'] = $this->defaultTimeout;
		}
		$this->request('changeMessageVisibility', [
			'ReceiptHandle' => $receiptHandle,
			'VisibilityTimeout' => $options['delay']
		]);
	}

	/**
	 * Messages can't be marked as failed on Sqs.
	 * To move a message to the dead-letter queue, configure the main queue
	 * with maxReceiveCount in the settings.
	 * http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
	 *
	 * @param string $receiptHandle
	 */
	public function abort(string $receiptHandle): void
	{
	}

	/**
	 * @inheritdoc
	 */
	public function finish(string $receiptHandle): bool
	{
		$this->request('deleteMessage', [
			'ReceiptHandle' => $receiptHandle
		]);

		return true;
	}

	/**
	 * @inheritdoc
	 */
	public function peek(int $limit = 1): array
	{
		$amount = $this->count();
		if ($amount === 0) {
			return [];
		}

		$limit = $limit > $amount ? $amount : $limit;

		$result = $this->request('receiveMessage', [
			'MaxNumberOfMessages' => $limit,
			'WaitTimeSeconds' => 20,
			'VisibilityTimeout' => 0,
			'AttributeNames' => [
				'ApproximateReceiveCount'
			]
		]);
		if (!$result->get('Messages')) {
			return [];
		}

		return array_map(function ($message) {
			return $this->transformSqsMessage($message);
		}, $result->get('Messages'));
	}

	/**
	 * @inheritdoc
	 */
	public function countReady(): int
	{
		$result = $this->request('getQueueAttributes', [
			'AttributeNames' => ['ApproximateNumberOfMessages']
		]);

		return intval($result->get('Attributes')['ApproximateNumberOfMessages']);
	}

	/**
	 * @inheritdoc
	 */
	public function countReserved(): int
	{
		return 0;
	}

	/**
	 * @inheritdoc
	 */
	public function countFailed(): int
	{
		return 0;
	}

	/**
	 * @inheritdoc
	 */
	public function flush(): void
	{
		$this->request('purgeQueue');
	}

	/**
	 * Turns a Sqs message into a JobQueue Message
	 *
	 * @param array $message
	 * @return Message
	 */
	protected function transformSqsMessage($message)
	{
		$receiptHandle = $message['ReceiptHandle'];
		$body = json_decode($message['Body'], true);
		$payload = $body['payload'];
		$numberOfReleases = $message['Attributes']['ApproximateReceiveCount'];

		return new Message($receiptHandle, $payload, $numberOfReleases);
	}

	/**
	 * Dispatches an aws-sdk request with the given $endpoint
	 *
	 * @param string $endpoint
	 * @param array $arguments
	 * @return \Aws\Result
	 */
	protected function request($endpoint, $arguments = [])
	{
		return $this->sqsClient->$endpoint($this->buildRequestArguments($arguments));
	}

	/**
	 * Creates an array with default request arguments to pass onto the aws-sdk
	 *
	 * @param $arguments
	 * @return array
	 */
	protected function buildRequestArguments($arguments)
	{
		$defaultArguments = [
			'QueueUrl' => $this->queueUrl
		];
		return array_merge_recursive($defaultArguments, $arguments);
	}

}