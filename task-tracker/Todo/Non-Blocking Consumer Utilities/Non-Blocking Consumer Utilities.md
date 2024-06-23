[[Todo]]

We want to provide trait implementations for consumer instances that allow non-blocking calls to `Consumer.begin_transaction`, `Consumer.send_offsets_to_transaction`, etc..

This will prevent tasks from blocking the executor while performing transaction/commit operations.

The current plan is to create a trait like so:
```
trait AsyncConsumer<T>: Consumer<T> {
	fn async_begin_transaction(&self) -> impl Future<Output=KafkaResult> {
		tokio::task::spawn_blocking(
			|| {
				self.begin_transaction(Duration::from_millis(2500))
			}
		)
	}
}
```
Note that the type of self may have to be something like `self: Arc<Self>` to satisfy tokio's lifetime requirements.