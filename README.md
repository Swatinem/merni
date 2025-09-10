# Mérni

> Hungarian: to measure
> [wiktionary](https://en.wiktionary.org/wiki/m%C3%A9r#Hungarian)

An opinionated metrics crate with low overhead, and thread-local aggregation.

## Quick start

With the `"datadog"` feature enabled, you can quickly initialize a global
metrics dispatcher, which will periodically flush metrics to Datadog.

```rust
use std::time::Duration;

#[tokio::main]
async fn main() {
    // Or `None`, which defaults to using the `DD_API_KEY` env.
    let flusher = merni::datadog("datadog API key").try_init().unwrap();

    // Later on, anywhere in your code:
    merni::counter!("some.counter": 1);

    // It is also possible to add tags, as well as units.
    // In this example, the value is converted to seconds, which is also used as unit.
    merni::distribution!("with.tags"@s: Duration::from_millis(10), "tag" => "value");

    // `None` means no timeout waiting for the flush.
    flusher.flush(None).await.unwrap_err(); // expecting an error as we have an invalid API key :-)
}
```

## Details

Mérni consists of some layers of abstraction.
At the core, there is the [`Dispatcher`], which dispatches metrics to a generic [`Sink`].
The dispatcher can then be registered globally using [`set_global_dispatcher`].

One of the sinks is the [`ThreadLocalAggregator`], enabled using the `"aggregator"` feature.
This specialized sink does thread-local pre-aggregation, before periodically
flushing the fully aggregated metrics to yet another generic [`AggregationSink`].

It is possible to drop down to the most basic APIs, and implement the [`Sink`]
trait directly.

```rust
struct PrintlnSink;
impl merni::Sink for PrintlnSink {
    fn emit(&self, metric: merni::Metric) {
        let mut tags = metric.tags().peekable();
        let tags = if tags.peek().is_some() {
            let mut tags_str = String::new();
            for (i, (k,v)) in tags.enumerate() {
                tags_str.push(if i == 0 { '[' } else { ',' });
                tags_str.push_str(k);
                tags_str.push(':');
                tags_str.push_str(v);
            }
            tags_str.push(']');
            tags_str
        } else {
            String::new()
        };
        println!("{}{tags}: {}", metric.key(), metric.value().get());
    }
}

let dispatcher = merni::Dispatcher::new(PrintlnSink);
merni::set_global_dispatcher(dispatcher).expect("global recorder already registered");

merni::counter!("some.counter": 1);
merni::counter!("with.tags": 1, "tag" => "value");
```
