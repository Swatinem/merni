use merni::{counter, distribution};

#[tokio::main]
async fn main() {
    let flusher = merni::datadog(None)
        .prefix("merni.test.")
        .try_init()
        .unwrap();

    for _ in 0..10 {
        counter!("counter": 1);
        distribution!("distribution": 1);
    }

    flusher.flush(None).await.unwrap();
}
