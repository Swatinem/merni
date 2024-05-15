use divan::Bencher;
use merni::{
    counter, distribution, gauge, set_local_dispatcher, Dispatcher, ThreadLocalAggregator,
};

mod benches;

// #[global_allocator]
// static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

#[divan::bench]
fn aggregator(bencher: Bencher) {
    const CASES: usize = 6;
    let count = 5 * CASES;
    bencher
        .counter(divan::counter::ItemsCount::new(count))
        .with_inputs(|| {
            let sink = ThreadLocalAggregator::new();
            Dispatcher::new(sink)
        })
        .bench_values(|dispatcher| {
            let _guard = set_local_dispatcher(dispatcher);

            for i in 0..count {
                match i % CASES {
                    0 => {
                        counter!("some.counter": i);
                    }
                    1 => {
                        counter!("some.tagged.counter": i, "tag_key" => "tag_value");
                    }
                    2 => {
                        gauge!("some.gauge": i);
                    }
                    3 => {
                        gauge!("some.tagged.gauge": i, "tag_key" => "tag_value");
                    }
                    4 => {
                        distribution!("some.distribution": i);
                    }
                    5 => {
                        distribution!("some.tagged.distribution": i, "tag_key" => "tag_value");
                    }
                    _ => unreachable!(),
                }
            }
        });
}

#[divan::bench]
fn _1_vec_string() {
    benches::vec_string()
}

#[divan::bench]
fn _2_boxed_string() {
    benches::boxed_string()
}

#[divan::bench]
fn _3_boxed_boxed() {
    benches::boxed_boxed()
}

#[divan::bench]
fn _4_thread_local() {
    benches::thread_local()
}

#[divan::bench]
fn _5_smallvec() {
    benches::smallvec()
}

#[divan::bench]
fn _6_smolstr() {
    benches::smolstr()
}

#[divan::bench]
fn _7_smallvec_smolstr() {
    benches::smallvec_smolstr()
}

#[divan::bench]
fn _8_smolbuf() {
    benches::smolbuf()
}
