use std::io::{self, Write};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use reqwest::header;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use zstd::stream::raw::{Encoder, Operation};
use zstd::zstd_safe::{InBuffer, OutBuffer};

use crate::{
    set_global_dispatcher, AggregatedMetric, AggregationSink, Aggregations, Dispatcher, MetricMeta,
    MetricType, MetricUnit, ThreadLocalAggregator,
};

type DatadogAggregator = Arc<ThreadLocalAggregator<io::Result<Vec<JoinHandle<()>>>>>;

/// Creates a [`DatadogBuilder`] with sensible defaults.
///
/// By default, it runs on the "current" tokio runtime, flushes metrics every 10 seconds,
/// and defaults to the `DD_API_KEY` env variable if no explicit Datadog API key has been given.
///
/// Calling [`try_init`](DatadogBuilder::try_init) will configure a global dispatcher and return a [`DatadogFlusher`].
pub fn datadog(api_key: impl Into<Option<String>>) -> DatadogBuilder {
    let api_key = api_key
        .into()
        .map(Ok)
        .unwrap_or_else(|| std::env::var("DD_API_KEY").map_err(io::Error::other));
    let ddog_site = match std::env::var("DD_SITE") {
        Ok(site) => Ok(Some(format!("https://api.{site}"))),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(io::Error::other(err)),
    };

    DatadogBuilder {
        runtime: None,
        flush_interval: Duration::from_secs(10),

        api_key,
        ddog_site,

        prefix: String::new(),
        global_tags: String::new(),
    }
}

/// A builder for configuring common datadog options.
pub struct DatadogBuilder {
    runtime: Option<Handle>,
    flush_interval: Duration,

    api_key: io::Result<String>,
    ddog_site: io::Result<Option<String>>,

    prefix: String,
    global_tags: String,
}

impl DatadogBuilder {
    /// Sets a global prefix to all the emitted metrics.
    ///
    /// For example, this could be `"myservice."`.
    pub fn prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Adds a global tag to all the emitted metrics.
    ///
    /// For example, this could be something like `"hostname"`, or similar.
    pub fn global_tag(mut self, key: &str, value: &str) -> Self {
        if !self.global_tags.is_empty() {
            self.global_tags.push(',');
        }
        let formatted_tag = format!("{key}:{value}");
        let formatted_tag = serde_json::to_string(&formatted_tag).unwrap();
        self.global_tags.push_str(&formatted_tag);
        self
    }

    /// Explicitly sets a tokio runtime [`Handle`] to use for the flusher thread.
    pub fn runtime(mut self, runtime: Handle) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Explicitly sets the upstream datadog site.
    ///
    /// This defaults to the `DD_SITE` env variable, or `https://api.datadoghq.com` otherwise.
    pub fn ddog_site(mut self, ddog_site: &str) -> Self {
        self.ddog_site = Ok(Some(ddog_site.into()));
        self
    }

    /// Explicitly sets a flush interval.
    ///
    /// This defaults to 10 seconds.
    pub fn flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }

    /// Turns the builder into a [`DatadogSink`].
    pub fn into_sink(self) -> io::Result<DatadogSink> {
        let runtime = self.runtime.unwrap_or_else(Handle::current);
        let api_key = self.api_key?;
        let ddog_site = self.ddog_site?;

        Ok(DatadogSink {
            runtime,
            client: reqwest::ClientBuilder::new()
                .build()
                .map_err(io::Error::other)?,
            api_key,
            ddog_site: ddog_site
                .as_deref()
                .unwrap_or(DD_SITE)
                .trim_end_matches('/')
                .into(),

            join_handles: Vec::new(),

            metric_buf: Vec::with_capacity(MAX_COMPRESSED),
            scratch_buf: String::new(),
            prefix: self.prefix,
            global_tags: self.global_tags,

            next_flush_len: MAX_COMPRESSED - THRESHOLD,
            bytes_written: 0,
            cctx: Encoder::new(0)?,
            compression_buffer: Vec::with_capacity(MAX_COMPRESSED),
        })
    }

    /// Initializes the datadog sink and aggregator, registering it as a global dispatcher.
    pub fn try_init(self) -> io::Result<DatadogFlusher> {
        let flush_interval = self.flush_interval;
        let datadog = self.into_sink()?;

        let aggregator = Arc::new(ThreadLocalAggregator::new(flush_interval, datadog));
        let dispatcher = Dispatcher::new(Arc::clone(&aggregator));
        set_global_dispatcher(dispatcher)
            .map_err(|_| io::Error::other("unable to set global dispatcher"))?;
        Ok(DatadogFlusher { aggregator })
    }
}

/// This is a wrapper struct that allows flushing aggregated metrics to Datadog.
pub struct DatadogFlusher {
    aggregator: DatadogAggregator,
}
impl DatadogFlusher {
    /// Flushes aggregated metrics to datadog
    pub async fn flush(&self, timeout: Option<Duration>) -> io::Result<()> {
        let tasks = self.aggregator.flush(timeout).map_err(io::Error::other)??;
        for task in tasks {
            task.await.map_err(io::Error::other)?;
        }

        Ok(())
    }
}

/// An aggregator sink which pushes metrics to Datadog, using the Datadog API.
pub struct DatadogSink {
    runtime: Handle,
    client: reqwest::Client,
    api_key: String,
    ddog_site: String,

    join_handles: Vec<JoinHandle<()>>,

    metric_buf: Vec<u8>,
    scratch_buf: String,
    prefix: String,
    global_tags: String,

    next_flush_len: usize,
    bytes_written: usize,
    cctx: Encoder<'static>,
    compression_buffer: Vec<u8>,
}

impl AggregationSink for DatadogSink {
    type Output = io::Result<Vec<JoinHandle<()>>>;

    fn emit(&mut self, metrics: Aggregations) -> Self::Output {
        self.emit_metrics(metrics)
    }
}

const THRESHOLD: usize = 1024;
const MAX_COMPRESSED: usize = 512000;
const MAX_UNCOMPRESSED: usize = 5242880;

const DD_SITE: &str = "https://api.datadoghq.com";
const DISTRIBUTION_ENDPOINT: &str = "/api/v1/distribution_points";
const METRICS_ENDPOINT: &str = "/api/v2/series";

impl DatadogSink {
    fn emit_metrics(&mut self, metrics: Aggregations) -> io::Result<Vec<JoinHandle<()>>> {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_secs();

        for (meta, value) in metrics.counters {
            self.push_metric(&meta, timestamp, value)?;
        }
        for (meta, value) in metrics.gauges {
            self.push_metric(&meta, timestamp, value.last)?;
        }
        self.flush(METRICS_ENDPOINT)?;

        for (meta, values) in metrics.distributions {
            self.push_distribution(&meta, timestamp, &values.values)?;
        }
        self.flush(DISTRIBUTION_ENDPOINT)?;

        Ok(std::mem::take(&mut self.join_handles))
    }

    fn flush(&mut self, endpoint: &str) -> io::Result<()> {
        if self.metric_buf.is_empty() && self.bytes_written == 0 {
            return Ok(());
        }
        self.metric_buf.extend_from_slice(br#"]}"#);

        self.flush_to_zstd()?;
        self.do_flush(endpoint)?;

        Ok(())
    }
    fn maybe_flush(&mut self, endpoint: &str) -> io::Result<()> {
        if self.metric_buf.len() >= self.next_flush_len {
            self.flush_to_zstd()?;
        }

        let compressed_left = MAX_COMPRESSED
            .checked_sub(self.compression_buffer.len())
            .ok_or(io::ErrorKind::QuotaExceeded)?;
        let uncompressed_left = MAX_UNCOMPRESSED
            .checked_sub(self.bytes_written)
            .ok_or(io::ErrorKind::QuotaExceeded)?;

        self.next_flush_len = compressed_left
            .min(uncompressed_left)
            .saturating_sub(THRESHOLD);
        if self.next_flush_len < THRESHOLD {
            self.metric_buf.extend_from_slice(br#"]}"#);
            self.flush_to_zstd()?;
            self.do_flush(endpoint)?;
        }

        Ok(())
    }
    fn flush_to_zstd(&mut self) -> io::Result<()> {
        let mut input = InBuffer::around(&self.metric_buf);
        let mut output = OutBuffer::around(&mut self.compression_buffer);

        self.cctx.run(&mut input, &mut output)?;

        self.bytes_written += self.metric_buf.len();
        self.metric_buf.clear();
        Ok(())
    }
    fn do_flush(&mut self, endpoint: &str) -> io::Result<()> {
        let mut output = OutBuffer::around(&mut self.compression_buffer);
        self.cctx.finish(&mut output, true)?;
        self.cctx.reinit()?;

        let request = self
            .client
            .post(format!("{}{endpoint}", self.ddog_site))
            .header("DD-API-KEY", &self.api_key)
            .header(header::ACCEPT, "application/json")
            .header(header::CONTENT_ENCODING, "zstd")
            .header(header::CONTENT_TYPE, "application/json")
            .body(self.compression_buffer.clone())
            .send();

        self.join_handles.push(self.runtime.spawn(async move {
            let response = request.await;
            response.unwrap().error_for_status().unwrap();
        }));

        self.bytes_written = 0;
        self.compression_buffer.clear();

        Ok(())
    }

    fn push_metric(
        &mut self,
        meta: &AggregatedMetric,
        timestamp: u64,
        value: f64,
    ) -> io::Result<()> {
        self.write_begin();
        self.write_meta(meta)?;
        self.write_type_and_unit(meta)?;

        self.metric_buf.write_fmt(format_args!(
            r#""points":[{{"timestamp":{timestamp},"value":{value}}}]}}"#
        ))?;

        self.maybe_flush(METRICS_ENDPOINT)
    }

    fn push_distribution(
        &mut self,
        meta: &AggregatedMetric,
        timestamp: u64,
        values: &[f64],
    ) -> io::Result<()> {
        self.write_begin();
        self.write_meta(meta)?;

        self.metric_buf
            .write_fmt(format_args!(r#""points":[[{timestamp},"#))?;
        serde_json::to_writer(&mut self.metric_buf, values).map_err(io::Error::other)?;
        self.metric_buf.extend_from_slice(br#"]]}"#);

        self.maybe_flush(DISTRIBUTION_ENDPOINT)
    }

    fn write_begin(&mut self) {
        if self.metric_buf.is_empty() {
            self.metric_buf.extend_from_slice(br#"{"series":["#);
        } else {
            self.metric_buf.push(b',');
        }
    }

    fn write_meta(&mut self, meta: &AggregatedMetric) -> io::Result<()> {
        self.scratch_buf.clear();
        self.scratch_buf.push_str(&self.prefix);
        self.scratch_buf.push_str(meta.key());

        self.metric_buf.extend_from_slice(br#"{"metric":"#);
        serde_json::to_writer(&mut self.metric_buf, &self.scratch_buf).map_err(io::Error::other)?;
        self.metric_buf.push(b',');

        let tags = meta.tags();
        if tags.len() > 0 || !self.global_tags.is_empty() {
            self.metric_buf.extend_from_slice(br#""tags":["#);
            self.metric_buf
                .extend_from_slice(self.global_tags.as_bytes());
            if !self.global_tags.is_empty() && tags.len() > 0 {
                self.metric_buf.push(b',');
            }
            for (i, tag) in tags.enumerate() {
                self.scratch_buf.clear();
                self.scratch_buf.push_str(tag.0);
                self.scratch_buf.push(':');
                self.scratch_buf.push_str(tag.1);

                if i > 0 {
                    self.metric_buf.push(b',');
                }
                serde_json::to_writer(&mut self.metric_buf, &self.scratch_buf)
                    .map_err(io::Error::other)?;
            }
            self.metric_buf.extend_from_slice(br#"],"#);
        }

        Ok(())
    }

    fn write_type_and_unit(&mut self, meta: &MetricMeta) -> io::Result<()> {
        self.metric_buf.extend_from_slice(br#""type":"#);
        let ty = match meta.ty() {
            MetricType::Counter => b'1',
            MetricType::Gauge => b'3',
            _ => b'0',
        };
        self.metric_buf.push(ty);
        self.metric_buf.push(b',');
        if meta.unit() != MetricUnit::Unknown {
            self.metric_buf.extend_from_slice(br#""unit":"#);
            serde_json::to_writer(&mut self.metric_buf, &meta.unit()).map_err(io::Error::other)?;
            self.metric_buf.push(b',');
        }

        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::tags::record_tags;

//     use super::*;

//     #[test]
//     fn builds_metrics_request() {
//         let mut sink = DatadogSink::new().unwrap();

//         sink.push_metric(
//             &AggregatedMetric {
//                 meta: MetricMeta::new(MetricType::Counter, MetricUnit::Bytes, "some.bytes"),
//                 tag_values: Default::default(),
//             },
//             12345,
//             123.45,
//         )
//         .unwrap();

//         sink.push_metric(
//             &AggregatedMetric {
//                 meta: MetricMeta::new(MetricType::Gauge, MetricUnit::Unknown, "a.gauge")
//                     .with_tags(&["a_tag"])
//                     .meta,
//                 tag_values: record_tags(&[&"a_value"]),
//             },
//             12346,
//             1234.567,
//         )
//         .unwrap();

//         sink.flush(METRICS_ENDPOINT).unwrap();

//         sink.push_distribution(
//             &AggregatedMetric {
//                 meta: MetricMeta::new(MetricType::Distribution, MetricUnit::Seconds, "a.timer"),
//                 tag_values: Default::default(),
//             },
//             12346,
//             &[1., 2., 3., 4.],
//         )
//         .unwrap();

//         sink.flush(DISTRIBUTION_ENDPOINT).unwrap();
//     }
// }
