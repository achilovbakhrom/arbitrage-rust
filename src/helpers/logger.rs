use std::io::stdout;
use tracing::info;
use tracing_appender::{ rolling::daily, non_blocking::{ NonBlocking, NonBlockingBuilder } };
use tracing_subscriber::{ fmt::{ self, format::FmtSpan }, prelude::*, EnvFilter };

pub struct Logger {}

impl Logger {
    pub fn init(level: &str) {
        let file_appender = daily("logs", "app.log");
        let (file_writer, file_guard) = NonBlocking::new(file_appender);

        let (stdout_writer, stdout_guard) = NonBlockingBuilder::default().finish(stdout());

        let stdout_layer = fmt::layer().with_writer(stdout_writer).with_span_events(FmtSpan::CLOSE);
        let file_layer = fmt::layer().with_writer(file_writer).with_span_events(FmtSpan::CLOSE);

        // 4. Compose and install the subscriber
        tracing_subscriber
            ::registry()
            .with(EnvFilter::new(level))
            .with(stdout_layer)
            .with(file_layer)
            .init();

        // 5. Keep both guards alive until program exit.
        //    If you drop them, the background threads shut down and you lose logs.
        std::mem::forget(stdout_guard);
        std::mem::forget(file_guard);

        info!("Logger initialized!");
    }
}
