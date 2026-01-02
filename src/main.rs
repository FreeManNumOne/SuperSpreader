mod config;
mod dashboard;
mod store;

// Trading bot modules (implemented next)
mod bot;
mod feed_handler;
mod hft_strategy;
mod market_selector;
mod paper_broker;
mod risk_engine;
mod utils;

use anyhow::Result;
use clap::Parser;

use crate::{config::Settings, store::SqliteStore};

#[derive(Debug, Parser)]
#[command(name = "superspreader", version)]
struct Cli {
    /// Override RUN_MODE (paper|scanner)
    #[arg(long)]
    mode: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let cli = Cli::parse();
    let mut settings = Settings::load()?;
    if let Some(m) = cli.mode {
        settings.run_mode = m.to_lowercase();
    }

    let store = SqliteStore::new(&settings.sqlite_path)?;
    store.init_db()?;

    log::info!(
        "app.start run_mode={} trade_mode={} execution_mode={} sqlite={}",
        settings.run_mode,
        settings.trade_mode,
        settings.execution_mode,
        store.path()
    );

    // Start dashboard server (optional) in the background.
    if settings.dashboard_enabled {
        let st = settings.clone();
        let db = store.clone();
        let url = format!("http://{}:{}/", st.dashboard_host, st.dashboard_port);
        tokio::spawn(async move {
            if let Err(e) = dashboard::serve_dashboard(st, db).await {
                log::error!("dashboard.error {}", e);
            }
        });

        if settings.dashboard_open_browser {
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(650)).await;
                let _ = std::process::Command::new("xdg-open").arg(&url).spawn();
            });
        }
    }

    // Run the bot (scanner or full paper trader).
    bot::run(settings, store).await?;
    Ok(())
}
