use anyhow::Result;

use crate::{
    config::Settings,
    feed_handler::FeedHandler,
    feed_handler::Tob,
    hft_strategy::HftStrategy,
    market_selector::MarketSelector,
    market_selector::SelectedMarket,
    paper_broker::{PaperBroker, Side},
    risk_engine::RiskEngine,
    store::SqliteStore,
    utils::now_ts,
};

use std::sync::Arc;
use tokio::sync::watch;

pub async fn run(settings: Settings, store: SqliteStore) -> Result<()> {
    let feed = FeedHandler::new(settings.clone());
    let feed_state = feed.state();
    let selector = MarketSelector::new(settings.clone(), store.clone(), feed_state.clone());

    let (selected_tx, selected_rx) =
        watch::channel::<Arc<Vec<crate::market_selector::SelectedMarket>>>(Arc::new(Vec::new()));

    // Start live feeds (WS + periodic orderbook polling).
    feed.spawn(selected_rx.clone(), store.clone());

    // Scanner loop: refresh Gamma markets every N seconds and update watchlist selection.
    {
        let store = store.clone();
        let selected_tx = selected_tx.clone();
        let refresh_secs = settings.market_refresh_secs;
        tokio::spawn(async move {
            let mut scan = tokio::time::interval(std::time::Duration::from_secs(refresh_secs));
            scan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut last_tokens: Vec<String> = Vec::new();
            loop {
                scan.tick().await;
                store
                    .upsert_runtime_status("scanner", "ok", "running", None, now_ts())
                    .ok();
                match selector.select().await {
                    Ok(markets) => {
                        store
                            .upsert_runtime_status(
                                "scanner",
                                "ok",
                                &format!("selected {}", markets.len()),
                                None,
                                now_ts(),
                            )
                            .ok();
                        // Only publish when the WS subscription set changes.
                        // This avoids spurious resubscribe work in the hot WS loop.
                        let mut tokens: Vec<String> = markets
                            .iter()
                            .filter_map(|m| m.clob_token_id.as_deref())
                            .map(|s| s.to_string())
                            .collect();
                        tokens.sort();
                        tokens.dedup();
                        if tokens != last_tokens {
                            last_tokens = tokens;
                            let _ = selected_tx.send(Arc::new(markets));
                        }
                    }
                    Err(e) => {
                        store
                            .upsert_runtime_status(
                                "scanner",
                                "error",
                                "scan_failed",
                                Some(&e.to_string()),
                                now_ts(),
                            )
                            .ok();
                    }
                }
            }
        });
    }

    if settings.run_mode == "scanner" {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        }
    }

    run_paper_trader(settings, store, feed_state, selected_rx).await
}

async fn run_paper_trader(
    settings: Settings,
    store: SqliteStore,
    feed: crate::feed_handler::FeedState,
    mut selected_rx: watch::Receiver<Arc<Vec<SelectedMarket>>>,
) -> Result<()> {
    let start_ts = now_ts();
    let mut broker = PaperBroker::new(settings.clone(), store.clone());
    let strat = HftStrategy::new(settings.clone());
    let risk = RiskEngine::new(settings.clone());

    if settings.paper_reset_on_start {
        store.clear_trading_state().ok();
        broker.orders.clear();
        broker.positions.clear();
        log::warn!("paper_state.reset_on_start sqlite={}", store.path());
    } else if settings.paper_rehydrate_portfolio {
        if let Ok(rows) = store.fetch_latest_positions(5000) {
            for r in rows {
                let mid = r
                    .get("market_id")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                if mid.is_empty() {
                    continue;
                }
                let qty = r.get("position").and_then(|x| x.as_f64()).unwrap_or(0.0);
                let avg = r.get("avg_price").and_then(|x| x.as_f64()).unwrap_or(0.0);
                let realized = r
                    .get("realized_pnl")
                    .and_then(|x| x.as_f64())
                    .unwrap_or(0.0);
                if qty == 0.0 && realized == 0.0 {
                    continue;
                }
                broker.positions.insert(
                    mid,
                    crate::paper_broker::Position {
                        qty,
                        avg_price: avg,
                        realized_pnl: realized,
                    },
                );
            }
            log::info!(
                "paper_state.rehydrated positions={}",
                broker.positions.len()
            );
        }
    }

    // Per-market quote state.
    let mut last_quote_ts: std::collections::HashMap<String, f64> =
        std::collections::HashMap::new();
    let mut last_fair: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
    let mut last_imb_sign: std::collections::HashMap<String, i32> =
        std::collections::HashMap::new();

    let mut loop_tick = tokio::time::interval(std::time::Duration::from_millis(settings.loop_ms));
    loop_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut snap_tick = tokio::time::interval(std::time::Duration::from_secs(1));
    snap_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut eval_tick =
        tokio::time::interval(std::time::Duration::from_secs(settings.eval_interval_secs));
    eval_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut arb_tick = tokio::time::interval(std::time::Duration::from_secs(5));
    arb_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = loop_tick.tick() => {
                let now = now_ts();
               let selected = selected_rx.borrow().clone(); // Arc clone (cheap)

               let mut ctx = TraderCtx {
                   settings: &settings,
                   store: &store,
                   risk: &risk,
                   strat: &strat,
                   broker: &mut broker,
                   now,
                   last_quote_ts: &mut last_quote_ts,
                   last_fair: &mut last_fair,
                   last_imb_sign: &mut last_imb_sign,
               };

               for m in selected.iter() {
                    if let Some(tob) = feed.get(&m.market_id) {
                       trade_one_market(&mut ctx, m, &tob)?;
                    }
                }

                // Very light heartbeat so the dashboard can surface errors quickly.
                store.upsert_runtime_status("trader", "ok", "running", None, now).ok();
            }
            _ = snap_tick.tick() => {
               let selected = selected_rx.borrow().clone();
               persist_snapshots(&store, &feed, &broker, selected.as_slice())?;
            }
            _ = eval_tick.tick() => {
               let selected = selected_rx.borrow().clone();
               log_eval(&settings, &feed, &broker, selected.as_slice(), start_ts);
               persist_telemetry_markdown(&store, &feed, &broker, selected.as_slice()).ok();
            }
            _ = arb_tick.tick() => {
               let selected = selected_rx.borrow().clone();
               try_event_basket_arb(&settings, &feed, &mut broker, selected.as_slice()).ok();
            }
            _ = selected_rx.changed() => {
                // market list changed; next loop tick will react.
            }
        }
    }
}

fn try_event_basket_arb(
    settings: &Settings,
    feed: &crate::feed_handler::FeedState,
    broker: &mut PaperBroker,
    selected: &[SelectedMarket],
) -> Result<()> {
    // Simple combinatorial arb heuristic:
    // For markets in the same Gamma event_id, treat YES outcomes as mutually exclusive.
    // - If sum(best_bid) > 1.02 -> sell basket
    // - If sum(best_ask) < 0.98 -> buy basket
    //
    // This uses only internal book data (no external alpha).
    use std::collections::HashMap;

    let mut by_event: HashMap<&str, Vec<&SelectedMarket>> = HashMap::new();
    for m in selected {
        if let Some(eid) = m.event_id.as_deref() {
            by_event.entry(eid).or_default().push(m);
        }
    }

    for (_eid, ms) in by_event {
        if ms.len() < 3 {
            continue;
        }
        let mut legs: Vec<(&str, f64, f64, Tob)> = vec![];
        for m in ms {
            if let Some(tob) = feed.get(&m.market_id) {
                let (Some(b), Some(a)) = (tob.best_bid, tob.best_ask) else {
                    continue;
                };
                if a <= b {
                    continue;
                }
                legs.push((m.market_id.as_str(), b, a, tob));
            }
        }
        if legs.len() < 3 {
            continue;
        }

        let sum_bids: f64 = legs.iter().map(|x| x.1).sum();
        let sum_asks: f64 = legs.iter().map(|x| x.2).sum();

        // Costs modeled per-leg; require stronger threshold than pure mispricing.
        let cost_bps = settings.cost_bps();
        let buy_edge = 1.0 - sum_asks;
        let sell_edge = sum_bids - 1.0;

        if sum_asks < 0.98 && buy_edge > (cost_bps / 10_000.0) * 1.5 {
            // Buy basket at ask (IOC). Small size to reduce model risk.
            let sz = (settings.base_order_size * 0.25).max(1.0);
            for (mid, _b, a, tob) in legs.iter() {
                let _ = broker.execute_ioc(mid, Side::Buy, *a, sz, "arb_buy_basket", tob)?;
            }
            log::info!(
                "arb.buy_basket sum_asks={:.3} edge={:.4} legs={}",
                sum_asks,
                buy_edge,
                legs.len()
            );
        } else if sum_bids > 1.02 && sell_edge > (cost_bps / 10_000.0) * 1.5 {
            // Sell basket at bid (IOC). Small size.
            let sz = (settings.base_order_size * 0.25).max(1.0);
            for (mid, b, _a, tob) in legs.iter() {
                let _ = broker.execute_ioc(mid, Side::Sell, *b, sz, "arb_sell_basket", tob)?;
            }
            log::info!(
                "arb.sell_basket sum_bids={:.3} edge={:.4} legs={}",
                sum_bids,
                sell_edge,
                legs.len()
            );
        }
    }

    Ok(())
}

fn log_eval(
    settings: &Settings,
    feed: &crate::feed_handler::FeedState,
    broker: &PaperBroker,
    selected: &[SelectedMarket],
    start_ts: f64,
) {
    let now = now_ts();
    let elapsed_h = ((now - start_ts).max(1.0)) / 3600.0;

    let fills = broker.counters.fills as f64;
    let trades_per_hour = fills / elapsed_h;

    let open_orders: Vec<_> = broker
        .orders
        .values()
        .filter(|o| o.status == "open")
        .collect();
    let open_n = open_orders.len() as f64;

    // Time-at-touch proxy: fraction of open orders priced at current touch.
    let mut touch_hits = 0.0;
    for o in &open_orders {
        if let Some(tob) = feed.get(&o.market_id) {
            match o.side {
                Side::Buy => {
                    if tob.best_bid.is_some_and(|b| (o.price - b).abs() < 1e-12) {
                        touch_hits += 1.0;
                    }
                }
                Side::Sell => {
                    if tob.best_ask.is_some_and(|a| (o.price - a).abs() < 1e-12) {
                        touch_hits += 1.0;
                    }
                }
            }
        }
    }
    let time_at_touch = if open_n > 0.0 {
        touch_hits / open_n
    } else {
        0.0
    };

    // Average spread/edge across selected markets.
    let mut spread_bps_sum = 0.0;
    let mut spread_n = 0.0;
    let mut lag_ms_sum = 0.0;
    let mut lag_n = 0.0;
    for m in selected {
        if let Some(tob) = feed.get(&m.market_id) {
            if let (Some(b), Some(a)) = (tob.best_bid, tob.best_ask) {
                if a > b {
                    let mid = 0.5 * (a + b);
                    if mid > 0.0 {
                        spread_bps_sum += ((a - b) / mid) * 10_000.0;
                        spread_n += 1.0;
                    }
                }
            }
            lag_ms_sum += (now - tob.ts).max(0.0) * 1000.0;
            lag_n += 1.0;
        }
    }
    let avg_spread_bps = if spread_n > 0.0 {
        spread_bps_sum / spread_n
    } else {
        0.0
    };
    let avg_lag_ms = if lag_n > 0.0 { lag_ms_sum / lag_n } else { 0.0 };

    let total_r = broker.realized_pnl_total();
    let (total_u, total_p) = {
        let mut u = 0.0;
        for (mid, p) in broker.positions.iter() {
            let mark = feed.get(mid).and_then(|t| t.mid()).unwrap_or(p.avg_price);
            u += (mark - p.avg_price) * p.qty;
        }
        (u, u + total_r)
    };

    log::info!(
         "eval pnl_total=${:.2} pnl_u=${:.2} pnl_r=${:.2} fills={} tph={:.1} open_orders={} time_at_touch={:.2} avg_spread_bps={:.1} cost_bps={:.1} avg_feed_lag_ms={:.1} churn/h={:.1}",
         total_p,
         total_u,
         total_r,
         broker.counters.fills,
         trades_per_hour,
         open_orders.len(),
         time_at_touch,
         avg_spread_bps,
         settings.cost_bps(),
         avg_lag_ms,
         (broker.counters.orders_cancelled as f64) / elapsed_h
     );
}

struct TraderCtx<'a> {
    settings: &'a Settings,
    store: &'a SqliteStore,
    risk: &'a RiskEngine,
    strat: &'a HftStrategy,
    broker: &'a mut PaperBroker,
    now: f64,
    last_quote_ts: &'a mut std::collections::HashMap<String, f64>,
    last_fair: &'a mut std::collections::HashMap<String, f64>,
    last_imb_sign: &'a mut std::collections::HashMap<String, i32>,
}

fn trade_one_market(ctx: &mut TraderCtx<'_>, m: &SelectedMarket, tob: &Tob) -> Result<()> {
    let (Some(bid), Some(ask)) = (tob.best_bid, tob.best_ask) else {
        return Ok(());
    };
    if ask <= bid {
        return Ok(());
    }

    let mid = 0.5 * (ask + bid);
    if !mid.is_finite() || mid <= 0.0 {
        return Ok(());
    }

    let spread_bps = ((ask - bid) / mid) * 10_000.0;
    let min_profitable_spread_bps = 1.5 * ctx.settings.cost_bps();

    let total_depth = tob.bid_depth_5 + tob.ask_depth_5;
    let imbalance = if total_depth > 0.0 {
        (tob.bid_depth_5 - tob.ask_depth_5) / total_depth
    } else {
        0.0
    };

    let is_active_market = tob.updates_ewma_per_min >= ctx.settings.min_updates_min;

    let decision = ctx
        .risk
        .can_quote(tob, ctx.now, is_active_market, min_profitable_spread_bps);
    if !decision.ok {
        // Pull orders if we can't trust the feed / risk says no.
        cancel_all_open_for_market(ctx.broker, &m.market_id)?;
        ctx.store
            .upsert_runtime_status(
                "risk",
                "warn",
                decision.reason.unwrap_or("reject"),
                Some(&m.market_id),
                ctx.now,
            )
            .ok();
        return Ok(());
    }

    let inv_qty = ctx.broker.position_qty(&m.market_id);
    let (fair, fair_source) = match ctx.strat.compute_fair(tob, tob.last_trade_ema) {
        Some(x) => x,
        None => return Ok(()),
    };

    // Quote update conditions.
    let prev_ts = ctx.last_quote_ts.get(&m.market_id).copied().unwrap_or(0.0);
    let prev_fair = ctx.last_fair.get(&m.market_id).copied().unwrap_or(fair);
    let prev_sign = ctx.last_imb_sign.get(&m.market_id).copied().unwrap_or(0);
    let sign = if imbalance > 0.05 {
        1
    } else if imbalance < -0.05 {
        -1
    } else {
        0
    };

    let should_requote = (ctx.now - prev_ts) >= 0.10
        && ((fair - prev_fair).abs() >= ctx.settings.mm_reprice_threshold || sign != prev_sign);

    // Always simulate fills, even if we don't requote this tick.
    let activity_score = (0.5 + (tob.updates_ewma_per_min / 10.0)).clamp(0.1, 5.0);
    let _fills = ctx
        .broker
        .simulate_fills_for_market(&m.market_id, tob, activity_score)?;

    // Snipe mode (internal microstructure only): imbalance spike.
    if imbalance.abs() > 0.3 && spread_bps >= min_profitable_spread_bps {
        let side = if imbalance > 0.0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let px = if side == Side::Buy { ask } else { bid };
        let sz = (ctx.settings.base_order_size * 0.5).max(ctx.settings.base_order_size.min(1.0));
        let _ = ctx
            .broker
            .execute_ioc(&m.market_id, side, px, sz, "snipe", tob)?;
    }

    if !should_requote {
        // Still persist quote telemetry (helps dashboard explain decisions).
        ctx.store
            .insert_quote_snapshot(
                ctx.now,
                &m.market_id,
                m.event_id.as_deref().unwrap_or("event:unknown"),
                tob.best_bid,
                tob.best_ask,
                Some(mid),
                Some(fair),
                fair_source,
                inv_qty,
                ask - bid,
                0.0,
                None,
                None,
            )
            .ok();
        return Ok(());
    }

    // Cancel stale/old orders (respect min quote life).
    cancel_stale_for_market(
        ctx.broker,
        &m.market_id,
        ctx.now,
        ctx.settings.mm_min_quote_life_secs,
    )?;

    // Compute and place grid.
    let intents = ctx
        .strat
        .quote_grid(fair, inv_qty, imbalance, tob.updates_ewma_per_min);
    let mut target_bid: Option<f64> = None;
    let mut target_ask: Option<f64> = None;

    for qi in intents {
        // Profitability: don't quote inside the profitable spread band.
        // (Maker capture needs room for slippage/latency modeled in paper).
        let allow = match qi.side {
            Side::Buy => qi.price <= bid,
            Side::Sell => qi.price >= ask,
        };
        if !allow {
            continue;
        }

        // Inventory guardrail.
        if qi.side == Side::Buy && (inv_qty + qi.size) > ctx.settings.max_inventory_usd {
            continue;
        }
        if qi.side == Side::Sell && (inv_qty - qi.size) < -ctx.settings.max_inventory_usd {
            continue;
        }

        // Avoid duplicate prices on same side.
        if has_open_order_at(ctx.broker, &m.market_id, qi.side, qi.price) {
            continue;
        }

        let oid = ctx
            .broker
            .place_limit(&m.market_id, qi.side, qi.price, qi.size, "mm")?;
        let _ = oid;
        match qi.side {
            Side::Buy => target_bid.get_or_insert(qi.price),
            Side::Sell => target_ask.get_or_insert(qi.price),
        };
    }

    ctx.store
        .insert_quote_snapshot(
            ctx.now,
            &m.market_id,
            m.event_id.as_deref().unwrap_or("event:unknown"),
            tob.best_bid,
            tob.best_ask,
            Some(mid),
            Some(fair),
            fair_source,
            inv_qty,
            ask - bid,
            0.0,
            target_bid,
            target_ask,
        )
        .ok();

    ctx.last_quote_ts.insert(m.market_id.clone(), ctx.now);
    ctx.last_fair.insert(m.market_id.clone(), fair);
    ctx.last_imb_sign.insert(m.market_id.clone(), sign);

    Ok(())
}

fn has_open_order_at(broker: &PaperBroker, market_id: &str, side: Side, price: f64) -> bool {
    broker.orders.values().any(|o| {
        o.status == "open"
            && o.market_id == market_id
            && o.side == side
            && (o.price - price).abs() < 1e-12
    })
}

fn cancel_all_open_for_market(broker: &mut PaperBroker, market_id: &str) -> Result<()> {
    let ids: Vec<String> = broker
        .orders
        .iter()
        .filter(|(_, o)| o.market_id == market_id && o.status == "open")
        .map(|(id, _)| id.clone())
        .collect();
    for id in ids {
        broker.cancel(&id)?;
    }
    Ok(())
}

fn cancel_stale_for_market(
    broker: &mut PaperBroker,
    market_id: &str,
    now: f64,
    min_life: f64,
) -> Result<()> {
    let ids: Vec<String> = broker
        .orders
        .iter()
        .filter(|(_, o)| {
            o.market_id == market_id && o.status == "open" && (now - o.created_ts) >= min_life
        })
        .map(|(id, _)| id.clone())
        .collect();
    for id in ids {
        broker.cancel(&id)?;
    }
    Ok(())
}

fn persist_snapshots(
    store: &SqliteStore,
    feed: &crate::feed_handler::FeedState,
    broker: &PaperBroker,
    selected: &[SelectedMarket],
) -> Result<()> {
    let now = now_ts();
    let mut total_u = 0.0;
    let total_r = broker.realized_pnl_total();

    let mut event_by_market: std::collections::HashMap<&str, &str> =
        std::collections::HashMap::new();
    for m in selected {
        if let Some(eid) = m.event_id.as_deref() {
            event_by_market.insert(m.market_id.as_str(), eid);
        }
    }

    // Persist per-market positions (including flat w/ realized != 0).
    for (mid, p) in broker.positions.iter() {
        let mark = feed.get(mid).and_then(|t| t.mid()).unwrap_or(p.avg_price);
        let u = (mark - p.avg_price) * p.qty;
        total_u += u;
        store.insert_position_snapshot(
            now,
            mid,
            event_by_market
                .get(mid.as_str())
                .copied()
                .unwrap_or("event:unknown"),
            p.qty,
            p.avg_price,
            mark,
            u,
            p.realized_pnl,
        )?;
    }

    // Ensure selected markets at least update quote snapshots even when flat.
    for m in selected {
        let _ = feed.get(&m.market_id);
    }

    let total = total_u + total_r;
    store.insert_pnl_snapshot(now, total_u, total_r, total)?;
    Ok(())
}

fn persist_telemetry_markdown(
    store: &SqliteStore,
    feed: &crate::feed_handler::FeedState,
    broker: &PaperBroker,
    selected: &[SelectedMarket],
) -> Result<()> {
    // Lightweight writer: reconstruct similar snapshot to ops/telemetry/latest.md
    let now = now_ts();
    let mut total_u = 0.0;
    let total_r = broker.realized_pnl_total();

    let mut open_rows: Vec<(String, f64, f64, f64, f64, f64)> = vec![]; // market_id, pos, avg, mark, u, r
    for (mid, p) in broker.positions.iter() {
        let mark = feed.get(mid).and_then(|t| t.mid()).unwrap_or(p.avg_price);
        let u = (mark - p.avg_price) * p.qty;
        total_u += u;
        open_rows.push((mid.clone(), p.qty, p.avg_price, mark, u, p.realized_pnl));
    }
    open_rows.sort_by(|a, b| b.5.partial_cmp(&a.5).unwrap_or(std::cmp::Ordering::Equal));

    let total = total_u + total_r;
    let mut md = String::new();
    md.push_str("# Trading snapshot\n\n");
    md.push_str(&format!("- generated: `{}`\n\n", now as u64));
    md.push_str("## PnL\n\n");
    md.push_str(&format!("- total: **${:.2}**\n", total));
    md.push_str(&format!("- unrealized: ${:.2}\n", total_u));
    md.push_str(&format!("- realized: ${:.2}\n\n", total_r));

    let open_nonzero = open_rows.iter().filter(|r| r.1 != 0.0).count();
    md.push_str(&format!("## Open positions ({})\n\n", open_nonzero));
    md.push_str("| market_id | pos | avg | mark | uPnL | rPnL |\n|---|---:|---:|---:|---:|---:|\n");
    for (mid, pos, avg, mark, u, r) in open_rows.iter().filter(|r| r.1 != 0.0).take(50) {
        md.push_str(&format!(
            "| `{}` | {:.2} | {:.3} | {:.3} | ${:.2} | ${:.2} |\n",
            mid, pos, avg, mark, u, r
        ));
    }

    // Recent fills & orders for transparency.
    let recent_fills = store.fetch_recent_fills(50).unwrap_or_default();
    md.push_str("\n## Recent fills (50)\n\n| ts | market_id | side | px | size |\n|---:|---|---|---:|---:|\n");
    for f in recent_fills {
        let ts = f.get("ts").and_then(|x| x.as_f64()).unwrap_or(0.0) as u64;
        let mid = f.get("market_id").and_then(|x| x.as_str()).unwrap_or("--");
        let side = f.get("side").and_then(|x| x.as_str()).unwrap_or("--");
        let px = f.get("price").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let sz = f.get("size").and_then(|x| x.as_f64()).unwrap_or(0.0);
        md.push_str(&format!(
            "| {} | `{}` | {} | {:.3} | {:.2} |\n",
            ts, mid, side, px, sz
        ));
    }

    let recent_orders = store.fetch_recent_orders(50, None).unwrap_or_default();
    md.push_str("\n## Recent orders (50)\n\n| ts | market_id | side | px | size | status | filled |\n|---:|---|---|---:|---:|---|---:|\n");
    for o in recent_orders {
        let ts = o.get("created_ts").and_then(|x| x.as_f64()).unwrap_or(0.0) as u64;
        let mid = o.get("market_id").and_then(|x| x.as_str()).unwrap_or("--");
        let side = o.get("side").and_then(|x| x.as_str()).unwrap_or("--");
        let px = o.get("price").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let sz = o.get("size").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let status = o.get("status").and_then(|x| x.as_str()).unwrap_or("--");
        let filled = o.get("filled_size").and_then(|x| x.as_f64()).unwrap_or(0.0);
        md.push_str(&format!(
            "| {} | `{}` | {} | {:.3} | {:.2} | {} | {:.2} |\n",
            ts, mid, side, px, sz, status, filled
        ));
    }

    std::fs::create_dir_all("ops/telemetry").ok();
    std::fs::write("ops/telemetry/latest.md", md)?;

    let _ = selected;
    Ok(())
}
