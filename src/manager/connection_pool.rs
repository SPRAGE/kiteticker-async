use crate::models::{Mode, TickerMessage};
use crate::ticker::KiteTickerAsync;
use crate::manager::{KiteManagerConfig, ConnectionStats, ChannelId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tokio::time::timeout;

/// Represents a single WebSocket connection with its metadata
#[derive(Debug)]
pub struct ManagedConnection {
    pub id: ChannelId,
    pub ticker: Option<KiteTickerAsync>,
    pub subscribed_symbols: HashMap<u32, Mode>,
    pub stats: Arc<RwLock<ConnectionStats>>,
    pub is_healthy: Arc<AtomicBool>,
    pub last_ping: Arc<AtomicU64>, // Unix timestamp
    pub task_handle: Option<JoinHandle<()>>,
    pub message_sender: mpsc::UnboundedSender<TickerMessage>,
}

impl ManagedConnection {
    pub fn new(id: ChannelId, message_sender: mpsc::UnboundedSender<TickerMessage>) -> Self {
        let mut stats = ConnectionStats::default();
        stats.connection_id = id.to_index();
        
        Self {
            id,
            ticker: None,
            subscribed_symbols: HashMap::new(),
            stats: Arc::new(RwLock::new(stats)),
            is_healthy: Arc::new(AtomicBool::new(false)),
            last_ping: Arc::new(AtomicU64::new(0)),
            task_handle: None,
            message_sender,
        }
    }
    
    /// Connect to WebSocket and start message processing
    pub async fn connect(
        &mut self,
        api_key: &str,
        access_token: &str,
        config: &KiteManagerConfig,
    ) -> Result<(), String> {
        // Connect to WebSocket
        let ticker = timeout(
            config.connection_timeout,
            KiteTickerAsync::connect(api_key, access_token)
        )
        .await
        .map_err(|_| "Connection timeout".to_string())?
        .map_err(|e| format!("Connection failed: {}", e))?;
        
        self.ticker = Some(ticker);
        self.is_healthy.store(true, Ordering::Relaxed);
        
        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.is_connected = true;
            stats.connection_uptime = Duration::ZERO;
        }
        
        Ok(())
    }
    
    /// Subscribe to symbols on this connection
    pub async fn subscribe_symbols(
        &mut self,
        symbols: &[u32],
        mode: Mode,
    ) -> Result<(), String> {
        if let Some(ticker) = self.ticker.take() {
            // Use the public subscribe method to get a subscriber
            let subscriber = ticker.subscribe(symbols, Some(mode.clone())).await?;
            
            // Update our symbol tracking
            for &symbol in symbols {
                self.subscribed_symbols.insert(symbol, mode.clone());
            }
            
            // Start message processing task
            let message_sender = self.message_sender.clone();
            let stats = Arc::clone(&self.stats);
            let is_healthy = Arc::clone(&self.is_healthy);
            let connection_id = self.id;
            
            let handle = tokio::spawn(async move {
                Self::message_processing_loop(
                    subscriber,
                    message_sender,
                    stats,
                    is_healthy,
                    connection_id,
                ).await;
            });
            
            self.task_handle = Some(handle);
            
            // Update stats
            {
                let mut stats = self.stats.write().await;
                stats.symbol_count = self.subscribed_symbols.len();
            }
            
            Ok(())
        } else {
            Err("Connection not established".to_string())
        }
    }
    
    /// Message processing loop for this connection
    async fn message_processing_loop(
        mut subscriber: crate::ticker::KiteTickerSubscriber,
        message_sender: mpsc::UnboundedSender<TickerMessage>,
        stats: Arc<RwLock<ConnectionStats>>,
        is_healthy: Arc<AtomicBool>,
        connection_id: ChannelId,
    ) {
        let mut last_message_time = Instant::now();
        
        log::info!("Starting message processing loop for connection {}", connection_id.to_index());
        
        loop {
            match timeout(Duration::from_secs(30), subscriber.next_message()).await {
                Ok(Ok(Some(message))) => {
                    last_message_time = Instant::now();
                    
                    // Update stats
                    {
                        let mut stats = stats.write().await;
                        stats.messages_received += 1;
                        stats.last_message_time = Some(last_message_time);
                    }
                    
                    // Forward message to parser (non-blocking)
                    if let Err(_) = message_sender.send(message) {
                        log::warn!("Connection {}: Parser channel full, dropping message", connection_id.to_index());
                        
                        // Update error stats
                        let mut stats = stats.write().await;
                        stats.errors_count += 1;
                    }
                }
                Ok(Ok(None)) => {
                    log::info!("Connection {} closed", connection_id.to_index());
                    is_healthy.store(false, Ordering::Relaxed);
                    break;
                }
                Ok(Err(e)) => {
                    log::error!("Connection {} error: {}", connection_id.to_index(), e);
                    
                    // Update error stats
                    let mut stats = stats.write().await;
                    stats.errors_count += 1;
                    
                    // Continue trying to receive messages
                }
                Err(_) => {
                    // Timeout - check if connection is still alive
                    if last_message_time.elapsed() > Duration::from_secs(60) {
                        log::warn!("Connection {} timeout - no messages for 60s", connection_id.to_index());
                        is_healthy.store(false, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }
        
        // Update connection status
        {
            let mut stats = stats.write().await;
            stats.is_connected = false;
        }
        is_healthy.store(false, Ordering::Relaxed);
    }
    
    /// Check if connection can accept more symbols
    pub fn can_accept_symbols(&self, count: usize, max_per_connection: usize) -> bool {
        self.subscribed_symbols.len() + count <= max_per_connection
    }
    
    /// Get current symbol count
    pub fn symbol_count(&self) -> usize {
        self.subscribed_symbols.len()
    }
    
    /// Check if connection is healthy
    pub fn is_healthy(&self) -> bool {
        self.is_healthy.load(Ordering::Relaxed)
    }
}
