//! Client entry point: the builder assembles the ws → codec → [session] →
//! reconnect stack and picks the spawner for the platform.

use std::sync::Arc;

use slozhn_client::Spawner;
use slozhn_client::reconnect::{
    AutoChannel, AutoConfig, FactoryError, FactoryOutput, KeepaliveConfig, TransportFactory,
};
use slozhn_session::SessionConfig;

/// Channel for tonic clients: `EchoClient::new(channel)`.
pub type Channel = AutoChannel;

pub fn builder(url: impl Into<String>) -> ChannelBuilder {
    ChannelBuilder {
        url: url.into(),
        resume: None,
        resume_explicit: false,
        reconnect: AutoConfig::default(),
        keepalive: Some(KeepaliveConfig::default()),
        frame: slozhn_frame::connection::Config::default(),
        headers: http::HeaderMap::new(),
    }
}

pub struct ChannelBuilder {
    url: String,
    resume: Option<SessionConfig>,
    resume_explicit: bool,
    reconnect: AutoConfig,
    keepalive: Option<KeepaliveConfig>,
    frame: slozhn_frame::connection::Config,
    headers: http::HeaderMap,
}

impl ChannelBuilder {
    /// Enable the session layer: streams survive disconnects (spec §8).
    /// The session reconnect backoff inherits `reconnect_config`; use
    /// [`Self::resume_with`] to control it independently.
    pub fn resume(mut self) -> Self {
        self.resume = Some(SessionConfig::default());
        self.resume_explicit = false;
        self
    }

    pub fn resume_with(mut self, cfg: SessionConfig) -> Self {
        self.resume = Some(cfg);
        self.resume_explicit = true;
        self
    }

    pub fn reconnect_config(mut self, cfg: AutoConfig) -> Self {
        self.reconnect = cfg;
        self
    }

    /// Configure raw-connection keepalive. `None` disables automatic pings.
    ///
    /// Session channels do not use logical keepalive pings during reconnect
    /// gaps; the session transport owns physical reconnect.
    pub fn keepalive_config(mut self, cfg: Option<KeepaliveConfig>) -> Self {
        self.keepalive = cfg;
        self
    }

    pub fn frame_config(mut self, cfg: slozhn_frame::connection::Config) -> Self {
        self.frame = cfg;
        self
    }

    /// WS-upgrade header (auth). Native only: the browser WebSocket cannot
    /// set headers — use query params or cookies. Compiled out on wasm32,
    /// so calling it there is a compile error rather than a runtime panic.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn header(
        mut self,
        name: http::header::HeaderName,
        value: http::header::HeaderValue,
    ) -> Self {
        self.headers.append(name, value);
        self
    }

    /// Build the channel. Lazy: the connection comes up on the first call.
    pub fn build(self) -> Channel {
        let Self {
            url,
            mut resume,
            resume_explicit,
            reconnect,
            keepalive,
            frame,
            headers,
        } = self;
        // .resume() (без явного конфига) наследует бэкофф из reconnect_config,
        // независимо от порядка вызовов билдера
        if let Some(cfg) = &mut resume
            && !resume_explicit
        {
            cfg.initial_backoff = reconnect.initial_backoff;
            cfg.max_backoff = reconnect.max_backoff;
        }
        let headers = Arc::new(headers);
        let (hooks, state_rx) = slozhn_frame::transport::ReconnectHooks::new();
        let session_hooks = hooks.clone();

        let factory: TransportFactory = Arc::new(move || {
            let url = url.clone();
            let headers = headers.clone();
            let frame = frame.clone();
            let resume = resume.clone();
            let session_hooks = session_hooks.clone();
            Box::pin(async move {
                match resume {
                    None => {
                        let t = raw_transport(&url, &headers)
                            .await
                            .map_err(FactoryError::connect)?;
                        Ok(FactoryOutput::Raw(t))
                    }
                    Some(session_cfg) => {
                        // slozhn_session::client::Factory is fixed to
                        // Result<_, String> — String is preserved verbatim,
                        // it already carries only WS-connect failures here.
                        let ws_factory: slozhn_session::client::Factory = {
                            let url = url.clone();
                            let headers = headers.clone();
                            Arc::new(move || {
                                let url = url.clone();
                                let headers = headers.clone();
                                Box::pin(async move { raw_transport(&url, &headers).await })
                            })
                        };
                        let (t, peer_hello) = slozhn_session::client::connect_session_hooked(
                            ws_factory,
                            frame,
                            session_cfg,
                            session_hooks.clone(),
                        )
                        .await
                        .map_err(FactoryError::handshake)?;
                        Ok(FactoryOutput::PreNegotiated(Box::pin(t), peer_hello))
                    }
                }
            })
        });
        AutoChannel::with_hooks_and_keepalive(
            factory,
            default_spawner(),
            reconnect,
            hooks,
            state_rx,
            keepalive,
        )
    }
}

async fn raw_transport(
    url: &str,
    headers: &http::HeaderMap,
) -> Result<slozhn_frame::transport::BoxFrameTransport, String> {
    let ws = slozhn_ws::connect(
        url,
        slozhn_ws::WsConfig {
            headers: headers.clone(),
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(Box::pin(slozhn_frame::codec::framed(ws)))
}

/// Platform default spawner: tokio on native, spawn_local in the browser.
/// For custom executors — use `AutoChannel::new` directly.
pub fn default_spawner() -> Spawner {
    #[cfg(not(target_arch = "wasm32"))]
    {
        Arc::new(|f| {
            tokio::spawn(f);
        })
    }
    #[cfg(target_arch = "wasm32")]
    {
        Arc::new(wasm_bindgen_futures::spawn_local)
    }
}
