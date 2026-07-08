//! Client entry point: the builder assembles the ws → codec → [session] →
//! reconnect stack and picks the spawner for the platform.

use std::sync::Arc;

use slozhn_client::reconnect::{AutoChannel, AutoConfig, FactoryOutput, TransportFactory};
use slozhn_client::Spawner;
use slozhn_session::SessionConfig;

/// Channel for tonic clients: `EchoClient::new(channel)`.
pub type Channel = AutoChannel;

pub fn builder(url: impl Into<String>) -> ChannelBuilder {
    ChannelBuilder {
        url: url.into(),
        resume: None,
        reconnect: AutoConfig::default(),
        frame: slozhn_frame::connection::Config::default(),
        headers: http::HeaderMap::new(),
    }
}

pub struct ChannelBuilder {
    url: String,
    resume: Option<SessionConfig>,
    reconnect: AutoConfig,
    frame: slozhn_frame::connection::Config,
    headers: http::HeaderMap,
}

impl ChannelBuilder {
    /// Enable the session layer: streams survive disconnects (spec §8).
    pub fn resume(mut self) -> Self {
        self.resume = Some(SessionConfig::default());
        self
    }

    pub fn resume_with(mut self, cfg: SessionConfig) -> Self {
        self.resume = Some(cfg);
        self
    }

    pub fn reconnect_config(mut self, cfg: AutoConfig) -> Self {
        self.reconnect = cfg;
        self
    }

    pub fn frame_config(mut self, cfg: slozhn_frame::connection::Config) -> Self {
        self.frame = cfg;
        self
    }

    /// WS-upgrade header (auth). Native only: the browser WebSocket
    /// cannot set headers — use query params or cookies.
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
        #[cfg(target_arch = "wasm32")]
        assert!(
            self.headers.is_empty(),
            "browser WebSocket cannot set headers; use query params or cookies"
        );

        let Self { url, resume, reconnect, frame, headers } = self;
        let headers = Arc::new(headers);

        let factory: TransportFactory = Arc::new(move || {
            let url = url.clone();
            let headers = headers.clone();
            let frame = frame.clone();
            let resume = resume.clone();
            Box::pin(async move {
                match resume {
                    None => {
                        let t = raw_transport(&url, &headers).await?;
                        Ok(FactoryOutput::Raw(t))
                    }
                    Some(session_cfg) => {
                        let ws_factory: slozhn_session::client::Factory = {
                            let url = url.clone();
                            let headers = headers.clone();
                            Arc::new(move || {
                                let url = url.clone();
                                let headers = headers.clone();
                                Box::pin(async move { raw_transport(&url, &headers).await })
                            })
                        };
                        let (t, peer_hello) = slozhn_session::client::connect_session(
                            ws_factory, frame, session_cfg,
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                        Ok(FactoryOutput::PreNegotiated(Box::pin(t), peer_hello))
                    }
                }
            })
        });
        AutoChannel::new(factory, default_spawner(), reconnect)
    }
}

async fn raw_transport(
    url: &str,
    headers: &http::HeaderMap,
) -> Result<slozhn_frame::transport::BoxFrameTransport, String> {
    let ws = slozhn_ws::connect(url, slozhn_ws::WsConfig { headers: headers.clone() })
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
